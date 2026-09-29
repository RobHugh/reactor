using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issues #375, #376 and #377 (parent #56): the accepting end routes an arriving Resume control
/// datagram by its target session id and authorises it by its secret (ADR 0009). Also the read-side
/// cases #373 could not write, because the codec is internal (ADR 0015): a malformed Resume is
/// dropped like any other malformed control datagram.
/// </summary>
/// <remarks>
/// The resuming party is a <see cref="HostilePeer"/> on its own UDP port, so a Resume's source differs
/// from the Peer's address. The accepting end's session id and secret are read off the Accept its
/// <see cref="LossyProxy"/> captured, at the offsets ADR 0009 documents. A padded fixed-size control
/// datagram cannot tell a body one byte short, or empty, from a full body whose trailing bytes happen
/// to be zero, so those two shapes are not tested: they are the same bytes as a valid Resume.
/// </remarks>
public class DatagramSessionResumeRoutingTests
{
    private const int ControlDatagramSize = 256;
    private const byte ResumeType = 6;
    private const byte AcceptType = 4;
    private const int ControlBodyOffset = 5;
    private const int ResumeBodyLength = 20;
    private const int ResumeSecretLength = 16;

    private const int AcceptSessionIdOffset = 13;
    private const int AcceptSecretOffset = 37;

    private static readonly TimeSpan _sendIdleInterval = TimeSpan.FromSeconds(1);

    private const int MissedHeartbeats = 3;

    private static readonly TimeSpan _receiveIdleDeadline = _sendIdleInterval * MissedHeartbeats;

    [Fact]
    public void Pump_ResumeForAnUnknownSessionId_LogsExactlyOneResumeUnmatchedNamingTheSource()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish();
        using HostilePeer resumer = new();
        byte[] resume = BuildResume(scenario.AccepterSessionId ^ 0x0100_0000, scenario.ResumeSecret);

        // Act
        resumer.SendDatagram(scenario.Accepter.Endpoint, resume);
        scenario.Accepter.PumpUntil(() => scenario.Accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        // Assert
        DiagnosticRecord record = scenario.Accepter.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.ResumeUnmatched).Which;
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
        record.Source.ToIPEndPoint().Address.Should().Be(IPAddress.Loopback);
        record.Level.Should().Be(DiagnosticLevel.Warn);
        record.Value0.Should().Be(0);
        record.Value1.Should().Be(6);
    }

    [Fact]
    public void Pump_ResumeForAnUnknownSessionId_RaisesNoEventAndLeavesTheHeldPeerUntouched()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish();
        using HostilePeer resumer = new();
        byte[] resume = BuildResume(scenario.AccepterSessionId ^ 0x0100_0000, scenario.ResumeSecret);
        PeerCounters before = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer);

        // Act
        resumer.SendDatagram(scenario.Accepter.Endpoint, resume);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = scenario.Accepter.PumpUntilDrained(
            _ => scenario.Accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));
        events.AddRange(scenario.Accepter.PumpAndDrainAll(3));

        // Assert
        scenario.Accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.ResumeUnmatched);
        events.Should().BeEmpty();
        scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).Should().Be(before);
    }

    [Theory]
    [InlineData(ControlDatagramSize - 1)]
    [InlineData(ControlDatagramSize + 1)]
    [InlineData(ControlBodyOffset)]
    public void Pump_ResumeOfTheWrongLength_IsDiscardedLikeAnyMalformedControlDatagram(int length)
    {
        // Arrange — a valid Resume with a real-looking body, cut or padded to the wrong size.
        using DatagramSessionReactorHarness accepter = new();
        using HostilePeer resumer = new();
        byte[] full = BuildResume(0x0102_0304, new byte[ResumeSecretLength]);
        byte[] datagram = new byte[length];
        full.AsSpan(0, Math.Min(length, full.Length)).CopyTo(datagram);

        // Act
        resumer.SendDatagram(accepter.Endpoint, datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDiscards > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: length);
    }

    [Fact]
    public void Pump_ResumeWithANonZeroByteRightAfterTheBody_IsDiscardedLikeAnyMalformedControlDatagram()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using HostilePeer resumer = new();
        byte[] datagram = BuildResume(0x0102_0304, new byte[ResumeSecretLength]);
        datagram[ControlBodyOffset + ResumeBodyLength] = 0x01;

        // Act
        resumer.SendDatagram(accepter.Endpoint, datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDiscards > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: ControlDatagramSize);
    }

    [Fact]
    public void Pump_WrongSecretResumeAgainstAnInterruptedPeer_LogsResumeUnmatchedWithValue0One()
    {
        // Arrange — the target session id is the real one, so only the secret is wrong.
        using Scenario scenario = Scenario.Establish();
        scenario.InterruptAccepter();
        using UdpClient resumer = NewResumer();

        // Act
        resumer.Send(BuildResume(scenario.AccepterSessionId, WrongSecret(scenario)), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntil(() => scenario.Accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        // Assert
        DiagnosticRecord record = scenario.Accepter.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.ResumeUnmatched).Which;
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
        record.Source.ToIPEndPoint().Should().Be((IPEndPoint)resumer.Client.LocalEndPoint!);
        record.Level.Should().Be(DiagnosticLevel.Warn);
        record.Value0.Should().Be(1);
        record.Value1.Should().Be(6);
    }

    [Fact]
    public void Pump_WrongSecretResumeAgainstAnInterruptedPeer_LeavesItInterruptedWithItsOriginalLingerExpiry()
    {
        // Arrange — Linger is 1s. The Resume lands 400ms after the interruption, so a timestamp that
        // moved to the Resume would push the close out to 1400ms.
        TimeSpan linger = TimeSpan.FromSeconds(1);
        TimeSpan resumeArrivesAfter = TimeSpan.FromMilliseconds(400);
        using Scenario scenario = Scenario.Establish(linger);
        scenario.InterruptAccepter();
        using UdpClient resumer = NewResumer();

        scenario.Clock.Advance(resumeArrivesAfter);

        // Act
        resumer.Send(BuildResume(scenario.AccepterSessionId, WrongSecret(scenario)), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntil(() => scenario.Accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        scenario.AdvanceAccepter(linger - resumeArrivesAfter - TimeSpan.FromMilliseconds(1));
        scenario.PumpAccepter();
        bool closedBeforeTheOriginalExpiry = scenario.AccepterEvents.Any(e => e.Kind == SessionEventKind.PeerClosed);

        scenario.Clock.Advance(TimeSpan.FromMilliseconds(1));
        scenario.PumpAccepter();

        // Assert — the Resume was refused, the Peer still held one millisecond short of the original expiry, and closed on it.
        scenario.Accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.ResumeUnmatched);
        closedBeforeTheOriginalExpiry.Should().BeFalse();
        scenario.AccepterEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.LingerExpired);
    }

    [Fact]
    public void Pump_WrongSecretResumeAgainstAnInterruptedPeer_RaisesNoPeerResumedAndLeavesResumesUnchanged()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish();
        scenario.InterruptAccepter();
        using UdpClient resumer = NewResumer();
        ulong resumesBefore = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).Resumes;

        // Act
        resumer.Send(BuildResume(scenario.AccepterSessionId, WrongSecret(scenario)), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntil(() => scenario.Accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));
        scenario.PumpAccepter();

        // Assert
        scenario.Accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.ResumeUnmatched);
        scenario.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerResumed);
        scenario.Accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.ResumeMatched);
        scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).Resumes.Should().Be(resumesBefore).And.Be(0);
    }

    [Fact]
    public void Pump_WrongSecretResumeAgainstAnInterruptedPeer_LeavesItsSendAddressUnchanged()
    {
        // Arrange — a long Linger so the Peer stays open across the send-idle intervals below.
        using Scenario scenario = Scenario.Establish(linger: TimeSpan.FromSeconds(30));
        scenario.InterruptAccepter();
        using UdpClient resumer = NewResumer();

        // Act — the wrong-secret Resume, then further send-idle intervals: the Interrupted Peer keeps
        // pinging, and where those Pings go is its send address.
        resumer.Send(BuildResume(scenario.AccepterSessionId, WrongSecret(scenario)), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntil(() => scenario.Accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        int pingsToTheProxyBefore = scenario.Proxy.Captures.Count(c => c.Direction == RelayDirection.UpstreamToClient);
        for (int i = 0; i < 3; i++)
        {
            scenario.AdvanceAccepter(_sendIdleInterval);
            scenario.PumpAccepter();
            scenario.Proxy.Pump();
        }

        // Assert
        scenario.Accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.ResumeUnmatched);
        scenario.Proxy.Captures.Count(c => c.Direction == RelayDirection.UpstreamToClient)
            .Should().BeGreaterThan(pingsToTheProxyBefore, "the Peer's Pings still reach the address it was established from");
        resumer.Available.Should().Be(0, "the wrong-secret Resume's source never becomes the Peer's send address");
    }

    [Fact]
    public void Pump_MatchingResumeAgainstAnInterruptedPeer_RaisesExactlyOnePeerResumedWithTheSamePeerId()
    {
        // Arrange — the Resume is built here, byte by byte, from the id and secret read off the Accept.
        using Scenario scenario = Scenario.Establish();
        scenario.InterruptAccepter();
        PeerId interrupted = scenario.AccepterEvents.Single(e => e.Kind == SessionEventKind.PeerInterrupted).Peer;
        using UdpClient resumer = NewResumer();

        // Act
        resumer.Send(BuildResume(scenario.AccepterSessionId, scenario.ResumeSecret), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntil(() => scenario.AccepterEvents.Any(e => e.Kind == SessionEventKind.PeerResumed));
        scenario.PumpAccepter();

        // Assert
        scenario.AccepterEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerResumed)
            .Which.Peer.Should().Be(interrupted).And.Be(scenario.AccepterPeer);
    }

    [Fact]
    public void Pump_MatchingResumeAgainstAnInterruptedPeer_LogsResumeMatchedWithValue0Zero()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish();
        scenario.InterruptAccepter();
        using UdpClient resumer = NewResumer();

        // Act
        resumer.Send(BuildResume(scenario.AccepterSessionId, scenario.ResumeSecret), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntil(() => scenario.Accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeMatched));

        // Assert
        DiagnosticRecord record = scenario.Accepter.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.ResumeMatched).Which;
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Peer);
        record.Peer.Should().Be(scenario.AccepterPeer);
        record.Level.Should().Be(DiagnosticLevel.Info);
        record.Value0.Should().Be(0);
        scenario.Accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.ResumeUnmatched);
    }

    [Fact]
    public void Pump_MatchingResumeAgainstAnInterruptedPeer_TakesItOutOfInterruptedSoTheOriginalLingerExpiryDoesNotCloseIt()
    {
        // Arrange — Linger is 1s and the Resume lands 400ms after the interruption. Were the Peer still
        // Interrupted from the original timestamp it would close at 1000ms.
        TimeSpan linger = TimeSpan.FromSeconds(1);
        TimeSpan resumeArrivesAfter = TimeSpan.FromMilliseconds(400);
        using Scenario scenario = Scenario.Establish(linger);
        scenario.InterruptAccepter();
        using UdpClient resumer = NewResumer();

        scenario.Clock.Advance(resumeArrivesAfter);

        // Act
        resumer.Send(BuildResume(scenario.AccepterSessionId, scenario.ResumeSecret), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntil(() => scenario.AccepterEvents.Any(e => e.Kind == SessionEventKind.PeerResumed));

        scenario.AdvanceAccepter(linger - resumeArrivesAfter);
        scenario.PumpAccepter();

        // Assert
        scenario.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    [Fact]
    public void Pump_MatchingResumeAgainstAPeerThatIsNotInterrupted_ChangesNothing()
    {
        // Arrange — the right secret, but nothing was interrupted, so there is nothing to resume.
        using Scenario scenario = Scenario.Establish();
        using UdpClient resumer = NewResumer();

        // Act
        resumer.Send(BuildResume(scenario.AccepterSessionId, scenario.ResumeSecret), scenario.Accepter.Endpoint);
        for (int i = 0; i < 20; i++)
        {
            scenario.PumpAccepter();
            Thread.Sleep(1);
        }

        // Assert
        scenario.AccepterEvents.Should().BeEmpty();
        scenario.Accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.ResumeMatched || r.Id == DiagnosticId.ResumeUnmatched);
    }

    /// <summary>A UDP socket on its own loopback port: the party sending a Resume, from an address that is not the Peer's.</summary>
    private static UdpClient NewResumer() => new(new IPEndPoint(IPAddress.Loopback, 0));

    /// <summary>The Peer's real secret with its last byte flipped: as close as a wrong secret gets.</summary>
    private static byte[] WrongSecret(Scenario scenario)
    {
        byte[] secret = [.. scenario.ResumeSecret];
        secret[^1] ^= 0xFF;
        return secret;
    }

    /// <summary>Builds a 256-byte, zero-padded Resume by hand: header, target session id, then the secret.</summary>
    private static byte[] BuildResume(uint targetSessionId, ReadOnlySpan<byte> secret)
    {
        byte[] datagram = new byte[ControlDatagramSize];
        BinaryPrimitives.WriteUInt32BigEndian(datagram, 0);
        datagram[4] = ResumeType;
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(ControlBodyOffset), targetSessionId);
        secret.CopyTo(datagram.AsSpan(ControlBodyOffset + sizeof(uint)));
        return datagram;
    }

    private static void AssertOneMalformedDiscard(DatagramSessionReactorHarness accepter, int expectedBytesReceived)
    {
        accepter.DrainToList().Should().BeEmpty();
        accepter.Reactor.Counters.UnknownSourceDiscards.Should().Be(1);
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.ResumeUnmatched);

        DiagnosticRecord record = accepter.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.DatagramMalformed)
            .Which;

        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
        record.Value0.Should().Be(expectedBytesReceived);
    }

    /// <summary>
    /// A connected dialler and accepter through a <see cref="LossyProxy"/> on one shared clock, plus
    /// the session id and resume secret the accepter issued, read off the Accept the proxy captured.
    /// </summary>
    private sealed class Scenario : IDisposable
    {
        private Scenario(
            DatagramSessionReactorHarness dialler,
            DatagramSessionReactorHarness accepter,
            LossyProxy proxy,
            FakeTimeProvider clock,
            PeerId accepterPeer)
        {
            Dialler = dialler;
            Accepter = accepter;
            Proxy = proxy;
            Clock = clock;
            AccepterPeer = accepterPeer;

            byte[] accept = proxy.Captures
                .Single(c => c.Direction == RelayDirection.UpstreamToClient && c.Payload[4] == AcceptType)
                .Payload;
            AccepterSessionId = BinaryPrimitives.ReadUInt32BigEndian(accept.AsSpan(AcceptSessionIdOffset));
            ResumeSecret = accept.AsSpan(AcceptSecretOffset, ResumeSecretLength).ToArray();
        }

        public DatagramSessionReactorHarness Dialler { get; }

        public DatagramSessionReactorHarness Accepter { get; }

        public LossyProxy Proxy { get; }

        public FakeTimeProvider Clock { get; }

        public PeerId AccepterPeer { get; }

        /// <summary>The session id the accepter asked to have stamped on datagrams sent to it.</summary>
        public uint AccepterSessionId { get; }

        /// <summary>The secret the accepter issued, which authorises a Resume against its Peer.</summary>
        public byte[] ResumeSecret { get; }

        /// <summary>Every event the accepter has drained so far, in order. Filled by this scenario's own pumping helpers, which drain after every Pump.</summary>
        public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> AccepterEvents { get; } = [];

        public static Scenario Establish(TimeSpan? linger = null)
        {
            FakeTimeProvider clock = new();
            DatagramSessionReactorHarness dialler = new(
                bindLoopback: false,
                sendIdleInterval: _sendIdleInterval,
                linger: linger,
                missedHeartbeats: MissedHeartbeats,
                timeProvider: clock);
            DatagramSessionReactorHarness accepter = new(
                sendIdleInterval: _sendIdleInterval,
                linger: linger,
                missedHeartbeats: MissedHeartbeats,
                timeProvider: clock);
            LossyProxy proxy = new(accepter.Endpoint, clock);

            dialler.Reactor.Connect(proxy.PublicEndPoint);
            PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);
            return new Scenario(dialler, accepter, proxy, clock, accepterPeer);
        }

        /// <summary>
        /// Advances the clock by <paramref name="interval"/> in the settle-then-advance shape
        /// (<see cref="DatagramSessionReactorHarness.SettleThenAdvance"/>) but drains the settle Pump into
        /// <see cref="AccepterEvents"/>, so an event raised on it is not cleared by the next Pump.
        /// </summary>
        public void AdvanceAccepter(TimeSpan interval)
        {
            Clock.Advance(interval - TimeSpan.FromMilliseconds(1));
            PumpAccepter();
            Clock.Advance(TimeSpan.FromMilliseconds(1));
        }

        /// <summary>Pumps the accepter once and drains that Pump into <see cref="AccepterEvents"/>.</summary>
        public void PumpAccepter()
        {
            Accepter.Reactor.Pump();
            AccepterEvents.AddRange(Accepter.DrainToList());
        }

        /// <summary>Pumps the accepter, draining after every Pump, until <paramref name="condition"/> holds or the real-time budget is spent.</summary>
        public void PumpAccepterUntil(Func<bool> condition)
        {
            PumpDeadline deadline = PumpDeadline.Start();
            while (true)
            {
                PumpAccepter();
                if (condition() || deadline.Expired)
                {
                    return;
                }

                PumpDeadline.Yield();
            }
        }

        /// <summary>
        /// Cuts the path and pumps the accepter past its receive-idle deadline until it has raised
        /// exactly one <see cref="SessionEventKind.PeerInterrupted"/>, draining after every Pump.
        /// </summary>
        public void InterruptAccepter()
        {
            Proxy.StopRelayingBothDirections();
            AdvanceAccepter(_receiveIdleDeadline);
            PumpAccepter();
            AccepterEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted);
        }

        public void Dispose()
        {
            Dialler.Dispose();
            Accepter.Dispose();
            Proxy.Dispose();
        }
    }
}
