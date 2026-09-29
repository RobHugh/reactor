using System.Buffers.Binary;
using System.Net;
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

        public static Scenario Establish()
        {
            FakeTimeProvider clock = new();
            DatagramSessionReactorHarness dialler = new(
                bindLoopback: false,
                sendIdleInterval: _sendIdleInterval,
                missedHeartbeats: MissedHeartbeats,
                timeProvider: clock);
            DatagramSessionReactorHarness accepter = new(
                sendIdleInterval: _sendIdleInterval,
                missedHeartbeats: MissedHeartbeats,
                timeProvider: clock);
            LossyProxy proxy = new(accepter.Endpoint, clock);

            dialler.Reactor.Connect(proxy.PublicEndPoint);
            PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);
            return new Scenario(dialler, accepter, proxy, clock, accepterPeer);
        }

        /// <summary>
        /// Cuts the path and pumps the accepter past its receive-idle deadline until it has raised
        /// exactly one <see cref="SessionEventKind.PeerInterrupted"/>, draining after every Pump.
        /// </summary>
        public void InterruptAccepter()
        {
            Proxy.StopRelayingBothDirections();
            DatagramSessionReactorHarness.SettleThenAdvance(Accepter, _receiveIdleDeadline);
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = Accepter.DrainToList();
            events.AddRange(Accepter.PumpAndDrainAll(1));
            events.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted);
        }

        public void Dispose()
        {
            Dialler.Dispose();
            Accepter.Dispose();
            Proxy.Dispose();
        }
    }
}
