using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;
using Scenario = ReactorSockets.Tests.DatagramSessionResumeScenario;

namespace ReactorSockets.Tests;

/// <summary>
/// The accepting end routes an arriving Resume control
/// datagram by its target session id and authorises it by its secret (ADR 0009). A malformed Resume
/// is dropped like any other malformed control datagram.
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
    [Fact]
    public void Pump_ResumeForAnUnknownSessionId_LogsExactlyOneResumeUnmatchedNamingTheSource()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish();
        using HostilePeer resumer = new();
        byte[] resume = Scenario.BuildResume(scenario.AccepterSessionId ^ 0x0100_0000, scenario.ResumeSecret);

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
        byte[] resume = Scenario.BuildResume(scenario.AccepterSessionId ^ 0x0100_0000, scenario.ResumeSecret);
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
    [InlineData(Scenario.ControlDatagramSize - 1)]
    [InlineData(Scenario.ControlDatagramSize + 1)]
    [InlineData(Scenario.ControlBodyOffset)]
    public void Pump_ResumeOfTheWrongLength_IsDiscardedLikeAnyMalformedControlDatagram(int length)
    {
        // Arrange — a valid Resume with a real-looking body, cut or padded to the wrong size.
        using DatagramSessionReactorHarness accepter = new();
        using HostilePeer resumer = new();
        byte[] full = Scenario.BuildResume(0x0102_0304, new byte[Scenario.ResumeSecretLength]);
        byte[] datagram = new byte[length];
        full.AsSpan(0, Math.Min(length, full.Length)).CopyTo(datagram);

        // Act
        resumer.SendDatagram(accepter.Endpoint, datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: length);
    }

    [Fact]
    public void Pump_ResumeWithANonZeroByteRightAfterTheBody_IsDiscardedLikeAnyMalformedControlDatagram()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using HostilePeer resumer = new();
        byte[] datagram = Scenario.BuildResume(0x0102_0304, new byte[Scenario.ResumeSecretLength]);
        datagram[Scenario.ControlBodyOffset + Scenario.ResumeBodyLength] = 0x01;

        // Act
        resumer.SendDatagram(accepter.Endpoint, datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: Scenario.ControlDatagramSize);
    }

    [Fact]
    public void Pump_WrongSecretResumeAgainstAnInterruptedPeer_LogsResumeUnmatchedWithValue0One()
    {
        // Arrange — the target session id is the real one, so only the secret is wrong.
        using Scenario scenario = Scenario.Establish();
        scenario.InterruptAccepter();
        using UdpClient resumer = Scenario.NewResumer();

        // Act
        resumer.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.WrongSecret()), scenario.Accepter.Endpoint);
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
        using UdpClient resumer = Scenario.NewResumer();

        scenario.Clock.Advance(resumeArrivesAfter);

        // Act
        resumer.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.WrongSecret()), scenario.Accepter.Endpoint);
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
        using UdpClient resumer = Scenario.NewResumer();
        ulong resumesBefore = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).Resumes;

        // Act
        resumer.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.WrongSecret()), scenario.Accepter.Endpoint);
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
        using UdpClient resumer = Scenario.NewResumer();

        // Act — the wrong-secret Resume, then further send-idle intervals: the Interrupted Peer keeps
        // pinging, and where those Pings go is its send address.
        resumer.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.WrongSecret()), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntil(() => scenario.Accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        int pingsToTheProxyBefore = scenario.Proxy.Captures.Count(c => c.Direction == RelayDirection.UpstreamToClient);
        for (int i = 0; i < 3; i++)
        {
            scenario.AdvanceAccepter(Scenario.SendIdleInterval);
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
        using UdpClient resumer = Scenario.NewResumer();

        // Act
        resumer.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.ResumeSecret), scenario.Accepter.Endpoint);
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
        using UdpClient resumer = Scenario.NewResumer();

        // Act
        resumer.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.ResumeSecret), scenario.Accepter.Endpoint);
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
        using UdpClient resumer = Scenario.NewResumer();

        scenario.Clock.Advance(resumeArrivesAfter);

        // Act
        resumer.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.ResumeSecret), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntil(() => scenario.AccepterEvents.Any(e => e.Kind == SessionEventKind.PeerResumed));

        scenario.AdvanceAccepter(linger - resumeArrivesAfter);
        scenario.PumpAccepter();

        // Assert
        scenario.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    [Fact]
    public void Pump_MatchingResumeAgainstAPeerThatIsNotInterrupted_RaisesNoEventAndLogsResumeMatched()
    {
        // Arrange — the right secret, but nothing was interrupted, so there is no Session event to raise.
        using Scenario scenario = Scenario.Establish();
        using UdpClient resumer = Scenario.NewResumer();

        // Act
        ulong receivedBefore = scenario.Accepter.Reactor.Counters.DatagramsReceived;
        resumer.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.ResumeSecret), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntilReceivedMoreThan(receivedBefore);

        // Assert
        scenario.AccepterEvents.Should().BeEmpty();
        scenario.Accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.ResumeMatched);
        scenario.Accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.ResumeUnmatched);
    }

    private static void AssertOneMalformedDiscard(DatagramSessionReactorHarness accepter, int expectedBytesReceived)
    {
        accepter.DrainToList().Should().BeEmpty();
        accepter.Reactor.Counters.UnknownSourceDatagrams.Should().Be(1);
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.ResumeUnmatched);

        DiagnosticRecord record = accepter.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.DatagramMalformed)
            .Which;

        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
        record.Value0.Should().Be(expectedBytesReceived);
    }

}
