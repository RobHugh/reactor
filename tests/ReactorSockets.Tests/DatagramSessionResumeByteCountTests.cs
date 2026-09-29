using System.Net.Sockets;
using FluentAssertions;
using Xunit;
using Scenario = ReactorSockets.Tests.DatagramSessionResumeScenario;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #56 (PR #392 review, D2): a Resume control datagram is charged to its Peer like a data Datagram,
/// because a Peer exists to charge (ADR 0013). The dialling end counts each Resume it sends in
/// <see cref="PeerCounters.SocketWrites"/> and <see cref="PeerCounters.BytesSent"/>. The accepting end counts
/// a matched Resume in <see cref="PeerCounters.BytesReceived"/>. A Resume that matches no Peer charges none.
/// </summary>
public class DatagramSessionResumeByteCountTests
{
    private const byte ResumeType = 6;

    [Fact]
    public void Pump_InterruptedDiallerSendingResumes_ChargesEveryDatagramItSendsToThePeer()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(Scenario.ResumableLinger);
        scenario.InterruptBothEnds();
        SettleProxy(scenario);
        PeerCounters before = scenario.Dialler.Reactor.CountersOf(scenario.DiallerPeer);
        int capturedBefore = ClientToUpstream(scenario).Count;

        // Act — three send-idle intervals, so at least two more Resumes go out.
        for (int i = 0; i < 3 * (int)(Scenario.SendIdleInterval / Scenario.StepInterval); i++)
        {
            scenario.Step(Scenario.StepInterval);
        }

        PeerCounters after = scenario.Dialler.Reactor.CountersOf(scenario.DiallerPeer);
        ulong writes = after.SocketWrites - before.SocketWrites;
        PumpDeadline deadline = PumpDeadline.Start();
        while ((ulong)(ClientToUpstream(scenario).Count - capturedBefore) < writes && !deadline.Expired)
        {
            scenario.Proxy.Pump();
            PumpDeadline.Yield();
        }

        SettleProxy(scenario);
        List<RelayCapture> sent = [.. ClientToUpstream(scenario).Skip(capturedBefore)];

        // Assert
        sent.Count(c => c.Payload[Scenario.ControlBodyOffset - 1] == ResumeType && IsControl(c.Payload))
            .Should().BeGreaterThanOrEqualTo(2, "the window must hold Resumes for this test to prove anything");
        writes.Should().Be((ulong)sent.Count, "every datagram the dialler sent to its Peer is one socket write");
        (after.BytesSent - before.BytesSent).Should().Be((ulong)sent.Sum(c => c.Payload.Length));
    }

    [Fact]
    public void Pump_MatchedResume_ChargesItsBytesToThePeersBytesReceived()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish();
        scenario.InterruptAccepter();
        using UdpClient resumer = Scenario.NewResumer();
        ulong before = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).BytesReceived;

        // Act
        resumer.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.ResumeSecret), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntil(() => scenario.AccepterEvents.Any(e => e.Kind == SessionEventKind.PeerResumed));
        scenario.PumpAccepter();

        // Assert
        scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).BytesReceived
            .Should().Be(before + Scenario.ControlDatagramSize);
    }

    [Fact]
    public void Pump_ResumeWithTheWrongSecret_ChargesNothingToThePeer()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish();
        scenario.InterruptAccepter();
        using UdpClient resumer = Scenario.NewResumer();
        ulong before = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).BytesReceived;

        // Act
        resumer.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.WrongSecret()), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntil(() => scenario.Accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));
        scenario.PumpAccepter();

        // Assert
        scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).BytesReceived.Should().Be(before);
    }

    /// <summary>A control datagram carries zero in its session field and is exactly 256 bytes.</summary>
    private static bool IsControl(byte[] payload) =>
        payload.Length == Scenario.ControlDatagramSize && !payload.AsSpan(0, sizeof(uint)).ContainsAnyExcept((byte)0);

    private static List<RelayCapture> ClientToUpstream(Scenario scenario) =>
        [.. scenario.Proxy.Captures.Where(c => c.Direction == RelayDirection.ClientToUpstream)];

    /// <summary>Pumps the proxy until several Pumps in a row read nothing new, so every datagram already sent has been captured.</summary>
    private static void SettleProxy(Scenario scenario)
    {
        PumpDeadline deadline = PumpDeadline.Start(TimeSpan.FromMilliseconds(500));
        int quiet = 0;
        while (quiet < 20 && !deadline.Expired)
        {
            int count = scenario.Proxy.Captures.Count;
            scenario.Proxy.Pump();
            quiet = scenario.Proxy.Captures.Count == count ? quiet + 1 : 0;
            PumpDeadline.Yield();
        }
    }
}
