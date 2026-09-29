using System.Net.Sockets;
using FluentAssertions;
using Xunit;
using Scenario = ReactorSockets.Tests.DatagramSessionResumeScenario;

namespace ReactorSockets.Tests;

/// <summary>
/// Issues #378 to #381 (parent #56): what a matched Resume does to the accepting end's Peer. Each test
/// drains after every Pump, because <see cref="DatagramSessionReactor.Pump"/> clears undrained events
/// and a "no event" assertion over undrained Pumps proves nothing.
/// </summary>
public class DatagramSessionMatchedResumeTests
{
    [Fact]
    public void Pump_MatchingResumeAgainstAnInterruptedPeer_CountsExactlyOneResume()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish();
        scenario.InterruptAccepter();
        using UdpClient resumer = Scenario.NewResumer();

        // Act
        SendMatchingResume(scenario, resumer);
        scenario.PumpAccepterUntil(() => scenario.AccepterEvents.Any(e => e.Kind == SessionEventKind.PeerResumed));
        scenario.PumpAccepter();

        // Assert
        scenario.AccepterEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerResumed);
        scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).Resumes.Should().Be(1);
    }

    [Fact]
    public void Pump_MatchingResumeAgainstAnInterruptedPeer_DoesNotResetByteOrMessageCounters()
    {
        // Arrange — traffic in both directions first, so every figure checked is above zero.
        using Scenario scenario = Scenario.Establish();
        scenario.Accepter.Reactor.Send(scenario.AccepterPeer, DeliveryMode.Unreliable, [1, 2, 3]).Should().Be(SendOutcome.Sent);
        scenario.Dialler.Reactor.Send(scenario.DiallerPeer, DeliveryMode.Unreliable, [4, 5, 6]).Should().Be(SendOutcome.Sent);
        for (int i = 0; i < 3; i++)
        {
            DatagramSessionReactorHarness.PumpRoundThroughProxy(scenario.Dialler, scenario.Proxy, scenario.Accepter);
            scenario.Accepter.DrainToList();
            scenario.Dialler.DrainToList();
        }

        scenario.InterruptAccepter();
        PeerCounters before = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer);
        using UdpClient resumer = Scenario.NewResumer();

        // Act
        SendMatchingResume(scenario, resumer);
        scenario.PumpAccepterUntil(() => scenario.AccepterEvents.Any(e => e.Kind == SessionEventKind.PeerResumed));
        PeerCounters after = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer);

        // Assert
        before.BytesSent.Should().BeGreaterThan(0);
        before.BytesReceived.Should().BeGreaterThan(0);
        before.MessagesReceived.Should().BeGreaterThan(0);
        after.BytesSent.Should().BeGreaterThanOrEqualTo(before.BytesSent);
        after.BytesReceived.Should().BeGreaterThanOrEqualTo(before.BytesReceived);
        after.MessagesReceived.Should().Be(before.MessagesReceived);
        after.Resumes.Should().Be(1);
    }

    private static void SendMatchingResume(Scenario scenario, UdpClient resumer) =>
        resumer.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.ResumeSecret), scenario.Accepter.Endpoint);
}
