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

    [Fact]
    public void Pump_MatchingResumeFromANewSource_SendsTheNextDatagramToTheNewSourceAndNoneToTheOldOne()
    {
        // Arrange — a long Linger so nothing closes the Peer while the send-idle intervals pass.
        using Scenario scenario = Scenario.Establish(linger: TimeSpan.FromSeconds(30));
        scenario.InterruptAccepter();
        using UdpClient resumer = Scenario.NewResumer();
        SendMatchingResume(scenario, resumer);
        scenario.PumpAccepterUntil(() => scenario.AccepterEvents.Any(e => e.Kind == SessionEventKind.PeerResumed));
        scenario.Proxy.Pump();
        int toTheProxyBefore = scenario.Proxy.Captures.Count(c => c.Direction == RelayDirection.UpstreamToClient);

        // Act — one send-idle interval later the Peer pings, and a ping goes to the Peer's send address.
        bool reachedTheResumer = false;
        for (int i = 0; i < 2 && !reachedTheResumer; i++)
        {
            scenario.AdvanceAccepter(Scenario.SendIdleInterval);
            scenario.PumpAccepter();
            scenario.Proxy.Pump();
            reachedTheResumer = WaitForDatagram(resumer);
        }

        // Assert
        reachedTheResumer.Should().BeTrue("the next datagram sent to the Peer goes to the Resume's source");
        scenario.Proxy.Captures.Count(c => c.Direction == RelayDirection.UpstreamToClient)
            .Should().Be(toTheProxyBefore, "nothing more is sent to the address the Peer was established from");
    }

    [Fact]
    public void Pump_DataDatagramWithTheRightSessionIdFromANewAddress_DoesNotMoveWhereThePeerIsSentTo()
    {
        // Arrange — a real data datagram the dialler sent, replayed verbatim from another address.
        using Scenario scenario = Scenario.Establish();
        scenario.Dialler.Reactor.Send(scenario.DiallerPeer, DeliveryMode.Unreliable, [7, 8, 9]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughProxy(scenario.Dialler, scenario.Proxy, scenario.Accepter);
        byte[] data = scenario.Proxy.Captures
            .Last(c => c.Direction == RelayDirection.ClientToUpstream && c.Payload.AsSpan(0, sizeof(uint)).ContainsAnyExcept((byte)0))
            .Payload;
        using UdpClient replayer = Scenario.NewResumer();
        int toTheProxyBefore = scenario.Proxy.Captures.Count(c => c.Direction == RelayDirection.UpstreamToClient);

        ulong receivedBefore = scenario.Accepter.Reactor.Counters.DatagramsReceived;

        // Act
        replayer.Send(data, scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntil(() => scenario.Accepter.Reactor.Counters.DatagramsReceived > receivedBefore);
        for (int i = 0; i < 2; i++)
        {
            scenario.AdvanceAccepter(Scenario.SendIdleInterval);
            scenario.PumpAccepter();
            scenario.Proxy.Pump();
        }

        // Assert
        WaitForDatagram(replayer, TimeSpan.FromMilliseconds(100)).Should().BeFalse("only a matched Resume moves the send address");
        scenario.Proxy.Captures.Count(c => c.Direction == RelayDirection.UpstreamToClient)
            .Should().BeGreaterThan(toTheProxyBefore, "the Peer's datagrams still go to the address it was established from");
    }

    private static bool WaitForDatagram(UdpClient client, TimeSpan? budget = null)
    {
        PumpDeadline deadline = PumpDeadline.Start(budget);
        while (client.Available == 0)
        {
            if (deadline.Expired)
            {
                return false;
            }

            PumpDeadline.Yield();
        }

        return true;
    }

    private static void SendMatchingResume(Scenario scenario, UdpClient resumer) =>
        resumer.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.ResumeSecret), scenario.Accepter.Endpoint);
}
