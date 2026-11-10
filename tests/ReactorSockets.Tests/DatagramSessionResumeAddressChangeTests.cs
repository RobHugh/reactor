using System.Net;
using FluentAssertions;
using Xunit;
using Scenario = ReactorSockets.Tests.DatagramSessionResumeScenario;

namespace ReactorSockets.Tests;

/// <summary>
/// A dialler and an accepter through <see cref="LossyProxy"/> on one shared clock
/// both raise <see cref="SessionEventKind.PeerInterrupted"/> when the path is cut, then
/// <see cref="SessionEventKind.PeerResumed"/> once the proxy's upstream-facing socket has moved to a new
/// port and relaying restarts, with the same <see cref="PeerId"/> as before. The accepter then sends to
/// the new port only. Issue #393 (P2) repeats the move onto a second loopback address.
/// </summary>
/// <remarks>
/// Every Pump is drained by <see cref="Scenario.Step"/>, so an event raised on any Pump is counted.
/// </remarks>
public class DatagramSessionResumeAddressChangeTests
{
    private static readonly SessionEventKind[] _lifecycle =
    [
        SessionEventKind.PeerInterrupted,
        SessionEventKind.PeerResumed,
    ];

    [Fact]
    public void Pump_PathCutThenMovedToNewPort_AccepterRaisesOneInterruptedThenOneResumedForTheSamePeerId()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(Scenario.ResumableLinger);

        // Act
        scenario.InterruptBothEnds();
        scenario.ResumeThroughNewPort();

        // Assert
        AssertLifecycle(scenario.AccepterEvents, scenario.AccepterPeer);
    }

    [Fact]
    public void Pump_PathCutThenMovedToNewPort_DiallerRaisesOneInterruptedThenOneResumedForTheSamePeerId()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(Scenario.ResumableLinger);

        // Act
        scenario.InterruptBothEnds();
        scenario.ResumeThroughNewPort();

        // Assert
        AssertLifecycle(scenario.DiallerEvents, scenario.DiallerPeer);
    }

    [Fact]
    public void Pump_AfterTheMove_AccepterSendsToTheNewPortOnly()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(Scenario.ResumableLinger);
        IPEndPoint oldPort = scenario.UpstreamFacingEndPoint;
        scenario.InterruptBothEnds();
        int capturesBeforeMove = scenario.Proxy.Captures.Count;

        // Act
        IPEndPoint newPort = scenario.ResumeThroughNewPort();
        for (int i = 0; i < 6; i++)
        {
            scenario.Step(Scenario.StepInterval);
        }

        // Assert
        newPort.Port.Should().NotBe(oldPort.Port);
        List<RelayCapture> fromAccepter = [.. scenario.Proxy.Captures
            .Skip(capturesBeforeMove)
            .Where(c => c.Direction == RelayDirection.UpstreamToClient)];
        fromAccepter.Should().NotBeEmpty("the accepter kept sending after the Resume");
        fromAccepter.Should().OnlyContain(c => c.ReceivedOn!.Equals(newPort), "every datagram after the move reached the new port");
        fromAccepter.Should().NotContain(c => c.ReceivedOn!.Equals(oldPort), "nothing further reached the old port");
    }

    [Fact]
    public void Pump_PathCutThenMovedToANewAddress_AccepterResumesAndSendsToTheNewAddressOnly()
    {
        // Arrange — 127.0.0.2 is another loopback address, so the source address changes, not just the port.
        using Scenario scenario = Scenario.Establish(Scenario.ResumableLinger);
        IPEndPoint oldEndPoint = scenario.UpstreamFacingEndPoint;
        scenario.InterruptBothEnds();
        int capturesBeforeMove = scenario.Proxy.Captures.Count;

        // Act
        IPEndPoint newEndPoint = scenario.ResumeThroughNewPort(IPAddress.Parse("127.0.0.2"));
        for (int i = 0; i < 6; i++)
        {
            scenario.Step(Scenario.StepInterval);
        }

        // Assert
        newEndPoint.Address.Should().NotBe(oldEndPoint.Address);
        AssertLifecycle(scenario.AccepterEvents, scenario.AccepterPeer);
        List<RelayCapture> fromAccepter = [.. scenario.Proxy.Captures
            .Skip(capturesBeforeMove)
            .Where(c => c.Direction == RelayDirection.UpstreamToClient)];
        fromAccepter.Should().NotBeEmpty("the accepter kept sending after the Resume");
        fromAccepter.Should().OnlyContain(c => c.ReceivedOn!.Equals(newEndPoint), "every datagram after the move reached the new address");
    }

    [Fact]
    public void Pump_AfterTheMove_EachEndStaysConnectedWithNoFurtherLifecycleEvents()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(Scenario.ResumableLinger);
        scenario.InterruptBothEnds();
        scenario.ResumeThroughNewPort();

        // Act
        for (int i = 0; i < 6; i++)
        {
            scenario.Step(Scenario.StepInterval);
        }

        // Assert
        AssertLifecycle(scenario.AccepterEvents, scenario.AccepterPeer);
        AssertLifecycle(scenario.DiallerEvents, scenario.DiallerPeer);
        scenario.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        scenario.DiallerEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    private static void AssertLifecycle(
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events,
        PeerId connected)
    {
        events.Where(e => _lifecycle.Contains(e.Kind)).Select(e => e.Kind)
            .Should().Equal(SessionEventKind.PeerInterrupted, SessionEventKind.PeerResumed);
        events.Where(e => _lifecycle.Contains(e.Kind)).Select(e => e.Peer)
            .Should().OnlyContain(p => p == connected, "Resume keeps the PeerId, generation included");
    }
}
