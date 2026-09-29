using System.Net.Sockets;
using FluentAssertions;
using Xunit;
using Scenario = ReactorSockets.Tests.DatagramSessionResumeScenario;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #404 (parent #57): a Goodbye forbids a Resume, so a valid-secret Resume aimed at a Peer in
/// receiver-side Closing (Goodbye received, Messages still draining) is refused. It logs
/// <see cref="DiagnosticId.ResumeUnmatched"/>, raises no <see cref="SessionEventKind.PeerResumed"/>, does
/// not move the Peer's remote address, and the Peer still closes with
/// <see cref="DisconnectReason.PeerGoodbye"/> once it has drained.
/// </summary>
/// <remarks>
/// A Peer stays in receiver-side Closing only until the start of the next Pump, so the Resume has to be
/// read in the same Pump as the Message and Goodbye. The dialler sends a Message then closes, the proxy
/// relays both, the resuming party sends its Resume straight to the accepter, and the accepter is
/// Pumped once. Every Pump is drained.
/// </remarks>
public class DatagramSessionResumeAtClosingPeerTests
{
    private const byte ResumeType = 6;

    [Fact]
    public void Pump_ValidResumeAtAPeerInReceiverSideClosing_LogsResumeUnmatchedAndRaisesNoPeerResumed()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(Scenario.ResumableLinger);
        using UdpClient resumer = Scenario.NewResumer();

        // Act
        DeliverMessageGoodbyeAndResumeInOnePump(scenario, resumer);

        // Assert
        scenario.AccepterEvents.Select(e => e.Kind).Should().Contain(SessionEventKind.PeerClosing,
            "the Peer was in receiver-side Closing when the Resume was read");
        DiagnosticRecord record = scenario.Accepter.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.ResumeUnmatched).Which;
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
        record.Value0.Should().Be(1, "the secret matched a Peer, which refused it");
        record.Value1.Should().Be(ResumeType);
        scenario.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerResumed);
    }

    [Fact]
    public void Pump_ValidResumeAtAPeerInReceiverSideClosing_DoesNotMoveTheRemoteAddress()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(Scenario.ResumableLinger);
        using UdpClient resumer = Scenario.NewResumer();

        // Act
        DeliverMessageGoodbyeAndResumeInOnePump(scenario, resumer);
        for (int i = 0; i < 4; i++)
        {
            scenario.Step(Scenario.StepInterval);
        }

        // Assert
        scenario.Accepter.Sink.Received.Should().Contain(r => r.Id == DiagnosticId.ResumeUnmatched);
        resumer.Available.Should().Be(0, "nothing was sent to the Resume's source");
    }

    [Fact]
    public void Pump_ValidResumeAtAPeerInReceiverSideClosing_StillClosesWithPeerGoodbyeAfterDraining()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(Scenario.ResumableLinger);
        using UdpClient resumer = Scenario.NewResumer();
        DeliverMessageGoodbyeAndResumeInOnePump(scenario, resumer);

        // Act
        scenario.PumpAccepter();

        // Assert
        scenario.AccepterEvents.Where(e => e.Kind == SessionEventKind.PeerClosed).Should().ContainSingle()
            .Which.Reason.Should().Be(DisconnectReason.PeerGoodbye);
        scenario.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerResumed);
    }

    [Fact]
    public void Pump_ValidResumeAtAPeerThatClosedLocally_LogsResumeUnmatchedAndRaisesNoPeerResumed()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(Scenario.ResumableLinger);
        using UdpClient resumer = Scenario.NewResumer();
        scenario.Accepter.Reactor.Close(scenario.AccepterPeer);

        // Act
        resumer.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.ResumeSecret), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntil(() => scenario.Accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));
        scenario.PumpAccepter();

        // Assert
        scenario.Accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.ResumeUnmatched)
            .Which.Value0.Should().Be(1);
        scenario.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerResumed);
        resumer.Available.Should().Be(0, "nothing was sent to the Resume's source");
    }

    /// <summary>
    /// The dialler sends one Message and closes, the proxy relays both to the accepter, the resuming party
    /// sends a valid Resume after them, and the accepter is Pumped once so it reads all three together.
    /// </summary>
    private static void DeliverMessageGoodbyeAndResumeInOnePump(Scenario scenario, UdpClient resumer)
    {
        scenario.Dialler.Reactor.Send(scenario.DiallerPeer, DeliveryMode.ReliableOrdered, [0xA0]).Should().Be(SendOutcome.Sent);
        scenario.Dialler.Reactor.Close(scenario.DiallerPeer);
        scenario.Dialler.Reactor.Pump();
        scenario.Dialler.DrainAndDiscard();
        scenario.Proxy.Pump();
        resumer.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.ResumeSecret), scenario.Accepter.Endpoint);
        PumpDeadline.Yield();
        scenario.PumpAccepter();
    }
}
