using System.Net.Sockets;
using FluentAssertions;
using Xunit;
using Scenario = ReactorSockets.Tests.DatagramSessionResumeScenario;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #405 (parent #57): once a Peer has closed because of a Goodbye, a Resume carrying that Session's
/// id and secret matches nothing. <see cref="DiagnosticId.ResumeUnmatched"/> is logged, and no
/// <see cref="SessionEventKind.PeerResumed"/> or <see cref="SessionEventKind.PeerConnected"/> is raised.
/// </summary>
/// <remarks>Every Pump is drained by the scenario's pumping helpers.</remarks>
public class DatagramSessionResumeAfterGoodbyeTests
{
    [Fact]
    public void Pump_ResumeReplayedAfterPeerClosedWithPeerGoodbye_LogsResumeUnmatchedAndRaisesNoPeerResumedOrPeerConnected()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(Scenario.ResumableLinger);
        using UdpClient resumer = Scenario.NewResumer();
        scenario.Dialler.Reactor.Close(scenario.DiallerPeer);
        for (int i = 0; i < 20 && !scenario.AccepterEvents.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            scenario.Step(Scenario.StepInterval);
        }

        scenario.AccepterEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.PeerGoodbye);
        scenario.Accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.ResumeUnmatched);
        int connectedBefore = scenario.AccepterCount(SessionEventKind.PeerConnected);

        // Act
        resumer.Send(Scenario.BuildResume(scenario.AccepterSessionId, scenario.ResumeSecret), scenario.Accepter.Endpoint);
        scenario.PumpAccepterUntil(() => scenario.Accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));
        scenario.PumpAccepter();

        // Assert
        DiagnosticRecord record = scenario.Accepter.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.ResumeUnmatched).Which;
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
        record.Value0.Should().Be(0);
        record.Value1.Should().Be(Scenario.ResumeType);
        scenario.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerResumed);
        scenario.AccepterCount(SessionEventKind.PeerConnected).Should().Be(connectedBefore);
    }
}
