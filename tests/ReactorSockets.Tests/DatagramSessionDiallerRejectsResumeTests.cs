using System.Net.Sockets;
using FluentAssertions;
using Xunit;
using Scenario = ReactorSockets.Tests.DatagramSessionResumeScenario;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #56 (PR #392 review, D3): only the accepting end is a Resume target. The dialling end is the
/// one that sends Resume, because only it can know its own new address, so a Resume arriving at a
/// dialled Peer is rejected like one naming no held session, even when it presents the dialler's own
/// session id and the secret the dialler issued. Both travel in the dialler's HelloWithCookie, so
/// without the rejection an observer of the handshake could move the dialler's send address.
/// </summary>
/// <remarks>
/// The dialler is bound to a loopback port (<c>bindDialler</c>) so the test can reach it directly; its
/// session id and secret are read off the HelloWithCookie the proxy captured.
/// </remarks>
public class DatagramSessionDiallerRejectsResumeTests
{
    [Fact]
    public void Pump_ResumeNamingADialledPeerWithItsOwnSecret_LogsResumeUnmatchedAndRaisesNoEvent()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(bindDialler: true);
        using UdpClient resumer = Scenario.NewResumer();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents = [];

        // Act
        resumer.Send(Scenario.BuildResume(scenario.DiallerSessionId, scenario.DiallerResumeSecret), scenario.Dialler.Endpoint);
        PumpDiallerUntil(scenario, diallerEvents, () => scenario.Dialler.Sink.Received.Any(IsResumeRecord));
        PumpDiallerUntil(scenario, diallerEvents, () => false, pumps: 5);

        // Assert
        DiagnosticRecord record = scenario.Dialler.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.ResumeUnmatched).Which;
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
        record.Value0.Should().Be(0, "a dialled Peer is not a Resume target, so no reservation was checked");
        record.Value1.Should().Be(6);
        scenario.Dialler.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.ResumeMatched);
        diallerEvents.Should().BeEmpty();
        scenario.Dialler.Reactor.CountersOf(scenario.DiallerPeer).Resumes.Should().Be(0);
    }

    [Fact]
    public void Pump_ResumeNamingADialledPeerWithItsOwnSecret_DoesNotMoveWhereTheDiallerSends()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(bindDialler: true);
        using UdpClient resumer = Scenario.NewResumer();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents = [];
        resumer.Send(Scenario.BuildResume(scenario.DiallerSessionId, scenario.DiallerResumeSecret), scenario.Dialler.Endpoint);
        PumpDiallerUntil(scenario, diallerEvents, () => scenario.Dialler.Sink.Received.Any(IsResumeRecord));
        int relayedBefore = ClientToUpstreamCount(scenario);

        // Act — two send-idle intervals, so the dialler Pings at least once more.
        for (int i = 0; i < 2 * (int)(Scenario.SendIdleInterval / Scenario.StepInterval); i++)
        {
            scenario.Step(Scenario.StepInterval);
        }

        // Assert
        ClientToUpstreamCount(scenario).Should().BeGreaterThan(relayedBefore, "the dialler still sends to the proxy");
        resumer.Available.Should().Be(0, "nothing the dialler sends reaches the party that sent the Resume");
    }

    private static bool IsResumeRecord(DiagnosticRecord record) =>
        record.Id is DiagnosticId.ResumeMatched or DiagnosticId.ResumeUnmatched;

    private static int ClientToUpstreamCount(Scenario scenario) =>
        scenario.Proxy.Captures.Count(c => c.Direction == RelayDirection.ClientToUpstream);

    /// <summary>
    /// Pumps the dialler, draining after every Pump into <paramref name="events"/>, until
    /// <paramref name="condition"/> holds, <paramref name="pumps"/> Pumps have run, or the real-time
    /// budget is spent.
    /// </summary>
    private static void PumpDiallerUntil(
        Scenario scenario,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events,
        Func<bool> condition,
        int pumps = int.MaxValue)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        for (int i = 0; i < pumps; i++)
        {
            scenario.Dialler.Reactor.Pump();
            events.AddRange(scenario.Dialler.DrainToList());
            if (condition() || deadline.Expired)
            {
                return;
            }

            PumpDeadline.Yield();
        }
    }
}
