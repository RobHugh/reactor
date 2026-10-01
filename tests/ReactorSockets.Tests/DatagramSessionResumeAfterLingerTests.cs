using System.Net.Sockets;
using FluentAssertions;
using Xunit;
using Scenario = ReactorSockets.Tests.DatagramSessionResumeScenario;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #390 (parent #56): once an Interrupted Session's effective Linger has expired, both ends have
/// closed it, so a Resume carrying the once-valid session id and secret matches nothing, and a fresh
/// Connect is admitted as a new Peer with a different <see cref="PeerId"/>.
/// </summary>
/// <remarks>
/// The path is cut, then both ends are pumped on the shared clock in half-interval steps, draining the
/// accepter through the scenario and the dialler here after every Pump, until both have closed.
/// </remarks>
public class DatagramSessionResumeAfterLingerTests
{
    private static readonly TimeSpan _linger = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan _step = TimeSpan.FromMilliseconds(500);

    [Fact]
    public void Pump_InterruptedSessionPastItsLinger_ClosesBothEndsWithLingerExpired()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(_linger);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents = [];

        // Act
        ExpireLinger(scenario, diallerEvents);

        // Assert
        scenario.AccepterEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.LingerExpired);
        diallerEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.LingerExpired);
    }

    [Fact]
    public void Pump_ResumeWithTheOnceValidIdAndSecretAfterLingerExpiry_LogsResumeUnmatchedAndRaisesNoPeerResumed()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(_linger);
        ExpireLinger(scenario, []);
        scenario.Accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.ResumeUnmatched);
        using UdpClient resumer = Scenario.NewResumer();

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
    }

    [Fact]
    public void Connect_AfterLingerExpiry_IsAdmittedAsANewPeerWithADifferentPeerId()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(_linger);
        PeerId expired = scenario.AccepterPeer;
        ExpireLinger(scenario, []);
        scenario.Proxy.ResumeRelayingBothDirections();
        int connectedBefore = scenario.AccepterEvents.Count(e => e.Kind == SessionEventKind.PeerConnected);

        // Act
        scenario.Dialler.Reactor.Connect(scenario.Proxy.PublicEndPoint);
        PumpUntilAccepterConnectsAgain(scenario, connectedBefore);

        // Assert
        List<PeerId> connected = [.. scenario.AccepterEvents
            .Where(e => e.Kind == SessionEventKind.PeerConnected)
            .Select(e => e.Peer)];
        connected.Should().HaveCount(connectedBefore + 1);
        PeerId fresh = connected[^1];
        fresh.Should().NotBe(expired);
        fresh.Origin.Should().Be(expired.Origin, "both ids come from the same accepting Reactor");
        (fresh.Index != expired.Index || fresh.Generation != expired.Generation).Should().BeTrue(
            $"the fresh id differs in Index ({expired.Index} -> {fresh.Index}) or Generation ({expired.Generation} -> {fresh.Generation})");
    }

    [Fact]
    public void Pump_DiallerAfterItsLingerExpired_SendsNoFurtherResumes()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(_linger);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents = [];
        ExpireLinger(scenario, diallerEvents);
        int resumesAtClose = Scenario.DiallerResumes(scenario.Proxy).Count;

        // Act
        for (int i = 0; i < 6; i++)
        {
            Step(scenario, diallerEvents);
        }

        // Assert
        resumesAtClose.Should().BeGreaterThan(0, "the dialler did send Resumes while Interrupted");
        Scenario.DiallerResumes(scenario.Proxy).Count.Should().Be(resumesAtClose);
    }

    private static void Step(Scenario scenario, List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents)
    {
        scenario.Clock.Advance(_step);
        scenario.Dialler.Reactor.Pump();
        diallerEvents.AddRange(scenario.Dialler.DrainToList());
        scenario.Proxy.Pump();
        scenario.PumpAccepter();
    }

    /// <summary>Cuts the path and steps both ends until each has raised PeerClosed, failing if that takes far longer than Linger plus the receive-idle deadline.</summary>
    private static void ExpireLinger(Scenario scenario, List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents)
    {
        scenario.Proxy.StopRelayingBothDirections();
        int maxSteps = (int)((_linger + Scenario.ReceiveIdleDeadline) / _step) * 2;
        for (int i = 0; i < maxSteps; i++)
        {
            Step(scenario, diallerEvents);
            if (scenario.AccepterEvents.Any(e => e.Kind == SessionEventKind.PeerClosed)
                && diallerEvents.Any(e => e.Kind == SessionEventKind.PeerClosed))
            {
                return;
            }
        }

        throw new InvalidOperationException("Both ends did not close within the expected time after the path was cut.");
    }

    private static void PumpUntilAccepterConnectsAgain(Scenario scenario, int connectedBefore)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            scenario.Dialler.Reactor.Pump();
            scenario.Dialler.DrainAndDiscard();
            scenario.Proxy.Pump();
            scenario.PumpAccepter();
            scenario.Proxy.Pump();
            if (scenario.AccepterEvents.Count(e => e.Kind == SessionEventKind.PeerConnected) > connectedBefore || deadline.Expired)
            {
                return;
            }

            PumpDeadline.Yield();
        }
    }
}
