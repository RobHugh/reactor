using FluentAssertions;
using Xunit;
using Scenario = ReactorSockets.Tests.DatagramSessionResumeScenario;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #388 (parent #56): one Session interrupted and resumed three times, with retransmissions
/// provoked between the resumes, reports <see cref="PeerCounters.Resumes"/> = 3 on each end and never
/// lowers <see cref="PeerCounters.BytesSent"/>, <see cref="PeerCounters.BytesReceived"/> or
/// <see cref="PeerCounters.PacketsRetransmitted"/> across any Resume (ADR 0013: counters are never reset
/// by a Resume).
/// </summary>
/// <remarks>
/// Each round drops the next datagram in both directions and sends one reliable Message from each end,
/// then steps until both ends' retransmission counters rise, so the retransmission assertions cannot
/// hold vacuously. Each counter is sampled immediately before and after every Resume.
/// </remarks>
public class DatagramSessionThreeResumesCounterTests
{
    private const int Rounds = 3;
    private const int MaxProvokeSteps = 200;

    [Fact]
    public void Pump_ThreeInterruptResumeRoundsWithRetransmissionsBetween_ReportsThreeResumesAndNeverLowersByteOrRetransmissionCounts()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(Scenario.ResumableLinger);
        ulong diallerProvoked = 0;
        ulong accepterProvoked = 0;

        // Live traffic first, so each end has a round-trip estimate; without one a resend waits a whole Linger.
        for (int i = 0; i < 4 * (int)(Scenario.SendIdleInterval / Scenario.StepInterval); i++)
        {
            scenario.Step(Scenario.StepInterval);
        }

        for (int round = 1; round <= Rounds; round++)
        {
            // Act
            (ulong diallerRise, ulong accepterRise) = ProvokeRetransmissionsOnBothEnds(scenario);
            diallerProvoked += diallerRise;
            accepterProvoked += accepterRise;
            scenario.InterruptBothEnds();

            PeerCounters diallerBefore = scenario.Dialler.Reactor.CountersOf(scenario.DiallerPeer);
            PeerCounters accepterBefore = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer);
            scenario.ResumeThroughNewPort();
            PeerCounters diallerAfter = scenario.Dialler.Reactor.CountersOf(scenario.DiallerPeer);
            PeerCounters accepterAfter = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer);

            // Assert
            diallerRise.Should().BeGreaterThan(0, $"round {round} provoked a retransmission from the dialler");
            accepterRise.Should().BeGreaterThan(0, $"round {round} provoked a retransmission from the accepter");
            diallerAfter.Resumes.Should().Be((ulong)round, $"the dialler has resumed {round} time(s)");
            accepterAfter.Resumes.Should().Be((ulong)round, $"the accepter has resumed {round} time(s)");
            AssertNeverDecreased(diallerBefore, diallerAfter, $"dialler, round {round}");
            AssertNeverDecreased(accepterBefore, accepterAfter, $"accepter, round {round}");
        }

        PeerCounters diallerFinal = scenario.Dialler.Reactor.CountersOf(scenario.DiallerPeer);
        PeerCounters accepterFinal = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer);
        diallerFinal.Resumes.Should().Be(3);
        accepterFinal.Resumes.Should().Be(3);
        diallerFinal.PacketsRetransmitted.Should().BeGreaterThanOrEqualTo(diallerProvoked);
        accepterFinal.PacketsRetransmitted.Should().BeGreaterThanOrEqualTo(accepterProvoked);
    }

    private static (ulong Dialler, ulong Accepter) ProvokeRetransmissionsOnBothEnds(Scenario scenario)
    {
        ulong diallerStart = scenario.Dialler.Reactor.CountersOf(scenario.DiallerPeer).PacketsRetransmitted;
        ulong accepterStart = scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).PacketsRetransmitted;

        scenario.Proxy.DropNext(RelayDirection.ClientToUpstream);
        scenario.Proxy.DropNext(RelayDirection.UpstreamToClient);
        scenario.Dialler.Reactor.Send(scenario.DiallerPeer, DeliveryMode.ReliableOrdered, [1, 2, 3])
            .Should().Be(SendOutcome.Sent);
        scenario.Accepter.Reactor.Send(scenario.AccepterPeer, DeliveryMode.ReliableOrdered, [4, 5, 6])
            .Should().Be(SendOutcome.Sent);

        for (int i = 0; i < MaxProvokeSteps; i++)
        {
            scenario.Step(Scenario.StepInterval);
            if (scenario.Dialler.Reactor.CountersOf(scenario.DiallerPeer).PacketsRetransmitted > diallerStart
                && scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).PacketsRetransmitted > accepterStart)
            {
                break;
            }
        }

        // Let the resend land and be acknowledged, so retention is free again.
        for (int i = 0; i < 10; i++)
        {
            scenario.Step(Scenario.StepInterval);
        }

        return (
            scenario.Dialler.Reactor.CountersOf(scenario.DiallerPeer).PacketsRetransmitted - diallerStart,
            scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).PacketsRetransmitted - accepterStart);
    }

    private static void AssertNeverDecreased(PeerCounters before, PeerCounters after, string end)
    {
        after.BytesSent.Should().BeGreaterThanOrEqualTo(before.BytesSent, $"{end}: BytesSent");
        after.BytesReceived.Should().BeGreaterThanOrEqualTo(before.BytesReceived, $"{end}: BytesReceived");
        after.PacketsRetransmitted.Should().BeGreaterThanOrEqualTo(before.PacketsRetransmitted, $"{end}: PacketsRetransmitted");
    }
}
