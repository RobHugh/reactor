using FluentAssertions;
using Xunit;
using Scenario = ReactorSockets.Tests.DatagramSessionResumeScenario;

namespace ReactorSockets.Tests;

/// <summary>
/// An Interrupted accepted datagram Peer leaves Interrupted when it receives a well-formed data
/// Datagram for its Session from exactly the address it already sends to, without waiting for a
/// Resume the dialling end has no reason to send. A data Datagram from the same IP on another port
/// does not resume it.
/// </summary>
/// <remarks>
/// Drives a real <see cref="DatagramSessionReactor"/> pair through a <see cref="LossyProxy"/> on a shared
/// <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/>. Every Pump is drained through the
/// scenario's own helpers, because a Pump clears any still-undrained Events when it starts.
/// </remarks>
public class DatagramSessionInterruptedAccepterResumeOnDataTests
{
    private static readonly TimeSpan _linger = TimeSpan.FromSeconds(10);

    /// <summary>Past the original Linger expiry, 10 s after the interruption.</summary>
    private static readonly TimeSpan _observationWindow = TimeSpan.FromSeconds(12);

    private const int MaxStallSteps = 100;

    [Fact]
    public void Pump_DiallerStallsPastAccepterDeadline_AccepterRaisesInterruptedThenResumed()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(linger: _linger);
        SeedRoundTrip(scenario);

        // Act
        StallDiallerUntilAccepterInterrupted(scenario);
        StepFor(scenario, _observationWindow);

        // Assert
        scenario.AccepterEvents
            .Where(e => e.Kind is SessionEventKind.PeerInterrupted or SessionEventKind.PeerResumed)
            .Select(e => (e.Kind, e.Peer))
            .Should().Equal(
                (SessionEventKind.PeerInterrupted, scenario.AccepterPeer),
                (SessionEventKind.PeerResumed, scenario.AccepterPeer));
    }

    [Fact]
    public void Pump_DiallerStallsPastAccepterDeadline_AccepterNeverCloses()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(linger: _linger);
        SeedRoundTrip(scenario);

        // Act
        StallDiallerUntilAccepterInterrupted(scenario);
        StepFor(scenario, _observationWindow);

        // Assert
        scenario.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    [Fact]
    public void Pump_DiallerStallsPastAccepterDeadline_DeliversEveryMessageOnceInOrder()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(linger: _linger);
        SeedRoundTrip(scenario);
        List<byte[]> sent = [];

        // Act
        SendReliable(scenario, sent);
        scenario.Step(Scenario.StepInterval);
        StallDiallerUntilAccepterInterrupted(scenario);
        SendReliable(scenario, sent);
        for (int i = 0; i < 10; i++)
        {
            StepFor(scenario, Scenario.SendIdleInterval);
            SendReliable(scenario, sent);
        }

        StepFor(scenario, _observationWindow);

        // Assert
        scenario.AccepterPayloads.Should().HaveCount(sent.Count).And.Equal(sent, (a, b) => a.AsSpan().SequenceEqual(b));
    }

    [Fact]
    public void Pump_DiallerStallsPastAccepterDeadline_CountsOneResumeAndLogsResumeMatched()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(linger: _linger);
        SeedRoundTrip(scenario);

        // Act
        StallDiallerUntilAccepterInterrupted(scenario);
        StepFor(scenario, _observationWindow);

        // Assert
        scenario.Accepter.Reactor.CountersOf(scenario.AccepterPeer).Resumes.Should().Be(1);
        DiagnosticRecord record = scenario.Accepter.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.ResumeMatched).Which;
        record.Value0.Should().Be(0);
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Peer);
        record.Peer.Should().Be(scenario.AccepterPeer);
    }

    [Fact]
    public void Pump_DiallerToAccepterLossOnly_AccepterResumesWhenLossEnds()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(linger: _linger);
        SeedRoundTrip(scenario);
        scenario.Proxy.SetPayloadDropRule(RelayDirection.ClientToUpstream, _ => true);
        for (int i = 0; scenario.AccepterCount(SessionEventKind.PeerInterrupted) == 0; i++)
        {
            i.Should().BeLessThan(MaxStallSteps, "the accepter must raise PeerInterrupted");
            scenario.Step(Scenario.StepInterval);
        }

        // Act
        scenario.Proxy.SetPayloadDropRule(RelayDirection.ClientToUpstream, null);
        StepFor(scenario, _observationWindow);

        // Assert
        scenario.AccepterCount(SessionEventKind.PeerResumed).Should().Be(1);
        scenario.AccepterCount(SessionEventKind.PeerClosed).Should().Be(0);
        scenario.DiallerCount(SessionEventKind.PeerInterrupted).Should().Be(0);
    }

    [Fact]
    public void Pump_DataFromANewPortWithoutAResume_DoesNotResumeTheAccepter()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(linger: _linger);
        SeedRoundTrip(scenario);
        StallDiallerUntilAccepterInterrupted(scenario);
        scenario.Proxy.MoveUpstreamFacingSocket();
        scenario.Proxy.SetPayloadDropRule(RelayDirection.ClientToUpstream, Scenario.IsResume);

        // Act
        StepFor(scenario, _observationWindow);

        // Assert
        scenario.AccepterCount(SessionEventKind.PeerResumed).Should().Be(0);
        scenario.AccepterEvents.Where(e => e.Kind == SessionEventKind.PeerClosed)
            .Should().ContainSingle().Which.Reason.Should().Be(DisconnectReason.LingerExpired);
    }

    [Fact]
    public void Pump_DataFromANewPortThenAResume_ResumesTheAccepter()
    {
        // Arrange
        using Scenario scenario = Scenario.Establish(linger: _linger);
        SeedRoundTrip(scenario);
        StallDiallerUntilAccepterInterrupted(scenario);
        scenario.Proxy.MoveUpstreamFacingSocket();
        scenario.Proxy.SetPayloadDropRule(RelayDirection.ClientToUpstream, Scenario.IsResume);

        // Act
        StepFor(scenario, TimeSpan.FromSeconds(2));
        scenario.Proxy.SetPayloadDropRule(RelayDirection.ClientToUpstream, null);
        StepFor(scenario, _observationWindow);

        // Assert
        scenario.AccepterCount(SessionEventKind.PeerResumed).Should().Be(1);
        scenario.AccepterCount(SessionEventKind.PeerClosed).Should().Be(0);
    }

    /// <summary>Runs the live path for a few send-idle intervals so each end has a round-trip estimate.</summary>
    private static void SeedRoundTrip(Scenario scenario)
    {
        for (int i = 0; i < 4 * (int)(Scenario.SendIdleInterval / Scenario.StepInterval); i++)
        {
            scenario.Step(Scenario.StepInterval);
        }
    }

    /// <summary>
    /// Advances the clock and Pumps only the accepter, with relaying on in both directions, until the accepter
    /// raises <see cref="SessionEventKind.PeerInterrupted"/>. The accepter's Pings queue in the dialler's socket.
    /// </summary>
    private static void StallDiallerUntilAccepterInterrupted(Scenario scenario)
    {
        for (int i = 0; scenario.AccepterCount(SessionEventKind.PeerInterrupted) != 1; i++)
        {
            i.Should().BeLessThan(MaxStallSteps, "the accepter must raise PeerInterrupted");
            scenario.Clock.Advance(Scenario.StepInterval);
            Scenario.PumpEndThenProxy(scenario.Proxy, RelayDirection.UpstreamToClient, scenario.Accepter, scenario.PumpAccepter);
        }
    }

    /// <summary>Steps the whole path in <see cref="Scenario.StepInterval"/> steps until <paramref name="duration"/> has passed.</summary>
    private static void StepFor(Scenario scenario, TimeSpan duration)
    {
        for (int i = 0; i < (int)(duration / Scenario.StepInterval); i++)
        {
            scenario.Step(Scenario.StepInterval);
        }
    }

    /// <summary>Sends one new <see cref="DeliveryMode.ReliableOrdered"/> Message from the dialler, asserts it was accepted and records its payload.</summary>
    private static void SendReliable(Scenario scenario, List<byte[]> sent)
    {
        byte[] payload = [(byte)sent.Count, 0xA5, (byte)(sent.Count * 3)];
        scenario.Dialler.Reactor.Send(scenario.DiallerPeer, DeliveryMode.ReliableOrdered, payload).Should().Be(SendOutcome.Sent);
        sent.Add(payload);
    }
}
