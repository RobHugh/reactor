using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #367 (parent #55): an Interrupted datagram Peer whose effective Linger
/// (<c>PeerSlot.EffectiveLinger</c>, issue #363's negotiated minimum) has elapsed since
/// <c>PeerSlot.InterruptedTimestamp</c> (issue #366) closes with <see cref="DisconnectReason.LingerExpired"/>.
/// </summary>
/// <remarks>
/// Drives a real <see cref="DatagramSessionReactor"/> pair through a <see cref="LossyProxy"/> on one
/// shared <see cref="FakeTimeProvider"/> (both harnesses and the proxy are constructed against the same
/// clock instance) — needed here, unlike <c>DatagramSessionInterruptedPeerPingsTests</c> and
/// <c>DatagramSessionInterruptedPeerIdleTests</c>, because the first two tests below assert on the
/// <em>accepting</em> side, whose own locally configured Linger deliberately disagrees with the
/// dialling side's: if the production code read <c>_options.Linger</c> (the accepter's own, longer,
/// value) instead of <c>slot.EffectiveLinger</c> (the negotiated minimum), the accepter would still be
/// holding the Peer open at the instants these tests check, and the "just outside" assertion would fail.
/// </remarks>
public class DatagramSessionLingerExpiryTests
{
    private static readonly TimeSpan _sendIdleInterval = TimeSpan.FromSeconds(1);

    private const int MissedHeartbeats = 3;

    private static readonly TimeSpan _receiveIdleDeadline = _sendIdleInterval * MissedHeartbeats;

    /// <summary>The dialling end's own configured Linger — the shorter of the two, and so the effective (negotiated) value.</summary>
    private static readonly TimeSpan _shorterLinger = TimeSpan.FromSeconds(5);

    /// <summary>The accepting end's own configured Linger — the longer of the two; a wrong (local) read of it, instead of the negotiated minimum, would hold the Peer open well past every boundary this file checks.</summary>
    private static readonly TimeSpan _longerLinger = TimeSpan.FromSeconds(8);

    [Fact]
    public void Pump_JustInsideTheEffectiveLingerAfterInterruption_ProducesNoPeerClosed()
    {
        // Arrange
        (DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, LossyProxy proxy) =
            BuildInterruptedAccepterScenario(
                out List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> allAccepterEvents);
        using (dialler)
        using (accepter)
        using (proxy)
        {
            // Act — one millisecond short of the effective Linger, measured from the Pump that stamped
            // InterruptedTimestamp.
            DatagramSessionReactorHarness.SettleThenAdvance(
                accepter, _shorterLinger - TimeSpan.FromMilliseconds(1));
            allAccepterEvents.AddRange(accepter.PumpAndDrainAll(1));

            // Assert — held open across every drain so far, including the interruption Pump's own drain.
            allAccepterEvents.Should().NotContain(
                e => e.Kind == SessionEventKind.PeerClosed,
                "the effective Linger — the negotiated minimum of 5s, not the accepter's own 8s — has not yet elapsed");
        }
    }

    [Fact]
    public void Pump_JustOutsideTheEffectiveLingerAfterInterruption_ProducesPeerClosedWithLingerExpired()
    {
        // Arrange
        (DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, LossyProxy proxy) =
            BuildInterruptedAccepterScenario(
                out List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> allAccepterEvents);
        using (dialler)
        using (accepter)
        using (proxy)
        {
            // Act — one millisecond short of the effective Linger, then the final millisecond that
            // crosses it, on the same clock the interruption Pump stamped InterruptedTimestamp from.
            DatagramSessionReactorHarness.SettleThenAdvance(
                accepter, _shorterLinger - TimeSpan.FromMilliseconds(1));
            allAccepterEvents.AddRange(accepter.PumpAndDrainAll(1));

            allAccepterEvents.Should().NotContain(
                e => e.Kind == SessionEventKind.PeerClosed,
                "one millisecond still short of the effective Linger");

            accepter.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
            allAccepterEvents.AddRange(accepter.PumpAndDrainAll(1));

            // Assert — closed at the instant the effective (negotiated minimum) Linger elapsed, naming
            // LingerExpired, even though the accepter's own configured Linger is 8s.
            allAccepterEvents.Should().ContainSingle(
                e => e.Kind == SessionEventKind.PeerClosed,
                "the effective Linger of 5s — the negotiated minimum, not the accepter's own 8s — has now elapsed")
                .Which.Reason.Should().Be(DisconnectReason.LingerExpired);
        }
    }

    [Fact]
    public void Pump_PeerNeverInterrupted_TrafficPastItsLingerDoesNotCloseIt()
    {
        // Arrange — a short Linger, well under the receive-idle deadline, and the proxy left relaying
        // normally throughout: every Ping either end queues keeps reaching the other, so neither side's
        // receive-idle deadline is ever crossed and this Peer is never Interrupted.
        FakeTimeProvider clock = new();
        TimeSpan linger = TimeSpan.FromMilliseconds(50);

        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false,
            sendIdleInterval: _sendIdleInterval,
            linger: linger,
            missedHeartbeats: MissedHeartbeats,
            timeProvider: clock);
        using DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: _sendIdleInterval,
            linger: linger,
            missedHeartbeats: MissedHeartbeats,
            timeProvider: clock);
        using LossyProxy proxy = new(accepter.Endpoint, clock);

        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> allDiallerEvents = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> allAccepterEvents = [];

        // Act — advance well past the 50 ms Linger, many times over, while traffic (Pings, at minimum)
        // keeps flowing both ways every send-idle interval — far longer than the receive-idle deadline
        // this Peer would need to cross to ever become Interrupted in the first place.
        for (int round = 0; round < 20; round++)
        {
            clock.Advance(_sendIdleInterval);
            dialler.Reactor.Pump();
            allDiallerEvents.AddRange(dialler.DrainToList());
            proxy.Pump();
            accepter.Reactor.Pump();
            allAccepterEvents.AddRange(accepter.DrainToList());
            proxy.Pump();
        }

        // Assert — 20 * 1s of elapsed time is 400x the 50 ms Linger, yet neither end ever interrupted or
        // closed this Peer: Linger only ever governs an already-Interrupted Peer, never one that keeps
        // being heard from.
        allDiallerEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerInterrupted || e.Kind == SessionEventKind.PeerClosed);
        allAccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerInterrupted || e.Kind == SessionEventKind.PeerClosed);

        // Assert — sanity: the Peer is still open and countable at the end of the run, confirming the
        // 20 rounds above actually drove a live Session rather than one that silently died some other
        // way this file's two event-based assertions would not have caught.
        Action stillOpen = () => dialler.Reactor.CountersOf(peer);
        stillOpen.Should().NotThrow("the Peer must still be open for Linger to have had nothing to close");
    }

    /// <summary>
    /// Establishes a dialler (Linger <see cref="_shorterLinger"/>) and an accepter (Linger
    /// <see cref="_longerLinger"/>) through a <see cref="LossyProxy"/> on one shared clock, cuts the path
    /// in both directions, and pumps the accepter until it drains exactly one
    /// <see cref="SessionEventKind.PeerInterrupted"/> — the accepter's effective (negotiated) Linger is
    /// then <see cref="_shorterLinger"/> (the dialler's own, smaller, announced value), even though the
    /// accepter's own locally configured Linger is <see cref="_longerLinger"/>. Returns the three
    /// disposable fixtures (still owned by the caller) and hands back the accumulated list of every event
    /// the accepter has drained so far, including the interruption Pump's own drain, so a caller's
    /// "no PeerClosed" assertion covers the whole run rather than only its own later drains.
    /// </summary>
    private static (DatagramSessionReactorHarness Dialler, DatagramSessionReactorHarness Accepter, LossyProxy Proxy)
        BuildInterruptedAccepterScenario(out List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> allAccepterEvents)
    {
        FakeTimeProvider clock = new();

        DatagramSessionReactorHarness dialler = new(
            bindLoopback: false,
            sendIdleInterval: _sendIdleInterval,
            linger: _shorterLinger,
            missedHeartbeats: MissedHeartbeats,
            timeProvider: clock);
        DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: _sendIdleInterval,
            linger: _longerLinger,
            missedHeartbeats: MissedHeartbeats,
            timeProvider: clock);
        LossyProxy proxy = new(accepter.Endpoint, clock);

        dialler.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        // Act — cut the path in both directions so the accepter stops hearing from the dialler.
        proxy.StopRelayingBothDirections();

        DatagramSessionReactorHarness.SettleThenAdvance(accepter, _receiveIdleDeadline);
        allAccepterEvents = accepter.PumpAndDrainAll(1);

        allAccepterEvents.Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerInterrupted,
            "the accepter's own receive-idle deadline was crossed once the path was cut");

        return (dialler, accepter, proxy);
    }
}
