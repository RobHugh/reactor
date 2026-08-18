using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 4.4.1f (issue #143): the capacity reservation issue #122 gave the listening side to an
/// <see cref="SessionEventKind.PeerInterrupted"/> Peer is symmetric with the local dialling side — a
/// <see cref="StreamSessionReactor.Connect"/> call takes a capacity Slot at the moment it is made, so a
/// Slot a held Peer still occupies must not be available to it either. Where a fresh inbound connection
/// is refused in-protocol (there is a far end to tell), a local <c>Connect</c> fails at the call itself
/// (the caller is right there) — the consumer-visible outcome differs, but the accounting underneath,
/// <c>HasFreeSlot</c>, is the same guard the listening side already reads. Every test here drives a real
/// <see cref="StreamSessionReactor"/> through the public L2 surface only (tests/CLAUDE.md, ADR 0015),
/// using <see cref="StreamSessionReactorHarness"/>'s capacity-fill and fresh-dial helpers (issue #138),
/// driven entirely by <see cref="StreamSessionReactorHarness.TimeProvider"/> — no test here sleeps.
/// </summary>
public class StreamSessionSlotReservationDialTests
{
    private static readonly TimeSpan _effectiveLinger = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Kills <paramref name="held"/>'s transport abruptly and pumps <paramref name="reactor"/> until its
    /// <see cref="SessionEventKind.PeerInterrupted"/> drains, leaving the reservation under test in
    /// place — a held Peer occupying a Slot that a subsequent local <c>Connect</c> must not be able to
    /// take.
    /// </summary>
    private static void InterruptHeldPeer(StreamSessionReactorHarness reactor, StreamSessionReactorHarness.HeldPeer held)
    {
        held.Counterparty.ResetAbruptly();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerInterrupted); i++)
        {
            reactor.Reactor.Pump();
            drained.AddRange(reactor.DrainToList());
        }

        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == held.Peer);
    }

    // =========================================================================================
    // A local Connect fails naming capacity rather than taking the held Peer's reserved Slot
    // =========================================================================================

    [Fact]
    public void Connect_WhileCapacityIsFullyOccupiedByAnInterruptedPeer_ThrowsNamingCapacityRatherThanTakingTheHeldSlot()
    {
        // Arrange
        using StreamSessionReactorHarness reactor = new();
        using StreamSessionReactorHarness target = new();
        List<StreamSessionReactorHarness.HeldPeer> held = reactor.FillToCapacity();
        InterruptHeldPeer(reactor, held[0]);

        // Act
        Action act = () => reactor.Reactor.Connect(target.Endpoint);

        // Assert — the same message Connect already throws when genuinely full (see
        // StreamSessionReactor.Connect), naming this Reactor's capacity figure.
        act.Should().Throw<InvalidOperationException>()
            .WithMessage($"*capacity of {reactor.Capacity}*");
    }

    // =========================================================================================
    // The held Peer survives that failed Connect untouched — same PeerId, still Interrupted
    // =========================================================================================

    [Fact]
    public void Connect_AfterFailingBecauseAnInterruptedPeerHoldsTheLastSlot_LeavesThatPeerInterruptedAndUnchanged()
    {
        // Arrange
        using StreamSessionReactorHarness reactor = new();
        using StreamSessionReactorHarness target = new();
        List<StreamSessionReactorHarness.HeldPeer> held = reactor.FillToCapacity();
        InterruptHeldPeer(reactor, held[0]);

        // Act
        Action act = () => reactor.Reactor.Connect(target.Endpoint);
        act.Should().Throw<InvalidOperationException>();

        // Assert — the same PeerId is still readable (Interrupted, not closed) and no PeerClosed
        // has been raised for it by the failed Connect attempt.
        Action stillReadable = () => reactor.Reactor.CountersOf(held[0].Peer);
        stillReadable.Should().NotThrow("the held Peer is still Interrupted, untouched by the refused Connect");
        reactor.PumpAndDrainAll(3).Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    // =========================================================================================
    // Once the effective Linger elapses and the held Peer closes, the same Connect succeeds
    // =========================================================================================

    [Fact]
    public void Connect_AfterTheEffectiveLingerClosesTheInterruptedPeerHoldingTheLastSlot_Succeeds()
    {
        // Arrange
        using StreamSessionReactorHarness reactor = new();
        using StreamSessionReactorHarness target = new();
        List<StreamSessionReactorHarness.HeldPeer> held = reactor.FillToCapacity();
        InterruptHeldPeer(reactor, held[0]);

        Action failingConnect = () => reactor.Reactor.Connect(target.Endpoint);
        failingConnect.Should().Throw<InvalidOperationException>();

        // Act — the effective Linger elapses, closing the held Peer and freeing its Slot.
        reactor.TimeProvider.Advance(_effectiveLinger);
        reactor.Reactor.Pump();
        reactor.DrainToList().Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerClosed && e.Reason == DisconnectReason.LingerExpired
                && e.Peer == held[0].Peer);

        // Act — the same dial, retried now that the Slot is free.
        PeerId dialled = reactor.Reactor.Connect(target.Endpoint);
        (List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> dialerEvents, _) =
            StreamSessionReactorHarness.PumpBothUntil(reactor, target, () => false, maxRounds: 10);

        // Assert
        dialerEvents.Should().ContainSingle()
            .Which.Should().BeEquivalentTo((SessionEventKind.PeerConnected, dialled, default(DisconnectReason)));
    }
}
