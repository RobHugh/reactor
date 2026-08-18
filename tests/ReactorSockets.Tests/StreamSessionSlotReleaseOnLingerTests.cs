using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 4.4.1d (issue #141): nothing new decides when a reserved Slot's hold ends — it lasts exactly
/// as long as the Interrupted Peer holding it is held (#122, #121). These tests prove the release that
/// falls out of that actually reaches the admission path: with a <see cref="StreamSessionReactor"/> at
/// capacity and one Peer Interrupted, a fresh connection attempted just before the effective Linger
/// elapses is refused for capacity, the held Peer closes naming <see cref="DisconnectReason.LingerExpired"/>
/// once the effective Linger elapses (#121), and a fresh connection attempted after that closure is
/// admitted and becomes an established Peer. Every test drives a real <see cref="StreamSessionReactor"/>
/// through the public L2 surface only (tests/CLAUDE.md, ADR 0015), using the capacity-fill and
/// fresh-dial harness helpers (#138), and is driven entirely by
/// <see cref="StreamSessionReactorHarness.TimeProvider"/> — no test here sleeps to observe timer
/// behaviour. The effective Linger under test is the harness default of 1 second on both ends
/// (<see cref="StreamSessionReactorHarness"/>'s own <c>Linger</c> and
/// <see cref="StreamSessionTestRecords.BuildHello"/>'s default <c>lingerMs</c>), so the negotiated
/// minimum (ADR 0011) is 1 second everywhere below.
/// </summary>
public class StreamSessionSlotReleaseOnLingerTests
{
    private const byte RefusalReasonCapacity = 2;
    private static readonly TimeSpan _effectiveLinger = TimeSpan.FromSeconds(1);

    // =========================================================================================
    // Just before the effective Linger elapses, the reservation still holds the Slot: refused
    // =========================================================================================

    [Fact]
    public void AttemptFreshConnection_JustBeforeTheEffectiveLingerElapsesWithAnInterruptedPeerReservingTheLastSlot_IsRefusedForCapacity()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);

        try
        {
            // Act — one millisecond short of the effective Linger deadline.
            accepter.TimeProvider.Advance(_effectiveLinger - TimeSpan.FromMilliseconds(1));
            using StreamSessionReactorHarness.InboundAttemptResult outcome = accepter.AttemptFreshConnection();

            // Assert — the reservation still occupies the only Slot, so the fresh dial is refused.
            outcome.Admitted.Should().BeFalse();
            outcome.RefusalReason.Should().Be(RefusalReasonCapacity);
            outcome.RefusalCapacity.Should().Be((uint)accepter.Capacity);
        }
        finally
        {
            foreach (StreamSessionReactorHarness.HeldPeer heldPeer in held)
            {
                heldPeer.Counterparty.Dispose();
            }
        }
    }

    // =========================================================================================
    // The effective Linger elapsing closes the held Peer, naming Linger expiry — #121, reproduced
    // here at capacity to establish the precondition the release test depends on
    // =========================================================================================

    [Fact]
    public void Pump_AfterTheEffectiveLingerElapsesOnAnInterruptedPeerReservingTheLastSlot_ClosesItNamingLingerExpired()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        PeerId interruptedPeer = held[0].Peer;
        accepter.InterruptHeldPeer(held[0]);

        try
        {
            // Act — the full effective Linger elapses.
            accepter.TimeProvider.Advance(_effectiveLinger);
            accepter.Reactor.Pump();

            // Assert
            accepter.DrainToList().Should().ContainSingle(
                e => e.Kind == SessionEventKind.PeerClosed && e.Reason == DisconnectReason.LingerExpired
                    && e.Peer == interruptedPeer);
        }
        finally
        {
            foreach (StreamSessionReactorHarness.HeldPeer heldPeer in held)
            {
                heldPeer.Counterparty.Dispose();
            }
        }
    }

    // =========================================================================================
    // Once Linger expiry closes the held Peer, the Slot it reserved is usable again: a fresh
    // connection attempted afterwards is admitted rather than refused
    // =========================================================================================

    [Fact]
    public void AttemptFreshConnection_AfterLingerExpiryClosesTheInterruptedPeerReservingTheLastSlot_IsAdmittedAndBecomesAnEstablishedPeer()
    {
        // Arrange — an Interrupted Peer reserving the last Slot, then the effective Linger elapses,
        // releasing the reservation (#121 closes the Peer). Guard that the release precondition this
        // test depends on actually took effect before driving the fresh connection under test.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        PeerId interruptedPeer = held[0].Peer;
        accepter.InterruptHeldPeer(held[0]);
        accepter.TimeProvider.Advance(_effectiveLinger);
        accepter.Reactor.Pump();
        accepter.DrainToList().Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerClosed && e.Reason == DisconnectReason.LingerExpired
                && e.Peer == interruptedPeer);

        try
        {
            // Act — a fresh connection attempted after the release.
            using StreamSessionReactorHarness.InboundAttemptResult outcome = accepter.AttemptFreshConnection();

            // Assert — admitted, not refused, and now a distinct established Peer occupying the freed Slot.
            outcome.Admitted.Should().BeTrue();
            outcome.Peer.Should().NotBeNull();
            outcome.Peer.Should().NotBe(interruptedPeer);
        }
        finally
        {
            foreach (StreamSessionReactorHarness.HeldPeer heldPeer in held)
            {
                heldPeer.Counterparty.Dispose();
            }
        }
    }
}
