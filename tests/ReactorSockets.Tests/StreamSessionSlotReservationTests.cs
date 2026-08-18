using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 4.4.1b (issue #139): an Interrupted Peer keeps reserving its Slot for the whole of Linger
/// (issue #121), so a busy Reactor never gives away the state a reconnection is coming back for
/// (ADR 0010). No production change lands with this ticket — a held Peer already occupies its Slot
/// until <c>ClosePeer</c> runs at Linger expiry, and both <c>AdmitOrRefuse</c> and its predecessor
/// gate read the same <c>_peers.HasFreeSlot</c> regardless of whether the Peer holding a Slot is
/// established or Interrupted. Every test here proves that reservation through the public L2 surface
/// only (tests/CLAUDE.md, ADR 0015), using the capacity-fill and fresh-dial harness helpers issue #138
/// added. Mirrors <see cref="StreamSessionHandshakeGuardTests"/>'s
/// <c>Connect_EveryCapacitySlotOccupied_IsRefusedWithReasonCapacityAndClosedCleanlyRatherThanReset</c>,
/// the house pattern for a capacity refusal, and <see cref="StreamSessionInterruptedTests"/> for how
/// an interruption is arranged.
/// </summary>
public class StreamSessionSlotReservationTests
{
    private const byte RefusalReasonCapacity = 2;

    // =========================================================================================
    // A fresh dial while an Interrupted Peer holds its Slot is refused, not handed the Slot
    // =========================================================================================

    [Fact]
    public void Connect_WhileAnInterruptedPeerHoldsItsSlot_IsRefusedWithReasonCapacityAndClosedCleanlyRatherThanReset()
    {
        // Arrange — every Slot occupied by a genuine established Peer (issue #138), then one is
        // interrupted by a reset rather than closed with a Goodbye (ADR 0010) — it keeps its Slot.
        using StreamSessionReactorHarness accepter = new(capacity: 2);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);

        // Act — a fresh connection arrives while the Interrupted Peer is still held, well inside Linger.
        using StreamSessionReactorHarness.InboundAttemptResult outcome = accepter.AttemptFreshConnection();

        // Assert — refused with the capacity reason naming this Reactor's capacity, never handed the
        // Interrupted Peer's Slot.
        outcome.Admitted.Should().BeFalse();
        outcome.Peer.Should().BeNull();
        outcome.RefusalReason.Should().Be(RefusalReasonCapacity);
        outcome.RefusalCapacity.Should().Be((uint)accepter.Capacity);

        // Assert — closed cleanly by the Reactor: a further read reports a graceful end-of-stream,
        // never a reset and never left open.
        Action act = () => outcome.Counterparty.ExpectGracefulClose();
        act.Should().NotThrow<SocketException>();

        held[1].Counterparty.Dispose();
    }

    // =========================================================================================
    // The Refusal is indistinguishable from the one every Slot holding an established Peer produces
    // =========================================================================================

    [Fact]
    public void Connect_WhileAnInterruptedPeerHoldsItsSlot_NamesTheSameCapacityFigureAsWhenEverySlotHoldsAnEstablishedPeer()
    {
        // Arrange — two Reactors of identical capacity: one with every Slot held by an established
        // Peer, the other with one of those Peers interrupted instead.
        using StreamSessionReactorHarness allEstablished = new(capacity: 2);
        List<StreamSessionReactorHarness.HeldPeer> establishedHeld = allEstablished.FillToCapacity();

        using StreamSessionReactorHarness oneInterrupted = new(capacity: 2);
        List<StreamSessionReactorHarness.HeldPeer> interruptedHeld = oneInterrupted.FillToCapacity();
        oneInterrupted.InterruptHeldPeer(interruptedHeld[0]);

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult establishedOutcome = allEstablished.AttemptFreshConnection();
        using StreamSessionReactorHarness.InboundAttemptResult interruptedOutcome = oneInterrupted.AttemptFreshConnection();

        // Assert — the dialler learns "full", not "gone": the same reason and the same capacity figure
        // either way, so a fresh dial cannot tell a held Interrupted Peer's Slot apart from an ordinary
        // full house.
        interruptedOutcome.Admitted.Should().BeFalse();
        interruptedOutcome.RefusalReason.Should().Be(establishedOutcome.RefusalReason);
        interruptedOutcome.RefusalCapacity.Should().Be(establishedOutcome.RefusalCapacity);

        foreach (StreamSessionReactorHarness.HeldPeer heldPeer in establishedHeld)
        {
            heldPeer.Counterparty.Dispose();
        }

        foreach (StreamSessionReactorHarness.HeldPeer heldPeer in interruptedHeld)
        {
            heldPeer.Counterparty.Dispose();
        }
    }

    // =========================================================================================
    // A reservation-driven refusal touches neither admission counter (issue #107, reading (b))
    // =========================================================================================

    [Fact]
    public void Connect_RefusedBecauseAnInterruptedPeerReservesTheLastSlot_TouchesNeitherAdmissionCounter()
    {
        // Arrange — the only Slot occupied, then the Peer holding it is interrupted, so the Slot is
        // reserved for the length of Linger rather than free.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);

        ulong deferralsBefore = accepter.Reactor.Counters.AdmissionDeferrals;
        ulong discardsBefore = accepter.Reactor.Counters.AdmissionDiscards;

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult outcome = accepter.AttemptFreshConnection();

        // Assert — the refusal happened, and the reservation is what caused it.
        outcome.Admitted.Should().BeFalse();
        outcome.RefusalReason.Should().Be(RefusalReasonCapacity);

        // Assert — a reserved Slot changes nothing about how the refusal is counted. Issue #107 settled
        // that a stream-profile capacity refusal is neither a Deferral (work left where a later Pump can
        // take it) nor a Discard (work the Reactor was holding and let go): it is answered on the wire
        // and the connection ends, so nothing waits and nothing is silently dropped. A reservation-driven
        // refusal takes that same RefuseForCapacity path and inherits the same rule. What an operator
        // reads for it is the PeerRefused Diagnostic, which names the remote, not a counter tick.
        accepter.Reactor.Counters.AdmissionDeferrals.Should().Be(deferralsBefore);
        accepter.Reactor.Counters.AdmissionDiscards.Should().Be(discardsBefore);
    }
}
