using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 4.4.1c (issue #140): a capacity refusal caused by an Interrupted Peer's reserved Slot
/// (slice 4.4.1, issue #122) must never disturb the Peer whose reservation caused it. The refusal
/// path (<c>StreamSessionReactor.RefuseForCapacity</c>) tracks a refused connection outside the Peer
/// table entirely — it never becomes a Peer at all — so the held Peer keeps its <see cref="PeerId"/>
/// (generation included), its counters keep answering unreset, and it stays Interrupted rather than
/// closed or resumed. This ticket pins that separation as observable behaviour through the public L2
/// surface only (tests/CLAUDE.md, ADR 0015), using the capacity-fill and fresh-dial harness helpers
/// issue #138 added to <see cref="StreamSessionReactorHarness"/>.
/// </summary>
public class StreamSessionSlotReservationHeldPeerTests
{
    /// <summary>The negotiated minimum Linger (ADR 0011) under the harness's and Hello's matching 1-second defaults.</summary>
    private static readonly TimeSpan _effectiveLinger = TimeSpan.FromSeconds(1);

    // =========================================================================================
    // A capacity refusal raises no event against the held Peer, and does not close it
    // =========================================================================================

    [Fact]
    public void AttemptFreshConnection_RefusedByAnInterruptedPeersReservation_RaisesNoEventAgainstTheHeldPeer()
    {
        // Arrange — one Slot, occupied then interrupted, so its reservation is the only thing capacity
        // has left to refuse against.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        PeerId id = held[0].Peer;
        accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult outcome = accepter.AttemptFreshConnection();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();

        // Assert — refused for capacity, and the refusal itself raised no further event naming the held
        // Peer: no second Interrupted, and — critically — no Closed.
        outcome.Admitted.Should().BeFalse();
        drained.Should().NotContain(e => e.Peer == id);

        held[0].Counterparty.Dispose();
    }

    // =========================================================================================
    // The held Peer's identity, generation included, is unchanged by the refusal
    // =========================================================================================

    [Fact]
    public void AttemptFreshConnection_RefusedByAnInterruptedPeersReservation_LeavesTheHeldPeersIdUnchanged()
    {
        // Arrange — capture the id the Session established under before the interruption, so the
        // assertion below is against the original generation rather than a value merely equal to it.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        PeerId originalId = held[0].Peer;
        accepter.InterruptHeldPeer(held[0]);

        // Act — refuse a fresh connection against the reserved Slot.
        using StreamSessionReactorHarness.InboundAttemptResult outcome = accepter.AttemptFreshConnection();

        // Assert — CountersOf still resolves the original PeerId. CountersOf throws on a stale
        // generation (StreamSessionReactor.CountersOf), so this failing to throw is itself proof the
        // refusal minted no new generation for the Slot and the id is exactly as it was.
        outcome.Admitted.Should().BeFalse();
        Action resolvesOriginalId = () => accepter.Reactor.CountersOf(originalId);
        resolvesOriginalId.Should().NotThrow();

        held[0].Counterparty.Dispose();
    }

    // =========================================================================================
    // The held Peer's counters stay readable, non-zero, and unreset by the refusal
    // =========================================================================================

    [Fact]
    public void AttemptFreshConnection_RefusedByAnInterruptedPeersReservation_LeavesTheHeldPeersCountersNonZeroAndUnreset()
    {
        // Arrange — one Message delivered before the interruption, so MessagesReceived is provably
        // non-zero going into the refusal.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        byte[] message = StreamSessionTestRecords.PayloadOf(16, 7);
        held[0].Counterparty.Send(StreamSessionTestRecords.BuildFrame(message));
        accepter.PumpUntil(() => accepter.Reactor.CountersOf(held[0].Peer).MessagesReceived == 1);
        accepter.DrainToList();
        PeerId id = held[0].Peer;
        accepter.InterruptHeldPeer(held[0]);
        ulong messagesReceivedBeforeRefusal = accepter.Reactor.CountersOf(id).MessagesReceived;

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult outcome = accepter.AttemptFreshConnection();

        // Assert — the count carried into the refusal survives it unreset (ADR 0013: a counter's
        // lifetime is the Peer's, not the transport's — and this refusal never touched the Peer at all).
        outcome.Admitted.Should().BeFalse();
        messagesReceivedBeforeRefusal.Should().Be(1);
        accepter.Reactor.CountersOf(id).MessagesReceived.Should().Be(messagesReceivedBeforeRefusal);

        held[0].Counterparty.Dispose();
    }

    // =========================================================================================
    // The held Peer stays Interrupted — never closed — across a capacity refusal
    // =========================================================================================

    [Fact]
    public void AttemptFreshConnection_RefusedByAnInterruptedPeersReservation_NeverClosesTheHeldPeer()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        PeerId id = held[0].Peer;
        accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult outcome = accepter.AttemptFreshConnection();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();

        // Assert — refused for capacity, and CountersOf still answers for the held Peer's id, which it
        // would not once that Peer had actually closed (see StreamSessionInterruptedTests, ADR 0013).
        outcome.Admitted.Should().BeFalse();
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed && e.Peer == id);
        Action stillAnswers = () => accepter.Reactor.CountersOf(id);
        stillAnswers.Should().NotThrow();

        held[0].Counterparty.Dispose();
    }

    // =========================================================================================
    // Several refusals served in succession leave the held Peer equally untouched
    // =========================================================================================

    [Fact]
    public void AttemptFreshConnection_ServedSeveralTimesInSuccession_LeavesTheHeldPeerEquallyUntouchedEachTime()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        PeerId id = held[0].Peer;
        accepter.InterruptHeldPeer(held[0]);
        ulong messagesReceivedBeforeRefusals = accepter.Reactor.CountersOf(id).MessagesReceived;

        // Act — three fresh connections in a row, each refused against the same reserved Slot; collect
        // each attempt's outcome, drained events, and the held Peer's counters as they stand right after.
        List<bool> admittedPerAttempt = [];
        List<List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)>> drainedPerAttempt = [];
        List<ulong> messagesReceivedPerAttempt = [];
        for (int attempt = 0; attempt < 3; attempt++)
        {
            using StreamSessionReactorHarness.InboundAttemptResult outcome = accepter.AttemptFreshConnection();
            admittedPerAttempt.Add(outcome.Admitted);
            drainedPerAttempt.Add(accepter.DrainToList());
            messagesReceivedPerAttempt.Add(accepter.Reactor.CountersOf(id).MessagesReceived);
        }

        // Assert — every one of the three refusals left the held Peer exactly as it was: refused for
        // capacity, no event raised against it, and its counters undisturbed.
        admittedPerAttempt.Should().AllSatisfy(admitted => admitted.Should().BeFalse());
        drainedPerAttempt.Should().AllSatisfy(drained => drained.Should().NotContain(e => e.Peer == id));
        messagesReceivedPerAttempt.Should().AllSatisfy(
            messagesReceived => messagesReceived.Should().Be(messagesReceivedBeforeRefusals));

        held[0].Counterparty.Dispose();
    }

    // =========================================================================================
    // A capacity refusal leaves the held Peer's Linger deadline intact — it still closes naming
    // Linger expiry rather than staying open forever or having already been torn down
    // =========================================================================================

    /// <summary>
    /// Issue #140's criterion is that the held Peer "is still Interrupted" across a capacity refusal,
    /// not merely that it has not yet been closed. A held Peer that stayed open (established or
    /// resumed) or one whose Slot leaked forever would both still pass a bare "no PeerClosed drained"
    /// assertion. Advancing time past its effective Linger and Pumping is the proof that distinguishes
    /// them: only a Peer genuinely still Interrupted closes at that instant, naming
    /// <see cref="DisconnectReason.LingerExpired"/> (see <see cref="StreamSessionLingerExpiryTests"/>).
    /// </summary>
    [Fact]
    public void AttemptFreshConnection_RefusedByAnInterruptedPeersReservation_LeavesItsLingerDeadlineIntact()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        PeerId id = held[0].Peer;
        accepter.InterruptHeldPeer(held[0]);

        // Act — the refusal itself, then the effective Linger elapsing on the still-Interrupted Peer.
        using StreamSessionReactorHarness.InboundAttemptResult outcome = accepter.AttemptFreshConnection();
        accepter.DrainToList();
        accepter.TimeProvider.Advance(_effectiveLinger);
        accepter.Reactor.Pump();

        // Assert — refused for capacity, and the held Peer's Linger deadline still lands exactly where
        // it would have without the refusal, closing it and naming Linger expiry.
        outcome.Admitted.Should().BeFalse();
        accepter.DrainToList().Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerClosed && e.Reason == DisconnectReason.LingerExpired
                && e.Peer == id);

        held[0].Counterparty.Dispose();
    }
}
