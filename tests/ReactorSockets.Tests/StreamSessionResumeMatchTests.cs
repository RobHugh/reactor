using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// An incoming connection presenting a valid resume secret is matched onto
/// the Interrupted Peer whose Slot is reserving it, and <b>the match runs before the capacity check</b>
/// (ADR 0011). A reconnecting Peer is not asking for a Slot, it already owns one, so checking capacity
/// first would refuse a reconnection into a Reactor holding the very state being reconnected to —
/// a failure that appears only once the Reactor is full, which is why the at-capacity test here is the
/// one that matters.
/// </summary>
/// <remarks>
/// Drives the public L2 surface only (tests/CLAUDE.md, ADR 0015): the resume secret is read off the
/// wire out of the Hello the Reactor itself sent
/// (<see cref="StreamSessionTestRecords.ReadResumeSecretFromHello"/>), never out of the Reactor, and
/// nothing here asserts how it is stored. That a match happened is observed as the Peer going on
/// receiving under the same <see cref="PeerId"/>. The <c>Resumed</c> event and the rewind have their
/// own test files. Built on the capacity-fill and interruption helpers, the same ones
/// <see cref="StreamSessionSlotReservationTests"/> uses to prove the reservation this match collects on.
/// </remarks>
public class StreamSessionResumeMatchTests
{
    private const byte RefusalReasonCapacity = 2;

    // =========================================================================================
    // The mandatory test: at capacity, the match runs first
    // =========================================================================================

    [Fact]
    public void Handshake_AResumeArrivingAtAReactorWithEverySlotOccupied_ResumesTheHeldPeerRatherThanBeingRefused()
    {
        // Arrange — every Slot occupied, one Peer interrupted so it holds its Slot for Linger. The
        // Reactor is full: there is no Slot to give a connection that asks for one.
        using StreamSessionReactorHarness accepter = new(capacity: 2);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);

        // Act — the interrupted Peer comes back over a replacement transport, presenting the secret
        // this Reactor announced to it, then sends a Message over the resumed Session.
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret));
        accepter.PumpUntilDrained(_ => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeMatched));
        resumed.Send(StreamSessionTestRecords.BuildFrame(StreamSessionTestRecords.PayloadOf(8, 42)));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> events =
            PumpUntilMessage(accepter);

        // Assert — the Message arrived under the held Peer's own id: the connection was matched onto
        // the reserved Slot, not refused for the capacity that Slot is part of. Reorder the match and
        // the capacity check in the Reactor and this fails with a Refusal instead.
        events.Should().ContainSingle(e => e.Kind == SessionEventKind.MessageReceived)
            .Which.Peer.Should().Be(held[0].Peer);

        held[1].Counterparty.Dispose();
    }

    [Fact]
    public void Handshake_AResumeArrivingAtAReactorWithEverySlotOccupied_IsNotAnsweredWithACapacityRefusal()
    {
        // Arrange — the same full Reactor holding the same Interrupted Peer.
        using StreamSessionReactorHarness accepter = new(capacity: 2);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret));
        accepter.PumpUntilDrained(_ => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeMatched));

        // Assert — a matched Resume is answered by the Session continuing, never by the
        // HandshakeRecordSize-shaped Refusal a fresh dial at this same Reactor would have got: what
        // actually arrives is the accepter's own reciprocal delivery-counter record (issue #127, D3),
        // a different and much shorter shape, so a read sized for a Refusal never completes.
        resumed.TryReceiveWithinTimeout(StreamSessionTestRecords.HandshakeRecordSize, out _, timeoutMs: 200)
            .Should().BeFalse();

        held[1].Counterparty.Dispose();
    }

    // =========================================================================================
    // The identity survives the transport
    // =========================================================================================

    [Fact]
    public void Handshake_APeerResumedAfterAnInterruption_KeepsTheSamePeerIdGenerationIncluded()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret));
        accepter.PumpUntilDrained(_ => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeMatched));
        resumed.Send(StreamSessionTestRecords.BuildFrame(StreamSessionTestRecords.PayloadOf(4, 9)));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> events =
            PumpUntilMessage(accepter);

        // Assert — PeerId carries the Slot's generation, and a generation advances only when a Slot is
        // emptied and reused. The same id here is therefore the same Slot, never renewed: nothing about
        // a Peer's identity is renewed by a Resume, which is the whole premise.
        PeerId resumedId = events.Single(e => e.Kind == SessionEventKind.MessageReceived).Peer;
        resumedId.Should().Be(held[0].Peer);
        resumedId.Generation.Should().Be(held[0].Peer.Generation);
    }

    [Fact]
    public void Handshake_APeerResumedAfterAnInterruption_StillReportsTheCountersItHadBeforeTheInterruption()
    {
        // Arrange — the Peer receives a Message and this end sends one, so both its counters have moved
        // off zero before its transport dies.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        held[0].Counterparty.Send(StreamSessionTestRecords.BuildFrame(StreamSessionTestRecords.PayloadOf(16, 3)));
        accepter.PumpUntil(() => accepter.Reactor.CountersOf(held[0].Peer).MessagesReceived > 0);
        accepter.Reactor.Send(held[0].Peer, StreamSessionTestRecords.PayloadOf(16, 4));
        accepter.Reactor.Pump();

        PeerCounters beforeInterruption = accepter.Reactor.CountersOf(held[0].Peer);
        beforeInterruption.MessagesReceived.Should().BeGreaterThan(0);
        beforeInterruption.RetentionHighWaterBytes.Should().BeGreaterThan(0);
        accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret));
        accepter.PumpUntilDrained(_ => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeMatched));

        // Assert — the match adopted the Peer's state rather than starting one: a Slot that had been
        // emptied and refilled would answer from zero, and a retention ring that had been reset would
        // have lost its high-water mark with the bytes it was holding.
        PeerCounters afterResume = accepter.Reactor.CountersOf(held[0].Peer);
        afterResume.MessagesReceived.Should().Be(beforeInterruption.MessagesReceived);
        afterResume.RetentionHighWaterBytes.Should().Be(beforeInterruption.RetentionHighWaterBytes);
    }

    [Fact]
    public void Handshake_AMessageBufferedButUndeliveredWhenTheTransportDied_IsStillDeliveredAfterTheResume()
    {
        // Arrange — a Message whose bytes all arrived, but which the consumer had not drained when the
        // transport was reset. Interrupting must not discard it, and neither must the match.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        byte[] payload = StreamSessionTestRecords.PayloadOf(12, 77);
        held[0].Counterparty.Send(StreamSessionTestRecords.BuildFrame(payload));
        accepter.Reactor.Pump();
        accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret));
        accepter.PumpUntilDrained(_ => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeMatched));
        resumed.Send(StreamSessionTestRecords.BuildFrame(StreamSessionTestRecords.PayloadOf(4, 5)));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> events =
            PumpUntilMessage(accepter);

        // Assert — the Peer goes on receiving under its own id, and the count of whole Messages this
        // Session has handed over includes the one that arrived before the interruption.
        events.Should().Contain(e => e.Kind == SessionEventKind.MessageReceived && e.Peer == held[0].Peer);
        accepter.Reactor.CountersOf(held[0].Peer).MessagesReceived.Should().Be(2);
    }

    // =========================================================================================
    // The capacity check is still reachable, and an unmatched secret disturbs nothing
    // =========================================================================================

    [Fact]
    public void Connect_AFreshDialArrivingAtAFullReactorAfterAResumeMatched_IsStillRefusedForCapacity()
    {
        // Arrange — a Reactor that has just matched a Resume, so the reordered check has run for real
        // on this instance rather than only in principle.
        using StreamSessionReactorHarness accepter = new(capacity: 2);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret));
        accepter.PumpUntilDrained(_ => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeMatched));

        // Act — an ordinary dial, asking for a Slot this Reactor does not have.
        using StreamSessionReactorHarness.InboundAttemptResult outcome = accepter.AttemptFreshConnection();

        // Assert — the match running ahead of the capacity check does not make the capacity check
        // unreachable: a connection that is not a Resume is refused for capacity.
        outcome.Admitted.Should().BeFalse();
        outcome.RefusalReason.Should().Be(RefusalReasonCapacity);
        outcome.RefusalCapacity.Should().Be((uint)accepter.Capacity);

        held[1].Counterparty.Dispose();
    }

    [Fact]
    public void Handshake_AResumePresentingASecretMatchingNoHeldPeer_LeavesTheHeldPeerResumableByTheRightSecret()
    {
        // Arrange — a held Interrupted Peer, and a connection presenting sixteen bytes that are not its
        // secret.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);
        byte[] wrongSecret = Enumerable.Range(1, StreamSessionTestRecords.ResumeSecretLength).Select(b => (byte)b).ToArray();
        wrongSecret.Should().NotEqual(held[0].ResumeSecret);

        // Act — the wrong secret first, then the right one over a second connection.
        using (StreamSessionReactorHarness.RawStreamPeer impostor = new(accepter.Endpoint))
        {
            impostor.Send(StreamSessionTestRecords.BuildResume(wrongSecret));
            accepter.PumpUntilDrained(_ => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));
        }

        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret));
        accepter.PumpUntilDrained(_ => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeMatched));
        resumed.Send(StreamSessionTestRecords.BuildFrame(StreamSessionTestRecords.PayloadOf(4, 6)));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> events =
            PumpUntilMessage(accepter);

        // Assert — the unmatched attempt took nothing from the held Peer: its Slot, its state and its
        // secret are all still there for the connection that can actually present it.
        events.Should().ContainSingle(e => e.Kind == SessionEventKind.MessageReceived)
            .Which.Peer.Should().Be(held[0].Peer);
    }

    [Fact]
    public void Handshake_AResumePresentingASecretMatchingNoHeldPeer_RaisesNoEventForAnyHeldPeer()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);
        byte[] wrongSecret = new byte[StreamSessionTestRecords.ResumeSecretLength];

        // Act
        using StreamSessionReactorHarness.RawStreamPeer impostor = new(accepter.Endpoint);
        impostor.Send(StreamSessionTestRecords.BuildResume(wrongSecret));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events =
            accepter.PumpUntilDrained(_ => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        // Assert — a stranger's failed claim is not something the Peer it named is told about, and it
        // is certainly not something that closes it.
        events.Should().BeEmpty();
    }

    [Fact]
    public void Handshake_AResumePresentingTheSecretOfAPeerWhoseTransportIsStillAlive_IsNotMatchedOntoIt()
    {
        // Arrange — an established Peer, never interrupted: its transport is alive and its Session is
        // running over it.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();

        // Act — a second connection claims that live Session.
        using StreamSessionReactorHarness.RawStreamPeer impostor = new(accepter.Endpoint);
        impostor.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret));
        accepter.PumpUntilDrained(_ => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        // Assert — a Resume reconnects a Session whose transport is gone; it does not move a live one
        // onto a different connection. The original transport is untouched and still delivering.
        held[0].Counterparty.Send(StreamSessionTestRecords.BuildFrame(StreamSessionTestRecords.PayloadOf(4, 8)));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> events =
            PumpUntilMessage(accepter);
        events.Should().ContainSingle(e => e.Kind == SessionEventKind.MessageReceived)
            .Which.Peer.Should().Be(held[0].Peer);
    }

    /// <summary>
    /// Pumps until at least one <see cref="SessionEventKind.MessageReceived"/> has drained, returning
    /// everything drained on the way. Delivery is not synchronous with a send, so the wait is bounded
    /// by a clock rather than a Pump count (<see cref="PumpDeadline"/>).
    /// </summary>
    private static List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> PumpUntilMessage(
        StreamSessionReactorHarness accepter,
        TimeSpan? budget = null)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> events = [];
        PumpDeadline deadline = PumpDeadline.Start(budget);
        while (true)
        {
            accepter.Reactor.Pump();
            events.AddRange(accepter.DrainToListWithPayload());
            if (events.Any(e => e.Kind == SessionEventKind.MessageReceived) || deadline.Expired)
            {
                return events;
            }

            PumpDeadline.Yield();
        }
    }
}
