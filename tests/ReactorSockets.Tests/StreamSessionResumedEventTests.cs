using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #128 (4.6.2): what the consumer sees once a Resume actually completes —
/// <see cref="SessionEventKind.PeerInterrupted"/> followed by <see cref="SessionEventKind.PeerResumed"/>,
/// the same <see cref="PeerId"/> throughout, and every existing per-Peer counter still accumulating
/// rather than reset. The rewind itself (issue #127) is proven in
/// <see cref="StreamSessionResumeRewindTests"/>; this file proves only what a consumer draining Events
/// and reading Counters observes once that rewind has happened.
/// </summary>
/// <remarks>
/// Drives the public L2 surface only (tests/CLAUDE.md, ADR 0015): every assertion here is on a drained
/// <see cref="SessionEventKind"/>, on <see cref="PeerId"/> equality by value, or on a
/// <see cref="PeerCounters"/> snapshot read through <see cref="StreamSessionReactor.CountersOf"/> —
/// never on the retention ring's or the counter set's own internal state.
/// </remarks>
public class StreamSessionResumedEventTests
{
    // =========================================================================================
    // Interrupted, then Resumed, with no PeerClosed between — the listening side
    // =========================================================================================

    [Fact]
    public void Handshake_AResumedPeer_ProducesInterruptedThenResumedWithNoPeerClosedBetween()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> interruptedDrain =
            accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> resumeDrain =
            accepter.PumpAndDrainAll(5);

        // Assert — Interrupted arrived first (from the earlier drain), Resumed followed for the same
        // Peer, and nothing closed it in between.
        interruptedDrain.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == held[0].Peer);
        resumeDrain.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerResumed && e.Peer == held[0].Peer);
        interruptedDrain.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        resumeDrain.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    [Fact]
    public void Handshake_AResumeRefusedForARetentionGap_NeverRaisesPeerResumed()
    {
        // Arrange — one Message sent; the Resume claims five were received, a count this end never
        // reached, so the rewind refuses and the Peer closes instead of resuming (D1/D5).
        using StreamSessionReactorHarness accepter = new(capacity: 1, retentionBytes: 4100);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.Reactor.Send(held[0].Peer, StreamSessionTestRecords.PayloadOf(8, 51)).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret, deliveryCount: 5));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained =
            accepter.PumpAndDrainAll(5);

        // Assert — closed, never resumed.
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerResumed);
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed && e.Peer == held[0].Peer);
    }

    // =========================================================================================
    // PeerId, generation included, is unchanged — asserted by value
    // =========================================================================================

    [Fact]
    public void Handshake_APeerResumed_CarriesTheSamePeerIdByValueGenerationIncluded()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained =
            accepter.PumpAndDrainAll(5);

        // Assert — every field compared by value, not reference: PeerId is a record struct, so a
        // reference comparison is not even available; this proves the fields themselves, one at a
        // time, actually match rather than trusting record equality alone.
        PeerId resumedId = drained.Single(e => e.Kind == SessionEventKind.PeerResumed).Peer;
        resumedId.Origin.Should().Be(held[0].Peer.Origin);
        resumedId.Index.Should().Be(held[0].Peer.Index);
        resumedId.Generation.Should().Be(held[0].Peer.Generation);
        resumedId.Should().Be(held[0].Peer);
    }

    // =========================================================================================
    // Three interruptions and resumes report Resumes = 3, and every other counter keeps accumulating
    // =========================================================================================

    [Fact]
    public void CountersOf_ASessionInterruptedAndResumedThreeTimes_ReportsThreeResumes()
    {
        // Arrange — a Peer that also receives a Message before each interruption, so MessagesReceived
        // and RetentionHighWaterBytes have something to accumulate across the three resumes.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        StreamSessionReactorHarness.HeldPeer current = held[0];

        for (int i = 0; i < 3; i++)
        {
            current.Counterparty.Send(StreamSessionTestRecords.BuildFrame(StreamSessionTestRecords.PayloadOf(4, (byte)(60 + i))));
            accepter.PumpUntil(() => accepter.Reactor.CountersOf(current.Peer).MessagesReceived > (ulong)i);
            accepter.InterruptHeldPeer(current);

            StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
            resumed.Send(StreamSessionTestRecords.BuildResume(current.ResumeSecret));
            accepter.PumpAndDrainAll(5);
            current = current with { Counterparty = resumed };
        }

        // Assert
        accepter.Reactor.CountersOf(current.Peer).Resumes.Should().Be(3);
        accepter.Reactor.CountersOf(current.Peer).MessagesReceived.Should().Be(3);

        current.Counterparty.Dispose();
    }

    [Fact]
    public void CountersOf_ASessionInterruptedAndResumedThreeTimes_NeverZeroesMessagesReceivedOrHighWaterBytes()
    {
        // Arrange — the same three-resume cycle, but this asserts the two pre-existing counters never
        // dip below what they held immediately before each interruption.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        StreamSessionReactorHarness.HeldPeer current = held[0];

        accepter.Reactor.Send(current.Peer, StreamSessionTestRecords.PayloadOf(8, 70)).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();

        ulong lastMessagesReceived = 0;
        ulong lastHighWaterBytes = accepter.Reactor.CountersOf(current.Peer).RetentionHighWaterBytes;
        lastHighWaterBytes.Should().BeGreaterThan(0);

        for (int i = 0; i < 3; i++)
        {
            current.Counterparty.Send(StreamSessionTestRecords.BuildFrame(StreamSessionTestRecords.PayloadOf(4, (byte)(80 + i))));
            accepter.PumpUntil(() => accepter.Reactor.CountersOf(current.Peer).MessagesReceived > lastMessagesReceived);
            accepter.InterruptHeldPeer(current);

            // Assert — the interruption itself never zeroes anything: both counters read the same
            // moments after Interrupted as they did the moment before.
            accepter.Reactor.CountersOf(current.Peer).MessagesReceived.Should().BeGreaterThanOrEqualTo(lastMessagesReceived);
            accepter.Reactor.CountersOf(current.Peer).RetentionHighWaterBytes.Should().BeGreaterThanOrEqualTo(lastHighWaterBytes);

            StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
            resumed.Send(StreamSessionTestRecords.BuildResume(current.ResumeSecret));
            accepter.PumpAndDrainAll(5);
            current = current with { Counterparty = resumed };

            // Assert — nor does the resume itself.
            PeerCounters afterResume = accepter.Reactor.CountersOf(current.Peer);
            afterResume.MessagesReceived.Should().BeGreaterThanOrEqualTo(lastMessagesReceived);
            afterResume.RetentionHighWaterBytes.Should().BeGreaterThanOrEqualTo(lastHighWaterBytes);
            lastMessagesReceived = afterResume.MessagesReceived;
            lastHighWaterBytes = afterResume.RetentionHighWaterBytes;
        }

        // Assert — three resumes in, both figures reflect the full accumulated history.
        accepter.Reactor.CountersOf(current.Peer).MessagesReceived.Should().Be(3);
        accepter.Reactor.CountersOf(current.Peer).Resumes.Should().Be(3);

        current.Counterparty.Dispose();
    }

    // =========================================================================================
    // No reconciliation: a Message sent before the interruption is delivered without being re-sent
    // =========================================================================================

    [Fact]
    public void Handshake_AMessageSentBeforeTheInterruption_ArrivesAfterTheResumeWithoutBeingReSentByTheTest()
    {
        // Arrange — one Message this end sent, flushed to the wire but never read before the reset, so
        // the counterparty can honestly claim to have received nothing.
        using StreamSessionReactorHarness accepter = new(capacity: 1, retentionBytes: 4100);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        byte[] payload = StreamSessionTestRecords.PayloadOf(8, 90);
        accepter.Reactor.Send(held[0].Peer, payload).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        accepter.InterruptHeldPeer(held[0]);

        // Act — the test never re-sends payload: only the Resume record goes out, and whatever the
        // rewind resends on its own is what the counterparty reads.
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret, deliveryCount: 0));
        accepter.PumpAndDrainAll(5);

        // Assert — the un-confirmed Message arrives on the resumed transport, framed exactly as it was
        // originally sent, with no re-send call in this test to have produced it.
        byte[] frame = StreamSessionTestRecords.BuildFrame(payload);
        resumed.TryReceiveWithinTimeout(12, out _, timeoutMs: 2000).Should().BeTrue(
            "the accepter's own reciprocal delivery-counter record (D3) arrives first");
        resumed.TryReceiveWithinTimeout(frame.Length, out byte[] received, timeoutMs: 2000).Should().BeTrue();
        received.Should().Equal(frame);
    }

    // =========================================================================================
    // The dialling side raises PeerResumed too, once its own reciprocal rewind resolves
    // =========================================================================================

    [Fact]
    public void Pump_ARedialledPeerWhoseReciprocalRewindResolves_RaisesPeerResumed()
    {
        // Arrange — a dialled Session, interrupted and re-dialled, its own Resume record already on the
        // wire and sitting with AwaitingResumeCounter set (mirrors StreamSessionResumeRewindTests' D3
        // fixture).
        using StreamSessionReactorHarness dialer = new(retentionBytes: 4100);
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 44);
        PeerId peer = dialer.Reactor.Connect(listener.Endpoint);
        RawStreamListener.Accepted far = listener.Accept();
        dialer.Reactor.Pump();
        far.Receive(StreamSessionTestRecords.HandshakeRecordSize);
        far.Send(StreamSessionTestRecords.BuildHello(resumeSecret: resumeSecret));

        PeerId? connected = null;
        for (int i = 0; i < 20 && connected is null; i++)
        {
            dialer.Reactor.Pump();
            foreach ((SessionEventKind Kind, PeerId Peer, DisconnectReason Reason) evt in dialer.DrainToList())
            {
                if (evt.Kind == SessionEventKind.PeerConnected)
                {
                    connected = evt.Peer;
                }
            }
        }

        connected.Should().Be(peer);

        far.ResetAbruptly();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == peer); i++)
        {
            dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(50));
            dialer.Reactor.Pump();
            drained.AddRange(dialer.DrainToList());
        }

        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == peer);

        dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(50));
        dialer.Reactor.Pump();
        listener.TryAccept(out RawStreamListener.Accepted redialed, timeoutMs: 2000).Should().BeTrue();
        dialer.Reactor.Pump();
        redialed.Receive(StreamSessionTestRecords.HandshakeRecordSize);

        // Act — the far end's first post-resume delivery-counter record resolves this end's own
        // reciprocal rewind.
        redialed.Send(StreamSessionTestRecords.BuildDeliveryCounterRecord(0));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> resumeDrain = [];
        for (int i = 0; i < 20 && !resumeDrain.Any(e => e.Kind == SessionEventKind.PeerResumed); i++)
        {
            dialer.Reactor.Pump();
            resumeDrain.AddRange(dialer.DrainToList());
        }

        // Assert — PeerResumed reaches the dialling side too, carrying the same PeerId, and the counter
        // set records it.
        resumeDrain.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerResumed && e.Peer == peer);
        dialer.Reactor.CountersOf(peer).Resumes.Should().Be(1);

        redialed.Dispose();
        far.Dispose();
    }
}
