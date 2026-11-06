using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #129 (4.6.3), the deliverable of issue #30: the increment-point test. #82 fixed the delivery
/// counter's increment point — a Message advances <see cref="PeerCounters.MessagesReceived"/> only when
/// it is handed to the frame arena, never merely because its bytes have all arrived — but never proved
/// what happens to those in-between bytes across a real interruption and Resume. That is this file's
/// job: put a Message in the peer's receive buffer, fully arrived but not yet handed to the arena
/// (#81's read-share exhaustion mechanism, exactly as <see cref="StreamSessionDeliveryCounterTests"/>
/// builds it), kill the transport with it still sitting there, resume, and assert every Message the far
/// end sent is delivered exactly once and this end's own report of what it received is always exactly
/// what it actually delivered — never inflated by a Message whose bytes were merely read. Getting the
/// increment point backwards makes the receiver over-report to the far end what it already has, so the
/// far end would never resend a Message genuinely still owed — losing it silently, only visible on a
/// reconnect.
/// </summary>
/// <remarks>
/// <para>
/// Drives the public L2 surface only (tests/CLAUDE.md, ADR 0015): every assertion is on a drained
/// <see cref="SessionEventKind.MessageReceived"/> payload, a <see cref="PeerCounters"/> snapshot, or the
/// raw bytes read off the wire — never on <see cref="Core.RetentionArena"/> or
/// <see cref="Core.FrameArena"/> state directly. Messages are distinguished by payload content
/// (<see cref="StreamSessionTestRecords.PayloadOf"/>'s marker byte), never by count alone, so a
/// duplicate and a loss read differently in a failed assertion.
/// </para>
/// <para>
/// <b>Why the held-back Message is not resent after this Resume.</b> #81's own
/// <c>RetryPeersWithBufferedMessages</c> reruns unconditionally at the very start of every
/// <see cref="StreamSessionReactor.Pump"/> — before that Pump's own dispatch step ever gets a chance to
/// notice a dead transport — and flushing an already-buffered whole Message touches no socket at all,
/// so it always succeeds regardless of the transport's fate. The Pump that discovers the interruption is
/// therefore also the Pump that hands the held-back Message to the arena: by the time this end reports
/// its own count to the far end (immediately on a matched Resume, issue #127's D3), that report already,
/// correctly, includes it. What this file asserts is therefore not <i>when</i> the Message is delivered.
/// It is that the report is always exactly what was actually delivered, never a count inflated by bytes
/// that were merely read, which is precisely what an early increment point would corrupt.
/// </para>
/// </remarks>
public class StreamSessionResumeIncrementPointTests
{
    private static byte[] BuildFrame(byte[] payload) => StreamSessionTestRecords.BuildFrame(payload);

    private static byte[] PayloadOf(int length, byte marker) => StreamSessionTestRecords.PayloadOf(length, marker);

    /// <summary>
    /// Compares two payloads by content. A collection of <see cref="byte"/> arrays compares its elements
    /// by reference by default, which would let a duplicate pass for the Message it duplicates; this ties
    /// every ordering assertion in this file to the marker bytes that actually tell one Message from
    /// another.
    /// </summary>
    private static bool SamePayload(byte[] actual, byte[] expected) => actual.AsSpan().SequenceEqual(expected);

    /// <summary>
    /// Pumps <paramref name="accepter"/> once, returning every <see cref="SessionEventKind.MessageReceived"/>
    /// payload drained from that single Pump — copied out immediately, since a payload aliases the frame
    /// arena and is invalid past the next Pump (ADR 0008).
    /// </summary>
    private static List<byte[]> PumpOnceAndCollectPayloads(StreamSessionReactorHarness accepter)
    {
        accepter.Reactor.Pump();
        return [.. accepter.DrainToListWithPayload()
            .Where(e => e.Kind == SessionEventKind.MessageReceived)
            .Select(e => e.Payload)];
    }

    [Fact]
    public void Resume_AMessageHeldBackByAnExhaustedReadShareAtInterruption_IsDeliveredExactlyOnceAndAccuratelyReported()
    {
        // Arrange — capacity 4, arena 200 bytes -> a 50-byte read share per Peer (mirrors
        // StreamSessionDeliveryCounterTests and StreamSessionReadShareTests). Three 20-byte Messages
        // total 60 bytes, so the third cannot be reserved alongside the first two in the same Frame: its
        // bytes are fully arrived at the front of the receive buffer but never handed to the arena.
        using StreamSessionReactorHarness accepter = new(
            capacity: 4, maxMessageSize: 64, retentionBytes: 68, receiveBufferSize: 100, arenaSize: 200);
        StreamSessionReactorHarness.HeldPeer held = accepter.EstablishOnePeer();

        byte[] first = PayloadOf(20, 1);
        byte[] second = PayloadOf(20, 2);
        byte[] third = PayloadOf(20, 3);

        // Act — all three frames arrive before any Pump runs, so the third's bytes are present from the
        // very first read; the read-share escape lets only the first two through this Frame.
        held.Counterparty.Send([.. BuildFrame(first), .. BuildFrame(second), .. BuildFrame(third)]);
        List<byte[]> delivered = PumpOnceAndCollectPayloads(accepter);

        // Assert — exactly the first two arrived this Frame. This is the assertion the increment point
        // is directly responsible for: an early increment (at "bytes read" rather than "handed to the
        // arena") would read 3 here even though only 2 Messages actually reached the consumer.
        delivered.Should().HaveCount(2);
        delivered[0].Should().Equal(first);
        delivered[1].Should().Equal(second);
        accepter.Reactor.CountersOf(held.Peer).MessagesReceived.Should().Be(2,
            "the third Message's bytes have all arrived but it lost the read-share race and was never handed to the arena");

        // Act — kill the transport with the third Message's bytes still sitting, uncounted, at the
        // front of the accepter's own receive buffer. Pump until the interruption is observed, keeping
        // every Message payload drained along the way.
        held.Counterparty.ResetAbruptly();
        bool interrupted = false;
        for (int i = 0; i < 20 && !interrupted; i++)
        {
            accepter.Reactor.Pump();
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> drainedThisPump =
                accepter.DrainToListWithPayload();
            delivered.AddRange(drainedThisPump.Where(e => e.Kind == SessionEventKind.MessageReceived).Select(e => e.Payload));
            interrupted = drainedThisPump.Any(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == held.Peer);
        }

        interrupted.Should().BeTrue("the reset must be observed as an interruption within the Pump budget");

        // Assert — every Message the far end sent before the reset reached the consumer exactly once:
        // the third, held back at the moment the transport died, is neither lost nor duplicated, and
        // this end's own count reflects precisely those three deliveries — never inflated by counting a
        // Message's bytes ahead of the arena.
        delivered.Should().HaveCount(3);
        delivered.Should().Equal([first, second, third], SamePayload);
        accepter.Reactor.CountersOf(held.Peer).MessagesReceived.Should().Be(3);

        // Act — resume with a fresh connection presenting the same secret.
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held.ResumeSecret, deliveryCount: 0));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drainedOnResume =
            accepter.PumpUntilDrained(events => events.Any(e => e.Kind == SessionEventKind.PeerResumed));

        drainedOnResume.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerResumed && e.Peer == held.Peer);

        // Assert — the accepter's own reciprocal delivery-counter record (D3), the first thing it emits
        // on a matched Resume, reports exactly 3, which is the claim this whole ticket rests on. A
        // receiver that counted the third Message's bytes early — rather than only once actually handed
        // to the arena — would have reported a wrong figure well before this point, and a far end
        // trusting that wrong report would leave Messages permanently unresent across a real gap.
        resumed.TryReceiveWithinTimeout(12, out byte[] counterRecord, timeoutMs: 2000).Should().BeTrue();
        StreamSessionTestRecords.TryReadDeliveryCounterRecord(counterRecord, out ulong reportedCount).Should().BeTrue();
        reportedCount.Should().Be((ulong)delivered.Count,
            "the accepter's own report to the far end must always equal exactly how many Messages it actually delivered");

        // Assert — nothing further arrives: the far end, trusting an accurate report, has nothing left
        // to resend.
        resumed.TryReceiveWithinTimeout(1, out _, timeoutMs: 200).Should().BeFalse();
    }

    [Fact]
    public void Resume_WithNothingBufferedAtTheMomentOfInterruption_StillDeliversEveryMessageExactlyOnce()
    {
        // Arrange — the happy path, which this file also covers but does not rest on: no
        // read-share exhaustion, nothing left buffered when the transport dies.
        using StreamSessionReactorHarness accepter = new(maxMessageSize: 64, retentionBytes: 68);
        StreamSessionReactorHarness.HeldPeer held = accepter.EstablishOnePeer();

        byte[] first = PayloadOf(10, 11);
        held.Counterparty.Send(BuildFrame(first));
        List<byte[]> delivered = PumpOnceAndCollectPayloads(accepter);
        delivered.Should().ContainSingle().Which.Should().Equal(first);
        accepter.Reactor.CountersOf(held.Peer).MessagesReceived.Should().Be(1);

        // Act — interrupt with nothing outstanding, then resume and send a second Message.
        accepter.InterruptHeldPeer(held);

        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held.ResumeSecret, deliveryCount: 0));
        accepter.PumpUntilDrained(_ => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeMatched));
        resumed.TryReceiveWithinTimeout(12, out byte[] counterRecord, timeoutMs: 2000).Should().BeTrue();
        StreamSessionTestRecords.TryReadDeliveryCounterRecord(counterRecord, out ulong reportedCount).Should().BeTrue();
        reportedCount.Should().Be(1, "nothing was left buffered at interruption, so the report matches exactly what was delivered");

        byte[] second = PayloadOf(10, 12);
        resumed.Send(BuildFrame(second));
        List<byte[]> afterResume = [];
        for (int i = 0; i < 20 && afterResume.Count == 0; i++)
        {
            afterResume.AddRange(PumpOnceAndCollectPayloads(accepter));
        }

        // Assert — both Messages arrived, each exactly once.
        afterResume.Should().ContainSingle().Which.Should().Equal(second);
        accepter.Reactor.CountersOf(held.Peer).MessagesReceived.Should().Be(2);

        delivered.AddRange(afterResume);
        delivered.Should().HaveCount(2);
        delivered.Should().Equal([first, second], SamePayload);
    }
}
