using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #127 (4.6.1): the rewind. After a Resume, the sender writes from the peer's reported delivery
/// counter — the bytes it writes are exactly the ones already framed and sitting in the retention ring,
/// nothing re-framed and nothing reconstructed (<see cref="RetentionArena.RewindSentToMessageCount"/>,
/// L0). Two directions, because a Resume record is one-way (issue #115): the listening side reads the
/// Resume record's own counter directly (D2, <see cref="StreamSessionReactor.ResumeHeldPeer"/>); the
/// dialling side has no such field to read, so it learns the far end's position from the first
/// delivery-counter record that arrives after its own Resume (D3,
/// <see cref="StreamSessionReactor.DeliverCompleteMessages"/>'s <c>DeliveryCounterReceived</c> arm).
/// </summary>
/// <remarks>
/// Drives the public L2 surface only (tests/CLAUDE.md, ADR 0015): every assertion here is on
/// <see cref="DiagnosticId.ResumeMatched"/>'s reported <c>Value0</c>, on the raw bytes a counterparty
/// actually reads or does not read off the wire, on the <see cref="SendOutcome"/> a <c>Send</c> call
/// itself reports, or on a drained <see cref="SessionEventKind"/> — never on the ring's own internal
/// sent or released offset. The listening-side tests build on
/// <see cref="StreamSessionReactorHarness"/> and <c>RawStreamPeer</c>, the same fixture
/// <see cref="StreamSessionResumeMatchTests"/> and <see cref="StreamSessionResumeDiagnosticsTests"/>
/// use; the dialling-side test builds on <see cref="StreamSessionRedialTests"/>' own
/// <c>RawStreamListener</c>, the mirror-image counterparty needed because the Reactor under test is the
/// one doing the dialling there. A frame this end sent but the counterparty never read before an abrupt
/// reset is exactly the scenario a Resume exists for: the OS accepted it into a send buffer, but the
/// reset discards whatever the counterparty had not yet drained from its own receive buffer, so the
/// counterparty can honestly claim (via <c>BuildResume</c>'s own <c>deliveryCount</c>) never to have
/// received it — the same gap TCP's own acknowledgement leaves that the issue's own description names.
/// </remarks>
public class StreamSessionResumeRewindTests
{
    // =========================================================================================
    // D2 — the listening side rewinds from the Resume record's own counter
    // =========================================================================================

    [Fact]
    public void Handshake_AResumeReportingNoneOfOneSentMessageReceived_RewindsAndResendsThatWholeFrame()
    {
        // Arrange — one Message sent and flushed to the wire, but never read by the counterparty before
        // its transport is reset: the framed bytes reached the OS, never the far application.
        using StreamSessionReactorHarness accepter = new(capacity: 1, retentionBytes: 4100);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        byte[] payload = StreamSessionTestRecords.PayloadOf(8, 1);
        accepter.Reactor.Send(held[0].Peer, payload).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        accepter.InterruptHeldPeer(held[0]);

        // Act — the peer comes back claiming to have received nothing.
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret, deliveryCount: 0));
        accepter.PumpAndDrainAll(5);

        // Assert — ResumeMatched's Value0 is exactly the framed size of the one Message never
        // confirmed, and that whole frame — nothing re-framed, nothing reconstructed — reaches the wire,
        // right behind this end's own reciprocal delivery-counter record (D3).
        byte[] frame = StreamSessionTestRecords.BuildFrame(payload);
        accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.ResumeMatched)
            .Which.Value0.Should().Be(frame.Length);

        ReadOwnDeliveryCounterRecord(resumed);
        resumed.TryReceiveWithinTimeout(frame.Length, out byte[] received, timeoutMs: 2000).Should().BeTrue();
        received.Should().Equal(frame);
    }

    [Fact]
    public void Handshake_AResumeReportingEverythingSentReceived_RewindsZeroBytesAndResendsNothing()
    {
        // Arrange — the same one Message, but the Resume's own counter names it as already received.
        using StreamSessionReactorHarness accepter = new(capacity: 1, retentionBytes: 4100);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        byte[] payload = StreamSessionTestRecords.PayloadOf(8, 2);
        accepter.Reactor.Send(held[0].Peer, payload).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret, deliveryCount: 1));
        accepter.PumpAndDrainAll(5);

        // Assert — a counter equal to everything sent rewinds nothing and resends nothing behind this
        // end's own reciprocal delivery-counter record (D3), which arrives regardless.
        accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.ResumeMatched)
            .Which.Value0.Should().Be(0);
        ReadOwnDeliveryCounterRecord(resumed);
        resumed.TryReceiveWithinTimeout(1, out _, timeoutMs: 200).Should().BeFalse();
    }

    [Fact]
    public void Handshake_AResumeReportingTheFirstOfTwoMessagesReceived_ResendsOnlyTheSecond()
    {
        // Arrange — two Messages sent, both flushed and both unread by the counterparty before reset.
        using StreamSessionReactorHarness accepter = new(capacity: 1, retentionBytes: 4100);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        byte[] payload1 = StreamSessionTestRecords.PayloadOf(6, 3);
        byte[] payload2 = StreamSessionTestRecords.PayloadOf(10, 4);
        accepter.Reactor.Send(held[0].Peer, payload1).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(held[0].Peer, payload2).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        accepter.InterruptHeldPeer(held[0]);

        // Act — the peer claims to have received the first of the two.
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret, deliveryCount: 1));
        accepter.PumpAndDrainAll(5);

        // Assert — Value0 is exactly the second Message's framed size, bytes below the reported counter
        // (the first Message) are never resent, and what does arrive is exactly the second frame.
        byte[] frame2 = StreamSessionTestRecords.BuildFrame(payload2);
        accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.ResumeMatched)
            .Which.Value0.Should().Be(frame2.Length);

        ReadOwnDeliveryCounterRecord(resumed);
        resumed.TryReceiveWithinTimeout(frame2.Length, out byte[] received, timeoutMs: 2000).Should().BeTrue();
        received.Should().Equal(frame2);
        resumed.TryReceiveWithinTimeout(1, out _, timeoutMs: 200).Should().BeFalse();
    }

    [Fact]
    public void Send_AfterAResumeFollowingAConfirmedRelease_StillHasTheReleasedRoomFree()
    {
        // Arrange — retention sized for exactly two small frames (MaxMessageSize lowered to match, so
        // RetentionBytes need not cover a full 4096-byte Message). A first Message is sent, actually
        // read by the counterparty, and confirmed back to the accepter with its own delivery-counter
        // record, releasing that Message's bytes before the interruption ever happens.
        const int maxMessageSize = 16;
        const int retentionBytes = 20;
        using StreamSessionReactorHarness accepter =
            new(capacity: 1, maxMessageSize: maxMessageSize, retentionBytes: retentionBytes);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        byte[] payloadA = StreamSessionTestRecords.PayloadOf(4, 5);
        byte[] frameA = StreamSessionTestRecords.BuildFrame(payloadA);
        accepter.Reactor.Send(held[0].Peer, payloadA).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        held[0].Counterparty.Receive(frameA.Length);
        held[0].Counterparty.Send(StreamSessionTestRecords.BuildDeliveryCounterRecord(1));
        accepter.Reactor.Pump();

        // Arrange — now interrupt with nothing outstanding, and resume naming exactly what was already
        // released: bytesRewound is 0, since there is nothing this end sent that the peer has not also
        // already confirmed.
        accepter.InterruptHeldPeer(held[0]);
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret, deliveryCount: 1));
        accepter.PumpAndDrainAll(5);

        // Act — a second Message, sized so it only fits if the first Message's released bytes are
        // still free: 12 framed bytes into 20 total room needs the 8 released bytes actually free.
        byte[] payloadC = StreamSessionTestRecords.PayloadOf(8, 6);
        SendOutcome outcome = accepter.Reactor.Send(held[0].Peer, payloadC);

        // Assert — retention released before the interruption is still free after the rewind: the
        // ring's head did not move backwards. A rewind that wrongly un-released those bytes would have
        // left only 12 bytes of the 20-byte bound free (20 - 8 retained), and this Send would refuse.
        outcome.Should().Be(SendOutcome.Sent);
    }

    [Fact]
    public void Handshake_AResumeReportingMoreMessagesThanThisEndEverSent_ClosesTheHeldPeerRatherThanResuming()
    {
        // Arrange — one Message sent; the Resume claims five were received, a count this end never
        // reached (RetentionRewindOutcome.AheadOfAppended, D1).
        using StreamSessionReactorHarness accepter = new(capacity: 1, retentionBytes: 4100);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        byte[] payload = StreamSessionTestRecords.PayloadOf(8, 7);
        accepter.Reactor.Send(held[0].Peer, payload).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret, deliveryCount: 5));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained =
            accepter.PumpAndDrainAll(5);

        // Assert — closed rather than resumed: no PeerResumed, no ResumeMatched record, and the held
        // Peer's own Slot is closed instead (D1/D2, the #130 placeholder reason).
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.ResumeMatched);
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed && e.Peer == held[0].Peer)
            .Which.Reason.Should().Be(DisconnectReason.ProtocolViolation);
    }

    // =========================================================================================
    // D3 — the dialling side's reciprocal rewind, driven by the first delivery-counter record
    // after its own Resume
    // =========================================================================================

    [Fact]
    public void Pump_ARedialThatHasSentItsResumeButHeardNothingBack_ResendsNothingYet()
    {
        // Arrange — a redialled dialling-side Peer, its own Resume record already on the wire, with
        // two unconfirmed Messages still queued from before the interruption.
        using StreamSessionReactorHarness dialer = new(retentionBytes: 4100);
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 21);
        (_, RawStreamListener.Accepted far, RawStreamListener.Accepted redialed, byte[][] _) =
            EstablishRedialWithTwoUnconfirmedMessages(dialer, listener, resumeSecret);
        far.Dispose();

        // Act — a further Pump, with the far end still silent: nothing about the far end's own read
        // position is known yet, so nothing may be resent.
        dialer.Reactor.Pump();

        // Assert
        redialed.TryReceiveWithinTimeout(1, out _, timeoutMs: 200).Should().BeFalse(
            "the dialling side must not resend before the far end's first post-resume delivery counter answers where its own read position is");

        redialed.Dispose();
    }

    [Fact]
    public void Pump_TheFarEndsFirstPostResumeDeliveryCounter_RewindsAndResendsOnlyWhatItDidNotReport()
    {
        // Arrange — the same redialled Peer with two unconfirmed Messages queued.
        using StreamSessionReactorHarness dialer = new(retentionBytes: 4100);
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 22);
        (_, RawStreamListener.Accepted far, RawStreamListener.Accepted redialed, byte[][] frames) =
            EstablishRedialWithTwoUnconfirmedMessages(dialer, listener, resumeSecret);
        far.Dispose();

        // Act — the far end's first record after the resume reports having received only the first of
        // the two Messages.
        redialed.Send(StreamSessionTestRecords.BuildDeliveryCounterRecord(1));
        dialer.Reactor.Pump();
        dialer.Reactor.Pump();

        // Assert — exactly the second Message's frame arrives, nothing more: the first is not resent,
        // because the far end's own report is what this end rewinds to, driven by the delivery-counter
        // record rather than a Resume record of its own (a Resume never arrives this direction).
        redialed.TryReceiveWithinTimeout(frames[1].Length, out byte[] received, timeoutMs: 2000).Should().BeTrue();
        received.Should().Equal(frames[1]);
        redialed.TryReceiveWithinTimeout(1, out _, timeoutMs: 200).Should().BeFalse();

        redialed.Dispose();
    }

    /// <summary>The cadence this file's dial-side helper advances the clock by mid-handshake and re-dial.</summary>
    private static readonly TimeSpan _cadence = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Establishes a dialled Session against <paramref name="listener"/>, sends two Messages that are
    /// flushed but never read by the far end, resets that far end abruptly, and pumps
    /// <paramref name="dialer"/> through its own re-dial and Resume so the redialled connection is
    /// sitting with <see cref="StreamSessionReactor"/>'s own <c>AwaitingResumeCounter</c> set — the
    /// starting point every D3 test in this file needs. Returns the original (now reset) far end, the
    /// redialled connection with the Resume record already drained off it, and the two Messages' own
    /// framed bytes.
    /// </summary>
    private static (PeerId Peer, RawStreamListener.Accepted Far, RawStreamListener.Accepted Redialed, byte[][] Frames)
        EstablishRedialWithTwoUnconfirmedMessages(
            StreamSessionReactorHarness dialer, RawStreamListener listener, byte[] resumeSecret)
    {
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

        connected.Should().Be(peer, "the dial must establish before this helper can send anything over it");

        byte[] payload1 = StreamSessionTestRecords.PayloadOf(4, 31);
        byte[] payload2 = StreamSessionTestRecords.PayloadOf(6, 32);
        dialer.Reactor.Send(peer, payload1).Should().Be(SendOutcome.Sent);
        dialer.Reactor.Send(peer, payload2).Should().Be(SendOutcome.Sent);
        dialer.Reactor.Pump();

        far.ResetAbruptly();
        bool interrupted = false;
        for (int i = 0; i < 20 && !interrupted; i++)
        {
            dialer.TimeProvider.Advance(_cadence);
            dialer.Reactor.Pump();
            interrupted = dialer.DrainToList().Any(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == peer);
        }

        interrupted.Should().BeTrue("the reset must be observed as an interruption within the Pump budget");

        dialer.TimeProvider.Advance(_cadence);
        dialer.Reactor.Pump();
        listener.TryAccept(out RawStreamListener.Accepted redialed, timeoutMs: 2000).Should().BeTrue(
            "the re-dial must land at the listener within the Pump budget");
        dialer.Reactor.Pump();

        byte[] resumeRecord = redialed.Receive(StreamSessionTestRecords.HandshakeRecordSize);
        resumeRecord[0].Should().Be(StreamSessionTestRecords.ResumeRecordType);

        return (peer, far, redialed,
        [
            StreamSessionTestRecords.BuildFrame(payload1),
            StreamSessionTestRecords.BuildFrame(payload2),
        ]);
    }

    /// <summary>
    /// Reads and validates the whole 12-byte delivery-counter record <see cref="StreamSessionReactor.ResumeHeldPeer"/>
    /// emits reciprocally the instant it matches a Resume (issue #127, D3) — every listening-side test
    /// in this file drains this off <paramref name="resumed"/> before asserting on whatever rewound
    /// bytes follow it, since it always reaches the wire first.
    /// </summary>
    private static void ReadOwnDeliveryCounterRecord(StreamSessionReactorHarness.RawStreamPeer resumed)
    {
        resumed.TryReceiveWithinTimeout(12, out byte[] record, timeoutMs: 2000).Should().BeTrue(
            "the listening side emits its own reciprocal delivery-counter record the instant it matches a Resume");
        StreamSessionTestRecords.TryReadDeliveryCounterRecord(record, out _).Should().BeTrue(
            "the record ahead of any rewound bytes must be the reciprocal delivery counter, not something else");
    }
}
