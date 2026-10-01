namespace ReactorSockets.Core;

/// <summary>
/// The mutable per-Peer counter storage a Peer holds for its own lifetime, incremented on the hot
/// path with a single add and read only through <see cref="Snapshot"/> (ADR 0013). Mirrors
/// <see cref="LinkCounterSet"/>'s Raw-profile shape.
/// </summary>
internal sealed class PeerCounterSet
{

    /// <summary>
    /// Records that one whole Message was handed to the frame arena for delivery. Called exactly
    /// once per delivered Message, at the point the arena reservation commits — never when the
    /// Message's bytes finish arriving in the per-Peer receive buffer and never while it sits
    /// buffered waiting on a read share or arena room, so a Message held back this Frame does not
    /// move this figure until the Frame that actually delivers it.
    /// </summary>
    public void RecordMessageReceived() => MessagesReceived++;

    /// <summary>
    /// The live count of whole Messages handed to the frame arena so far — the same figure
    /// <see cref="Snapshot"/> exposes as <see cref="PeerCounters.MessagesReceived"/>, but read directly
    /// rather than through a snapshot that also demands an unrelated
    /// <see cref="RetentionArena.HighWaterBytes"/> lookup. <see cref="StreamSessionReactor"/>'s
    /// per-Pump delivery-counter emission (issue #116, #25's second trigger) reads this to build the
    /// <c>confirmedThroughMessage</c> body it sends the peer, once per Pump in which
    /// <see cref="RecordMessageReceived"/> was called at least once — a plain property is what lets it
    /// do that without constructing a throwaway <see cref="PeerCounters"/> just to discard the byte
    /// figure.
    /// </summary>
    public ulong MessagesReceived { get; private set; }

    /// <summary>
    /// Records that this Peer's transport was successfully resumed after an Interruption (issue #128).
    /// Called exactly once per <see cref="SessionEventKind.PeerResumed"/> raised — on the listening
    /// side's <c>ResumeHeldPeer</c> once its rewind succeeds, and on the dialling side's
    /// <c>DeliverCompleteMessages</c> once its own reciprocal rewind succeeds — never on a resume
    /// refused for a retention gap or any other reason. The datagram profile calls it when a matched
    /// Resume takes an Interrupted accepting-end Peer out of Interrupted, and when an Interrupted
    /// dialling-end Peer that has sent a Resume hears its session again. Never called by anything that resets a Peer's
    /// other counters: <see cref="RetentionArena.ResetShareHolder"/> is reachable only from
    /// <c>ClosePeer</c>, never from a resume path, so this and every counter beside it keep accumulating
    /// across as many resumes as a Peer lives through (ADR 0013).
    /// </summary>
    public void RecordResume() => Resumes++;

    /// <summary>
    /// The live count of successful resumes so far — the same figure <see cref="Snapshot"/> exposes as
    /// <see cref="PeerCounters.Resumes"/>. Monotonic and never reset, exactly like
    /// <see cref="MessagesReceived"/>: a resume history worth a whole Session's lifetime is the point,
    /// not a per-transport figure that a reconnect would erase.
    /// </summary>
    public ulong Resumes { get; private set; }

    /// <summary>
    /// Records that one packet record's sequence number fell outside the per-Space receive window
    /// (subtask 6.2.1, issue #168; ADR 0013) — too far ahead of or behind that Space's
    /// <see cref="Session.ReceiveWindow.Base"/> to correlate against it. Called from
    /// <see cref="DatagramSessionReactor"/>'s data path once a record classifies as
    /// <see cref="Session.ReceiveWindowOutcome.OutOfWindow"/> (issue #172).
    /// </summary>
    public void RecordOutOfWindowPacket() => OutOfWindowPackets++;

    /// <summary>
    /// The live count of out-of-window packets so far — the same figure <see cref="Snapshot"/> exposes
    /// as <see cref="PeerCounters.OutOfWindowPackets"/>. Monotonic and never reset, exactly like
    /// <see cref="MessagesReceived"/>.
    /// </summary>
    public ulong OutOfWindowPackets { get; private set; }

    /// <summary>
    /// Records that one packet record's sequence number was already accounted for by the per-Space
    /// receive window (subtask 6.2.1, issue #168; ADR 0013) — a duplicate, most often a retransmission
    /// the far end sent because it never saw this end's acknowledgement. Called from
    /// <see cref="DatagramSessionReactor"/>'s data path once a record classifies as
    /// <see cref="Session.ReceiveWindowOutcome.Duplicate"/> (issue #172).
    /// </summary>
    public void RecordDuplicatePacket() => DuplicatePackets++;

    /// <summary>
    /// The live count of duplicate packets so far — the same figure <see cref="Snapshot"/> exposes as
    /// <see cref="PeerCounters.DuplicatePackets"/>. Monotonic and never reset, exactly like
    /// <see cref="MessagesReceived"/>.
    /// </summary>
    public ulong DuplicatePackets { get; private set; }

    /// <summary>
    /// Records <paramref name="count"/> sequences an unreliable Space's receive window retired
    /// un-admitted by sliding its base forward (issues #173, #176; ADR 0013) — a gap the window has
    /// passed by rather than one still reachable inside its span. Called from
    /// <see cref="DatagramSessionReactor"/>'s data path once a record reports
    /// <see cref="Session.ReceiveWindow.LastSlideLoss"/> greater than zero, whichever of the two
    /// unreliable Spaces it was classified against — only those two slide. A single arrival can slide the
    /// window past more than one gap at once, which is why this takes a count rather than being called
    /// once per lost sequence: adding the whole figure in one op keeps the hot path a single add
    /// regardless of how wide the jump was. No Diagnostic accompanies this — a lossy path must not
    /// fill the Sink with volume (ADR 0013).
    /// </summary>
    public void RecordUnreliableLoss(ulong count) => UnreliableLossPackets += count;

    /// <summary>
    /// The live count of sequences lost from either unreliable Space so far — the same figure
    /// <see cref="Snapshot"/> exposes as <see cref="PeerCounters.UnreliableLossPackets"/>. Monotonic
    /// and never reset, exactly like <see cref="MessagesReceived"/>.
    /// </summary>
    public ulong UnreliableLossPackets { get; private set; }

    /// <summary>
    /// Records that one sequence arrived beyond a reliable Space's ack horizon, so its retransmit timer
    /// was withheld rather than started (subtask 6.3, issue #41; ADR 0013). Called from the horizon
    /// gate (issue #185) once a record classifies as beyond <c>[base, base + 32]</c> for the Space it
    /// belongs to. No Diagnostic accompanies this — a deferral can recur every
    /// Pump under sustained load, which is exactly why this is a Counter (ADR 0013).
    /// </summary>
    public void RecordHorizonDeferral() => HorizonDeferrals++;

    /// <summary>
    /// The live count of horizon deferrals so far — the same figure <see cref="Snapshot"/> exposes as
    /// <see cref="PeerCounters.HorizonDeferrals"/>. Monotonic and never reset, exactly like
    /// <see cref="MessagesReceived"/>.
    /// </summary>
    public ulong HorizonDeferrals { get; private set; }

    /// <summary>
    /// Records <paramref name="count"/> packet records this Peer has resent (subtask 6.4.6, issue #196;
    /// ADR 0013), from the expiry sweep (issue #198). Takes a count rather
    /// than being called once per record for the same reason as <see cref="RecordUnreliableLoss"/>: one
    /// Pump's sweep can report many expired sequences retransmitted at once, and adding the whole figure
    /// in one op keeps the hot path a single add regardless of how many sequences it covered.
    /// </summary>
    public void RecordPacketsRetransmitted(ulong count) => PacketsRetransmitted += count;

    /// <summary>
    /// The live count of packet records retransmitted so far — the same figure <see cref="Snapshot"/>
    /// exposes as <see cref="PeerCounters.PacketsRetransmitted"/>. Monotonic and never reset, exactly
    /// like <see cref="MessagesReceived"/>.
    /// </summary>
    public ulong PacketsRetransmitted { get; private set; }

    /// <summary>
    /// Records <paramref name="length"/> bytes of one Datagram a flush has handed the transport for
    /// this Peer, header included (subtask 6.7.4, issue #231; ADR 0013). Called from
    /// <see cref="DatagramSessionReactor"/>'s <c>FlushPeer</c> at the size the coalescing packer
    /// actually wrote, regardless of whether the send call reports success — the bytes were handed to
    /// the transport either way.
    /// </summary>
    public void RecordBytesSent(ulong length) => BytesSent += length;

    /// <summary>
    /// The live count of bytes sent to this Peer so far — the same figure <see cref="Snapshot"/>
    /// exposes as <see cref="PeerCounters.BytesSent"/>. Monotonic and never reset, exactly like
    /// <see cref="MessagesReceived"/>.
    /// </summary>
    public ulong BytesSent { get; private set; }

    /// <summary>
    /// Records <paramref name="length"/> bytes of one Datagram attributed to this Peer on arrival
    /// (subtask 6.7.4, issue #231; ADR 0013). Called from <see cref="DatagramSessionReactor"/>'s
    /// <c>ParseDataDatagram</c> once a Datagram both names this Peer's session id and proves
    /// well-formed — the same proof that gates ack application and record classification — at the
    /// whole Datagram's length, header included.
    /// </summary>
    public void RecordBytesReceived(ulong length) => BytesReceived += length;

    /// <summary>
    /// The live count of bytes received from this Peer so far — the same figure <see cref="Snapshot"/>
    /// exposes as <see cref="PeerCounters.BytesReceived"/>. Monotonic and never reset, exactly like
    /// <see cref="MessagesReceived"/>.
    /// </summary>
    public ulong BytesReceived { get; private set; }

    /// <summary>
    /// Records one socket send call made on this Peer's transport (issue #325; ADR 0013). Called
    /// immediately after each send call, whatever its outcome — a short or failed write is still a call
    /// the Reactor paid for.
    /// </summary>
    public void RecordSocketWrite() => SocketWrites++;

    /// <summary>
    /// The live count of socket send calls made for this Peer so far — the same figure
    /// <see cref="Snapshot"/> exposes as <see cref="PeerCounters.SocketWrites"/>. Monotonic and never
    /// reset, exactly like <see cref="MessagesReceived"/>.
    /// </summary>
    public ulong SocketWrites { get; private set; }

    /// <summary>
    /// Records that this Peer had a whole Message ready and the frame arena refused it because the
    /// Peer had already used its read share this Frame (issue #61). Called from both Session profiles'
    /// arena reservation on <see cref="ArenaReservationOutcome.ShareExhausted"/>.
    /// </summary>
    public void RecordReadShareExhaustion() => ReadShareExhaustions++;

    /// <summary>
    /// The live count of read-share exhaustions so far — the same figure <see cref="Snapshot"/>
    /// exposes as <see cref="PeerCounters.ReadShareExhaustions"/>. Monotonic and never reset, exactly
    /// like <see cref="MessagesReceived"/>.
    /// </summary>
    public ulong ReadShareExhaustions { get; private set; }

    /// <summary>
    /// Takes a point-in-time snapshot of every counter belonging to this Peer.
    /// <paramref name="retentionHighWaterBytes"/> is pulled fresh from <see cref="RetentionArena"/> by
    /// the caller (ADR 0005 — Counters are pulled, not pushed) rather than held here, since this set
    /// holds only what a Peer increments on its own hot path.
    /// </summary>
    public PeerCounters Snapshot(ulong retentionHighWaterBytes) =>
        new(
            MessagesReceived,
            retentionHighWaterBytes,
            Resumes,
            OutOfWindowPackets,
            DuplicatePackets,
            UnreliableLossPackets,
            HorizonDeferrals,
            PacketsRetransmitted,
            BytesSent,
            BytesReceived,
            SocketWrites,
            ReadShareExhaustions);
}
