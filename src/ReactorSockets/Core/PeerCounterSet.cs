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
    /// refused for a retention gap or any other reason. Never called by anything that resets a Peer's
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
    /// Takes a point-in-time snapshot of every counter belonging to this Peer.
    /// <paramref name="retentionHighWaterBytes"/> is pulled fresh from <see cref="RetentionArena"/> by
    /// the caller (ADR 0005 — Counters are pulled, not pushed) rather than held here, since this set
    /// holds only what a Peer increments on its own hot path.
    /// </summary>
    public PeerCounters Snapshot(ulong retentionHighWaterBytes) => new(MessagesReceived, retentionHighWaterBytes, Resumes);
}
