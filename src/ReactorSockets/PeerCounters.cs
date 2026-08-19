namespace ReactorSockets;

/// <summary>
/// A point-in-time snapshot of one Peer's counters (ADR 0013): monotonic figures running from that
/// Peer's establishment, with no reset, fetched by value and keyed by <see cref="PeerId"/> — mirrors
/// <see cref="LinkCounters"/>'s Raw-profile shape and the same "no accessor enumerates every Peer's
/// counters in one call" rule.
/// </summary>
/// <param name="MessagesReceived">
/// Whole Messages handed to the frame arena for delivery — incremented at that point only, never
/// when a Message's bytes finish arriving or while it sits buffered waiting for a read share or
/// arena room (subtask 3.4.4, issue #82). This is what makes a share exhaustion visible: a Frame in
/// which bytes plainly arrived but this figure did not move is a Message still buffered, not one
/// dropped.
/// </param>
/// <param name="RetentionHighWaterBytes">
/// The most bytes this Peer's retention ring has ever held at once (written minus released) since
/// the Peer was established — <see cref="Core.RetentionArena.HighWaterBytes(int)"/> read fresh at
/// snapshot time, never pushed. Monotonic for the Peer's lifetime: draining the ring after a burst
/// lowers what is currently retained but never lowers this figure (subtask 3.5.5, issue #87).
/// </param>
/// <param name="Resumes">
/// How many times this Peer's transport has been successfully resumed after an Interruption (issue
/// #128, ADR 0013). Incremented once per <see cref="SessionEventKind.PeerResumed"/> raised for this
/// Peer, on both the listening and dialling sides, and never on a refused resume. Earns its own
/// member rather than being derived by the consumer counting <see cref="SessionEventKind.PeerResumed"/>
/// Events: a Session profile drains Events into whatever cadence the consumer chooses, so counting
/// them back out is not generally possible once drained, while this figure is a Counter — read fresh
/// by value at any time, the same as every other figure here. Monotonic and never reset: a resume
/// carries no counter's history away with it, so a retransmission count spanning several resumes is
/// how a flapping path shows itself instead of being hidden by a zeroing on every reconnect.
/// </param>
public readonly record struct PeerCounters(ulong MessagesReceived, ulong RetentionHighWaterBytes, ulong Resumes);
