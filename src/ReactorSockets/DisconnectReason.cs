namespace ReactorSockets;

/// <summary>
/// Why a Peer's Session ended, defined and kept stable by this library rather than borrowed from a
/// platform enumeration. Deliberately not shared with the Raw profile's <see cref="CloseReason"/>
/// (ADR 0008): most of the members a Session needs are protocol vocabulary Raw can never produce, and
/// a single shared enumeration would leave one profile documenting members it cannot emit.
/// </summary>
/// <remarks>
/// <para>
/// Explicit values throughout, house style since <c>RawLinkLegality</c> (PR #70): a member is added at
/// the end with the next free number, never inserted, so a value a consumer has already persisted
/// keeps its meaning across a library upgrade.
/// </para>
/// <para>
/// Every place this library maps something else onto a <see cref="DisconnectReason"/> — a wire
/// <see cref="Session.RefusalReason"/>, a malformed-framing outcome, a Goodbye control record — states
/// that mapping as its own table with a discard arm that refuses rather than permits, the same house
/// rule <c>RawLinkLegality</c> follows: an input this library does not recognise fails closed instead
/// of being guessed at.
/// </para>
/// </remarks>
public enum DisconnectReason
{
    /// <summary>The consumer called <c>Close</c> on this Peer.</summary>
    LocalClose = 0,

    /// <summary>
    /// The two ends do not speak the same protocol version. The version match is exact — there is no
    /// negotiation, no lowest-common-version rule and no compatibility range — so this is produced
    /// both by the end that refused and by the end whose Hello was refused.
    /// </summary>
    VersionMismatch = 1,

    /// <summary>
    /// The remote end had no capacity Slot free for this Session and said so. Distinct from silence:
    /// a Reactor at capacity answers a Hello it received rather than dropping it, so the dialler learns
    /// the difference between "full" and "gone".
    /// </summary>
    RemoteAtCapacity = 2,

    /// <summary>
    /// The underlying transport failed or ended before or during the handshake: a reset, a dial that
    /// could not connect, or a stream that closed before a full handshake record arrived. Distinct
    /// from every other member here because nothing about the protocol was at fault — the byte stream
    /// itself did not survive to carry it.
    /// </summary>
    TransportFailed = 3,

    /// <summary>
    /// An established Peer's Message framing broke this profile's rules: a length prefix exceeding
    /// the announced <c>MaxMessageSize</c>, a length prefix of zero, or a stream that ended before a
    /// length prefix or the Message body it named had fully arrived. Distinct from
    /// <see cref="TransportFailed"/>: the byte stream itself survived long enough to carry the
    /// violation, and what is wrong is the bytes it carried rather than the transport underneath it.
    /// </summary>
    ProtocolViolation = 4,

    /// <summary>
    /// The remote Peer sent a Goodbye: it is ending the Session on purpose rather than failing (issue
    /// #23). Produced once whatever this end still held for it has finished flushing — immediately, if
    /// nothing was outstanding when the Goodbye arrived, or after the <c>Closing</c> Session it entered
    /// (<see cref="SessionEventKind.PeerClosing"/>) has drained.
    /// </summary>
    PeerGoodbye = 5,
}
