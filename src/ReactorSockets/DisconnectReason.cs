namespace ReactorSockets;

/// <summary>
/// Why a Peer's Session ended, defined and kept stable by this library rather than borrowed from a
/// platform enumeration. Deliberately not shared with the Raw profile's <see cref="CloseReason"/>
/// (ADR 0008): most of the members a Session needs are protocol vocabulary Raw can never produce, and
/// a single shared enumeration would leave one profile documenting members it cannot emit.
/// </summary>
/// <remarks>
/// This slice establishes Sessions and refuses them; it does not yet carry data, close them
/// gracefully or resume them. The roster is therefore only what the handshake itself can produce, and
/// grows as the Goodbye exchange, the liveness deadline and Resume land in later slices. Every member
/// added later is added at the end, so a value a consumer has persisted keeps its meaning.
/// </remarks>
public enum DisconnectReason
{
    /// <summary>The consumer called <c>Close</c> on this Peer.</summary>
    LocalClose,

    /// <summary>
    /// The two ends do not speak the same protocol version. The version match is exact — there is no
    /// negotiation, no lowest-common-version rule and no compatibility range — so this is produced
    /// both by the end that refused and by the end whose Hello was refused.
    /// </summary>
    VersionMismatch,

    /// <summary>
    /// The remote end had no capacity Slot free for this Session and said so. Distinct from silence:
    /// a Reactor at capacity answers a cookie-validated Hello rather than dropping it, so the dialler
    /// learns the difference between "full" and "gone".
    /// </summary>
    RemoteAtCapacity,
}
