namespace ReactorSockets;

/// <summary>
/// Which kind of <see cref="SessionEvent"/> was drained, and therefore which of its fields carry
/// meaning. Shared by both Session profiles (ADR 0010).
/// </summary>
/// <remarks>
/// This slice establishes and refuses Sessions; it carries no data. The roster is therefore the two
/// events a handshake can produce, and grows with the data plane, the Goodbye exchange and Resume in
/// later slices. Members are appended, never inserted.
/// </remarks>
public enum SessionEventKind
{
    /// <summary>
    /// A Session is established and the Peer is addressable. Raised on both ends: on the dialler when
    /// the Accept arrives, on the accepting end when a cookie-validated Hello is admitted.
    /// </summary>
    PeerConnected = 1,

    /// <summary>
    /// A Session ended, or a dial never became one. <see cref="SessionEvent.Reason"/> says why, and
    /// the <see cref="PeerId"/> is dead from this event onwards.
    /// </summary>
    PeerClosed = 2,
}
