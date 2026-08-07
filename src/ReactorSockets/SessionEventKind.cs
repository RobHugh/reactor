namespace ReactorSockets;

/// <summary>
/// Which kind of <see cref="SessionEvent"/> was drained, and therefore which of its fields carry
/// meaning. Shared by both Session profiles (ADR 0010).
/// </summary>
/// <remarks>
/// The roster grows with Resume across later slices. Members are appended, never inserted.
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

    /// <summary>
    /// A whole Message was reassembled from the Peer's stream and copied into the frame arena.
    /// <see cref="SessionEvent.Payload"/> carries it, valid only until the next Pump (ADR 0008).
    /// </summary>
    MessageReceived = 3,

    /// <summary>
    /// The Peer's Goodbye arrived while this end still had bytes of its own outstanding for it — still
    /// being flushed to the socket. The Peer accepts no further <c>Send</c>s worth making from here
    /// (issue #23) and a <see cref="SessionEventKind.PeerClosed"/> carrying
    /// <see cref="DisconnectReason.PeerGoodbye"/> follows once that flush drains. Never raised when
    /// nothing was outstanding — a Goodbye that finds the Peer already caught up closes it directly,
    /// with no zero-Frame Closing observable in between.
    /// </summary>
    PeerClosing = 4,
}
