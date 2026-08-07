namespace ReactorSockets;

/// <summary>
/// One event drained from a Session Reactor. A <see langword="ref struct"/> because the data plane
/// that lands in a later slice delivers a Message as a span aliasing the frame arena directly, which
/// must never outlive the Frame it was drained in (ADR 0008); the shape is fixed now so that adding
/// that field later is not a breaking change. <see cref="Kind"/> determines exactly which other
/// fields are populated.
/// </summary>
public readonly ref struct SessionEvent
{
    private SessionEvent(SessionEventKind kind, PeerId peer, DisconnectReason reason)
    {
        Kind = kind;
        Peer = peer;
        Reason = reason;
    }

    /// <summary>Which kind of event this is.</summary>
    public SessionEventKind Kind { get; }

    /// <summary>The Peer this event concerns. Always populated.</summary>
    public PeerId Peer { get; }

    /// <summary>
    /// Why the Session ended. Populated only when <see cref="Kind"/> is
    /// <see cref="SessionEventKind.PeerClosed"/>.
    /// </summary>
    public DisconnectReason Reason { get; }

    /// <summary>
    /// Builds a <see cref="SessionEventKind.PeerConnected"/> event. Carries only <see cref="Peer"/> —
    /// the address on the far end was either supplied by the consumer's own dial or is a claim the
    /// Reactor has no business promoting to an identity, and the wire session id is a demultiplexing
    /// token this library owns, not something a consumer addresses anything with.
    /// </summary>
    internal static SessionEvent ForPeerConnected(PeerId peer) =>
        new(SessionEventKind.PeerConnected, peer, default);

    /// <summary>Builds a <see cref="SessionEventKind.PeerClosed"/> event naming why the Session ended.</summary>
    internal static SessionEvent ForPeerClosed(PeerId peer, DisconnectReason reason) =>
        new(SessionEventKind.PeerClosed, peer, reason);
}
