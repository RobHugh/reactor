namespace ReactorSockets;

/// <summary>
/// One event drained from a Session Reactor. A <see langword="ref struct"/> because
/// <see cref="Payload"/> aliases the frame arena directly, and must never outlive the Frame it was
/// drained in (ADR 0008). <see cref="Kind"/> determines exactly which other fields are populated.
/// </summary>
public readonly ref struct SessionEvent
{
    private SessionEvent(SessionEventKind kind, PeerId peer, DisconnectReason reason, ReadOnlySpan<byte> payload)
    {
        Kind = kind;
        Peer = peer;
        Reason = reason;
        Payload = payload;
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
    /// The whole Message this event carries, aliasing the frame arena. Populated only when
    /// <see cref="Kind"/> is <see cref="SessionEventKind.MessageReceived"/>, and valid only until the
    /// next Pump (ADR 0008) — copy it out before draining or pumping again.
    /// </summary>
    public ReadOnlySpan<byte> Payload { get; }

    /// <summary>
    /// Builds a <see cref="SessionEventKind.PeerConnected"/> event. Carries only <see cref="Peer"/> —
    /// the address on the far end was either supplied by the consumer's own dial or is a claim the
    /// Reactor has no business promoting to an identity, and the wire session id is a demultiplexing
    /// token this library owns, not something a consumer addresses anything with.
    /// </summary>
    internal static SessionEvent ForPeerConnected(PeerId peer) =>
        new(SessionEventKind.PeerConnected, peer, default, default);

    /// <summary>Builds a <see cref="SessionEventKind.PeerClosed"/> event naming why the Session ended.</summary>
    internal static SessionEvent ForPeerClosed(PeerId peer, DisconnectReason reason) =>
        new(SessionEventKind.PeerClosed, peer, reason, default);

    /// <summary>
    /// Builds a <see cref="SessionEventKind.MessageReceived"/> event carrying a whole Message aliasing
    /// the frame arena it was just copied into.
    /// </summary>
    internal static SessionEvent ForMessageReceived(PeerId peer, ReadOnlySpan<byte> payload) =>
        new(SessionEventKind.MessageReceived, peer, default, payload);

    /// <summary>
    /// Builds a <see cref="SessionEventKind.PeerClosing"/> event. Carries only <see cref="Peer"/> — the
    /// reason a Closing Peer will eventually be gone is always the Goodbye that put it there, so there
    /// is nothing for <see cref="Reason"/> to add.
    /// </summary>
    internal static SessionEvent ForPeerClosing(PeerId peer) =>
        new(SessionEventKind.PeerClosing, peer, default, default);

    /// <summary>
    /// Builds a <see cref="SessionEventKind.PeerInterrupted"/> event. Carries only <see cref="Peer"/> —
    /// nothing has ended, so there is no <see cref="DisconnectReason"/> to report.
    /// </summary>
    internal static SessionEvent ForPeerInterrupted(PeerId peer) =>
        new(SessionEventKind.PeerInterrupted, peer, default, default);

    /// <summary>
    /// Builds a <see cref="SessionEventKind.PeerResumed"/> event. Carries only <see cref="Peer"/> —
    /// nothing has ended, so there is no <see cref="DisconnectReason"/> to report, exactly as
    /// <see cref="ForPeerInterrupted"/>.
    /// </summary>
    internal static SessionEvent ForPeerResumed(PeerId peer) =>
        new(SessionEventKind.PeerResumed, peer, default, default);
}
