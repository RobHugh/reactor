namespace ReactorSockets.Core;

/// <summary>
/// One Frame-queued event, cheap enough to add on the hot path and translated to the public
/// <see cref="SessionEvent"/> only when <c>Drain</c> actually asks for it (ADR 0008). Shared by both
/// Session profiles: the datagram profile never populates <see cref="PayloadOffset"/> or
/// <see cref="PayloadLength"/>, since it has no <see cref="SessionEventKind.MessageReceived"/> yet, but
/// the shape itself — which Peer, which kind, why it closed — is identical either way.
/// </summary>
internal readonly record struct PendingEvent
{
    public SessionEventKind Kind { get; init; }

    public PeerId Peer { get; init; }

    public DisconnectReason Reason { get; init; }

    /// <summary>
    /// The committed Message's offset into the frame arena. Populated only when <see cref="Kind"/>
    /// is <see cref="SessionEventKind.MessageReceived"/>.
    /// </summary>
    public int PayloadOffset { get; init; }

    /// <summary>
    /// The committed Message's length. Populated only when <see cref="Kind"/> is
    /// <see cref="SessionEventKind.MessageReceived"/>.
    /// </summary>
    public int PayloadLength { get; init; }
}
