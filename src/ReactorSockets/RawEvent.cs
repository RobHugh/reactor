namespace ReactorSockets;

/// <summary>
/// One event drained from a <see cref="RawReactor"/>. A <see langword="ref struct"/> because
/// <see cref="Payload"/> aliases the frame arena directly and must never outlive the Frame it was
/// drained in (ADR 0008). <see cref="Kind"/> determines exactly which other fields are populated —
/// this slice implements <see cref="RawEventKind.DatagramReceived"/>, <see cref="RawEventKind.Closed"/>,
/// <see cref="RawEventKind.Accepted"/> and <see cref="RawEventKind.Connected"/>; the remaining stream
/// kinds join this roster as the rest of slice 2 lands.
/// </summary>
public readonly ref struct RawEvent
{
    private RawEvent(RawEventKind kind, LinkId link, LinkId listener, Source source, ReadOnlySpan<byte> payload, CloseReason reason)
    {
        Kind = kind;
        Link = link;
        Listener = listener;
        Source = source;
        Payload = payload;
        Reason = reason;
    }

    /// <summary>Which kind of event this is.</summary>
    public RawEventKind Kind { get; }

    /// <summary>
    /// The Link this event concerns. Always populated — for <see cref="RawEventKind.Accepted"/> this
    /// is the new connection's identity, not the listener's (see <see cref="Listener"/> for that).
    /// </summary>
    public LinkId Link { get; }

    /// <summary>
    /// The listening Link that produced this accepted connection. Populated only when
    /// <see cref="Kind"/> is <see cref="RawEventKind.Accepted"/>; a consumer running several listeners
    /// needs this to know which one just fired.
    /// </summary>
    public LinkId Listener { get; }

    /// <summary>
    /// Who claims to have sent this datagram — an unauthenticated hint, never an identity. Populated
    /// only when <see cref="Kind"/> is <see cref="RawEventKind.DatagramReceived"/>.
    /// </summary>
    public Source Source { get; }

    /// <summary>
    /// The datagram's bytes, valid only until the next <see cref="RawReactor.Pump"/>. Populated only
    /// when <see cref="Kind"/> is <see cref="RawEventKind.DatagramReceived"/>.
    /// </summary>
    public ReadOnlySpan<byte> Payload { get; }

    /// <summary>Why the Link closed. Populated only when <see cref="Kind"/> is <see cref="RawEventKind.Closed"/>.</summary>
    public CloseReason Reason { get; }

    /// <summary>Builds a <see cref="RawEventKind.DatagramReceived"/> event.</summary>
    internal static RawEvent ForDatagramReceived(LinkId link, Source source, ReadOnlySpan<byte> payload) =>
        new(RawEventKind.DatagramReceived, link, default, source, payload, default);

    /// <summary>Builds a <see cref="RawEventKind.Closed"/> event.</summary>
    internal static RawEvent ForClosed(LinkId link, CloseReason reason) =>
        new(RawEventKind.Closed, link, default, default, default, reason);

    /// <summary>
    /// Builds a <see cref="RawEventKind.Accepted"/> event naming both the listener that fired and the
    /// new connection it produced. Carries no address (ADR 0014): <c>RemoteOf</c>/<c>LocalOf</c> are
    /// Link state, constant for the Link's lifetime, and answering that question here would make
    /// <see cref="Source"/> stop being exclusively the per-datagram claim.
    /// </summary>
    internal static RawEvent ForAccepted(LinkId listener, LinkId link) =>
        new(RawEventKind.Accepted, link, listener, default, default, default);

    /// <summary>
    /// Builds a <see cref="RawEventKind.Connected"/> event for a dial that completed successfully.
    /// Carries only <see cref="Link"/> — the address a dial reaches was supplied by the consumer's own
    /// call to <c>Connect</c>, so repeating it here would tell them nothing new.
    /// </summary>
    internal static RawEvent ForConnected(LinkId link) =>
        new(RawEventKind.Connected, link, default, default, default, default);
}
