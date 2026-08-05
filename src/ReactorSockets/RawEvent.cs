namespace ReactorSockets;

/// <summary>
/// One event drained from a <see cref="RawReactor"/>. A <see langword="ref struct"/> because
/// <see cref="Payload"/> aliases the frame arena directly and must never outlive the Frame it was
/// drained in (ADR 0008). <see cref="Kind"/> determines exactly which other fields are populated —
/// this slice implements <see cref="RawEventKind.DatagramReceived"/> and <see cref="RawEventKind.Closed"/>
/// only.
/// </summary>
public readonly ref struct RawEvent
{
    private RawEvent(RawEventKind kind, LinkId link, Source source, ReadOnlySpan<byte> payload, CloseReason reason)
    {
        Kind = kind;
        Link = link;
        Source = source;
        Payload = payload;
        Reason = reason;
    }

    /// <summary>Which kind of event this is.</summary>
    public RawEventKind Kind { get; }

    /// <summary>The Link this event concerns. Always populated.</summary>
    public LinkId Link { get; }

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
        new(RawEventKind.DatagramReceived, link, source, payload, default);

    /// <summary>Builds a <see cref="RawEventKind.Closed"/> event.</summary>
    internal static RawEvent ForClosed(LinkId link, CloseReason reason) =>
        new(RawEventKind.Closed, link, default, default, reason);
}
