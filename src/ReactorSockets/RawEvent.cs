namespace ReactorSockets;

/// <summary>
/// One event drained from a <see cref="RawReactor"/>. A <see langword="ref struct"/> because
/// <see cref="Payload"/> aliases the frame arena directly and must never outlive the Frame it was
/// drained in (ADR 0008). <see cref="Kind"/> determines exactly which other fields are populated —
/// the kinds are <see cref="RawEventKind.DatagramReceived"/>, <see cref="RawEventKind.Closed"/>,
/// <see cref="RawEventKind.Accepted"/>, <see cref="RawEventKind.Connected"/>,
/// <see cref="RawEventKind.SegmentReceived"/>, <see cref="RawEventKind.EndOfStream"/> and
/// <see cref="RawEventKind.Writable"/> — the complete Raw profile roster.
/// </summary>
public readonly ref struct RawEvent
{
    private RawEvent(RawEventKind kind, LinkId link, LinkId listeningLink, Source source, ReadOnlySpan<byte> payload, CloseReason reason)
    {
        Kind = kind;
        Link = link;
        ListeningLink = listeningLink;
        Source = source;
        Payload = payload;
        Reason = reason;
    }

    /// <summary>Which kind of event this is.</summary>
    public RawEventKind Kind { get; }

    /// <summary>
    /// The Link this event concerns. Always populated — for <see cref="RawEventKind.Accepted"/> this
    /// is the new connection's identity, not the listening Link's (see <see cref="ListeningLink"/>
    /// for that).
    /// </summary>
    public LinkId Link { get; }

    /// <summary>
    /// The listening Link that produced this accepted connection. Populated only when
    /// <see cref="Kind"/> is <see cref="RawEventKind.Accepted"/>; a consumer listening on several
    /// Links needs this to know which one just fired. Named for the Link rather than a "Listener"
    /// deliberately: listening is a role a Link plays, not a kind of thing this library names.
    /// </summary>
    public LinkId ListeningLink { get; }

    /// <summary>
    /// Who claims to have sent this datagram — an unauthenticated hint, never an identity. Populated
    /// only when <see cref="Kind"/> is <see cref="RawEventKind.DatagramReceived"/>.
    /// </summary>
    public Source Source { get; }

    /// <summary>
    /// The received bytes, valid only until the next <see cref="RawReactor.Pump"/>. Populated when
    /// <see cref="Kind"/> is <see cref="RawEventKind.DatagramReceived"/> or
    /// <see cref="RawEventKind.SegmentReceived"/>. For a Segment this is exactly the run of bytes that
    /// arrived on this Link in this Frame — no claim of being a whole anything, and nothing is held
    /// across a Pump to make one (ADR 0008).
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
    /// Builds a <see cref="RawEventKind.Accepted"/> event naming both the listening Link that fired
    /// and the new connection it produced. Carries no address (ADR 0014): <see cref="RawReactor.RemoteOf"/>
    /// and <see cref="RawReactor.LocalOf"/> are Link state, constant for the Link's lifetime, and
    /// answering that question here would make
    /// <see cref="Source"/> stop being exclusively the per-datagram claim.
    /// </summary>
    internal static RawEvent ForAccepted(LinkId listeningLink, LinkId link) =>
        new(RawEventKind.Accepted, link, listeningLink, default, default, default);

    /// <summary>
    /// Builds a <see cref="RawEventKind.Connected"/> event for a dial that completed successfully.
    /// Carries only <see cref="Link"/> — the address a dial reaches was supplied by the consumer's own
    /// call to <c>Connect</c>, so repeating it here would tell them nothing new.
    /// </summary>
    internal static RawEvent ForConnected(LinkId link) =>
        new(RawEventKind.Connected, link, default, default, default, default);

    /// <summary>
    /// Builds a <see cref="RawEventKind.SegmentReceived"/> event carrying exactly the run of bytes
    /// that arrived on <paramref name="link"/> this Frame.
    /// </summary>
    internal static RawEvent ForSegmentReceived(LinkId link, ReadOnlySpan<byte> payload) =>
        new(RawEventKind.SegmentReceived, link, default, default, payload, default);

    /// <summary>
    /// Builds a <see cref="RawEventKind.EndOfStream"/> event. Carries only <see cref="Link"/> — there
    /// is nothing else to say: the remote will send nothing further while this Link stays open for
    /// sending, and this is deliberately not a <see cref="RawEventKind.Closed"/>.
    /// </summary>
    internal static RawEvent ForEndOfStream(LinkId link) =>
        new(RawEventKind.EndOfStream, link, default, default, default, default);

    /// <summary>
    /// Builds a <see cref="RawEventKind.Writable"/> event. Carries only <see cref="Link"/> — this is a
    /// reply to a short <c>Send</c> the consumer already made on it, not new information about the
    /// remote end.
    /// </summary>
    internal static RawEvent ForWritable(LinkId link) =>
        new(RawEventKind.Writable, link, default, default, default, default);
}
