namespace ReactorSockets;

/// <summary>
/// Which transport a Link wraps, and therefore which of <see cref="RawReactor"/>'s kind-sensitive
/// members it admits. A Link's kind is fixed when it is bound and never changes, so this is the whole
/// of what <see cref="RawLinkLegality"/> needs to decide a call.
/// </summary>
/// <remarks>
/// Internal, and deliberately so: no consumer ever names a kind. A Link is a Link on the public
/// surface, and asking one what it is made of is not something the L2 contract offers (ADR 0003,
/// ADR 0008) — a consumer knows which kind it bound because it chose the member that bound it.
/// </remarks>
internal enum LinkKind
{
    /// <summary>A bound UDP socket, which sends with <c>SendTo</c> and answers many Sources.</summary>
    Datagram,

    /// <summary>
    /// A TCP connection, whether accepted or dialled — the two are indistinguishable once registered
    /// and admit exactly the same operations.
    /// </summary>
    Stream,

    /// <summary>
    /// A listening TCP socket. Listening is a role a Link plays rather than a separate species of
    /// thing, which is why this member is an adjective and there is no such term as Listener.
    /// </summary>
    Listening,
}

/// <summary>
/// The members of <see cref="RawReactor"/>'s public surface whose legality depends on what kind of
/// Link they are handed. Members that are legal on every kind — <c>Close</c>, <c>LocalOf</c>,
/// <c>CountersOf</c> — have no member here, because there is nothing for the matrix to say about them.
/// </summary>
internal enum RawOperation
{
    /// <summary>Stream-only. <see cref="RawReactor.Send"/>.</summary>
    Send,

    /// <summary>Datagram-only. <see cref="RawReactor.SendTo"/>.</summary>
    SendTo,

    /// <summary>Stream-only. <see cref="RawReactor.Shutdown"/>.</summary>
    Shutdown,

    /// <summary>Stream-only. <see cref="RawReactor.RemoteOf"/>.</summary>
    RemoteOf,
}

/// <summary>
/// The Link legality matrix: for every kind-sensitive operation and every kind of Link, either nothing
/// (the call is legal) or the prose explaining the refusal. Stated once, here, rather than inferred a
/// branch at a time inside each member.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a table and not a chain of type tests.</b> Each member used to establish the kind by
/// elimination — <c>if (slot is not StreamLinkSlot)</c> then report "illegal on a listening Link" —
/// which is only correct while exactly three kinds exist. A fourth would have made all four members
/// confidently name the wrong kind in a public error message. Here every pair is written down, so no
/// kind is ever inferred from the absence of the others.
/// </para>
/// <para>
/// <b>The discard arm refuses rather than permits.</b> C# cannot prove a switch over enums exhaustive
/// — an enum can hold values no member names — so a discard is mandatory whatever we do. Making it
/// refuse means a kind whose row is missing fails closed: it is rejected everywhere until someone
/// writes its row, which the first test to touch it will say loudly. Permitting by default would have
/// relocated the original defect rather than fixed it. The arm is unreachable today; it is a net, not
/// a branch.
/// </para>
/// <para>
/// The other half of that safety is <c>LinkSlot.Kind</c> being abstract: a new kind of Link cannot
/// compile without declaring which <see cref="LinkKind"/> it is, so nobody adds one without being sent
/// here.
/// </para>
/// </remarks>
internal static class RawLinkLegality
{
    /// <summary>
    /// The reason <paramref name="operation"/> may not be called on a Link of kind
    /// <paramref name="kind"/>, or <see langword="null"/> when the call is legal.
    /// </summary>
    /// <remarks>
    /// The refusals name no <see cref="LinkId"/>, because an illegality is a fact about the call and
    /// the kind rather than about which Link was passed — unlike "is not open", which is precisely a
    /// statement about one id.
    /// </remarks>
    public static string? RefusalFor(RawOperation operation, LinkKind kind) => (operation, kind) switch
    {
        (RawOperation.Send, LinkKind.Stream) => null,
        (RawOperation.Send, LinkKind.Datagram) => "Send is illegal on a datagram Link — use SendTo.",
        (RawOperation.Send, LinkKind.Listening) => "Send is illegal on a listening Link.",

        (RawOperation.SendTo, LinkKind.Datagram) => null,
        (RawOperation.SendTo, LinkKind.Stream) => "SendTo is illegal on a stream Link — use Send.",
        (RawOperation.SendTo, LinkKind.Listening) => "SendTo is illegal on a listening Link.",

        (RawOperation.Shutdown, LinkKind.Stream) => null,
        (RawOperation.Shutdown, LinkKind.Datagram) => "Shutdown is illegal on a datagram Link.",
        (RawOperation.Shutdown, LinkKind.Listening) => "Shutdown is illegal on a listening Link.",

        (RawOperation.RemoteOf, LinkKind.Stream) => null,
        (RawOperation.RemoteOf, LinkKind.Datagram) =>
            "RemoteOf is illegal on a datagram Link, which is not a stream Link — a bound UDP socket answers many senders rather than one remote.",
        (RawOperation.RemoteOf, LinkKind.Listening) =>
            "RemoteOf is illegal on a listening Link, which is not a stream Link — a listening socket answers no remote at all.",

        _ => $"{operation} is illegal on this Link.",
    };
}
