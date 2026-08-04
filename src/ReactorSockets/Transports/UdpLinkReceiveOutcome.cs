namespace ReactorSockets.Transports;

/// <summary>
/// The outcome of a single <see cref="UdpLink.Receive"/> call, separating the four platform-visible
/// cases L2 must treat differently (ADR 0008): a normal delivery, a datagram larger than the scratch
/// buffer, an ICMP port-unreachable surfacing as <c>WSAECONNRESET</c> on this receive, and anything
/// else.
/// </summary>
internal enum UdpLinkReceiveOutcome
{
    /// <summary>The datagram fit in the scratch buffer and was received whole.</summary>
    Delivered,

    /// <summary>
    /// The datagram exceeded the scratch buffer. On Windows the OS discards it before delivery; the
    /// same is enforced here on every platform so truncation behaves identically everywhere.
    /// </summary>
    Truncated,

    /// <summary>
    /// An ICMP port-unreachable for an earlier send surfaced as <c>WSAECONNRESET</c> on this receive.
    /// The Link is unaffected; nothing was received.
    /// </summary>
    Unreachable,

    /// <summary>The receive failed for a reason that ends the Link.</summary>
    Failed,
}
