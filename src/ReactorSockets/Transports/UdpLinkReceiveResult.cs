using System.Net.Sockets;

namespace ReactorSockets.Transports;

/// <summary>
/// The result of one <see cref="UdpLink.Receive"/> call. A <see langword="ref struct"/> because
/// <see cref="Payload"/> aliases the caller-supplied scratch buffer and must not outlive it.
/// </summary>
internal readonly ref struct UdpLinkReceiveResult
{
    private UdpLinkReceiveResult(UdpLinkReceiveOutcome outcome, Source source, ReadOnlySpan<byte> payload, SocketError error)
    {
        Outcome = outcome;
        Source = source;
        Payload = payload;
        Error = error;
    }

    /// <summary>Which of the four outcomes this receive resolved to.</summary>
    public UdpLinkReceiveOutcome Outcome { get; }

    /// <summary>
    /// Who the datagram claims to be from. Populated for <see cref="UdpLinkReceiveOutcome.Delivered"/> and,
    /// when the platform makes it available, <see cref="UdpLinkReceiveOutcome.Truncated"/>.
    /// </summary>
    public Source Source { get; }

    /// <summary>
    /// The received bytes, sliced from the caller's scratch buffer. Populated only for
    /// <see cref="UdpLinkReceiveOutcome.Delivered"/>.
    /// </summary>
    public ReadOnlySpan<byte> Payload { get; }

    /// <summary>
    /// The platform error, populated for <see cref="UdpLinkReceiveOutcome.Unreachable"/> and
    /// <see cref="UdpLinkReceiveOutcome.Failed"/>.
    /// </summary>
    public SocketError Error { get; }

    public static UdpLinkReceiveResult ForDelivered(Source source, ReadOnlySpan<byte> payload) =>
        new(UdpLinkReceiveOutcome.Delivered, source, payload, default);

    public static UdpLinkReceiveResult ForTruncated(Source source) =>
        new(UdpLinkReceiveOutcome.Truncated, source, default, default);

    public static UdpLinkReceiveResult ForUnreachable(SocketError error) =>
        new(UdpLinkReceiveOutcome.Unreachable, default, default, error);

    public static UdpLinkReceiveResult ForFailed(SocketError error) =>
        new(UdpLinkReceiveOutcome.Failed, default, default, error);
}
