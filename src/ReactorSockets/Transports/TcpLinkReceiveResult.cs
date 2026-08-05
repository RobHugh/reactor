using System.Net.Sockets;

namespace ReactorSockets.Transports;

/// <summary>
/// The result of one <see cref="TcpLink.Receive"/> call. A <see langword="ref struct"/> because
/// <see cref="Payload"/> aliases the caller-supplied scratch buffer and must not outlive it.
/// </summary>
internal readonly ref struct TcpLinkReceiveResult
{
    private TcpLinkReceiveResult(TcpLinkReceiveOutcome outcome, ReadOnlySpan<byte> payload, SocketError error)
    {
        Outcome = outcome;
        Payload = payload;
        Error = error;
    }

    /// <summary>Which of the three outcomes this receive resolved to.</summary>
    public TcpLinkReceiveOutcome Outcome { get; }

    /// <summary>
    /// The received bytes, sliced from the caller's scratch buffer. Populated only for
    /// <see cref="TcpLinkReceiveOutcome.Delivered"/>. No claim is made that this is a whole anything —
    /// framing is the consumer's decision, not this library's.
    /// </summary>
    public ReadOnlySpan<byte> Payload { get; }

    /// <summary>The platform error, populated only for <see cref="TcpLinkReceiveOutcome.Failed"/>.</summary>
    public SocketError Error { get; }

    public static TcpLinkReceiveResult ForDelivered(ReadOnlySpan<byte> payload) =>
        new(TcpLinkReceiveOutcome.Delivered, payload, default);

    public static TcpLinkReceiveResult ForEndOfStream() =>
        new(TcpLinkReceiveOutcome.EndOfStream, default, default);

    public static TcpLinkReceiveResult ForFailed(SocketError error) =>
        new(TcpLinkReceiveOutcome.Failed, default, error);
}
