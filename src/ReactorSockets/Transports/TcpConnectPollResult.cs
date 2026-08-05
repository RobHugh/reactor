using System.Net.Sockets;

namespace ReactorSockets.Transports;

/// <summary>The result of one <see cref="TcpLink.PollConnect"/> call.</summary>
internal readonly struct TcpConnectPollResult
{
    private TcpConnectPollResult(TcpConnectPollOutcome outcome, SocketError error)
    {
        Outcome = outcome;
        Error = error;
    }

    /// <summary>Which of the three outcomes this poll resolved to.</summary>
    public TcpConnectPollOutcome Outcome { get; }

    /// <summary>The platform error. Populated only for <see cref="TcpConnectPollOutcome.Failed"/>.</summary>
    public SocketError Error { get; }

    /// <summary>Builds a <see cref="TcpConnectPollOutcome.Pending"/> result.</summary>
    public static TcpConnectPollResult ForPending() => new(TcpConnectPollOutcome.Pending, default);

    /// <summary>Builds a <see cref="TcpConnectPollOutcome.Connected"/> result.</summary>
    public static TcpConnectPollResult ForConnected() => new(TcpConnectPollOutcome.Connected, default);

    /// <summary>Builds a <see cref="TcpConnectPollOutcome.Failed"/> result carrying the platform error.</summary>
    public static TcpConnectPollResult ForFailed(SocketError error) => new(TcpConnectPollOutcome.Failed, error);
}
