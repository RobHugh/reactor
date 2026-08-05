using System.Net.Sockets;

namespace ReactorSockets.Transports;

/// <summary>The result of one <see cref="TcpLink.Shutdown"/> call.</summary>
internal readonly struct TcpLinkShutdownResult
{
    private TcpLinkShutdownResult(bool isSuccess, SocketError error)
    {
        IsSuccess = isSuccess;
        Error = error;
    }

    /// <summary>Whether the socket's send half was shut down successfully.</summary>
    public bool IsSuccess { get; }

    /// <summary>The platform error, populated only when <see cref="IsSuccess"/> is <see langword="false"/>.</summary>
    public SocketError Error { get; }

    /// <summary>Builds a successful result.</summary>
    public static TcpLinkShutdownResult ForSuccess() => new(true, default);

    /// <summary>Builds a failed result carrying the platform error.</summary>
    public static TcpLinkShutdownResult ForFailed(SocketError error) => new(false, error);
}
