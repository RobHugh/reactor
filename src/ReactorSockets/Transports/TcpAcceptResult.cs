using System.Net.Sockets;

namespace ReactorSockets.Transports;

/// <summary>The result of one <see cref="TcpListenLink.TryAccept"/> call.</summary>
internal readonly struct TcpAcceptResult
{
    private TcpAcceptResult(TcpAcceptOutcome outcome, Socket? socket, SocketError error)
    {
        Outcome = outcome;
        Socket = socket;
        Error = error;
    }

    /// <summary>Which of the three outcomes this accept attempt resolved to.</summary>
    public TcpAcceptOutcome Outcome { get; }

    /// <summary>The newly accepted socket. Populated only for <see cref="TcpAcceptOutcome.Accepted"/>.</summary>
    public Socket? Socket { get; }

    /// <summary>The platform error. Populated only for <see cref="TcpAcceptOutcome.Failed"/>.</summary>
    public SocketError Error { get; }

    /// <summary>Builds a <see cref="TcpAcceptOutcome.NoPendingConnection"/> result.</summary>
    public static TcpAcceptResult ForNoPendingConnection() => new(TcpAcceptOutcome.NoPendingConnection, null, default);

    /// <summary>Builds a <see cref="TcpAcceptOutcome.Accepted"/> result carrying the new socket.</summary>
    public static TcpAcceptResult ForAccepted(Socket socket) => new(TcpAcceptOutcome.Accepted, socket, default);

    /// <summary>Builds a <see cref="TcpAcceptOutcome.Failed"/> result carrying the platform error.</summary>
    public static TcpAcceptResult ForFailed(SocketError error) => new(TcpAcceptOutcome.Failed, null, error);
}
