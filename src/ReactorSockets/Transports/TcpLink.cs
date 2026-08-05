using System.Net;
using System.Net.Sockets;

namespace ReactorSockets.Transports;

/// <summary>
/// L1: one connected TCP socket, produced either by a listening Link's accept or (a later sub-issue
/// of this slice) by dialling out. Carries only the socket and its bound local address for now —
/// reads, sends and half-close join this type as the sub-issues that need them land, exactly as
/// <see cref="UdpLink"/> was built out incrementally in its own slice.
/// </summary>
internal sealed class TcpLink : IDisposable
{
    private TcpLink(Socket socket, IPEndPoint localEndPoint)
    {
        Socket = socket;
        LocalEndPoint = localEndPoint;
    }

    /// <summary>The socket's own local address, constant for this Link's lifetime.</summary>
    public IPEndPoint LocalEndPoint { get; }

    /// <summary>The underlying socket, registered with the readiness dispatcher by the caller.</summary>
    public Socket Socket { get; }

    /// <summary>
    /// Wraps a socket a listening Link just pulled off its backlog. TCP binds the local end
    /// implicitly on accept, so <see cref="LocalEndPoint"/> is read back from the socket rather than
    /// supplied by the caller.
    /// </summary>
    public static TcpLink FromAccepted(Socket acceptedSocket) =>
        new(acceptedSocket, (IPEndPoint)acceptedSocket.LocalEndPoint!);

    public void Dispose() => Socket.Dispose();
}
