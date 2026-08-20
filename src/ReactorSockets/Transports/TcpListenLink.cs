using System.Net;
using System.Net.Sockets;

namespace ReactorSockets.Transports;

/// <summary>
/// L1: one listening TCP socket. "Listener" is a role this Link plays for a consumer, not a kind of
/// thing this library names (ADR 0008) — this type exists only because L2 needs somewhere to hold the
/// listening socket and its accept behaviour, exactly as <see cref="UdpLink"/> holds a bound datagram
/// socket's.
/// </summary>
/// <remarks>
/// <see cref="Socket.DualMode"/> is never set, for the same reason as <see cref="UdpLink"/>: a consumer
/// wanting both address families binds twice. On Windows, <c>ExclusiveAddressUse</c> is set
/// explicitly so binds are exclusive on every platform identically. The backlog length passed to
/// <see cref="Socket.Listen()"/> is left at the runtime's own default (the largest the platform
/// supports) rather than a knob this library invents — how deep an OS actually queues pending
/// connections beyond that is platform behaviour this library does not control and does not attempt
/// to size (ADR 0007). A consumer who needs a specific backlog depth is asking the OS a question this
/// library has no authority to answer on its behalf.
/// </remarks>
internal sealed class TcpListenLink : IDisposable
{
    private TcpListenLink(Socket socket, IPEndPoint localEndPoint)
    {
        Socket = socket;
        LocalEndPoint = localEndPoint;
    }

    /// <summary>The socket's own bound address, constant for this Link's lifetime.</summary>
    public IPEndPoint LocalEndPoint { get; }

    /// <summary>The underlying socket, registered with the readiness dispatcher by the caller.</summary>
    public Socket Socket { get; }

    /// <summary>
    /// Whether a connection is waiting to be pulled off the backlog right now. A non-destructive,
    /// non-blocking check (<see cref="Socket.Poll(int, SelectMode)"/> with a zero timeout) — calling
    /// it never removes anything from the backlog, so it is safe to use purely to decide whether a
    /// connection was left behind once the admission cap is spent for a Frame.
    /// </summary>
    public bool HasPendingConnection => Socket.Poll(0, SelectMode.SelectRead);

    /// <summary>
    /// Binds a new TCP socket to <paramref name="endpoint"/> and puts it into the listening state.
    /// Disposes the socket and throws an <see cref="InvalidOperationException"/> naming
    /// <paramref name="endpoint"/> if the bind fails — a duplicated fixture or an undisposed Reactor
    /// is reported identically on every platform rather than silently splitting traffic (ADR 0014).
    /// The underlying <see cref="SocketException"/>, with its platform <see cref="SocketError"/>, is
    /// preserved as the inner exception.
    /// </summary>
    public static TcpListenLink Listen(IPEndPoint endpoint)
    {
        Socket socket = new(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            if (OperatingSystem.IsWindows())
            {
                socket.ExclusiveAddressUse = true;
            }

            socket.Bind(endpoint);
            socket.Listen();
        }
        catch (SocketException exception)
        {
            socket.Dispose();
            throw new InvalidOperationException(
                $"Listen on {endpoint} failed: {exception.SocketErrorCode} — binds are exclusive, so a port already in use fails at registration rather than being shared.",
                exception);
        }
        catch
        {
            socket.Dispose();
            throw;
        }

        // `!` works around a framework limitation: Socket.LocalEndPoint is typed EndPoint? for the
        // whole lifetime of a Socket, but a Socket that has returned from Bind always has one, and
        // nothing above can observe this instance before that. There is no overload or annotation
        // that expresses "bound" in the type.
        return new TcpListenLink(socket, (IPEndPoint)socket.LocalEndPoint!);
    }

    /// <summary>
    /// Pulls one connection off the backlog if one is waiting. Never blocks: a
    /// <see cref="HasPendingConnection"/> check gates the call, so an empty backlog is reported as
    /// <see cref="TcpAcceptOutcome.NoPendingConnection"/> rather than the caller's thread stalling in
    /// <see cref="Socket.Accept"/>.
    /// </summary>
    /// <remarks>
    /// A failed accept (for example <see cref="SocketError.ConnectionReset"/> when a client aborted
    /// the handshake before this call ran) never closes the listening socket. Closing it over
    /// one bad connection out of many would stop admitting every future connection over something a
    /// stranger triggered, which is exactly the kind of decision Raw does not make for a consumer —
    /// the caller is told and free to keep pumping.
    /// </remarks>
    public TcpAcceptResult TryAccept()
    {
        if (!HasPendingConnection)
        {
            return TcpAcceptResult.ForNoPendingConnection();
        }

        try
        {
            return TcpAcceptResult.ForAccepted(Socket.Accept());
        }
        catch (SocketException exception)
        {
            return TcpAcceptResult.ForFailed(exception.SocketErrorCode);
        }
    }

    /// <summary>Closes the underlying listening socket, releasing the port so it can be rebound.</summary>
    public void Dispose() => Socket.Dispose();
}
