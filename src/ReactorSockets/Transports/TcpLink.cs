using System.Net;
using System.Net.Sockets;

namespace ReactorSockets.Transports;

/// <summary>
/// L1: one connected (or connecting) TCP socket, produced either by a listening Link's accept or by
/// dialling out. Carries the socket and its bound local address, and — like <see cref="UdpLink"/> —
/// is built out incrementally in its own slice: reads (<see cref="Receive"/>), sends
/// (<see cref="Send"/>) and half-close (<see cref="Shutdown"/>) all land here.
/// </summary>
internal sealed class TcpLink : IDisposable
{
    private TcpLink(Socket socket, IPEndPoint localEndPoint, IPEndPoint remoteEndPoint)
    {
        Socket = socket;
        LocalEndPoint = localEndPoint;
        RemoteEndPoint = remoteEndPoint;
    }

    /// <summary>The socket's own local address, constant for this Link's lifetime.</summary>
    public IPEndPoint LocalEndPoint { get; }

    /// <summary>
    /// The remote end's address, constant for this Link's lifetime (ADR 0014) — set once, here, and never
    /// re-read from the socket afterwards, because both the ways a <see cref="TcpLink"/> comes to
    /// exist already know it without asking: an accepted socket reports it immediately since it is
    /// already connected, and a dialled socket's remote is exactly the endpoint the caller asked
    /// <see cref="Dial"/> to reach, true whether or not the handshake has resolved yet — the same
    /// reasoning <see cref="LocalEndPoint"/> already relies on for a dial in flight.
    /// </summary>
    public IPEndPoint RemoteEndPoint { get; }

    /// <summary>The underlying socket, registered with the readiness dispatcher by the caller.</summary>
    public Socket Socket { get; }

    /// <summary>
    /// Wraps a socket a listening Link just pulled off its backlog. TCP binds the local end
    /// implicitly on accept, so <see cref="LocalEndPoint"/> is read back from the socket rather than
    /// supplied by the caller — and so is <see cref="RemoteEndPoint"/>, since an accepted socket is
    /// already connected and therefore already knows who to. Put into non-blocking mode here, for the
    /// same reason <see cref="Dial"/> puts a dialled socket into non-blocking mode: <see cref="Send"/>
    /// must never stall the caller's thread waiting for buffer room, and <see cref="Socket.Accept()"/>
    /// hands back a blocking socket by default regardless of the listening socket's own mode.
    /// </summary>
    public static TcpLink FromAccepted(Socket acceptedSocket)
    {
        acceptedSocket.Blocking = false;

        // `!` works around a framework limitation: Socket.LocalEndPoint and RemoteEndPoint stay typed
        // EndPoint? for a socket's whole lifetime, but a socket handed back by Accept is connected
        // before it is returned, so both are populated by the time this runs.
        return new(acceptedSocket, (IPEndPoint)acceptedSocket.LocalEndPoint!, (IPEndPoint)acceptedSocket.RemoteEndPoint!);
    }

    /// <summary>
    /// Begins connecting a new TCP socket to <paramref name="endpoint"/> and returns immediately —
    /// this call never blocks and the handshake is left to run against the OS. <see cref="PollConnect"/>,
    /// called from a later Pump, is what learns whether it succeeded or failed; nothing here throws for
    /// an outcome the handshake itself produces, because a dial's completion is Pump-driven Event data
    /// (<c>Connected</c> or a <c>Closed</c> with a reason), never an exception a registration call raises.
    /// </summary>
    /// <remarks>
    /// The socket is put into non-blocking mode before <see cref="Socket.Connect(System.Net.EndPoint)"/>
    /// is called, so the call almost always throws <see cref="SocketError.WouldBlock"/> immediately —
    /// the expected shape of "connecting, ask again later" for a non-blocking BSD-style socket. A
    /// synchronous failure (for example an immediate loopback refusal, which some platforms can resolve
    /// without a round trip) lands in the same catch and is not re-thrown either, for the same reason:
    /// this method registers a dial, it does not complete one. <see cref="PollConnect"/> reports
    /// whichever already happened exactly as it would report one still in flight.
    /// </remarks>
    public static TcpLink Dial(IPEndPoint endpoint)
    {
        Socket socket = new(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
        {
            Blocking = false
        };
        try
        {
            socket.Connect(endpoint);
        }
        catch (SocketException)
        {
            // Expected: see the remarks above. Whatever happened — in-flight or already resolved —
            // PollConnect is what reports it, on the Pump that follows.
        }

        // `!` works around a framework limitation, as in FromAccepted above: Connect — blocking or not,
        // successful or not — always binds a local end first, but Socket.LocalEndPoint stays typed
        // EndPoint? for the socket's whole lifetime regardless.
        return new TcpLink(socket, (IPEndPoint)socket.LocalEndPoint!, endpoint);
    }

    /// <summary>
    /// Checks whether a dial begun by <see cref="Dial"/> has resolved yet, without blocking. A
    /// connecting socket becomes writable on success and reports a pending error via
    /// <see cref="SocketOptionName.Error"/> on failure — checking both, rather than either alone, is
    /// the standard cross-platform way to learn a non-blocking connect's outcome, since a single
    /// <see cref="SelectMode"/> is not documented to distinguish the two cases identically on every
    /// platform this library targets.
    /// </summary>
    public TcpConnectPollResult PollConnect()
    {
        bool writable = Socket.Poll(0, SelectMode.SelectWrite);
        bool errored = Socket.Poll(0, SelectMode.SelectError);
        if (!writable && !errored)
        {
            return TcpConnectPollResult.ForPending();
        }

        // `!` works around a framework limitation: GetSocketOption's object-returning overload is typed
        // object? because the generic signature covers options that yield a byte[] or nothing at all,
        // but SocketOptionName.Error is documented to return a boxed int on every platform.
        int errorCode = (int)Socket.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Error)!;
        return errorCode == 0
            ? TcpConnectPollResult.ForConnected()
            : TcpConnectPollResult.ForFailed((SocketError)errorCode);
    }

    /// <summary>
    /// Reads whatever bytes are on the socket right now into <paramref name="scratch"/>, without
    /// blocking — called only after the readiness dispatcher has already found this Link's socket
    /// readable, so the read itself never stalls. TCP is a stream: this returns exactly the run of
    /// bytes the OS handed back and makes no claim it is a whole anything (ADR 0008); reassembly is
    /// not this type's job and not this library's.
    /// </summary>
    /// <remarks>
    /// A read of zero bytes is TCP's own signal that the remote shut down its send half — a FIN,
    /// whether sent as an explicit half-close or as the first half of a full close — and is reported
    /// as <see cref="TcpLinkReceiveOutcome.EndOfStream"/> rather than folded into
    /// <see cref="TcpLinkReceiveOutcome.Failed"/>: it says nothing about this Link's own ability to
    /// still send. A reset arrives here as an ordinary <see cref="SocketException"/> and is reported
    /// as <see cref="TcpLinkReceiveOutcome.Failed"/>, carrying whatever <see cref="SocketError"/> the
    /// platform gave — it is L2's job, not this type's, to translate a reset into
    /// <see cref="ReactorSockets.CloseReason.ResetByRemote"/>.
    /// </remarks>
    public TcpLinkReceiveResult Receive(Span<byte> scratch)
    {
        try
        {
            int received = Socket.Receive(scratch, SocketFlags.None);
            return received == 0
                ? TcpLinkReceiveResult.ForEndOfStream()
                : TcpLinkReceiveResult.ForDelivered(scratch[..received]);
        }
        catch (SocketException exception)
        {
            return TcpLinkReceiveResult.ForFailed(exception.SocketErrorCode);
        }
    }

    /// <summary>
    /// Takes what the socket will accept of <paramref name="payload"/> right now and reports the
    /// count — never blocks, never copies, never queues (ADR 0008). A non-blocking socket's send
    /// buffer can be full with nothing wrong: that surfaces as <see cref="SocketError.WouldBlock"/>
    /// from the runtime, translated here into a successful send of zero bytes rather than a failure,
    /// because nothing about the Link or the connection failed — the caller simply offered more than
    /// there was room for at this instant. Any other <see cref="SocketException"/> is a real failure
    /// and is reported as one.
    /// </summary>
    public TcpLinkSendResult Send(ReadOnlySpan<byte> payload)
    {
        try
        {
            int sent = Socket.Send(payload, SocketFlags.None);
            return TcpLinkSendResult.ForSent(sent);
        }
        catch (SocketException exception) when (exception.SocketErrorCode == SocketError.WouldBlock)
        {
            return TcpLinkSendResult.ForSent(0);
        }
        catch (SocketException exception)
        {
            return TcpLinkSendResult.ForFailed(exception.SocketErrorCode);
        }
    }

    /// <summary>
    /// Sends this Link's own FIN, closing the send half while the Link stays open for reading — what
    /// lets a consumer build a request-response protocol that signals end-of-request by closing the
    /// write half, without losing the ability to read a reply. Half-close, not full close: nothing
    /// about the read direction changes here, and closing the socket outright is <c>Close</c>'s job,
    /// not this one's.
    /// </summary>
    public TcpLinkShutdownResult Shutdown()
    {
        try
        {
            Socket.Shutdown(SocketShutdown.Send);
            return TcpLinkShutdownResult.ForSuccess();
        }
        catch (SocketException exception)
        {
            return TcpLinkShutdownResult.ForFailed(exception.SocketErrorCode);
        }
    }

    /// <summary>Closes the underlying socket, releasing the port so it can be rebound.</summary>
    public void Dispose() => Socket.Dispose();
}
