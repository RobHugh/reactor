using System.Net;
using System.Net.Sockets;

namespace ReactorSockets.Transports;

/// <summary>
/// L1: one connected (or connecting) TCP socket, produced either by a listening Link's accept or by
/// dialling out. Carries only the socket and its bound local address for now — reads, sends and
/// half-close join this type as the sub-issues that need them land, exactly as <see cref="UdpLink"/>
/// was built out incrementally in its own slice.
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
        return new TcpLink(socket, (IPEndPoint)socket.LocalEndPoint!);
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
    /// <see cref="ReactorSockets.CloseReason.ResetByPeer"/>.
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

    public void Dispose() => Socket.Dispose();
}
