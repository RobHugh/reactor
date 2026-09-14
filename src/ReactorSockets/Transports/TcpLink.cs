using System.Net;
using System.Net.Sockets;

namespace ReactorSockets.Transports;

/// <summary>
/// L1: one connected (or connecting) TCP socket, produced either by a listening Link's accept or by
/// dialling out. Carries the socket and its bound local address, and — like <see cref="UdpLink"/> —
/// is built out incrementally in its own slice: reads (<see cref="Receive"/>), sends
/// (<see cref="Send(ReadOnlySpan{byte})"/>) and half-close (<see cref="Shutdown"/>) all land here.
/// </summary>
internal sealed class TcpLink : IDisposable
{
    private TcpLink(
        Socket socket,
        IPEndPoint localEndPoint,
        IPEndPoint remoteEndPoint,
        SocketError dialError = SocketError.Success)
    {
        Socket = socket;
        LocalEndPoint = localEndPoint;
        RemoteEndPoint = remoteEndPoint;
        _dialError = dialError;
    }

    /// <summary>
    /// The outcome of a dial that had already failed by the time <see cref="Dial"/> returned, captured
    /// there because it cannot be re-read afterwards, and preferred over the socket's own pending error
    /// by <see cref="PollConnect"/>. <see cref="SocketError.Success"/> for an accepted Link and for a
    /// dial still in flight, which is every dial that has not failed yet.
    /// </summary>
    private readonly SocketError _dialError;

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
    /// Whether bytes are waiting to be read right now. A non-destructive, non-blocking check
    /// (<see cref="Socket.Poll(int, SelectMode)"/> with a zero timeout), the same shape
    /// <see cref="TcpListenLink.HasPendingConnection"/> already uses for a backlog. Exists for a caller
    /// reading outside the readiness dispatcher's own callback — one that wants whatever already
    /// arrived without waiting for the next Pump to report this Handle readable — since
    /// <see cref="Receive"/> on a socket with nothing ready reports a failure, not an empty read, and
    /// that failure would be indistinguishable from a real one.
    /// </summary>
    /// <remarks>
    /// A remote that half-closed also polls readable here, which is correct for this property's one
    /// purpose: <see cref="Receive"/> then returns
    /// <see cref="TcpLinkReceiveOutcome.EndOfStream"/> — a real outcome the caller must act on, not
    /// the "nothing ready" this guards against.
    /// </remarks>
    public bool HasPendingBytes => Socket.Poll(0, SelectMode.SelectRead);

    /// <summary>
    /// Wraps a socket a listening Link just pulled off its backlog. TCP binds the local end
    /// implicitly on accept, so <see cref="LocalEndPoint"/> is read back from the socket rather than
    /// supplied by the caller — and so is <see cref="RemoteEndPoint"/>, since an accepted socket is
    /// already connected and therefore already knows who to. Put into non-blocking mode here, for the
    /// same reason <see cref="Dial"/> puts a dialled socket into non-blocking mode: <see cref="Send(ReadOnlySpan{byte})"/>
    /// must never stall the caller's thread waiting for buffer room, and <see cref="Socket.Accept()"/>
    /// hands back a blocking socket by default regardless of the listening socket's own mode. Nagle is
    /// disabled here too, unconditionally: <see cref="TcpLink"/> is shared by both profiles (ADR 0007),
    /// and <see cref="Send(ReadOnlySpan{byte})"/> never batches or queues (ADR 0008) for either one, so a Session's
    /// documented Nagle-off profile property (ADR 0011) is simply the honest setting for a socket whose
    /// writes are already unbatched everywhere this type is used.
    /// </summary>
    /// <param name="acceptedSocket">The socket a listening Link just pulled off its backlog.</param>
    /// <param name="sendBufferSize">
    /// <c>SO_SNDBUF</c> for this socket, or <see langword="null"/> for the OS default. Safe to apply
    /// after the connection already exists, unlike <c>SO_RCVBUF</c> — see
    /// <see cref="ApplySendBufferSize"/>.
    /// </param>
    public static TcpLink FromAccepted(Socket acceptedSocket, int? sendBufferSize = null)
    {
        acceptedSocket.Blocking = false;
        acceptedSocket.NoDelay = true;
        ApplySendBufferSize(acceptedSocket, sendBufferSize);

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
    /// <para>
    /// The socket is put into non-blocking mode before <see cref="Socket.Connect(System.Net.EndPoint)"/>
    /// is called, so the call throws <see cref="SocketError.WouldBlock"/> for a dial that is genuinely
    /// in flight — the expected shape of "connecting, ask again later" for a non-blocking BSD-style
    /// socket. That is not the only shape, and the two others are both platform-dependent, which is
    /// why a dial's outcome is captured here rather than left entirely to <see cref="PollConnect"/>.
    /// </para>
    /// <para>
    /// A dial can fail <i>synchronously</i>, throwing something other than <see cref="SocketError.WouldBlock"/>
    /// — Linux answers an unreachable network this way, where Windows defers nearly everything to the
    /// pending error. That code is kept rather than swallowed: the socket has no error pending for
    /// <see cref="PollConnect"/> to find precisely because it was returned directly, so a discarded one
    /// leaves a failed dial polling writable and clean, indistinguishable from a connected socket.
    /// </para>
    /// <para>
    /// A dial can also fail <i>between</i> the call and this method returning, which loopback does
    /// routinely: the refusal lands within microseconds. Its error must be read off the socket before
    /// <see cref="Socket.LocalEndPoint"/> is touched, because on Unix that getter completes the
    /// framework's own non-blocking-connect bookkeeping by reading <c>SO_ERROR</c> — and <c>SO_ERROR</c>
    /// is read-and-clear, so the first reader consumes it. Reading it here, in this order, is what keeps
    /// an ordinary refused connection reportable at all on Linux; with the reads the other way round the
    /// refusal is destroyed inside this method and the dial goes on to report Connected.
    /// </para>
    /// </remarks>
    /// <param name="endpoint">The address to dial.</param>
    /// <param name="sendBufferSize">
    /// <c>SO_SNDBUF</c> for this socket, or <see langword="null"/> for the OS default. Applied before
    /// <see cref="Socket.Connect(System.Net.EndPoint)"/> only because there is a natural place for it
    /// here, not because it has to be — see <see cref="ApplySendBufferSize"/>.
    /// </param>
    public static TcpLink Dial(IPEndPoint endpoint, int? sendBufferSize = null)
    {
        Socket socket = new(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
        {
            Blocking = false,
            NoDelay = true
        };

        ApplySendBufferSize(socket, sendBufferSize);

        SocketError dialError = SocketError.Success;
        try
        {
            socket.Connect(endpoint);
        }
        catch (SocketException exception) when (exception.SocketErrorCode is SocketError.WouldBlock)
        {
            // In flight, and the overwhelmingly common case: PollConnect reports it on a later Pump.
        }
        catch (SocketException exception)
        {
            dialError = exception.SocketErrorCode;
        }

        if (dialError is SocketError.Success)
        {
            // `!` works around a framework limitation, as in PollConnect below: GetSocketOption's
            // object-returning overload is typed object? because the generic signature covers options
            // yielding a byte[] or nothing, but SocketOptionName.Error always yields a boxed int.
            int pendingError = (int)socket.GetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Error)!;
            dialError = (SocketError)pendingError;
        }

        // `!` works around a framework limitation, as in FromAccepted above: Connect — blocking or not,
        // successful or not — always binds a local end first, but Socket.LocalEndPoint stays typed
        // EndPoint? for the socket's whole lifetime regardless. Read last, for the reason in the remarks.
        return new TcpLink(socket, (IPEndPoint)socket.LocalEndPoint!, endpoint, dialError);
    }

    /// <summary>
    /// Checks whether a dial begun by <see cref="Dial"/> has resolved yet, without blocking. A
    /// connecting socket becomes writable on success and reports a pending error via
    /// <see cref="SocketOptionName.Error"/> on failure — checking both, rather than either alone, is
    /// the standard cross-platform way to learn a non-blocking connect's outcome, since a single
    /// <see cref="SelectMode"/> is not documented to distinguish the two cases identically on every
    /// platform this library targets.
    /// </summary>
    /// <remarks>
    /// An error <see cref="Dial"/> already captured wins over anything read here, and is not a
    /// duplicate of it: a dial that failed before <see cref="Dial"/> returned leaves nothing behind for
    /// this method to find — either because the error was returned synchronously and never made pending
    /// at all, or because reading it there consumed it — while a socket in that state still polls
    /// writable with a clean <c>SO_ERROR</c>. Checking the socket first would therefore report Connected
    /// for exactly the dials that failed earliest.
    /// </remarks>
    public TcpConnectPollResult PollConnect()
    {
        if (_dialError is not SocketError.Success)
        {
            return TcpConnectPollResult.ForFailed(_dialError);
        }

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
    /// The gather form of <see cref="Send(ReadOnlySpan{byte})"/> (issue #132): offers
    /// <paramref name="segments"/> to the socket as one call — <c>WSASend</c> on Windows,
    /// <c>writev</c> elsewhere — so bytes living in two unrelated buffers reach the wire in a single
    /// write rather than one write each. Identical in every other respect: it still takes only what
    /// the socket will accept right now, still reports that count, still never blocks, copies or
    /// queues, and still translates <see cref="SocketError.WouldBlock"/> into a successful send of
    /// zero bytes. The reported count spans the segments in order, so a caller that offered two
    /// segments learns how far into them the socket reached by subtracting the first's length.
    /// </summary>
    /// <remarks>
    /// This adds no queueing and no policy — the two things ADR 0008 rules out of a Link — it only
    /// lets one caller-owned write describe bytes that are not contiguous. The public Raw surface is
    /// unchanged: this overload is internal, reached only by the stream Session profile's own flush.
    /// </remarks>
    public TcpLinkSendResult Send(IList<ArraySegment<byte>> segments)
    {
        try
        {
            int sent = Socket.Send(segments, SocketFlags.None);
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

    /// <summary>
    /// Sets <c>SO_SNDBUF</c> on <paramref name="socket"/> when <paramref name="sendBufferSize"/> is
    /// supplied, and does nothing at all when it is not — an unset knob leaves the OS default and its
    /// autotuning exactly as they were, which is what every caller that does not care about the send
    /// buffer wants.
    /// </summary>
    /// <remarks>
    /// Unlike <c>SO_RCVBUF</c>, which must be set before the connection exists or Linux will neither
    /// clamp the advertised window nor stop receive autotuning (commit 8426ff3), <c>SO_SNDBUF</c> can be
    /// set at any point in a socket's life: it bounds the send queue that is consulted on every write,
    /// and setting it takes the socket off send autotuning from that moment on. That is why
    /// <see cref="FromAccepted"/> may apply it to an already-connected socket. Both platforms treat the
    /// value as a request rather than an exact size — Linux doubles it and holds it to its own
    /// <c>tcp_wmem</c> floor — so this bounds how much the kernel will absorb before a write reports
    /// <see cref="SocketError.WouldBlock"/>, and never promises a precise byte count.
    /// </remarks>
    private static void ApplySendBufferSize(Socket socket, int? sendBufferSize)
    {
        if (sendBufferSize is int size)
        {
            socket.SendBufferSize = size;
        }
    }

    /// <summary>Closes the underlying socket, releasing the port so it can be rebound.</summary>
    public void Dispose() => Socket.Dispose();
}
