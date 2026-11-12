using System.Net;
using System.Net.Sockets;

namespace ReactorSockets.Transports;

/// <summary>
/// L1: one bound UDP socket. Raw does not reassemble — a Segment is by definition whatever arrived in
/// one <see cref="Receive"/> call, nothing is held across a Pump, and this type carries no state beyond
/// the socket and its bound address (ADR 0008, ADR 0012).
/// </summary>
/// <remarks>
/// <see cref="Socket.DualMode"/> is never set: <see cref="IPAddress.Any"/> binds IPv4, <see cref="IPAddress.IPv6Any"/>
/// binds IPv6, and a consumer wanting both binds twice (ADR 0014). On Windows, <c>ExclusiveAddressUse</c>
/// is set explicitly so that binds are exclusive on every platform identically rather than by the
/// accident of <c>SO_REUSEADDR</c>'s Windows semantics.
/// </remarks>
internal sealed class UdpLink : IDisposable
{
    // Linux socket option numbers, which have no BCL name because the BCL exposes no portable
    // equivalent — see EnableIcmpErrorReporting for why they are set at all.
    private const int LinuxIpProtoIp = 0;
    private const int LinuxIpRecvErr = 11;
    private const int LinuxIpProtoIpv6 = 41;
    private const int LinuxIpv6RecvErr = 25;

    private readonly EndPoint _receiveFromPlaceholder;

    private UdpLink(Socket socket, IPEndPoint localEndPoint)
    {
        Socket = socket;
        LocalEndPoint = localEndPoint;
        _receiveFromPlaceholder = new IPEndPoint(
            localEndPoint.AddressFamily == AddressFamily.InterNetworkV6 ? IPAddress.IPv6Any : IPAddress.Any, 0);
    }

    /// <summary>The socket's own bound address, constant for this Link's lifetime.</summary>
    public IPEndPoint LocalEndPoint { get; }

    /// <summary>The underlying socket, registered with the readiness dispatcher by the caller.</summary>
    public Socket Socket { get; }

    /// <summary>
    /// Binds a new UDP socket to <paramref name="endpoint"/>. Disposes the socket and throws an
    /// <see cref="InvalidOperationException"/> naming <paramref name="endpoint"/> if the bind fails —
    /// a duplicated fixture or an undisposed Reactor is reported identically on every platform rather
    /// than silently splitting traffic (ADR 0014). The underlying <see cref="SocketException"/>, with
    /// its platform <see cref="SocketError"/>, is preserved as the inner exception.
    /// </summary>
    public static UdpLink Bind(IPEndPoint endpoint)
    {
        Socket socket = new(endpoint.AddressFamily, SocketType.Dgram, ProtocolType.Udp);
        try
        {
            if (OperatingSystem.IsWindows())
            {
                socket.ExclusiveAddressUse = true;
            }

            // Non-blocking for the same reason TcpLink is: every read this Link performs is driven by a
            // readiness dispatch, and a dispatch is not a promise that anything is queued. Linux asserts
            // POLLERR with no POLLIN for a queued ICMP error, so a readable callback routinely arrives
            // with an empty receive queue — and a blocking ReceiveFrom there parks the whole Pump, and
            // with it the single thread the Reactor is driven from, for as long as the process lives.
            socket.Blocking = false;
            EnableIcmpErrorReporting(socket, endpoint.AddressFamily);
            socket.Bind(endpoint);
        }
        catch (SocketException exception)
        {
            socket.Dispose();
            throw new InvalidOperationException(
                $"Bind to {endpoint} failed: {exception.SocketErrorCode} — binds are exclusive, so a port already in use fails at registration rather than being shared.",
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
        return new UdpLink(socket, (IPEndPoint)socket.LocalEndPoint!);
    }

    /// <summary>
    /// Asks Linux to deliver ICMP errors — a port unreachable from an earlier send, chiefly — to an
    /// unconnected datagram socket, which it does not do by default. Windows reports them without
    /// being asked (as <c>WSAECONNRESET</c>), so this is what makes
    /// <see cref="UdpLinkReceiveOutcome.Unreachable"/> mean the same thing on both platforms rather
    /// than being a Windows-only event a Linux consumer never sees.
    /// </summary>
    /// <remarks>
    /// Set through <see cref="Socket.SetRawSocketOption"/> because the BCL has no portable name for
    /// <c>IP_RECVERR</c>: it is a Linux extension with no Windows counterpart, so
    /// <see cref="SocketOptionName"/> does not cover it. A failure to set it is swallowed rather than
    /// failing the Bind — the option is a diagnostic improvement, and a kernel or sandbox that refuses
    /// it leaves this Link working exactly as it did before, simply quieter about unreachable
    /// destinations.
    /// </remarks>
    private static void EnableIcmpErrorReporting(Socket socket, AddressFamily family)
    {
        if (!OperatingSystem.IsLinux())
        {
            return;
        }

        (int level, int name) = family == AddressFamily.InterNetworkV6
            ? (LinuxIpProtoIpv6, LinuxIpv6RecvErr)
            : (LinuxIpProtoIp, LinuxIpRecvErr);

        try
        {
            socket.SetRawSocketOption(level, name, BitConverter.GetBytes(1));
        }
        catch (SocketException)
        {
        }
    }

    /// <summary>
    /// Reads one datagram into <paramref name="scratch"/>. A read that fills <paramref name="scratch"/>
    /// completely is consumed and reported as <see cref="UdpLinkReceiveOutcome.Truncated"/>
    /// rather than left at the head of the queue, identically on Windows and Unix (ADR 0008). An
    /// earlier send's ICMP port-unreachable is reported as
    /// <see cref="UdpLinkReceiveOutcome.Unreachable"/> without affecting the Link — it arrives here as
    /// <c>WSAECONNRESET</c> on Windows and, thanks to the <c>IP_RECVERR</c> set at Bind, as
    /// <c>ECONNREFUSED</c> on Linux. Two spellings of one event, so both are read as the same outcome;
    /// the platform code itself is still carried on the result for a Sink that wants it. A read that
    /// finds nothing queued is <see cref="UdpLinkReceiveOutcome.WouldBlock"/>, not a failure — the
    /// socket is non-blocking, so this returns rather than waiting.
    /// </summary>
    /// <remarks>
    /// The caller passes a buffer one byte longer than the largest datagram it accepts. A read that
    /// fills the whole buffer therefore means the datagram was at least one byte too large, and is
    /// reported as truncated. This holds on every platform: Unix truncates silently and delivers
    /// exactly <paramref name="scratch"/>'s length, while Windows throws
    /// <see cref="SocketError.MessageSize"/>, which is the same event and is reported the same way.
    /// A datagram of exactly the accepted size leaves one byte unused and is delivered whole.
    /// </remarks>
    public UdpLinkReceiveResult Receive(Span<byte> scratch)
    {
        EndPoint remoteEndPoint = _receiveFromPlaceholder;
        try
        {
            int received = Socket.ReceiveFrom(scratch, SocketFlags.None, ref remoteEndPoint);
            if (received == scratch.Length)
            {
                Source truncatedSource = remoteEndPoint is IPEndPoint resolved ? Source.FromIPEndPoint(resolved) : default;
                return UdpLinkReceiveResult.ForTruncated(truncatedSource);
            }

            Source source = Source.FromIPEndPoint((IPEndPoint)remoteEndPoint);
            return UdpLinkReceiveResult.ForDelivered(source, scratch[..received]);
        }
        catch (SocketException exception) when (exception.SocketErrorCode == SocketError.MessageSize)
        {
            Source source = remoteEndPoint is IPEndPoint resolved ? Source.FromIPEndPoint(resolved) : default;
            return UdpLinkReceiveResult.ForTruncated(source);
        }
        catch (SocketException exception) when (
            exception.SocketErrorCode is SocketError.ConnectionReset or SocketError.ConnectionRefused)
        {
            return UdpLinkReceiveResult.ForUnreachable(exception.SocketErrorCode);
        }
        catch (SocketException exception) when (exception.SocketErrorCode == SocketError.WouldBlock)
        {
            return UdpLinkReceiveResult.ForWouldBlock();
        }
        catch (SocketException exception)
        {
            return UdpLinkReceiveResult.ForFailed(exception.SocketErrorCode);
        }
    }

    /// <summary>
    /// Sends <paramref name="payload"/> to <paramref name="destination"/> in one datagram. All-or-nothing
    /// (ADR 0008): a partial send is never a state the caller has to interpret differently from success
    /// or failure.
    /// </summary>
    public UdpLinkSendResult SendTo(Source destination, ReadOnlySpan<byte> payload)
    {
        try
        {
            int sent = Socket.SendTo(payload, SocketFlags.None, destination.ToIPEndPoint());
            return UdpLinkSendResult.ForSent(sent, payload.Length);
        }
        catch (SocketException exception)
        {
            return UdpLinkSendResult.ForFailed(exception.SocketErrorCode);
        }
    }

    public void Dispose() => Socket.Dispose();
}
