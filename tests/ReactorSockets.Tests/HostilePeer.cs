using System.Net;

namespace ReactorSockets.Tests;

/// <summary>
/// A counterparty that puts exactly the bytes it is handed on the wire, with no framing, validation
/// or interpretation of its own — the instrument tests use to feed a Session Reactor
/// malformed datagrams, wrong versions, invalid cookies, unknown flag bits and oversized length
/// prefixes. Built entirely from the public Raw surface: this type is an ordinary
/// <see cref="RawReactor"/> consumer, exactly like the code it is used to test, so nothing it does exercises a capability that code would not also have.
/// </summary>
/// <remarks>
/// A hostile peer's whole point is that it never validates its own output — every method here takes
/// a caller-supplied <see cref="ReadOnlySpan{T}"/> of bytes and puts it on the wire unexamined. A
/// caller that wants to send something structurally invalid builds the bytes itself. This type
/// supplies no framing to be structurally invalid <em>with</em>.
/// </remarks>
internal sealed class HostilePeer : IDisposable
{
    private readonly RawReactor _reactor;

    /// <summary>
    /// Constructs a hostile peer with its own Reactor, sized for a handful of concurrent sends —
    /// <paramref name="capacity"/> only bounds how many Links this peer can hold open at once, never
    /// anything about the target it attacks.
    /// </summary>
    public HostilePeer(int capacity = 4)
    {
        _reactor = new RawReactor(new ReactorOptions { Capacity = capacity });
    }

    /// <summary>
    /// Sends <paramref name="payload"/> to <paramref name="destination"/> as one UDP datagram, exactly
    /// as given — no length prefix, no version byte, no cookie, whatever the caller put in is what
    /// crosses the wire. Binds and closes a throwaway Link for the one send, since a hostile peer's
    /// datagrams are typically independent single shots rather than a sustained conversation.
    /// </summary>
    public void SendDatagram(IPEndPoint destination, ReadOnlySpan<byte> payload)
    {
        IPAddress anyForFamily = destination.AddressFamily == System.Net.Sockets.AddressFamily.InterNetworkV6
            ? IPAddress.IPv6Any
            : IPAddress.Any;
        LinkId link = _reactor.Bind(new IPEndPoint(anyForFamily, 0));
        try
        {
            Result result = _reactor.SendTo(link, Source.FromIPEndPoint(destination), payload);
            if (!result.IsSuccess)
            {
                throw new InvalidOperationException($"HostilePeer failed to send a datagram to {destination}: {result.Error}");
            }
        }
        finally
        {
            _reactor.Close(link);
        }
    }

    /// <summary>
    /// Dials <paramref name="destination"/> over TCP, Pumps until the handshake resolves, then writes
    /// <paramref name="payload"/> exactly as given — no framing of any kind. Returns the connected
    /// <see cref="LinkId"/> so the caller can keep writing further hostile bytes on the same
    /// connection, or close it themselves to provoke a mid-stream reset. Throws if the dial does not
    /// resolve within <paramref name="budget"/>, or resolves to a failure, since a hostile peer that
    /// cannot even connect has nothing to be hostile with.
    /// </summary>
    public LinkId ConnectAndSend(IPEndPoint destination, ReadOnlySpan<byte> payload, TimeSpan? budget = null)
    {
        LinkId link = _reactor.Connect(destination);
        bool connected = false;
        PumpDeadline deadline = PumpDeadline.Start(budget);
        while (!connected)
        {
            _reactor.Pump();
            foreach (RawEvent evt in _reactor.Drain())
            {
                if (evt.Kind == RawEventKind.Connected && evt.Link == link)
                {
                    connected = true;
                }
                else if (evt.Kind == RawEventKind.Closed && evt.Link == link)
                {
                    throw new InvalidOperationException($"HostilePeer's dial to {destination} closed with {evt.Reason} before connecting.");
                }
            }

            if (connected)
            {
                break;
            }

            if (deadline.Expired)
            {
                throw new InvalidOperationException($"HostilePeer's dial to {destination} did not resolve within its Pump budget.");
            }

            PumpDeadline.Yield();
        }

        SendResult sendResult = _reactor.Send(link, payload);
        if (!sendResult.IsSuccess)
        {
            throw new InvalidOperationException($"HostilePeer failed to send on its connection to {destination}: {sendResult.Error}");
        }

        return link;
    }

    /// <summary>
    /// Writes <paramref name="payload"/> on a connection <see cref="ConnectAndSend"/> already
    /// established, unexamined as before — used to send a second, third or malformed-continuation
    /// write on the same stream.
    /// </summary>
    public void SendMore(LinkId link, ReadOnlySpan<byte> payload)
    {
        SendResult result = _reactor.Send(link, payload);
        if (!result.IsSuccess)
        {
            throw new InvalidOperationException($"HostilePeer failed a further send on {link}: {result.Error}");
        }
    }

    /// <summary>Advances this peer's own Reactor by one Pump, for a caller that needs to drive it directly.</summary>
    public void Pump() => _reactor.Pump();

    /// <summary>Closes <paramref name="link"/> on this peer's Reactor — used to provoke a reset or an ordinary close mid-test.</summary>
    public void Close(LinkId link) => _reactor.Close(link);

    public void Dispose() => _reactor.Dispose();
}
