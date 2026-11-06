using System.Net;

namespace ReactorSockets.Tests;

/// <summary>Which side of a <see cref="WireTap"/> a captured datagram travelled — the dialling Peer toward the fixed accepter, or the reply back.</summary>
internal enum TapDirection
{
    /// <summary>From the dial-facing Link toward the fixed accepter endpoint.</summary>
    DiallerToAccepter,

    /// <summary>From the fixed accepter endpoint back toward the most recent dialler.</summary>
    AccepterToDialler,
}

/// <summary>One datagram this <see cref="WireTap"/> relayed, in the order it arrived at the tap.</summary>
/// <param name="Direction">Which way the datagram was travelling.</param>
/// <param name="Payload">A defensive copy of the exact bytes relayed — safe to keep past the Pump that captured it.</param>
internal readonly record struct TapCapture(TapDirection Direction, byte[] Payload);

/// <summary>
/// A <see cref="RawReactor"/> relaying UDP datagrams unmodified between a dial-facing address and a
/// fixed accepter endpoint while keeping a byte-for-byte copy of every one, tagged with its direction
/// and kept in arrival order, so tests that assert on datagram bytes start from a captured buffer
/// instead of building their own. Built entirely from the
/// public Raw surface, and driven by the same <see cref="TimeProvider"/> as the rest of a test's
/// fixture, exactly as <see cref="LossyProxy"/> is.
/// </summary>
/// <remarks>
/// <para>
/// Unlike <see cref="LossyProxy"/> this type never drops, duplicates, delays or reorders — every
/// datagram is forwarded the moment it is captured, in the same Pump, so neither Session Reactor on
/// either side of the tap can observe any difference from a direct dialogue. The only thing this type
/// adds to a plain relay is the copy it keeps for the test to inspect afterwards.
/// </para>
/// <para>
/// Like <see cref="LossyProxy"/> this tap tracks exactly one dialler at a time — whichever address
/// last sent it a datagram — which is what a single Session-under-test dialogued through the tap to
/// one counterparty needs. A test wanting several simultaneous diallers needs several taps.
/// </para>
/// </remarks>
internal sealed class WireTap : IDisposable
{
    private readonly RawReactor _reactor;
    private readonly LinkId _diallerFacing;
    private readonly LinkId _accepterFacing;
    private readonly IPEndPoint _accepter;
    private readonly List<TapCapture> _captures = [];
    private Source? _lastDialler;

    /// <summary>
    /// Constructs a tap that relays toward <paramref name="accepter"/>, timing nothing of its own —
    /// pass the same <see cref="TimeProvider"/> the rest of a test's fixture uses so a captured
    /// datagram's arrival lines up with the fixture's own clock.
    /// </summary>
    public WireTap(IPEndPoint accepter, TimeProvider timeProvider)
    {
        _accepter = accepter;
        _reactor = new RawReactor(new ReactorOptions { Capacity = 2, TimeProvider = timeProvider });
        _diallerFacing = _reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        _accepterFacing = _reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
    }

    /// <summary>The address a dialler should be told to send to — this tap relays whatever arrives here toward <c>accepter</c>.</summary>
    public IPEndPoint PublicEndPoint => _reactor.LocalOf(_diallerFacing);

    /// <summary>Every datagram this tap has relayed so far, in arrival order, each a defensive copy safe to keep past this tap's next Pump.</summary>
    public IReadOnlyList<TapCapture> Captures => _captures;

    /// <summary>How many datagrams this tap has captured travelling in <paramref name="direction"/> so far.</summary>
    public int CountInDirection(TapDirection direction) => _captures.Count(capture => capture.Direction == direction);

    /// <summary>The payloads captured travelling in <paramref name="direction"/> so far, in arrival order, each safe to keep past this tap's next Pump.</summary>
    public IEnumerable<byte[]> PayloadsInDirection(TapDirection direction) =>
        _captures.Where(capture => capture.Direction == direction).Select(capture => capture.Payload);

    /// <summary>
    /// Advances this tap's own Reactor by one Pump, capturing and relaying whatever arrived: a
    /// datagram from the dial-facing Link is captured as <see cref="TapDirection.DiallerToAccepter"/>
    /// and sent on to <c>accepter</c>; a datagram from the accepter-facing Link is captured as
    /// <see cref="TapDirection.AccepterToDialler"/> and sent back to whichever address last dialled
    /// in, if any has. Every relay happens in the same Pump the datagram was captured in, unmodified
    /// and in order, so neither side ever observes the tap.
    /// </summary>
    public void Pump()
    {
        _reactor.Pump();
        foreach (RawEvent evt in _reactor.Drain())
        {
            if (evt.Kind != RawEventKind.DatagramReceived)
            {
                continue;
            }

            byte[] payload = evt.Payload.ToArray();

            if (evt.Link == _diallerFacing)
            {
                _lastDialler = evt.Source;
                _captures.Add(new TapCapture(TapDirection.DiallerToAccepter, payload));
                _reactor.SendTo(_accepterFacing, Source.FromIPEndPoint(_accepter), payload);
            }
            else if (evt.Link == _accepterFacing && _lastDialler is Source dialler)
            {
                _captures.Add(new TapCapture(TapDirection.AccepterToDialler, payload));
                _reactor.SendTo(_diallerFacing, dialler, payload);
            }
        }
    }

    public void Dispose() => _reactor.Dispose();
}
