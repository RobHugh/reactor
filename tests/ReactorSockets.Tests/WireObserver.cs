using System.Net;

namespace ReactorSockets.Tests;

/// <summary>
/// A passive counterparty that records exactly the bytes a Session Reactor actually put on the
/// network, so a later slice's tests can assert encoding without reaching inside the type doing the
/// encoding — the second instrument the parent slice spec's Further Notes calls for. Built entirely
/// from the public Raw surface: this is an ordinary <see cref="RawReactor"/> consumer that binds or
/// listens, Pumps, and copies out whatever arrives before the next Pump invalidates it.
/// </summary>
/// <remarks>
/// Every payload this type reports is a defensive copy taken during <see cref="Pump"/>, never a span
/// into the underlying Reactor's frame arena — the whole reason a wire observer exists is to let a
/// caller inspect bytes after the Pump that received them, which a <see cref="RawEvent"/>'s own
/// <see cref="RawEvent.Payload"/> cannot do (ADR 0008: it is valid only until the next Pump).
/// </remarks>
internal sealed class WireObserver : IDisposable
{
    private readonly RawReactor _reactor;
    private readonly List<byte[]> _receivedDatagrams = [];
    private readonly List<byte[]> _receivedSegments = [];
    private readonly List<LinkId> _acceptedLinks = [];

    /// <summary>Constructs an observer with its own Reactor, sized for the Links the caller registers on it.</summary>
    public WireObserver(int capacity = 4)
    {
        _reactor = new RawReactor(new ReactorOptions { Capacity = capacity });
    }

    /// <summary>Every datagram payload observed so far, in arrival order, each a defensive copy safe to keep past this observer's next Pump.</summary>
    public IReadOnlyList<byte[]> ReceivedDatagrams => _receivedDatagrams;

    /// <summary>Every stream Segment payload observed so far, in arrival order — concatenate these yourself if you need the whole run a peer wrote in one call.</summary>
    public IReadOnlyList<byte[]> ReceivedSegments => _receivedSegments;

    /// <summary>Every stream Link this observer's listener has accepted so far, in acceptance order.</summary>
    public IReadOnlyList<LinkId> AcceptedLinks => _acceptedLinks;

    /// <summary>
    /// Sends raw bytes back over a Link this observer has accepted, standing in for the counterparty's
    /// own protocol traffic — e.g. the handshake record a Session Reactor's dial needs back before it
    /// will establish. This observer records only what it receives; it never interprets or frames what
    /// it sends, so the caller is responsible for handing it bytes already shaped the way the Session
    /// protocol under test expects.
    /// </summary>
    public void SendOnAcceptedLink(LinkId link, byte[] bytes) => _reactor.Send(link, bytes);

    /// <summary>Binds a UDP Link to observe datagrams sent to it, returning the address a Session Reactor should be told to send to.</summary>
    public IPEndPoint BindUdp(IPEndPoint endpoint)
    {
        LinkId link = _reactor.Bind(endpoint);
        return _reactor.LocalOf(link);
    }

    /// <summary>Listens for TCP connections to observe the Segments written on them, returning the address a Session Reactor should be told to connect to.</summary>
    public IPEndPoint ListenTcp(IPEndPoint endpoint)
    {
        LinkId listener = _reactor.Listen(endpoint);
        return _reactor.LocalOf(listener);
    }

    /// <summary>
    /// Advances this observer's Reactor by one Pump and copies out every datagram, Segment and
    /// accepted Link the Frame produced, so they outlive the arena that briefly held them.
    /// </summary>
    public void Pump()
    {
        _reactor.Pump();
        foreach (RawEvent evt in _reactor.Drain())
        {
            switch (evt.Kind)
            {
                case RawEventKind.DatagramReceived:
                    _receivedDatagrams.Add(evt.Payload.ToArray());
                    break;
                case RawEventKind.SegmentReceived:
                    _receivedSegments.Add(evt.Payload.ToArray());
                    break;
                case RawEventKind.Accepted:
                    _acceptedLinks.Add(evt.Link);
                    break;
            }
        }
    }

    /// <summary>
    /// Pumps up to <paramref name="maxPumps"/> times, stopping as soon as <paramref name="condition"/>
    /// is satisfied — the same bounded-poll shape <see cref="RawReactorHarness.PumpUntil"/> uses, for
    /// the same reason: loopback delivery is real OS scheduling, not a decision this process controls.
    /// </summary>
    public void PumpUntil(Func<bool> condition, int maxPumps = 20)
    {
        for (int i = 0; i < maxPumps && !condition(); i++)
        {
            Pump();
        }
    }

    public void Dispose() => _reactor.Dispose();
}
