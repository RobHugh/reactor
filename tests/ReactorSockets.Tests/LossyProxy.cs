using System.Net;

namespace ReactorSockets.Tests;

/// <summary>Which side of a <see cref="LossyProxy"/> a scheduled fault applies to.</summary>
internal enum RelayDirection
{
    /// <summary>Client-facing Link toward the upstream endpoint.</summary>
    ClientToUpstream,

    /// <summary>Upstream endpoint back toward the most recent client.</summary>
    UpstreamToClient,
}

/// <summary>
/// A <see cref="RawReactor"/> relaying UDP datagrams between a client-facing address and a fixed
/// upstream endpoint, dropping, duplicating, reordering and delaying under explicit test control — the
/// third instrument the parent slice spec's Further Notes calls for. Built entirely from the public
/// Raw surface, and driven by the same <see cref="TimeProvider"/> as everything else under test, so a
/// delayed relay's release is a deterministic function of <see cref="TimeProvider.GetUtcNow"/> rather
/// than a wall-clock sleep.
/// </summary>
/// <remarks>
/// <para>
/// Faults are scheduled, not randomised: <see cref="DropNext"/>, <see cref="DuplicateNext"/>,
/// <see cref="DelayNext"/> and <see cref="ReorderNextTwo"/> each queue exactly one instruction for the
/// next relay this proxy performs in the named direction, so a test that wants "the third datagram
/// this way is dropped" pumps two ordinary relays through first rather than tuning a probability until
/// it reproduces. A relay with no fault queued passes through immediately and unmodified.
/// </para>
/// <para>
/// This proxy tracks exactly one client at a time — whichever address last sent it a datagram — which
/// is what every use in slices 3 through 8 needs: one Session under test, dialogued through the proxy
/// to a counterparty. A test wanting several simultaneous clients needs several proxies.
/// </para>
/// </remarks>
internal sealed class LossyProxy : IDisposable
{
    private readonly RawReactor _reactor;
    private readonly TimeProvider _timeProvider;
    private readonly LinkId _clientFacing;
    private readonly LinkId _upstreamFacing;
    private readonly IPEndPoint _upstream;
    private readonly Queue<Fault> _clientToUpstreamFaults = new();
    private readonly Queue<Fault> _upstreamToClientFaults = new();
    private readonly List<DelayedRelay> _delayed = [];
    private byte[]? _heldForReorderClientToUpstream;
    private byte[]? _heldForReorderUpstreamToClient;
    private Source? _lastClient;

    /// <summary>
    /// Constructs a proxy that relays toward <paramref name="upstream"/>, timing its delayed relays
    /// off <paramref name="timeProvider"/> — pass the same <see cref="FakeTimeProvider"/> the rest of a
    /// test's fixture uses, so advancing it releases a delayed relay deterministically.
    /// </summary>
    public LossyProxy(IPEndPoint upstream, TimeProvider timeProvider)
    {
        _upstream = upstream;
        _timeProvider = timeProvider;
        _reactor = new RawReactor(new ReactorOptions { Capacity = 2, TimeProvider = timeProvider });
        _clientFacing = _reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        _upstreamFacing = _reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
    }

    /// <summary>The address a client should be told to send to — this proxy relays whatever arrives here toward <c>upstream</c>.</summary>
    public IPEndPoint PublicEndPoint => _reactor.LocalOf(_clientFacing);

    /// <summary>Queues a drop for the next relay in <paramref name="direction"/>: the datagram is read off the wire and never forwarded.</summary>
    public void DropNext(RelayDirection direction) => QueueFor(direction).Enqueue(Fault.Drop);

    /// <summary>Queues a duplicate for the next relay in <paramref name="direction"/>: the datagram is forwarded twice.</summary>
    public void DuplicateNext(RelayDirection direction) => QueueFor(direction).Enqueue(Fault.Duplicate);

    /// <summary>Queues a delay for the next relay in <paramref name="direction"/>: the datagram is held until this proxy's <see cref="TimeProvider"/> reaches <paramref name="delay"/> past the moment it arrived.</summary>
    public void DelayNext(RelayDirection direction, TimeSpan delay) => QueueFor(direction).Enqueue(Fault.ForDelay(delay));

    /// <summary>
    /// Queues a reorder for the next relay in <paramref name="direction"/>: that datagram is held back
    /// and forwarded only after the one immediately following it in the same direction, swapping their
    /// wire order.
    /// </summary>
    public void ReorderNextTwo(RelayDirection direction) => QueueFor(direction).Enqueue(Fault.Reorder);

    /// <summary>
    /// Advances this proxy's own Reactor by one Pump, relaying whatever arrived and releasing any
    /// delayed relay whose deadline this proxy's <see cref="TimeProvider"/> has now reached.
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

            if (evt.Link == _clientFacing)
            {
                _lastClient = evt.Source;
                Relay(RelayDirection.ClientToUpstream, _upstreamFacing, Source.FromIPEndPoint(_upstream), evt.Payload.ToArray());
            }
            else if (evt.Link == _upstreamFacing && _lastClient is Source client)
            {
                Relay(RelayDirection.UpstreamToClient, _clientFacing, client, evt.Payload.ToArray());
            }
        }

        ReleaseDueDelayedRelays();
    }

    /// <summary>
    /// Applies the next queued fault (or none) to one relay. A <see cref="FaultKind.Reorder"/> fault
    /// holds <paramref name="payload"/> back entirely rather than sending it now — it is released only
    /// once the <em>next</em> relay in the same direction has been sent, whatever that next relay's own
    /// fault turns out to be, so the two datagrams swap wire order without either's own content or
    /// fault handling changing.
    /// </summary>
    private void Relay(RelayDirection direction, LinkId from, Source to, byte[] payload)
    {
        Queue<Fault> faults = QueueFor(direction);
        Fault fault = faults.Count > 0 ? faults.Dequeue() : Fault.None;

        if (fault.Kind == FaultKind.Reorder)
        {
            SetHeld(direction, payload);
            return;
        }

        ApplyFault(fault, from, to, payload);

        byte[]? held = TakeHeld(direction);
        if (held is not null)
        {
            Send(from, to, held);
        }
    }

    private void ApplyFault(Fault fault, LinkId from, Source to, byte[] payload)
    {
        switch (fault.Kind)
        {
            case FaultKind.None:
                Send(from, to, payload);
                break;
            case FaultKind.Drop:
                break;
            case FaultKind.Duplicate:
                Send(from, to, payload);
                Send(from, to, payload);
                break;
            case FaultKind.Delay:
                _delayed.Add(new DelayedRelay(from, to, payload, _timeProvider.GetUtcNow() + fault.Delay));
                break;
        }
    }

    private void SetHeld(RelayDirection direction, byte[] payload)
    {
        if (direction == RelayDirection.ClientToUpstream)
        {
            _heldForReorderClientToUpstream = payload;
        }
        else
        {
            _heldForReorderUpstreamToClient = payload;
        }
    }

    private byte[]? TakeHeld(RelayDirection direction)
    {
        if (direction == RelayDirection.ClientToUpstream)
        {
            byte[]? held = _heldForReorderClientToUpstream;
            _heldForReorderClientToUpstream = null;
            return held;
        }
        else
        {
            byte[]? held = _heldForReorderUpstreamToClient;
            _heldForReorderUpstreamToClient = null;
            return held;
        }
    }

    private void ReleaseDueDelayedRelays()
    {
        DateTimeOffset now = _timeProvider.GetUtcNow();
        for (int i = _delayed.Count - 1; i >= 0; i--)
        {
            DelayedRelay relay = _delayed[i];
            if (relay.ReleaseAt <= now)
            {
                Send(relay.From, relay.To, relay.Payload);
                _delayed.RemoveAt(i);
            }
        }
    }

    private void Send(LinkId from, Source to, byte[] payload) => _reactor.SendTo(from, to, payload);

    private Queue<Fault> QueueFor(RelayDirection direction) =>
        direction == RelayDirection.ClientToUpstream ? _clientToUpstreamFaults : _upstreamToClientFaults;

    public void Dispose() => _reactor.Dispose();

    private enum FaultKind
    {
        None,
        Drop,
        Duplicate,
        Delay,
        Reorder,
    }

    private readonly record struct Fault(FaultKind Kind, TimeSpan Delay)
    {
        public static readonly Fault None = new(FaultKind.None, default);
        public static readonly Fault Drop = new(FaultKind.Drop, default);
        public static readonly Fault Duplicate = new(FaultKind.Duplicate, default);
        public static readonly Fault Reorder = new(FaultKind.Reorder, default);

        public static Fault ForDelay(TimeSpan delay) => new(FaultKind.Delay, delay);
    }

    private readonly record struct DelayedRelay(LinkId From, Source To, byte[] Payload, DateTimeOffset ReleaseAt);
}
