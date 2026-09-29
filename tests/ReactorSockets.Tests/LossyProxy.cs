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
/// One datagram a <see cref="LossyProxy"/> observed arriving, captured before any fault was applied —
/// issue #307's instrument for asserting "no probe datagram is ever emitted" at the wire rather than at
/// an internal flag. Captured at arrival rather than at forwarding on purpose: a datagram this proxy's
/// own size threshold or every-Nth rule then drops still left the sender and still belongs on the wire
/// this assertion reasons about, the same way a real network would have carried it right up to the point
/// something dropped it.
/// </summary>
/// <param name="Direction">Which way the datagram was travelling when this proxy observed it.</param>
/// <param name="Payload">A defensive copy of the exact bytes observed — safe to keep past this proxy's next Pump.</param>
internal readonly record struct RelayCapture(RelayDirection Direction, byte[] Payload);

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
    private LinkId _upstreamFacing;
    private readonly IPEndPoint _upstream;
    private readonly Queue<Fault> _clientToUpstreamFaults = new();
    private readonly Queue<Fault> _upstreamToClientFaults = new();
    private readonly List<DelayedRelay> _delayed = [];
    private readonly List<RelayCapture> _captures = [];
    private readonly List<RelayCapture> _forwarded = [];
    private Func<byte[], bool>? _clientToUpstreamPayloadDropRule;
    private Func<byte[], bool>? _upstreamToClientPayloadDropRule;
    private byte[]? _heldForReorderClientToUpstream;
    private byte[]? _heldForReorderUpstreamToClient;
    private Source? _lastClient;
    private int _clientToUpstreamSizeThreshold = NoSizeThreshold;
    private int _upstreamToClientSizeThreshold = NoSizeThreshold;
    private int _clientToUpstreamLossEveryNth = NoLossEveryNth;
    private int _upstreamToClientLossEveryNth = NoLossEveryNth;
    private int _clientToUpstreamLossCount;
    private int _upstreamToClientLossCount;

    /// <summary>
    /// The size threshold value meaning "no standing size threshold" — every payload is forwarded
    /// regardless of length. Passing this to <see cref="SetSizeThreshold"/> clears whatever threshold
    /// was standing on that direction, because no ordinary payload can exceed it.
    /// </summary>
    public const int NoSizeThreshold = int.MaxValue;

    /// <summary>
    /// The value meaning "no standing every-Nth loss rule" — every relay that direction is a candidate
    /// to pass, subject only to the size threshold and the one-shot queue. Passing this to
    /// <see cref="SetLossEveryNth"/> clears whatever every-Nth rule was standing on that direction, the
    /// same way <see cref="NoSizeThreshold"/> clears the size threshold.
    /// </summary>
    public const int NoLossEveryNth = 0;

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

    /// <summary>
    /// Every datagram this proxy has observed arriving so far, in either direction and in arrival order,
    /// captured before any standing rule or one-shot fault was applied to it (see <see cref="RelayCapture"/>).
    /// Read-only and purely additive to this type's existing relay behaviour — nothing here changes what
    /// is forwarded, delayed, dropped, duplicated or reordered.
    /// </summary>
    public IReadOnlyList<RelayCapture> Captures => _captures;

    /// <summary>
    /// Every datagram this proxy actually put back on the wire, in the order it went out and in the
    /// direction it went — the counterpart to <see cref="Captures"/>, which records arrivals before any
    /// rule was applied. The difference between the two lists is exactly what this proxy's standing
    /// rules and one-shot faults dropped, which is what lets a caller derive how much of a set actually
    /// reached the far end from the proxy itself rather than assuming it (issue #317). A duplicated
    /// relay appears twice and a delayed one appears at the Pump that released it, because both are
    /// what the far end genuinely saw.
    /// </summary>
    public IReadOnlyList<RelayCapture> Forwarded => _forwarded;

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
    /// Sets (or, with <see cref="NoSizeThreshold"/>, clears) a standing size threshold on
    /// <paramref name="direction"/>: while it stands, every relay that direction whose payload length
    /// exceeds <paramref name="maxPayloadLength"/> is read off the wire and never forwarded, and every
    /// relay at or below it passes unchanged. Unlike <see cref="DropNext"/> and the other one-shot
    /// faults, this is not an instruction consumed by the next relay — it is a property of the path that
    /// holds for as long as it is set, because the condition it models (a link that carries small
    /// datagrams but black-holes large ones) has to be true of every relay for the length of a test, not
    /// just the next one. The proxy never inspects a payload beyond its length, so it neither knows nor
    /// cares what the oversized datagrams actually are.
    /// </summary>
    public void SetSizeThreshold(RelayDirection direction, int maxPayloadLength)
    {
        if (direction == RelayDirection.ClientToUpstream)
        {
            _clientToUpstreamSizeThreshold = maxPayloadLength;
        }
        else
        {
            _upstreamToClientSizeThreshold = maxPayloadLength;
        }
    }

    /// <summary>
    /// Sets (or, with <see cref="NoLossEveryNth"/>, clears) a standing every-Nth loss rule on
    /// <paramref name="direction"/>: while it stands, every <paramref name="everyNth"/>th relay that
    /// direction reaches this rule is read off the wire and never forwarded, and every other relay
    /// passes this rule unchanged. This is expressed as a count rather than a probability because a
    /// probability makes the negative test it exists for flaky and its failures unreproducible — "a
    /// tenth of everything is lost" written as a 10% chance needs a seed nobody wants to record to
    /// reproduce a failure, where every tenth relay dropped is the same tenth every run. It matches
    /// <see cref="DropNext"/> and friends in being scheduled rather than randomised, and matches
    /// <see cref="SetSizeThreshold"/> in being a standing property of the path rather than a one-shot
    /// instruction: setting it restarts counting from zero for <paramref name="direction"/>, even if the
    /// same value was already standing, and it stands until cleared. <paramref name="everyNth"/> must be
    /// non-negative; a negative count does not correspond to any dropping schedule and is rejected with
    /// <see cref="ArgumentOutOfRangeException"/> rather than silently coerced into one.
    /// </summary>
    public void SetLossEveryNth(RelayDirection direction, int everyNth)
    {
        if (everyNth < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(everyNth), everyNth, "Every-Nth loss count must be non-negative.");
        }

        if (direction == RelayDirection.ClientToUpstream)
        {
            _clientToUpstreamLossEveryNth = everyNth;
            _clientToUpstreamLossCount = 0;
        }
        else
        {
            _upstreamToClientLossEveryNth = everyNth;
            _upstreamToClientLossCount = 0;
        }
    }

    /// <summary>
    /// Sets (or, with <see langword="null"/>, clears) a standing payload rule on
    /// <paramref name="direction"/>: while it stands, every relay that direction whose payload
    /// <paramref name="shouldDrop"/> answers <see langword="true"/> for is read off the wire and never
    /// forwarded, and every other relay passes this rule unchanged. This is the only rule here that
    /// looks at a payload's content rather than only its length, and it exists because neither other
    /// standing rule can express partial delivery of a named set (issue #317): a size threshold
    /// black-holes every datagram of a given size, and an every-Nth count cannot name which datagrams
    /// it drops. A caller that needs "these Fragments and no others, retransmissions included, never
    /// arrive" — which is what keeps an incomplete Fragment set incomplete for a whole run — supplies
    /// that as a predicate over bytes it already knows how to decode. The proxy itself still knows
    /// nothing about the protocol: it hands the exact bytes over and does whatever it is told.
    /// </summary>
    /// <remarks>
    /// Consulted after <see cref="SetSizeThreshold"/> and before <see cref="SetLossEveryNth"/>, for the
    /// same reason the threshold comes first: a datagram this rule kills never reaches the every-Nth
    /// rule, so it does not advance that rule's count and cannot shift which later relay that rule
    /// drops. It consumes no queued one-shot fault either, so a fault queued behind it stays queued for
    /// the next relay that actually goes out.
    /// </remarks>
    public void SetPayloadDropRule(RelayDirection direction, Func<byte[], bool>? shouldDrop)
    {
        if (direction == RelayDirection.ClientToUpstream)
        {
            _clientToUpstreamPayloadDropRule = shouldDrop;
        }
        else
        {
            _upstreamToClientPayloadDropRule = shouldDrop;
        }
    }

    /// <summary>
    /// Installs a payload drop rule that matches everything, in both directions at once (issue #352 and
    /// the liveness tests that follow it): the shape every "the two Sessions can no longer hear each
    /// other, but both Reactors are still Pumping" test needs, without either direction's Reactor
    /// noticing a socket error or a proxy shutdown — from each Reactor's own view, its datagrams simply
    /// stop arriving, exactly as a real path failure would look.
    /// </summary>
    public void StopRelayingBothDirections()
    {
        SetPayloadDropRule(RelayDirection.ClientToUpstream, _ => true);
        SetPayloadDropRule(RelayDirection.UpstreamToClient, _ => true);
    }

    /// <summary>
    /// Undoes <see cref="StopRelayingBothDirections"/>: clears the payload drop rule on both directions,
    /// so relaying resumes. It clears only those two rules, so any size threshold, every-Nth loss rule
    /// or queued one-shot fault a test set separately keeps standing, and datagrams the stop dropped
    /// stay dropped.
    /// </summary>
    public void ResumeRelayingBothDirections()
    {
        SetPayloadDropRule(RelayDirection.ClientToUpstream, null);
        SetPayloadDropRule(RelayDirection.UpstreamToClient, null);
    }

    /// <summary>
    /// Replaces the upstream-facing socket with a freshly bound one, modelling a NAT rebinding: every
    /// relay after this call reaches upstream from a new source port, upstream's replies to the new
    /// address still reach the client, and the old socket is closed so replies sent to the old address
    /// are never relayed. Composes with <see cref="StopRelayingBothDirections"/> and
    /// <see cref="ResumeRelayingBothDirections"/> in either order, and leaves every standing rule,
    /// queued fault and held reorder datagram untouched. Delayed relays already scheduled are re-pointed
    /// at the new socket so they release from the new port instead of the closed one.
    /// </summary>
    /// <returns>The new upstream-facing address, the source upstream will now see.</returns>
    public IPEndPoint MoveUpstreamFacingSocket()
    {
        LinkId old = _upstreamFacing;

        // The Reactor's capacity is exactly two Links, so the old one must close before the new one binds.
        // Its Closed event is skipped by Pump, and datagrams still queued on it are discarded with it.
        _reactor.Close(old);
        _upstreamFacing = _reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));

        for (int i = 0; i < _delayed.Count; i++)
        {
            if (_delayed[i].From == old)
            {
                _delayed[i] = _delayed[i] with { From = _upstreamFacing };
            }
        }

        return _reactor.LocalOf(_upstreamFacing);
    }

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
                byte[] payload = evt.Payload.ToArray();
                _captures.Add(new RelayCapture(RelayDirection.ClientToUpstream, payload));
                Relay(RelayDirection.ClientToUpstream, _upstreamFacing, Source.FromIPEndPoint(_upstream), payload);
            }
            else if (evt.Link == _upstreamFacing && _lastClient is Source client)
            {
                byte[] payload = evt.Payload.ToArray();
                _captures.Add(new RelayCapture(RelayDirection.UpstreamToClient, payload));
                Relay(RelayDirection.UpstreamToClient, _clientFacing, client, payload);
            }
        }

        ReleaseDueDelayedRelays();
    }

    /// <summary>
    /// Applies the three standing rules, then the next queued fault (or none), to one relay. The size
    /// threshold is consulted first and unconditionally: a payload it kills is read off the wire and
    /// dropped without the every-Nth loss rule ever seeing it, so a relay the threshold drops does not
    /// advance that rule's count — the datagram never reached the loss rule, so counting it would drop
    /// the wrong relay later. The payload drop rule (<see cref="SetPayloadDropRule"/>) is consulted
    /// second, on the same reasoning and with the same consequence: a datagram it kills advances no
    /// count and consumes no queued fault. The every-Nth loss rule is consulted third, still ahead of the one-shot
    /// queue: a relay it drops consumes no queued fault, so a fault queued behind a standing loss rule
    /// stays queued for the next relay that actually goes out — a fault never applied to anything has not
    /// happened yet. Either standing rule dropping a relay is not a relay for reorder purposes either: it
    /// does not release a held-back datagram and does not itself become the held datagram. A
    /// <see cref="FaultKind.Reorder"/> fault holds <paramref name="payload"/> back entirely rather than
    /// sending it now — it is released only once the <em>next</em> relay in the same direction has been
    /// sent, whatever that next relay's own fault turns out to be, so the two datagrams swap wire order
    /// without either's own content or fault handling changing.
    /// </summary>
    private void Relay(RelayDirection direction, LinkId from, Source to, byte[] payload)
    {
        if (payload.Length > ThresholdFor(direction))
        {
            return;
        }

        if (PayloadDropRuleFor(direction) is Func<byte[], bool> shouldDrop && shouldDrop(payload))
        {
            return;
        }

        if (ShouldDropByEveryNth(direction))
        {
            return;
        }

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

    /// <summary>
    /// Puts one datagram back on the wire and records it in <see cref="Forwarded"/>. Every path that
    /// forwards anything — an unfaulted relay, each half of a duplicate, a released delay, a released
    /// reorder — goes through here, which is what makes <see cref="Forwarded"/> a complete account of
    /// what left this proxy rather than a partial one. The direction is recovered from
    /// <paramref name="from"/>: a datagram leaving the client-facing Link is on its way back to the
    /// client, and one leaving the upstream-facing Link is on its way to upstream.
    /// </summary>
    private void Send(LinkId from, Source to, byte[] payload)
    {
        _forwarded.Add(new RelayCapture(
            from == _clientFacing ? RelayDirection.UpstreamToClient : RelayDirection.ClientToUpstream,
            payload));
        _reactor.SendTo(from, to, payload);
    }

    private Func<byte[], bool>? PayloadDropRuleFor(RelayDirection direction) =>
        direction == RelayDirection.ClientToUpstream ? _clientToUpstreamPayloadDropRule : _upstreamToClientPayloadDropRule;

    private Queue<Fault> QueueFor(RelayDirection direction) =>
        direction == RelayDirection.ClientToUpstream ? _clientToUpstreamFaults : _upstreamToClientFaults;

    private int ThresholdFor(RelayDirection direction) =>
        direction == RelayDirection.ClientToUpstream ? _clientToUpstreamSizeThreshold : _upstreamToClientSizeThreshold;

    /// <summary>
    /// Advances the every-Nth loss count for <paramref name="direction"/> by one relay reaching this
    /// rule and reports whether this is the Nth one, resetting the count back to zero when it is — so
    /// the rule drops the Nth, 2Nth, 3Nth, … relay counted from whenever it was last set. Returns
    /// <see langword="false"/> without touching the count when no rule stands on this direction.
    /// </summary>
    private bool ShouldDropByEveryNth(RelayDirection direction)
    {
        int everyNth = LossEveryNthFor(direction);
        if (everyNth == NoLossEveryNth)
        {
            return false;
        }

        ref int count = ref direction == RelayDirection.ClientToUpstream
            ? ref _clientToUpstreamLossCount
            : ref _upstreamToClientLossCount;

        count++;
        if (count < everyNth)
        {
            return false;
        }

        count = 0;
        return true;
    }

    private int LossEveryNthFor(RelayDirection direction) =>
        direction == RelayDirection.ClientToUpstream ? _clientToUpstreamLossEveryNth : _upstreamToClientLossEveryNth;

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
