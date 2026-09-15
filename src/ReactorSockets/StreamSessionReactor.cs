using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using ReactorSockets.Core;
using ReactorSockets.Session;
using ReactorSockets.Transports;

namespace ReactorSockets;

/// <summary>
/// The stream Session profile (ADR 0010): Sessions established over TCP, addressed as Peers,
/// delivering <c>u32</c> length-prefixed Messages rather than the byte runs the transport gives.
/// Slices 3.1-3.3 validate <see cref="StreamSessionOptions"/> and its allocation bill at
/// construction, bind the declared endpoint set, and carry a Peer through the handshake to an
/// established Session. Slice 3.4 adds the data plane: each established Peer reassembles Messages in
/// its own persistent receive buffer and delivers each whole one as a
/// <see cref="SessionEventKind.MessageReceived"/> event copied into the shared frame arena. Retention,
/// write batching and the Goodbye exchange are not yet built and land in the sub-issues that follow.
/// </summary>
/// <remarks>
/// <para>
/// There is no cookie, no ack, no sequence space, no fragment and no RTO in this profile: TCP already
/// provides what those were built for. The datagram Session protocol is deliberately not carried over
/// TCP, because that would put retransmission on top of retransmission.
/// </para>
/// <para>
/// <b>Send timing (ADR 0011).</b> Nagle (<c>TCP_NODELAY</c>) is off, unconditionally, on every stream
/// Session socket — it is not a knob on <see cref="StreamSessionOptions"/> and cannot be turned back
/// on. In its place, every <see cref="Send"/> queued since the last <see cref="Pump"/> is written to
/// the wire together, once, by that <see cref="Pump"/> call: batching we do ourselves, on our own
/// schedule, rather than the connection's delayed acknowledgements deciding when a small Message may
/// leave. That is a better version of what Nagle exists for, without Nagle's cost — a small Message
/// waiting up to a round trip behind outstanding data — so a consumer measuring latency needs to know
/// which of the two is doing the batching: theirs, by calling <see cref="Pump"/> less often, or ours,
/// always.
/// </para>
/// </remarks>
public sealed class StreamSessionReactor : IDisposable, ISessionEventSource
{
    private readonly SessionReactorLifecycle _lifecycle = new();
    private readonly StreamSessionOptions _options;
    private readonly ReadinessDispatcher _dispatcher;
    private readonly DiagnosticGateway _diagnostics;
    private readonly ReactorCounterSet _counters;
    private readonly FrameArena _arena;
    private readonly RetentionArena _retention;
    private readonly ShareHolderTable<PeerSlot> _peers;
    private readonly RandomNumberGenerator _entropy;
    private readonly PumpIntervalObserver _pumpIntervalObserver;
    private readonly List<TcpListenLink> _listenLinks = [];
    private readonly List<SlotHandle> _connectingPeers = [];
    private readonly List<PendingEvent> _pendingEvents = [];
    private readonly List<RefusalSlot> _refusals = [];
    private readonly List<PendingAdmission> _pendingAdmissions = [];
    private readonly HashSet<SlotHandle> _peersWithBufferedMessages = [];
    private readonly HashSet<SlotHandle> _peersWithPendingRetentionWrites = [];
    private readonly HashSet<SlotHandle> _peersWithGoodbyeOwed = [];
    private readonly HashSet<SlotHandle> _peersWithAdvancedReceivedCount = [];
    private readonly HashSet<SlotHandle> _peersPendingClose = [];
    private readonly HashSet<SlotHandle> _peersWithRedialScheduled = [];

    // The two fixed-size segment lists a delivery counter riding a retention write is described by
    // (issue #132): the counter record plus one unsent run, or the counter record plus both runs of a
    // wrapped one. Allocated once with the Reactor and rewritten in place on every gather write, for
    // the same reason PeerSlot.DeliveryCounterRecord is — this fires on every Pump a two-way Peer's
    // received count advances, which is steady state rather than an exceptional path, and
    // Socket.Send's gather overload takes an IList whose Count is the segment count, so a single
    // oversized array cannot stand in for both shapes.
    private readonly ArraySegment<byte>[] _counterAndOneRun = new ArraySegment<byte>[2];
    private readonly ArraySegment<byte>[] _counterAndBothRuns = new ArraySegment<byte>[3];
    private readonly int _admissionCap;
    private int _admissionsThisFrame;
    private int _capDeferralsThisFrame;

    /// <summary>
    /// Validates <paramref name="options"/> — every knob and cross-check, then the frame arena's
    /// floor — before anything is allocated or bound, then binds every declared endpoint in the order
    /// supplied. A bind that fails throws here, naming the endpoint: binding is part of what this
    /// Reactor is, so there is no half-built Reactor to hand back.
    /// </summary>
    /// <param name="options">
    /// Composed rather than inherited (ADR 0011): the shared L0 budget plus this profile's own
    /// required and defaulted knobs, with no shared base between profiles.
    /// </param>
    public StreamSessionReactor(StreamSessionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        int arenaSize = options.ResolveArenaSize();

        _options = options;
        _dispatcher = new ReadinessDispatcher();
        _counters = new ReactorCounterSet();
        _diagnostics = new DiagnosticGateway(options.Reactor.Sink, options.Reactor.MinimumLevel, _counters);
        _arena = new FrameArena(arenaSize, options.Reactor.Capacity);
        // The true floor of any single TryAppend call this Reactor ever makes: the Goodbye sentinel
        // (AppendGoodbyeOrDefer) is exactly LengthPrefixSize bytes with no payload at all, and Send
        // enforces no lower bound on message.Length, so a zero-payload Message frames to the same
        // LengthPrefixSize bytes. Neither ever writes fewer bytes than that, so this is the minimum
        // RetentionArena can be told without it rejecting a write this Reactor actually makes.
        _retention = new RetentionArena(
            options.RetentionBytes,
            options.Reactor.Capacity,
            MessageFramingCodec.LengthPrefixSize);
        _peers = new ShareHolderTable<PeerSlot>(options.Reactor.Capacity, ReactorOrigin.Next());
        _entropy = options.Entropy;
        _admissionCap = options.Reactor.ResolvedAdmissionCap;
        _pumpIntervalObserver = new PumpIntervalObserver(options.Reactor.TimeProvider);

        try
        {
            foreach (IPEndPoint endpoint in options.Endpoints)
            {
                BindAndRegister(endpoint);
            }
        }
        catch
        {
            _listenLinks.DisposeAndClear(_dispatcher, link => link.Socket);
            throw;
        }
    }

    /// <summary>
    /// The endpoints this Reactor is bound to right now, in the order they were declared. A snapshot:
    /// it answers what is bound, and there is no member that binds another — there is no rebinding
    /// (ADR 0014), and unlike the datagram profile a stream dial never binds a wildcard socket of its
    /// own, since TCP's own connect already binds an ephemeral local port implicitly.
    /// </summary>
    public IReadOnlyList<IPEndPoint> BoundEndpoints
    {
        get
        {
            EnsureUsable();

            IPEndPoint[] snapshot = new IPEndPoint[_listenLinks.Count];
            for (int index = 0; index < _listenLinks.Count; index++)
            {
                snapshot[index] = _listenLinks[index].LocalEndPoint;
            }

            return snapshot;
        }
    }

    /// <summary>A point-in-time snapshot of this Reactor's own counters (ADR 0013). No reset.</summary>
    public ReactorCounters Counters
    {
        get
        {
            EnsureUsable();
            return _counters.Snapshot();
        }
    }

    /// <summary>
    /// The interval this Reactor's own Pump calls are currently observed to arrive at
    /// (<see cref="PumpIntervalObserver"/>), read fresh from every Pump. This is the derived floor
    /// the re-dial schedule (<see cref="RedialSchedule"/>, issue #125) starts its first delay at: a
    /// retry sooner than the next Pump cannot happen. Internal because a consumer never constructs a
    /// <see cref="RedialSchedule"/> themselves — this Reactor does, on their behalf, once a re-dial is
    /// needed.
    /// </summary>
    internal TimeSpan ObservedPumpInterval => _pumpIntervalObserver.Interval;

    /// <inheritdoc cref="ISessionEventSource.PendingEventCount"/>
    int ISessionEventSource.PendingEventCount => _pendingEvents.Count;

    /// <summary>
    /// One synchronous advance: records this call's arrival against the last one
    /// (<see cref="ObservedPumpInterval"/>, issue #112) before anything else, then wipes the previous
    /// Frame's events — which is also the moment a Peer
    /// still <see cref="SessionEventKind.PeerClosing"/> only because the last Frame's Messages had not
    /// been drained past yet becomes eligible to close (<see cref="RetryPeersPendingClose"/>) — then
    /// admits ready connections, advances every in-flight dial and reads whatever bound socket is
    /// ready right now, running the handshake for everything that arrived. Every Peer whose received
    /// count advanced somewhere in that work — this Frame's own reads, or a held-back Message finally
    /// delivered by <see cref="RetryPeersWithBufferedMessages"/> — then has its delivery counter emitted
    /// exactly once (<see cref="EmitDeliveryCounters"/>, issue #116/#25's second trigger), before every
    /// Peer's queued retention bytes are flushed exactly once
    /// (<see cref="FlushPendingRetentionWrites"/>, #90/3.6.2) regardless of how many <see cref="Send"/>
    /// calls queued them. Finally, every established Peer's two independent idle timers (#26) are
    /// enforced in one scan (<see cref="EnforceIdleTimers"/>): a Peer from which nothing has been
    /// received for <see cref="PeerSlot.RemoteSendIdleInterval"/> × <see cref="StreamSessionOptions.ResolvedMissedHeartbeats"/>
    /// moves to <see cref="SessionEventKind.PeerInterrupted"/> (issue #117, revised by issue #119), an
    /// already-Interrupted Peer instead closes with <see cref="DisconnectReason.LingerExpired"/> once
    /// its effective Linger has elapsed since the interruption (issue #121), and every surviving Peer
    /// that has written nothing
    /// to the wire — including whatever this Frame's own flush just carried — for at least
    /// <see cref="StreamSessionOptions.SendIdleInterval"/> sends the same delivery-counter record as a
    /// heartbeat (issue #114/#26's first idle timer), unconditionally and regardless of whether anything
    /// was ever received from it. Once idle timers may have closed an Interrupted Peer on Linger expiry
    /// (issue #121), every Interrupted Peer this Reactor itself dialled is given its re-dial attempt for
    /// this Frame (<see cref="ProcessRedials"/>, issue #125) — a schedule derived from the observed Pump
    /// interval just recorded above and the effective Linger, never a separate attempt-count knob, so a
    /// Peer whose Linger closed it this same Frame is never also given one. Throws
    /// <see cref="InvalidOperationException"/> if called from any thread other than the one that
    /// constructed this Reactor, or from inside a Pump already in progress.
    /// </summary>
    public void Pump()
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();

        _lifecycle.InPump = true;
        try
        {
            _pumpIntervalObserver.Observe();
            _pendingEvents.Clear();
            _arena.Reset();
            _admissionsThisFrame = 0;
            _capDeferralsThisFrame = 0;
            RetryPeersPendingClose();
            RetryPeersWithBufferedMessages();
            RetryGoodbyeAppends();
            _dispatcher.Pump();
            EmitDeliveryCounters();
            FlushPendingRetentionWrites();
            EnforceIdleTimers();
            ProcessConnectingPeers();
            ProcessRedials();
            ReportDeclinedAdmissions();
        }
        finally
        {
            _lifecycle.InPump = false;
        }
    }

    /// <summary>
    /// Drains every event produced this Frame — since the last <see cref="Pump"/> — in the order it was
    /// produced.
    /// </summary>
    public SessionEventEnumerator Drain()
    {
        EnsureUsable();
        return new(this);
    }

    /// <summary>
    /// Begins a Session with <paramref name="destination"/>, returning the <see cref="PeerId"/> that
    /// will address it. The Peer occupies a capacity Slot from this moment, exactly as a dialling Link
    /// does in <see cref="RawReactor"/>: the handshake is work this Reactor is holding, and pretending
    /// otherwise would let a consumer dial past capacity and discover it only on the far side of a
    /// round trip. <see cref="SessionEventKind.PeerConnected"/> or <see cref="SessionEventKind.PeerClosed"/>
    /// resolves it on a later Pump.
    /// </summary>
    public PeerId Connect(IPEndPoint destination)
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();
        ArgumentNullException.ThrowIfNull(destination);

        if (!_peers.HasFreeSlot)
        {
            throw new InvalidOperationException(
                $"This Reactor's capacity of {_options.Reactor.Capacity} Peers is fully occupied — close a Peer before dialling another.");
        }

        TcpLink link = TcpLink.Dial(destination, _options.SendBufferSize);
        PeerSlot slot = new()
        {
            Link = link,
            LocalResumeSecret = NextResumeSecret(),
            State = HandshakeState.Connecting,
            ReceiveBuffer = new byte[_options.ResolvedReceiveBufferSize],
            DeliveryCounterRecord = new byte[MessageFramingCodec.DeliveryCounterRecordSize],
            DialedEndpoint = destination,
        };

        // `HasFreeSlot` was just checked and this Reactor is single-threaded (ADR 0002), so `TryAdd`
        // cannot fail here — the guard is defensive rather than a case this branch expects to take.
        if (_peers.TryAdd(slot) is not SlotHandle handle)
        {
            link.Dispose();
            throw new InvalidOperationException(
                $"This Reactor's capacity of {_options.Reactor.Capacity} Peers is fully occupied — close a Peer before dialling another.");
        }

        slot.Handle = handle;
        _connectingPeers.Add(handle);

        return ToPeerId(handle);
    }

    /// <summary>
    /// Announces this end is ending the Session with <paramref name="peer"/> on purpose: attempts to
    /// append a Goodbye control record to whatever this Peer's retention region already holds, so it
    /// is the last thing flushed and everything queued before it is delivered first, then forbids any
    /// further <see cref="Send"/> to it. If retention has no room for it yet, the attempt is retried
    /// on every later Pump (<see cref="RetryGoodbyeAppends"/>) until it commits — <see cref="Close"/>
    /// must never leave the far end open indefinitely because the Goodbye it promised was silently
    /// dropped (issue #23 review finding), and this end never closes itself locally until its own
    /// Goodbye has actually been appended and flushed. The <see cref="SessionEventKind.PeerClosed"/>
    /// carrying <see cref="DisconnectReason.LocalClose"/> is queued once that flush fully drains —
    /// immediately, on a later Pump, if nothing was queued when <see cref="Close"/> was called, or
    /// after however many Pumps a short write (or a retention append retry) takes to carry everything
    /// already retained plus the Goodbye itself. Idempotent: a stale <see cref="PeerId"/>, or a Peer
    /// already closing, is a no-op — a Peer closing under a caller, or a caller that closes the same
    /// Peer twice, is an ordinary race rather than a programmer error.
    /// </summary>
    public void Close(PeerId peer)
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();

        SlotHandle handle = ToHandle(peer);
        if (_peers.TryResolve(handle) is not PeerSlot slot || slot.LocalGoodbyeSent)
        {
            return;
        }

        slot.LocalGoodbyeSent = true;
        if (slot.PendingCloseReasonAfterRetentionDrains is not null)
        {
            // A remote Goodbye already put this Peer into Closing (or would have closed it outright,
            // in which case _peers.TryResolve above would already have failed) — the peer already
            // knows this Session is ending, so there is nothing further to announce on the wire.
            return;
        }

        AppendGoodbyeOrDefer(handle, slot);
    }

    /// <summary>
    /// Attempts to append this end's own Goodbye sentinel to <paramref name="handle"/>'s retention
    /// region. On <see cref="RetentionOutcome.Committed"/>, marks the write pending
    /// (<see cref="FlushPendingRetentionWrites"/> carries it out) and records
    /// <see cref="PeerSlot.PendingCloseReasonAfterRetentionDrains"/> as
    /// <see cref="DisconnectReason.LocalClose"/> — set only now, once the Goodbye is truly queued
    /// behind whatever else this Peer's retention already held, never eagerly at <see cref="Close"/>:
    /// setting it before the append succeeds is exactly the bug this method closes, since
    /// <see cref="TryCloseAfterRetentionDrained"/> would otherwise see retention run dry once the
    /// pre-<see cref="Close"/> Sends flush and close this end locally without the Goodbye it promised
    /// ever having reached the wire. The <c>??=</c> is what keeps that record first-writer-wins even
    /// on the one path that reaches here without passing <see cref="Close"/>'s guard: a
    /// <see cref="Close"/> whose append was deferred, followed by an inbound Goodbye recording
    /// <see cref="DisconnectReason.PeerGoodbye"/>, followed by <see cref="RetryGoodbyeAppends"/>
    /// finally committing — the far end's reason was recorded first and stays.
    /// On <see cref="RetentionOutcome.Exhausted"/>, the attempt is
    /// recorded in <see cref="_peersWithGoodbyeOwed"/> for <see cref="RetryGoodbyeAppends"/> to retry
    /// on a later Pump, mirroring the shape #81's <see cref="RetryPeersWithBufferedMessages"/>
    /// established — that set is for Messages a Peer sent this end; this one is for the single control
    /// record this end owes the far end.
    /// </summary>
    private void AppendGoodbyeOrDefer(SlotHandle handle, PeerSlot slot)
    {
        Span<byte> goodbye = stackalloc byte[MessageFramingCodec.LengthPrefixSize];
        MessageFramingCodec.WriteGoodbye(goodbye);
        RetentionOutcome outcome = _retention.TryAppend(handle.Index, goodbye);
        switch (outcome)
        {
            case RetentionOutcome.Committed:
                slot.HasPendingRetentionWrite = true;
                _peersWithPendingRetentionWrites.Add(handle);
                slot.PendingCloseReasonAfterRetentionDrains ??= DisconnectReason.LocalClose;
                _peersPendingClose.Add(handle);
                _peersWithGoodbyeOwed.Remove(handle);
                break;

            case RetentionOutcome.Exhausted:
                // No room yet for the Goodbye sentinel behind whatever Sends already queued.
                // RetryGoodbyeAppends re-attempts this on a later Pump once bytes are released — Close
                // must never leave the ending unannounced just because retention happened to be full
                // the moment it was called.
                _peersWithGoodbyeOwed.Add(handle);
                break;

            default:
                throw new InvalidOperationException($"Unrecognised {nameof(RetentionOutcome)}: {outcome}.");
        }
    }

    /// <summary>
    /// Re-attempts <see cref="AppendGoodbyeOrDefer"/> for every Peer whose <see cref="Close"/> found
    /// retention exhausted at the time. Runs once, at the start of every Pump, after an earlier
    /// Frame's flush may have released the room the first attempt lacked — the same position and
    /// snapshot-then-clear shape as <see cref="RetryPeersWithBufferedMessages"/> (#81), so a Peer whose
    /// append keeps failing is simply re-added to <see cref="_peersWithGoodbyeOwed"/> by
    /// <see cref="AppendGoodbyeOrDefer"/> itself rather than left off this list.
    /// </summary>
    private void RetryGoodbyeAppends()
    {
        if (_peersWithGoodbyeOwed.Count == 0)
        {
            return;
        }

        SlotHandle[] pending = [.. _peersWithGoodbyeOwed];
        _peersWithGoodbyeOwed.Clear();

        foreach (SlotHandle handle in pending)
        {
            if (_peers.TryResolve(handle) is PeerSlot slot)
            {
                AppendGoodbyeOrDefer(handle, slot);
            }
        }
    }

    /// <summary>
    /// A point-in-time snapshot of one Peer's counters (ADR 0013). Throws if the Peer is not open.
    /// Mirrors <see cref="RawReactor.CountersOf(LinkId)"/>'s shape for the Raw profile.
    /// </summary>
    public PeerCounters CountersOf(PeerId peer)
    {
        EnsureUsable();

        SlotHandle handle = ToHandle(peer);
        PeerSlot slot = _peers.TryResolve(handle)
            ?? throw new ArgumentException($"Peer {peer} is not open.", nameof(peer));

        return slot.Counters.Snapshot(_retention.HighWaterBytes(handle.Index));
    }

    /// <summary>
    /// Frames <paramref name="message"/> as a <c>u32</c> big-endian length prefix followed by its
    /// bytes (<see cref="MessageFramingCodec"/>, ADR 0010) and appends the framed bytes to
    /// <paramref name="peer"/>'s retention region. Nothing is written to the socket here, ever (#90,
    /// 3.6.2 write batching) — a successful append only sets this Peer's pending-write marker
    /// (<see cref="PeerSlot.HasPendingRetentionWrite"/>), which the next <see cref="Pump"/>'s explicit
    /// flush step (<see cref="FlushPendingRetentionWrites"/>) drains in a single write carrying
    /// everything queued, including whatever was already sitting there. A Send with no following Pump
    /// call puts nothing on the wire. Refuses once a Goodbye has passed in either direction — this
    /// end's own (<see cref="PeerSlot.LocalGoodbyeSent"/>) or the far end's arrived and this Peer is
    /// already <see cref="SessionEventKind.PeerClosing"/> or on its way to
    /// <see cref="SessionEventKind.PeerClosed"/> (<see cref="PeerSlot.PendingCloseReasonAfterRetentionDrains"/>)
    /// — since a Message queued after a Goodbye is not one the far end has any business receiving
    /// (issue #23).
    /// </summary>
    /// <remarks>
    /// <see cref="PeerSlot.IsInterrupted"/> is deliberately not one of the refusal conditions above
    /// (issue #120, ADR 0010's backpressure rule): a Send against an Interrupted Peer is exactly as
    /// welcome as one against a live Peer, because holding retention across the interruption for a
    /// possible Resume is the entire point of not closing the Peer in the first place. The append
    /// behaves identically either way — the difference is only downstream, in
    /// <see cref="FlushRetention"/>'s own <c>IsInterrupted</c> guard, which leaves these bytes queued
    /// rather than attempting a dead socket. Once the ring genuinely has no room,
    /// <see cref="RetentionOutcome.Exhausted"/> reports the same way it would for a live Peer — the
    /// ring does not know or care why nothing has drained it — so an interruption that outlasts the
    /// send rate reaches <see cref="SendOutcome.RetentionExhausted"/> exactly like a single oversized
    /// Message or a sustained rate would, the third of the three paths ADR 0010 names to it.
    /// </remarks>
    /// <param name="peer">The Peer to send to. Throws if not open, matching <see cref="CountersOf"/>.</param>
    /// <param name="message">
    /// The Message payload, unframed. Throws if longer than <see cref="StreamSessionOptions.MaxMessageSize"/> —
    /// the same floor the receive side enforces, so this can never frame a Message the far end's
    /// announced limit would reject.
    /// </param>
    public SendOutcome Send(PeerId peer, ReadOnlySpan<byte> message)
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();

        if (message.Length > _options.MaxMessageSize)
        {
            throw new ArgumentException(
                $"message of {message.Length} bytes exceeds MaxMessageSize of {_options.MaxMessageSize}.",
                nameof(message));
        }

        SlotHandle handle = ToHandle(peer);
        PeerSlot slot = _peers.TryResolve(handle) ?? throw new ArgumentException($"Peer {peer} is not open.", nameof(peer));

        if (slot.LocalGoodbyeSent || slot.PendingCloseReasonAfterRetentionDrains is not null)
        {
            return SendOutcome.Closed;
        }

        Span<byte> framed = new byte[MessageFramingCodec.LengthPrefixSize + message.Length];
        MessageFramingCodec.WriteLengthPrefix(framed, (uint)message.Length);
        message.CopyTo(framed[MessageFramingCodec.LengthPrefixSize..]);

        RetentionOutcome outcome = _retention.TryAppend(handle.Index, framed);
        if (outcome == RetentionOutcome.Committed)
        {
            slot.HasPendingRetentionWrite = true;
            _peersWithPendingRetentionWrites.Add(handle);
        }

        return outcome switch
        {
            RetentionOutcome.Committed => SendOutcome.Sent,
            RetentionOutcome.Exhausted => SendOutcome.RetentionExhausted,
            _ => throw new InvalidOperationException($"Unrecognised {nameof(RetentionOutcome)}: {outcome}."),
        };
    }

    /// <summary>
    /// Releases every socket this Reactor bound or accepted. A Reactor owns every socket it bound, so
    /// a consumer that drops one without disposing it leaks those sockets until each
    /// <see cref="Socket"/>'s finalizer runs. No <see cref="SessionEventKind.PeerClosed"/> events are
    /// queued: nothing can drain a disposed Reactor. Idempotent; every other member throws
    /// <see cref="ObjectDisposedException"/> afterwards.
    /// </summary>
    /// <remarks>
    /// <see cref="StreamSessionOptions.Entropy"/> is deliberately not disposed. It may be a source the
    /// consumer holds and uses elsewhere, and a Reactor disposing a value it was merely handed would be
    /// reaching outside what it owns.
    /// </remarks>
    public void Dispose()
    {
        if (_lifecycle.IsDisposed)
        {
            return;
        }

        EnsureCallingThreadIsConstructingThread();
        EnsureNotInsideAPump();

        foreach (PeerSlot slot in _peers.DrainAll())
        {
            _dispatcher.Unregister(slot.Link.Socket);
            slot.Link.Dispose();
        }

        foreach (RefusalSlot refusal in _refusals)
        {
            _dispatcher.Unregister(refusal.Link.Socket);
            refusal.Link.Dispose();
        }

        foreach (PendingAdmission pending in _pendingAdmissions)
        {
            _dispatcher.Unregister(pending.Link.Socket);
            pending.Link.Dispose();
        }

        _refusals.Clear();
        _pendingAdmissions.Clear();
        _connectingPeers.Clear();
        _pendingEvents.Clear();
        _peersWithBufferedMessages.Clear();
        _peersWithPendingRetentionWrites.Clear();
        _peersPendingClose.Clear();
        _peersWithGoodbyeOwed.Clear();
        _peersWithAdvancedReceivedCount.Clear();
        _lifecycle.IsDisposed = true;
        _listenLinks.DisposeAndClear(_dispatcher, link => link.Socket);
    }

    /// <inheritdoc cref="ISessionEventSource.GetPendingEvent"/>
    SessionEvent ISessionEventSource.GetPendingEvent(int index)
    {
        PendingEvent pending = _pendingEvents[index];
        if (pending.Kind.TryCreateCommonSessionEvent(pending.Peer, pending.Reason, out SessionEvent common))
        {
            return common;
        }

        return pending.Kind switch
        {
            SessionEventKind.MessageReceived => SessionEvent.ForMessageReceived(
                pending.Peer, _arena.GetCommitted(pending.PayloadOffset, pending.PayloadLength)),
            SessionEventKind.PeerClosing => SessionEvent.ForPeerClosing(pending.Peer),
            SessionEventKind.PeerInterrupted => SessionEvent.ForPeerInterrupted(pending.Peer),
            SessionEventKind.PeerResumed => SessionEvent.ForPeerResumed(pending.Peer),
            _ => throw new InvalidOperationException($"{pending.Kind} is not a recognised SessionEventKind."),
        };
    }

    /// <summary>
    /// Binds one listening socket and registers it for readiness dispatch and accept. The binding
    /// rules are unchanged from the Raw profile's (ADR 0014): exclusive, no dual-mode, no hostname, no
    /// configure callback.
    /// </summary>
    private void BindAndRegister(IPEndPoint endpoint)
    {
        TcpListenLink link = TcpListenLink.Listen(endpoint);
        _listenLinks.Add(link);
        _dispatcher.Register(link.Socket, () => OnListeningLinkReadable(link));
    }

    /// <summary>
    /// Accepts as many pending connections as the shared admission cap still allows this Frame,
    /// stopping the moment it is spent or the backlog runs dry — whichever comes first. Every accepted
    /// connection is admitted as a Peer or, if this Reactor has no capacity Slot free, refused
    /// in-protocol and closed (subtask 3.3): a burst of connections beyond capacity is an
    /// accept-refuse-close cycle inside the admission budget, never a silent drop and never left
    /// waiting in the backlog the way a cap-exhausted Frame's remainder is.
    /// </summary>
    private void OnListeningLinkReadable(TcpListenLink link)
    {
        while (true)
        {
            if (_admissionsThisFrame >= _admissionCap)
            {
                if (link.HasPendingConnection)
                {
                    _capDeferralsThisFrame++;
                }

                return;
            }

            TcpAcceptResult result = link.TryAccept();
            switch (result.Outcome)
            {
                case TcpAcceptOutcome.NoPendingConnection:
                    return;
                case TcpAcceptOutcome.Failed:
                    // No Diagnostic is logged here: there is no Peer to attribute it to. The listening
                    // socket itself is unaffected — see TcpListenLink.TryAccept's own remarks — so the
                    // next Pump simply tries again.
                    return;
                case TcpAcceptOutcome.Accepted when result.IsAccepted:
                    _admissionsThisFrame++;
                    HoldUntilFirstRecord(result.Socket);
                    break;
            }
        }
    }

    /// <summary>
    /// Holds a socket a listening Link just accepted outside <see cref="_peers"/> until its first
    /// handshake record has arrived in full, deciding nothing before then. Nothing is sent to it and
    /// no capacity Slot is touched: the record is what says whether this connection is a fresh dial
    /// asking for a Slot or a Resume claiming one it already owns, and that difference has to be known
    /// before the capacity check runs (issue #123, ADR 0011). Whatever the far end has already sent is
    /// read here rather than left for the next Pump, so a dialler whose record landed before this
    /// accept is still answered on the Frame that accepted it — the same reason
    /// <see cref="RefuseForCapacity"/> drains before it closes. A connection with nothing waiting is
    /// left for the dispatcher, which is what <see cref="TcpLink.HasPendingBytes"/> is checked for.
    /// </summary>
    /// <remarks>
    /// Accepting first and deciding after is unchanged from before this held state existed: the
    /// admission cap alone bounds how many of these accept-then-decide cycles one Frame performs
    /// (Scope: "a burst of a thousand inbound connections is a thousand accept-refuse-close cycles").
    /// What changed is only how much is known at the decision point.
    /// </remarks>
    private void HoldUntilFirstRecord(Socket acceptedSocket)
    {
        TcpLink link = TcpLink.FromAccepted(acceptedSocket, _options.SendBufferSize);
        PendingAdmission pending = new() { Link = link };
        _pendingAdmissions.Add(pending);
        _dispatcher.Register(link.Socket, () => OnPendingAdmissionReadable(pending));
        if (link.HasPendingBytes)
        {
            OnPendingAdmissionReadable(pending);
        }
    }

    /// <summary>
    /// Accumulates a held connection's first handshake record across Pumps, exactly as
    /// <see cref="OnPeerReadable"/> does for a Peer that already has a Slot. A record already filled is
    /// never read again — the decision it triggered owns this connection from that point on. A
    /// transport that ends or fails before a whole record arrived is dropped without ever having been
    /// anything: no Slot was taken and no Peer was reported, so there is nothing to close.
    /// </summary>
    private void OnPendingAdmissionReadable(PendingAdmission pending)
    {
        if (pending.BytesFilled >= StreamHandshakeCodec.RecordSize)
        {
            return;
        }

        TcpLinkReceiveResult result = pending.Link.Receive(pending.Record.AsSpan(pending.BytesFilled));
        switch (result.Outcome)
        {
            case TcpLinkReceiveOutcome.Delivered:
                pending.BytesFilled += result.Payload.Length;
                if (pending.BytesFilled == StreamHandshakeCodec.RecordSize)
                {
                    OnPendingAdmissionRecordFilled(pending);
                }

                break;
            case TcpLinkReceiveOutcome.EndOfStream:
            case TcpLinkReceiveOutcome.Failed:
                DiscardPendingAdmission(pending);
                break;
        }
    }

    /// <summary>
    /// Decides what a held connection is, now that its whole first record has arrived. The order of
    /// the three arms is the point of issue #123 and ADR 0011: <b>the resume match runs before the
    /// capacity check</b>. A reconnecting Peer is not asking for a Slot, it already owns one — the very
    /// Slot its Interrupted self is reserving — so checking capacity first would refuse a reconnection
    /// into a Reactor holding the state being reconnected to, which is the failure reserving exists to
    /// prevent and which only appears once the Reactor is full.
    /// </summary>
    /// <remarks>
    /// A Resume matching no held Peer is closed rather than admitted: a Resume carries no version and
    /// no announced parameters (issue #115), so there is nothing in one to establish a fresh Session
    /// from, and treating it as a silent success would hand back a Session that does not exist. What an
    /// operator reads for that, and for the other refusal paths, is
    /// <see cref="DiagnosticId.ResumeUnmatched"/> (issue #124). A record naming no known type is dropped
    /// here too, ahead of the capacity check, so the discard arm still refuses rather than answering it
    /// with a Refusal that implies it was understood. A Hello announcing a secret a reservation is
    /// holding is refused ahead of the capacity check as well, since it is a claim on that reserved
    /// Session and not a request for a Slot (<see cref="IsHelloClaimingAReservedSession"/>). Everything
    /// else — an ordinary Hello, a Refusal, and how each is answered — reaches
    /// <see cref="OnHandshakeRecordFilled"/> through <see cref="AdmitPendingAdmission"/> unchanged.
    /// </remarks>
    private void OnPendingAdmissionRecordFilled(PendingAdmission pending)
    {
        if (!StreamHandshakeCodec.TryReadMessageType(pending.Record, out StreamHandshakeMessageType type))
        {
            DiscardPendingAdmission(pending);
            return;
        }

        if (type == StreamHandshakeMessageType.Resume)
        {
            if (!TryResumeHeldPeer(pending))
            {
                DiscardPendingAdmission(pending);
            }

            return;
        }

        if (type == StreamHandshakeMessageType.Hello
            && IsHelloClaimingAReservedSession(pending, out int reservationsChecked))
        {
            LogResumeUnmatched(pending, reservationsChecked, StreamHandshakeMessageType.Hello);
            DiscardPendingAdmission(pending);
            return;
        }

        if (!_peers.HasFreeSlot)
        {
            RefuseForCapacity(pending);
            return;
        }

        AdmitPendingAdmission(pending);
    }

    /// <summary>
    /// Answers whether a held connection's Hello is really a claim on a reserved Session rather than a
    /// fresh dial: it announces the resume secret one of this Reactor's Interrupted Peers is holding
    /// its Slot against, but arrives in the one record shape that also re-announces the Session's
    /// parameters. That is the third way a Resume is refused (issue #124), and it is a protocol
    /// violation rather than something to accept or ignore. A Resume has no field for parameters by
    /// construction (issue #115), and re-announcing them would break the retention ring underneath the
    /// Session: retention holds framed bytes admitted against the <c>MaxMessageSize</c> in force when
    /// they were sent, so a party announcing a smaller one has made retained Messages illegal, leaving
    /// only dropping them — silently breaking reliable delivery where a consumer cannot observe it —
    /// or sending them anyway, which makes the announced limit a lie.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A genuine fresh dial announces the secret <em>it</em> minted for the Session it is opening, so
    /// it reaches the same scan and matches nothing. The connection is closed rather than answered with
    /// a Refusal for the same reason every other protocol violation in this profile is: the wire
    /// <see cref="RefusalReason"/> roster names in-protocol declines a well-behaved dialler can act on,
    /// and this is not one. What an operator reads for it is
    /// <see cref="DiagnosticId.ResumeUnmatched"/>, logged by the caller.
    /// </para>
    /// <para>
    /// This is the whole of the third refusal path. Issue #124 also describes it as reachable by "a
    /// Resume from a party that has changed its mind about the Session it is resuming", but that shape
    /// is unrepresentable: a Resume carries a secret and a delivery counter and nothing else (issue
    /// #115), so there is no field in one for a changed parameter to arrive in. A Hello is the only
    /// record that can both present the secret and re-announce the parameters, which is why it is the
    /// only case there is to check.
    /// </para>
    /// </remarks>
    private bool IsHelloClaimingAReservedSession(PendingAdmission pending, out int reservationsChecked)
    {
        Span<byte> announcedSecret = stackalloc byte[StreamSessionProtocol.ResumeSecretLength];
        if (!StreamHandshakeCodec.TryRead(pending.Record, out _, out _, out _, out _, announcedSecret))
        {
            reservationsChecked = 0;
            return false;
        }

        return MatchInterruptedSlot(announcedSecret, out reservationsChecked) is not null;
    }

    /// <summary>
    /// Gives a held connection a capacity Slot and carries it into the handshake it was already
    /// holding: this end's own Hello goes out first, then the record already read is dispatched exactly
    /// as it would have been had it arrived after the Slot existed.
    /// </summary>
    private void AdmitPendingAdmission(PendingAdmission pending)
    {
        TcpLink link = ReleasePendingAdmission(pending);
        PeerSlot slot = new()
        {
            Link = link,
            LocalResumeSecret = NextResumeSecret(),
            State = HandshakeState.AwaitingPeerHandshake,
            ReceiveBuffer = new byte[_options.ResolvedReceiveBufferSize],
            DeliveryCounterRecord = new byte[MessageFramingCodec.DeliveryCounterRecordSize],
        };

        // `HasFreeSlot` was just checked and this Reactor is single-threaded (ADR 0002), so `TryAdd`
        // cannot fail here — the guard is defensive rather than a case this branch expects to take.
        if (_peers.TryAdd(slot) is not SlotHandle handle)
        {
            link.Dispose();
            return;
        }

        slot.Handle = handle;
        pending.Record.CopyTo(slot.HandshakeBuffer, 0);
        slot.HandshakeBytesFilled = StreamHandshakeCodec.RecordSize;
        RegisterPeerTransport(handle, slot);
        SendHandshakeRecord(handle, slot);
        OnHandshakeRecordFilled(handle, slot);
    }

    /// <summary>
    /// Matches a held connection's Resume record onto the Interrupted Peer whose Slot is reserving the
    /// Session it names, answering whether one was found. The secret authorises the match and never
    /// selects it — <see cref="MatchInterruptedSlot"/> owns the scan and the constant-time comparison.
    /// An established Peer is never a candidate: its transport is alive, and a second connection
    /// claiming a live Session is not a reconnection. A claim that matches nothing is refused, saying
    /// so to an operator as <see cref="DiagnosticId.ResumeUnmatched"/> and to the party as a close.
    /// </summary>
    /// <remarks>
    /// The delivery counter the record carries drives the rewind (issue #127); the <c>Resumed</c> event
    /// the consumer sees is issue #128.
    /// </remarks>
    private bool TryResumeHeldPeer(PendingAdmission pending)
    {
        Span<byte> presentedSecret = stackalloc byte[StreamSessionProtocol.ResumeSecretLength];
        if (!StreamHandshakeCodec.TryReadResume(pending.Record, presentedSecret, out ulong deliveryCount))
        {
            return false;
        }

        if (MatchInterruptedSlot(presentedSecret, out int candidatesChecked) is not PeerSlot matched)
        {
            LogResumeUnmatched(pending, candidatesChecked, StreamHandshakeMessageType.Resume);
            return false;
        }

        ResumeHeldPeer(matched, ReleasePendingAdmission(pending), deliveryCount);
        return true;
    }

    /// <summary>
    /// Scans every Interrupted Peer's reservation for one whose resume secret is
    /// <paramref name="presentedSecret"/>, reporting through <paramref name="candidatesChecked"/> how
    /// many reservations were held to be scanned at all — 0 says nothing was being held, which is what
    /// tells a once-valid secret arriving after its Linger expired apart from a secret that was never
    /// right. Every candidate is compared, in constant time and without stopping at the first that
    /// matches, so neither the comparison nor the loop leaks which Slot answered or whether any did —
    /// the same discipline <see cref="CookieMinter"/> applies on the datagram side.
    /// </summary>
    private PeerSlot? MatchInterruptedSlot(ReadOnlySpan<byte> presentedSecret, out int candidatesChecked)
    {
        PeerSlot? matched = null;
        int candidates = 0;
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is not PeerSlot slot || !slot.IsInterrupted)
            {
                continue;
            }

            candidates++;
            if (CryptographicOperations.FixedTimeEquals(slot.LocalResumeSecret, presentedSecret))
            {
                matched = slot;
            }
        }

        candidatesChecked = candidates;
        return matched;
    }

    /// <summary>
    /// Logs the <see cref="DiagnosticId.ResumeUnmatched"/> record for a held connection whose claim on
    /// an existing Session was refused. Subject is the claimed Source, read off the still-connected
    /// transport before it is released: an unmatched claim comes from a party that is by construction
    /// not a Peer, so there is no Peer to name.
    /// </summary>
    private void LogResumeUnmatched(PendingAdmission pending, int candidatesChecked, StreamHandshakeMessageType type)
    {
        if (!_diagnostics.IsEnabled(DiagnosticLevel.Warn))
        {
            return;
        }

        _diagnostics.Log(DiagnosticRecord.ForResumeUnmatched(
            Source.FromIPEndPoint(pending.Link.RemoteEndPoint), candidatesChecked, (long)type));
    }

    /// <summary>
    /// Puts a matched Resume's transport under the held Peer that was reserving the Slot, leaving that
    /// Peer's identity and state exactly as <see cref="InterruptPeer"/> left them: the same
    /// <see cref="PeerId"/>, generation included, the same counters, the same retention ring and the
    /// same undelivered inbound Messages. The identity survives the transport, which is the whole
    /// premise of a Resume — nothing here is renewed, and nothing is discarded.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Only what described the transport that died is cleared: the half-read handshake buffer and any
    /// record remainders that were still draining to a socket which no longer exists, and
    /// <see cref="PeerSlot.InterruptedTimestamp"/>, whose Linger deadline no longer applies. Both idle
    /// timers restart from now for the same reason they are stamped at establishment
    /// (<see cref="OnHelloRecordReceived"/>) — the interruption's stale timestamps would otherwise read
    /// as intervals already elapsed. A Peer that queued retention while Interrupted (issue #120) is put
    /// back on <see cref="_peersWithPendingRetentionWrites"/>, which <see cref="InterruptPeer"/> cleared
    /// only because it would have handed <see cref="FlushPendingRetentionWrites"/> a disposed socket;
    /// there is a live one again now.
    /// </para>
    /// <para>
    /// <b>The rewind (issue #127).</b> <paramref name="deliveryCount"/> — the Resume record's own
    /// delivery counter — is where the peer says it actually got to, so this end's outbound retention
    /// rewinds its sent cursor back to that Message boundary via
    /// <see cref="RetentionArena.RewindSentToMessageCount"/> before anything else here runs. Anything
    /// but <see cref="RetentionRewindOutcome.Rewound"/> means the peer's own counter names a position
    /// this end cannot honour — ahead of what it ever sent, or behind what it has already released — so
    /// this Peer is closed instead of resumed: no <see cref="SessionEventKind.PeerResumed"/> event, no
    /// <see cref="DiagnosticId.ResumeMatched"/> record. A non-zero rewind marks this Peer's pending-write
    /// bit so the next <see cref="FlushPendingRetentionWrites"/> resends exactly the bytes the rewind
    /// un-sent — the same one flush path every other retention write already rides, never a bespoke one.
    /// <see cref="RetentionArena.ReleaseThroughMessageCount"/> is also applied for
    /// <paramref name="deliveryCount"/>: the peer's reported counter is a delivery confirmation like any
    /// other, and releasing on it can only ever match or trail what the rewind's own guard already
    /// verified, so it is safe to apply unconditionally here.
    /// </para>
    /// <para>
    /// <b>The reciprocal rewind (issue #127, D3).</b> A Resume record is one-way — it carries only what
    /// the dialling side reports, never what this end reports back — so the dialling side has no way to
    /// learn where <em>this</em> end's own read position actually is except by asking. This end asks
    /// immediately, riding the same <see cref="SendDeliveryCounterOrArm"/> a received-count advance
    /// already uses (issue #116), rather than inventing a second wire shape for the same fact.
    /// </para>
    /// <para>
    /// <b>What this rewind does not touch (issue #262).</b> Every step above is outbound: it renegotiates
    /// only where <em>this end's own sent</em> retention resumes from, at the Message granularity
    /// <paramref name="deliveryCount"/> is expressed in. Nothing here reads or clears anything this end
    /// has received — an inbound reassembly buffer, were this profile to grow one, would be untouched by
    /// a rewind that never looks at it. The Datagram profile's own receive-window Fragment holding
    /// (<see cref="DatagramSessionReactor.HoldFragment"/>) already relies on exactly this separation:
    /// "resume at Message granularity" is a statement about the negotiated resume point, never about
    /// discarding whatever a receive window is still holding piece by piece underneath it.
    /// </para>
    /// </remarks>
    /// <param name="slot">The Interrupted Peer the match found.</param>
    /// <param name="link">The freshly accepted transport presenting the Resume.</param>
    /// <param name="deliveryCount">
    /// The Resume record's own delivery counter — the peer's claim of how many of this end's Messages
    /// it had already received before the transport was lost.
    /// </param>
    private void ResumeHeldPeer(PeerSlot slot, TcpLink link, ulong deliveryCount)
    {
        slot.Link = link;

        RetentionRewindOutcome rewindOutcome =
            _retention.RewindSentToMessageCount(slot.Handle.Index, deliveryCount, out ulong bytesRewound);
        if (rewindOutcome != RetentionRewindOutcome.Rewound)
        {
            ClosePeer(slot.Handle, DisconnectReasonForRewindRefusal(rewindOutcome));
            return;
        }

        slot.IsInterrupted = false;
        slot.InterruptedTimestamp = 0;
        slot.HandshakeBytesFilled = 0;
        slot.PendingHandshakeBytesToSend = null;
        slot.QueuedHandshakeSend = null;
        slot.PendingDeliveryCounterBytesToSend = null;
        slot.DeliveryCounterRidesRetentionWrite = false;
        slot.State = HandshakeState.Established;

        long now = _options.Reactor.TimeProvider.GetTimestamp();
        slot.LastSendTimestamp = now;
        slot.LastReceiveTimestamp = now;

        RegisterPeerTransport(slot.Handle, slot);

        // The peer's own reported counter is a delivery confirmation exactly like any other. The
        // rewind's own AheadOfAppended guard above already ruled out a count this end never sent, so
        // this can only ever release through a Message this share-holder truly appended — it can only
        // return Released, and it never moves the released total backwards, so it cannot undo anything
        // the rewind above needs preserved (issue #127's own "released stays released" guarantee).
        _retention.ReleaseThroughMessageCount(slot.Handle.Index, deliveryCount);

        if (slot.HasPendingRetentionWrite || bytesRewound > 0)
        {
            slot.HasPendingRetentionWrite = true;
            _peersWithPendingRetentionWrites.Add(slot.Handle);
        }

        // D3: tells the dialling side where this end's own read position is, since the Resume record
        // it just presented had no field for that. SendDeliveryCounterOrArm's own short-send remainder
        // guard makes this safe to call unconditionally rather than gating it on _peersWithAdvancedReceivedCount.
        SendDeliveryCounterOrArm(slot.Handle, slot);

        // Issue #128: the rewind above is what makes this end's Resume complete, so PeerResumed and the
        // Resumes counter are raised here rather than any earlier point in the match — a rewind refused
        // above closed the Peer instead and never reaches this line.
        slot.Counters.RecordResume();
        _pendingEvents.Add(new PendingEvent { Kind = SessionEventKind.PeerResumed, Peer = ToPeerId(slot.Handle) });

        if (_diagnostics.IsEnabled(DiagnosticLevel.Info))
        {
            _diagnostics.Log(DiagnosticRecord.ForResumeMatched(ToPeerId(slot.Handle), bytesRewound: (long)bytesRewound));
        }
    }

    /// <summary>
    /// Maps a <see cref="RetentionRewindOutcome"/> other than <see cref="RetentionRewindOutcome.Rewound"/>
    /// onto the <see cref="DisconnectReason"/> a refused rewind closes the Peer with (issue #130). Shared
    /// by both rewind call sites — <see cref="ResumeHeldPeer"/>'s own rewind off the Resume record's
    /// counter, and <see cref="DeliverCompleteMessages"/>'s <c>DeliveryCounterReceived</c> arm's
    /// reciprocal rewind — so the two refusal paths cannot drift onto different reasons for the same
    /// outcome. States the mapping as its own table with a discard arm that refuses rather than permits
    /// (the <c>RawLinkLegality</c> house rule <see cref="DisconnectReason"/>'s own remarks describe):
    /// <see cref="RetentionRewindOutcome.BelowReleasedHead"/> is this ticket's gap case — the peer's
    /// counter names bytes this end's ring already released — and maps to
    /// <see cref="DisconnectReason.RetentionGap"/> because the fix is a configuration one, more
    /// retention or more Linger. <see cref="RetentionRewindOutcome.AheadOfAppended"/> maps to
    /// <see cref="DisconnectReason.ProtocolViolation"/> instead: the peer is claiming Messages this end
    /// never sent at all, which no amount of retention capacity would have made valid, so it belongs
    /// with the other protocol-level refusals rather than this member. Any future
    /// <see cref="RetentionRewindOutcome"/> this switch does not name falls into the same
    /// <see cref="DisconnectReason.ProtocolViolation"/> arm rather than being read as a rewind that
    /// somehow succeeded.
    /// </summary>
    private static DisconnectReason DisconnectReasonForRewindRefusal(RetentionRewindOutcome outcome) => outcome switch
    {
        RetentionRewindOutcome.BelowReleasedHead => DisconnectReason.RetentionGap,
        RetentionRewindOutcome.AheadOfAppended => DisconnectReason.ProtocolViolation,
        _ => DisconnectReason.ProtocolViolation,
    };

    /// <summary>
    /// Takes a held connection out of <see cref="_pendingAdmissions"/> and off the dispatcher, handing
    /// its transport to the caller, which owns it from here — every arm out of the held state either
    /// gives the transport to a Peer, to a <see cref="RefusalSlot"/>, or disposes it.
    /// </summary>
    private TcpLink ReleasePendingAdmission(PendingAdmission pending)
    {
        _pendingAdmissions.Remove(pending);
        _dispatcher.Unregister(pending.Link.Socket);
        return pending.Link;
    }

    /// <summary>
    /// Drops a held connection outright, saying nothing on the wire. No Slot was ever taken and no
    /// <see cref="SessionEventKind.PeerConnected"/> was ever raised, so there is no Peer to close and
    /// nothing for a consumer to see.
    /// </summary>
    private void DiscardPendingAdmission(PendingAdmission pending) => ReleasePendingAdmission(pending).Dispose();

    /// <summary>
    /// Refuses a held connection because every capacity Slot is occupied, without ever giving it one:
    /// a Refusal record naming this Reactor's capacity is sent and the connection is then closed. Never
    /// counted as an Admission Deferral or Discard — a refusal says something on the wire and ends the
    /// connection, which is a different outcome from silently leaving declined work for a later Pump.
    /// <para>
    /// A Slot an Interrupted Peer is reserving for the length of its Linger (ADR 0010) counts as
    /// occupied here, so a fresh dial arriving against a reservation is refused down this same path and
    /// is counted the same way: neither admission counter moves (issue #107). The capacity figure the
    /// Refusal names is this Reactor's configured capacity either way, so a dialler cannot tell a
    /// reserved Slot apart from one holding an established Peer — it learns "full", never "gone".
    /// </para>
    /// <para>
    /// Reached only once the connection's first record has been read and found not to be a Resume this
    /// Reactor could match (<see cref="OnPendingAdmissionRecordFilled"/>, issue #123). That ordering is
    /// what keeps a reconnection out of this path: the Peer a Resume comes back for is the very reason
    /// the Slot it needs is occupied, so refusing it for capacity would refuse it for its own presence.
    /// </para>
    /// </summary>
    private void RefuseForCapacity(PendingAdmission pending)
    {
        TcpLink link = ReleasePendingAdmission(pending);
        Source remote = Source.FromIPEndPoint(link.RemoteEndPoint);
        if (_diagnostics.IsEnabled(DiagnosticLevel.Info))
        {
            _diagnostics.Log(DiagnosticRecord.ForPeerRefusedCapacity(remote, _peers.Capacity));
        }

        RefusalSlot refusal = new() { Link = link };
        _refusals.Add(refusal);
        _dispatcher.Register(link.Socket, () => DrainRefusal(refusal), () => OnRefusalWritable(refusal));

        // The record that got this connection here is already read, but a dialler that sent anything
        // behind it may have more sitting in the receive buffer — draining that now, before the Refusal
        // is sent, is what keeps the close below clean rather than a reset (unread inbound data at
        // close time is what forces one).
        DrainRefusal(refusal);

        byte[] record = new byte[StreamHandshakeCodec.RecordSize];
        StreamHandshakeCodec.WriteRefusal(record, RefusalReason.Capacity, (uint)_peers.Capacity, 0);
        SendRefusalRecord(refusal, record);
    }

    /// <summary>Sends a Refusal record, arming for a short send exactly as a Hello record does.</summary>
    private void SendRefusalRecord(RefusalSlot refusal, byte[] record)
    {
        if (SendPendingRecord(refusal.Link, null, record, out ReadOnlyMemory<byte> remainder) == PendingRecordSendOutcome.ShortSend)
        {
            refusal.PendingBytesToSend = remainder;
            return;
        }

        CloseRefusal(refusal);
    }

    /// <summary>Flushes whatever remains of a Refusal record a short send left behind, then closes.</summary>
    private void OnRefusalWritable(RefusalSlot refusal)
    {
        if (refusal.PendingBytesToSend is not { Length: > 0 } pending)
        {
            return;
        }

        if (SendPendingRecord(refusal.Link, null, pending, out ReadOnlyMemory<byte> remainder) == PendingRecordSendOutcome.ShortSend)
        {
            refusal.PendingBytesToSend = remainder;
            return;
        }

        CloseRefusal(refusal);
    }

    /// <summary>
    /// Reads and discards whatever a connection being refused sends — nothing it contains is ever
    /// examined, since a capacity refusal needs nothing from the far end and this exists solely to keep
    /// the eventual close from racing an unread Hello into a reset.
    /// </summary>
    private void DrainRefusal(RefusalSlot refusal)
    {
        Span<byte> scratch = stackalloc byte[64];
        while (true)
        {
            TcpLinkReceiveResult result = refusal.Link.Receive(scratch);
            if (result.Outcome != TcpLinkReceiveOutcome.Delivered || result.Payload.Length < scratch.Length)
            {
                return;
            }
        }
    }

    private void CloseRefusal(RefusalSlot refusal)
    {
        _dispatcher.Unregister(refusal.Link.Socket);
        refusal.Link.Dispose();
        _refusals.Remove(refusal);
    }

    /// <summary>
    /// Polls every Peer still dialling for a resolved TCP connect, in the order each was begun. A
    /// resolved dial is removed from the tracking list whether it succeeded or failed; one still in
    /// flight is left for the next Pump to check again. Each entry's generation is re-checked against
    /// the Slot before polling, so a Peer the consumer closed mid-dial is dropped rather than polling
    /// whatever now occupies that Slot.
    /// </summary>
    private void ProcessConnectingPeers()
    {
        int surviving = 0;
        for (int i = 0; i < _connectingPeers.Count; i++)
        {
            SlotHandle handle = _connectingPeers[i];
            if (_peers.TryResolve(handle) is not PeerSlot slot)
            {
                continue;
            }

            TcpConnectPollResult result = slot.Link.PollConnect();
            switch (result.Outcome)
            {
                case TcpConnectPollOutcome.Pending:
                    _connectingPeers[surviving++] = handle;
                    break;
                case TcpConnectPollOutcome.Connected:
                    slot.State = HandshakeState.AwaitingPeerHandshake;
                    RegisterPeerTransport(handle, slot);
                    SendHandshakeRecord(handle, slot);
                    break;
                case TcpConnectPollOutcome.Failed:
                    ClosePeer(handle, DisconnectReason.TransportFailed);
                    break;
            }
        }

        _connectingPeers.RemoveRange(surviving, _connectingPeers.Count - surviving);
    }

    private void RegisterPeerTransport(SlotHandle handle, PeerSlot slot) =>
        _dispatcher.Register(slot.Link.Socket, () => OnPeerReadable(handle), () => OnPeerWritable(handle));

    /// <summary>
    /// Writes this end's Hello record and sends as much of it as the socket will take right now.
    /// A short send arms this Peer's socket for <see cref="OnPeerWritable"/>, which flushes whatever
    /// remains — the record is 41 bytes, so a short send is not expected in practice, but nothing here
    /// assumes the whole record lands in one syscall.
    /// </summary>
    private void SendHandshakeRecord(SlotHandle handle, PeerSlot slot)
    {
        byte[] record = new byte[StreamHandshakeCodec.RecordSize];
        StreamHandshakeCodec.Write(
            record,
            StreamSessionProtocol.Version,
            (uint)_options.MaxMessageSize,
            (ulong)_options.SendIdleInterval.TotalMilliseconds,
            (ulong)_options.Linger.TotalMilliseconds,
            slot.LocalResumeSecret);

        SendRecordOrArm(handle, slot, record, closeReasonAfterSend: null);
    }

    /// <summary>
    /// Refuses a Peer whose Hello record announced a version this Reactor does not speak: sends a
    /// Refusal naming both versions, then closes with <see cref="DisconnectReason.VersionMismatch"/>
    /// once it is fully on the wire. The full Hello that revealed the mismatch has already been read in
    /// its entirety by the time this runs, so there is nothing unread left to force a reset on close.
    /// </summary>
    private void RefuseVersionAndClose(SlotHandle handle, PeerSlot slot, uint remoteVersion)
    {
        Source remote = Source.FromIPEndPoint(slot.Link.RemoteEndPoint);
        if (_diagnostics.IsEnabled(DiagnosticLevel.Info))
        {
            _diagnostics.Log(DiagnosticRecord.ForPeerRefusedVersion(remote, remoteVersion, StreamSessionProtocol.Version));
        }

        byte[] record = new byte[StreamHandshakeCodec.RecordSize];
        StreamHandshakeCodec.WriteRefusal(record, RefusalReason.Version, remoteVersion, StreamSessionProtocol.Version);
        SendRecordOrArm(handle, slot, record, closeReasonAfterSend: DisconnectReason.VersionMismatch);
    }

    /// <summary>
    /// Sends a handshake or Refusal record for <paramref name="slot"/>, arming for a short send exactly
    /// as before. When <paramref name="closeReasonAfterSend"/> is supplied, the Peer is closed with it
    /// the moment the record is fully on the wire — immediately if the send completes now, or from
    /// <see cref="OnPeerWritable"/> once a short send's remainder finishes flushing.
    /// </summary>
    /// <remarks>
    /// If this end's own earlier record (its Hello) short-sent and is still draining, its remainder in
    /// <see cref="PeerSlot.PendingHandshakeBytesToSend"/> must not be overwritten by a Refusal that
    /// becomes necessary before that flush finishes (#98) — writing <paramref name="record"/> to the
    /// wire now would interleave it into the still-unfinished one. This queues it in
    /// <see cref="PeerSlot.QueuedHandshakeSend"/> instead; <see cref="OnPeerWritable"/> sends it, in
    /// order, once the pending remainder is fully flushed.
    /// </remarks>
    private void SendRecordOrArm(SlotHandle handle, PeerSlot slot, byte[] record, DisconnectReason? closeReasonAfterSend)
    {
        if (slot.PendingHandshakeBytesToSend is { Length: > 0 })
        {
            slot.QueuedHandshakeSend = (record, closeReasonAfterSend);
            return;
        }

        switch (SendPendingRecord(slot.Link, slot.Counters, record, out ReadOnlyMemory<byte> remainder))
        {
            case PendingRecordSendOutcome.Failed:
                ClosePeer(handle, closeReasonAfterSend ?? DisconnectReason.TransportFailed);
                break;

            case PendingRecordSendOutcome.ShortSend:
                slot.PendingHandshakeBytesToSend = remainder;
                slot.PendingCloseReason = closeReasonAfterSend;
                break;

            case PendingRecordSendOutcome.Complete:
                if (closeReasonAfterSend is DisconnectReason reason)
                {
                    ClosePeer(handle, reason);
                }

                break;
        }
    }

    /// <summary>
    /// Flushes whatever remains of a handshake record a short send left behind, closes if a reason is
    /// pending on it, then, once any short-sent delivery-counter remainder
    /// (<see cref="PeerSlot.PendingDeliveryCounterBytesToSend"/>, issue #116) is also flushed, marks
    /// this Peer's retention pending again, so a short retention write's own re-arm
    /// (<see cref="FlushRetention"/>) is picked up by this Frame's explicit flush step
    /// (<see cref="FlushPendingRetentionWrites"/>) rather than writing to the socket directly here. This
    /// is what keeps every retention write routed through exactly one call site: a socket that is both
    /// freshly re-armed from a short write and independently marked pending by a same-Frame
    /// <see cref="Send"/> still gets flushed exactly once.
    /// </summary>
    private void OnPeerWritable(SlotHandle handle)
    {
        if (_peers.TryResolve(handle) is not PeerSlot slot)
        {
            return;
        }

        if (slot.PendingHandshakeBytesToSend is { Length: > 0 } pending)
        {
            switch (SendPendingRecord(slot.Link, slot.Counters, pending, out ReadOnlyMemory<byte> remainder))
            {
                case PendingRecordSendOutcome.Failed:
                    ClosePeer(handle, slot.PendingCloseReason ?? DisconnectReason.TransportFailed);
                    return;

                case PendingRecordSendOutcome.ShortSend:
                    slot.PendingHandshakeBytesToSend = remainder;
                    return;

                case PendingRecordSendOutcome.Complete:
                    slot.PendingHandshakeBytesToSend = null;
                    if (slot.PendingCloseReason is DisconnectReason reason)
                    {
                        ClosePeer(handle, reason);
                        return;
                    }

                    if (slot.QueuedHandshakeSend is { } queued)
                    {
                        slot.QueuedHandshakeSend = null;
                        SendRecordOrArm(handle, slot, queued.Record, queued.CloseReasonAfterSend);
                        return;
                    }

                    break;
            }
        }

        if (slot.PendingDeliveryCounterBytesToSend is { Length: > 0 } pendingCounter)
        {
            switch (SendPendingRecord(slot.Link, slot.Counters, pendingCounter, out ReadOnlyMemory<byte> remainder))
            {
                case PendingRecordSendOutcome.Failed:
                    InterruptOrCloseOnTransportFailure(handle, slot);
                    return;

                case PendingRecordSendOutcome.ShortSend:
                    // Mirrors SendDeliveryCounterOrArm's own remainder-shrinkage check: a ShortSend that
                    // sent nothing (EWOULDBLOCK before anything was accepted) leaves remainder at its
                    // full pre-call length and must not count as outbound activity.
                    if (remainder.Length < pendingCounter.Length)
                    {
                        MarkSendActivity(slot);
                    }

                    slot.PendingDeliveryCounterBytesToSend = remainder;
                    return;

                case PendingRecordSendOutcome.Complete:
                    slot.PendingDeliveryCounterBytesToSend = null;
                    MarkSendActivity(slot);
                    break;
            }
        }

        slot.HasPendingRetentionWrite = true;
        _peersWithPendingRetentionWrites.Add(handle);
    }

    /// <summary>Whether one attempted socket send of a short handshake/Refusal record failed, fully landed, or left a remainder.</summary>
    private enum PendingRecordSendOutcome
    {
        Failed,
        ShortSend,
        Complete,
    }

    /// <summary>
    /// The four-step shape both the Refusal send (<see cref="SendRefusalRecord"/>/<see cref="OnRefusalWritable"/>)
    /// and the handshake send (<see cref="SendRecordOrArm"/>/<see cref="OnPeerWritable"/>) repeat twice
    /// each (issue #103): send once, and either the send failed, the whole record landed, or a short
    /// send left <paramref name="remainder"/> for the caller to store and re-arm for. Arming on a short
    /// send happens here, since every caller does the same thing with it; what a failure or a complete
    /// send means — close, or close only if a reason was given — differs per caller and is left to it.
    /// <paramref name="counters"/> is the Peer the write is charged to (issue #325), or
    /// <see langword="null"/> for a Refusal, which has no Peer.
    /// </summary>
    private PendingRecordSendOutcome SendPendingRecord(
        TcpLink link,
        PeerCounterSet? counters,
        ReadOnlyMemory<byte> record,
        out ReadOnlyMemory<byte> remainder)
    {
        TcpLinkSendResult result = link.Send(record.Span);
        counters?.RecordSocketWrite();
        if (!result.IsSuccess)
        {
            remainder = default;
            return PendingRecordSendOutcome.Failed;
        }

        if (result.BytesSent < record.Length)
        {
            remainder = record[result.BytesSent..];
            _dispatcher.ArmWritable(link.Socket);
            return PendingRecordSendOutcome.ShortSend;
        }

        remainder = default;
        return PendingRecordSendOutcome.Complete;
    }

    /// <summary>
    /// Emits one delivery-counter control record to every Peer whose received count advanced somewhere
    /// in this Frame's reads (<see cref="_peersWithAdvancedReceivedCount"/>, populated by
    /// <see cref="DeliverCompleteMessages"/>) — issue #116/#25's second emission trigger, alongside the
    /// send-idle timer #114 lands separately. Runs once per Pump, after the readiness dispatcher and
    /// before <see cref="FlushPendingRetentionWrites"/>, which is what makes "once per Pump per Peer"
    /// true of this method the same way #90/3.6.2 makes it true of that one: however many Messages a
    /// Peer delivered this Frame, its handle is added to the set at most once each, so the record sent
    /// here always carries the Frame's final count rather than one per Message.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>No new timer, no new constant (issue #116).</b> This rides the Pump boundary the consumer
    /// already chose by calling <see cref="Pump"/> at whatever rate suits it, rather than a trigger every
    /// <c>N</c> received Messages — <c>N</c> would be a constant about the consumer's own traffic that
    /// governs the *peer's* retention, a number the consumer has no way to choose sensibly even if asked.
    /// The cost is bounded by construction: nothing is emitted for a Peer whose received count did not
    /// move, and at most one record per Peer regardless of how many Messages did.
    /// </para>
    /// <para>
    /// <b>Why this bypasses <see cref="RetentionArena.TryAppend"/> entirely, unlike the Goodbye sentinel
    /// (contrast <see cref="AppendGoodbyeOrDefer"/>).</b> <see cref="RetentionArena.TryAppend"/> counts
    /// every accepted write as one more appended "Message" and records its byte boundary in the ring the
    /// *peer's own* <c>ReleaseThroughMessageCount</c> confirmation later walks by ordinal position — a
    /// Goodbye can safely ride that path because it is appended exactly once, only after every real
    /// Message this end will ever send, so no legitimate confirmation from the peer can ever name its
    /// boundary (see <see cref="RetentionArena"/>'s class remarks). A delivery-counter record emitted
    /// here has neither property: it recurs every Pump the received count advances, so appending it
    /// through <see cref="RetentionArena.TryAppend"/> would (a) sit *between* real Messages in the
    /// boundary ring, shifting every later Message's ordinal position out from under the peer's own
    /// counter and corrupting exactly what <c>ReleaseThroughMessageCount</c> relies on, and (b) never
    /// itself be released — a peer's own delivery counter confirms Messages, never a control record it
    /// received (see <see cref="DeliverCompleteMessages"/>'s <c>DeliveryCounterReceived</c> arm) — so its
    /// bytes would sit in this end's own ring forever, a slow leak that eventually starves legitimate
    /// <see cref="Send"/> calls of room for no reason connected to real traffic. Writing it directly to
    /// the socket here, with its own short-write remainder
    /// (<see cref="PeerSlot.PendingDeliveryCounterBytesToSend"/>) exactly as a handshake or Refusal
    /// record already does (<see cref="SendPendingRecord"/>), avoids both failure modes: it never competes
    /// for retention-ring room and never perturbs a Message's boundary index.
    /// </para>
    /// <para>
    /// <b>How it costs a two-way Peer nothing (issue #132, #25's stated cost).</b> Bypassing the ring
    /// says where the bytes may not come from, not that they need a write of their own. When this Peer
    /// already has a retention write due this Pump (<see cref="PeerSlot.HasPendingRetentionWrite"/>),
    /// this method writes the record into <see cref="PeerSlot.DeliveryCounterRecord"/> and marks it to
    /// ride (<see cref="PeerSlot.DeliveryCounterRidesRetentionWrite"/>) instead of sending it;
    /// <see cref="FlushRetention"/> then offers the record and the unsent retention runs to the socket
    /// as one gather write (<see cref="TcpLink.Send(IList{ArraySegment{byte}})"/>). The record stays
    /// outside the ring — it is still this end's own buffer, never a retained Message, so no boundary
    /// ordinal moves and nothing awaits release — while a Peer with continuous two-way traffic pays no
    /// additional record and no additional syscall for the counter. A purely receiving Peer has no
    /// write to ride and gets the standalone send below, which is the one write per Pump #25 bounds it
    /// at. The ride is declined whenever the flush would not carry it out this Pump — a counter
    /// remainder still draining (<see cref="PeerSlot.PendingDeliveryCounterBytesToSend"/>), or a Peer
    /// <see cref="PeerSlot.AwaitingResumeCounter"/>, whose flush is a deliberate no-op (issue #127) and
    /// which is exactly the Peer the far end is waiting on a counter from.
    /// </para>
    /// <para>
    /// <b>Ordering.</b> This runs before <see cref="FlushPendingRetentionWrites"/> in <see cref="Pump"/>,
    /// so a counter due this Frame reaches the wire ahead of anything this Frame's own
    /// <see cref="Send"/> calls queued — harmless, since the counter names Messages the *peer* sent, an
    /// entirely separate stream position from this end's own outbound Messages the peer's parser
    /// distinguishes by sentinel regardless of interleaving. What must never happen is a counter reaching
    /// the wire once a Goodbye has passed for this Peer in either direction — this end's own
    /// (<see cref="PeerSlot.LocalGoodbyeSent"/>, set the instant <see cref="Close"/> accepts
    /// responsibility for this end's Goodbye, before its bytes necessarily reach the wire) or the far
    /// end's (<see cref="PeerSlot.PendingCloseReasonAfterRetentionDrains"/>, set the instant an inbound
    /// Goodbye is classified) — the same pair <see cref="Send"/> already refuses on, so nothing this end
    /// writes ever lands behind either Goodbye and hands the far end
    /// <see cref="DiagnosticId.DataAfterGoodbye"/> to close on (issue #106).
    /// </para>
    /// </remarks>
    private void EmitDeliveryCounters()
    {
        if (_peersWithAdvancedReceivedCount.Count == 0)
        {
            return;
        }

        // Snapshot and clear first, the same shape RetryPeersWithBufferedMessages and
        // FlushPendingRetentionWrites both use: nothing here repopulates this set synchronously within
        // the loop, but taking the defensive shape keeps the invariant obvious without relying on that.
        SlotHandle[] pending = [.. _peersWithAdvancedReceivedCount];
        _peersWithAdvancedReceivedCount.Clear();

        foreach (SlotHandle handle in pending)
        {
            if (_peers.TryResolve(handle) is not PeerSlot slot
                || slot.LocalGoodbyeSent
                || slot.PendingCloseReasonAfterRetentionDrains is not null
                || slot.IsInterrupted)
            {
                // A Goodbye has passed for this Peer — this end's own (LocalGoodbyeSent) or the far
                // end's, recorded the instant DeliverCompleteMessages classifies GoodbyeReceived even
                // before this end's own close follows (PendingCloseReasonAfterRetentionDrains) — so
                // nothing further belongs on the wire behind it, exactly the rule Send enforces for the
                // same two flags. A Message queued ahead of an inbound Goodbye in the same read can still
                // advance the received count and land this handle in the set above; skipping it here
                // rather than in the populating site keeps that one recording site simple and puts the
                // "is it still safe to write" question where every other outbound gate already asks it.
                // IsInterrupted (issue #119) guards the same way against a Peer whose read advanced this
                // count and whose socket then failed a write in the very same Frame's dispatcher pass —
                // its transport is already gone, so there is nowhere left to send this record.
                continue;
            }

            if (slot.HasPendingRetentionWrite
                && slot.PendingDeliveryCounterBytesToSend is not { Length: > 0 }
                && !slot.AwaitingResumeCounter)
            {
                // Ride this Pump's already-due retention write rather than opening one of this own
                // (issue #132). Building the record now rather than at flush time keeps the count this
                // record names the same one the standalone path would have named — nothing between here
                // and FlushPendingRetentionWrites reads from a socket, so the two are identical, and
                // writing it here keeps SendDeliveryCounterOrArm the only other place that touches this
                // buffer.
                MessageFramingCodec.WriteDeliveryCounter(slot.DeliveryCounterRecord, slot.Counters.MessagesReceived);
                slot.DeliveryCounterRidesRetentionWrite = true;
                continue;
            }

            SendDeliveryCounterOrArm(handle, slot);
        }
    }

    /// <summary>
    /// Writes this end's delivery-counter record — <paramref name="slot"/>'s own
    /// <see cref="PeerCounterSet.MessagesReceived"/> at this instant — into that slot's preallocated
    /// <see cref="PeerSlot.DeliveryCounterRecord"/> buffer and sends it, for
    /// <see cref="EmitDeliveryCounters"/>. Nothing is allocated here: this fires on every Pump a receiving
    /// Peer's count advances, which is the steady state for one-way traffic rather than an exceptional
    /// path. Skips building a fresh record entirely when an earlier one is still draining a short send
    /// (<see cref="PeerSlot.PendingDeliveryCounterBytesToSend"/>): interleaving a second record's bytes
    /// ahead of the first's undelivered remainder would corrupt both on the wire, and since the count is
    /// cumulative and idempotent on the peer's side (<see cref="RetentionArena.ReleaseThroughMessageCount"/>'s
    /// own remarks — a repeated or stale count is a no-op, never an error) there is nothing lost by
    /// letting the in-flight one finish and picking up the latest count whenever this Peer's received
    /// count next advances.
    /// </summary>
    private void SendDeliveryCounterOrArm(SlotHandle handle, PeerSlot slot)
    {
        if (slot.PendingDeliveryCounterBytesToSend is { Length: > 0 })
        {
            return;
        }

        MessageFramingCodec.WriteDeliveryCounter(slot.DeliveryCounterRecord, slot.Counters.MessagesReceived);

        switch (SendPendingRecord(slot.Link, slot.Counters, slot.DeliveryCounterRecord, out ReadOnlyMemory<byte> remainder))
        {
            case PendingRecordSendOutcome.Failed:
                InterruptOrCloseOnTransportFailure(handle, slot);
                break;

            case PendingRecordSendOutcome.ShortSend:
                // A short send only ever means bytes were withheld, not that zero went out (see
                // SendPendingRecord/TcpLink.Send — an EWOULDBLOCK before anything is accepted is also a
                // ShortSend, with a full-length remainder and nothing actually written): what changed on
                // the wire is exactly remainder's own shrinkage from the record's full length, so this
                // still counts as outbound activity whenever remainder is shorter than the record itself.
                slot.PendingDeliveryCounterBytesToSend = remainder;
                if (remainder.Length < slot.DeliveryCounterRecord.Length)
                {
                    MarkSendActivity(slot);
                }

                break;

            case PendingRecordSendOutcome.Complete:
                MarkSendActivity(slot);
                break;
        }
    }

    /// <summary>
    /// Records that this end just wrote outbound bytes to <paramref name="slot"/>'s wire — a Message, a
    /// Goodbye, or a delivery-counter record, whichever this end's own traffic happened to send last —
    /// as the moment the send-idle timer (issue #114) next counts silence from. Every call site already
    /// knows some positive number of bytes actually reached the socket before calling this; this method
    /// only stamps <em>when</em>, not <em>how much</em>, since the timer cares about silence on the wire
    /// and nothing about volume.
    /// </summary>
    private void MarkSendActivity(PeerSlot slot) =>
        slot.LastSendTimestamp = _options.Reactor.TimeProvider.GetTimestamp();

    /// <summary>
    /// Records that a run of bytes just arrived from <paramref name="slot"/>'s wire — the only evidence
    /// this end has that the peer's transport is still there (issue #117): over TCP, sending proves
    /// nothing, since the kernel accepts written bytes into a buffer whether or not anything is at the
    /// far end, but a byte the OS handed back on a read came from somewhere. Stamped on every
    /// <see cref="TcpLinkReceiveOutcome.Delivered"/> read (<see cref="OnEstablishedPeerReadable"/>),
    /// before framing even classifies what those bytes are — a bare heartbeat carrying no Messages
    /// resets the receive-idle deadline exactly as a Message does, because both are bytes this end did
    /// not have a moment ago.
    /// </summary>
    private void MarkReceiveActivity(PeerSlot slot) =>
        slot.LastReceiveTimestamp = _options.Reactor.TimeProvider.GetTimestamp();

    /// <summary>
    /// Whether <paramref name="slot"/> has gone silent past this end's receive-idle deadline (issue
    /// #117, #26's second idle timer): <see cref="PeerSlot.RemoteSendIdleInterval"/> — the interval the
    /// peer itself announced in its Hello, never this end's own <see cref="StreamSessionOptions.SendIdleInterval"/>
    /// — multiplied by <see cref="StreamSessionOptions.ResolvedMissedHeartbeats"/>. A count only means
    /// something relative to the peer's own emission interval, which is why this deadline is computed
    /// rather than configured, and why no round-trip term is added to it (ADR 0010's figure would have
    /// had to be invented, since this profile has no RTT estimator — a dimensionless count of 2 or more
    /// satisfies "exceeds the send interval" by construction instead). Worked example: a peer announcing
    /// a 10-second send-idle interval, at the default <see cref="StreamSessionOptions.ResolvedMissedHeartbeats"/>
    /// of 3, is declared lost after 30 seconds of silence from it — three of its own heartbeats missed.
    /// </summary>
    private bool HasExceededReceiveIdleDeadline(PeerSlot slot) =>
        _options.Reactor.TimeProvider.GetElapsedTime(slot.LastReceiveTimestamp)
            >= slot.RemoteSendIdleInterval * _options.ResolvedMissedHeartbeats;

    /// <summary>
    /// Whether an <see cref="PeerSlot.IsInterrupted"/> Peer's effective Linger
    /// (<see cref="PeerSlot.EffectiveLinger"/>, negotiated at handshake and already reported via
    /// <c>LingerNegotiated</c>) has run out since <see cref="PeerSlot.InterruptedTimestamp"/> (issue
    /// #121). Linger is what closes an Interrupted Peer, not the transport loss that produced it — the
    /// loss already happened, potentially long before this check ever runs, so the deadline is measured
    /// from the moment this end stopped waiting on that transport rather than from anything about the
    /// transport itself.
    /// </summary>
    private bool HasExceededLinger(PeerSlot slot) =>
        _options.Reactor.TimeProvider.GetElapsedTime(slot.InterruptedTimestamp) >= slot.EffectiveLinger;

    /// <summary>
    /// Reactor's two independent idle timers (#26): runs once per <see cref="Pump"/>, after
    /// <see cref="FlushPendingRetentionWrites"/>, over a single scan of every occupied Slot
    /// (<see cref="ShareHolderTable{T}.OccupantAt"/>) rather than two, since neither timer's trigger
    /// produces an event of its own to enqueue a handle from the way <see cref="EmitDeliveryCounters"/>
    /// rides <see cref="_peersWithAdvancedReceivedCount"/> — the only way to know a Peer has gone quiet,
    /// on either timer, is to ask every established one, every Pump. The scan costs at most
    /// <see cref="ShareHolderTable{T}.Capacity"/> array reads, all of it against already-allocated
    /// state; nothing here allocates. Sharing the loop does not couple the two decisions: the
    /// receive-idle check (issue #117) is evaluated and acted on, independently, before the send-idle
    /// check (issue #114) ever runs for that same Slot, so a Peer that receives nothing yet keeps
    /// sending fine is judged purely on <see cref="HasExceededReceiveIdleDeadline"/> — never on whether
    /// it also happened to send something this Frame (issue #118's requirement that the send timer stay
    /// unconditional on what was received).
    /// </summary>
    /// <remarks>
    /// Receive-idle expiry (#117, revised by #119) is checked first, ahead of the Goodbye gate below:
    /// a Peer whose transport has actually died moves to <c>Interrupted</c>
    /// (<see cref="InterruptOrCloseOnTransportFailure"/>) regardless of whichever side's Goodbye may or
    /// may not be pending — or closes outright if a Goodbye had already decided this Peer's fate, that
    /// helper's own job — and once it acts there is nothing left in this Slot for the send-idle check
    /// to act on, so the loop moves to the next index. An already-Interrupted Peer skips both of those
    /// timers — its socket is gone, so neither has anything left to measure or write to — and is
    /// instead checked against its own Linger deadline (<see cref="HasExceededLinger"/>, issue #121):
    /// once that elapses this Peer closes with <see cref="DisconnectReason.LingerExpired"/>, which is
    /// also what ends the limbo a <see cref="Close"/> against an Interrupted Peer would otherwise leave
    /// forever pending (issue #92) — <see cref="ClosePeer"/> is called directly here rather than through
    /// <see cref="InterruptOrCloseOnTransportFailure"/>, since that helper answers "a live Peer's
    /// transport just failed" and this is "an already-Interrupted Peer's Linger ran out", a different
    /// question with an already-known answer.
    /// </remarks>
    private void EnforceIdleTimers()
    {
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is not PeerSlot slot || slot.State != HandshakeState.Established)
            {
                continue;
            }

            if (slot.IsInterrupted)
            {
                if (HasExceededLinger(slot))
                {
                    ClosePeer(slot.Handle, DisconnectReason.LingerExpired);
                }

                continue;
            }

            if (HasExceededReceiveIdleDeadline(slot))
            {
                InterruptOrCloseOnTransportFailure(slot.Handle, slot);
                continue;
            }

            if (slot.LocalGoodbyeSent || slot.PendingCloseReasonAfterRetentionDrains is not null)
            {
                // Mirrors EmitDeliveryCounters' own gate: nothing belongs on the wire once a Goodbye has
                // passed for this Peer in either direction (issue #106).
                continue;
            }

            if (_options.Reactor.TimeProvider.GetElapsedTime(slot.LastSendTimestamp) < _options.SendIdleInterval)
            {
                continue;
            }

            SendDeliveryCounterOrArm(slot.Handle, slot);
        }
    }

    /// <summary>
    /// Visits every Peer whose pending-write marker (<see cref="PeerSlot.HasPendingRetentionWrite"/>)
    /// is set — from a <see cref="Send"/> this Frame or an earlier one, or from a short write's own
    /// re-arm landing via <see cref="OnPeerWritable"/> above — and flushes it exactly once
    /// (<see cref="FlushRetention"/>), clearing the marker first so a Peer <see cref="FlushRetention"/>
    /// re-arms for a further short write starts this Frame with a clean marker rather than one already
    /// (spuriously) set. Runs once per <see cref="Pump"/>, after the readiness dispatcher, which is what
    /// makes "at most one write per Peer per Pump" an enforced property of this method rather than an
    /// incidental one of how readiness happened to fire (#90, 3.6.2 write batching): a Peer that
    /// received any number of <see cref="Send"/> calls since the last Pump is serviced here in a single
    /// pass over everything <see cref="RetentionArena.GetUnsent"/> currently reports, and a Peer with
    /// no marker set — because nothing was ever sent to it, or a Send with no following Pump — is never
    /// visited at all.
    /// </summary>
    private void FlushPendingRetentionWrites()
    {
        if (_peersWithPendingRetentionWrites.Count == 0)
        {
            return;
        }

        // Snapshot and clear first, mirroring RetryPeersWithBufferedMessages: FlushRetention's own
        // short-write re-arm flows back through OnPeerWritable on a later Pump, not synchronously
        // within this loop, so there is no risk of this set being repopulated while it is enumerated —
        // but taking the same defensive shape keeps this method's invariant obvious without relying on
        // that timing.
        SlotHandle[] pending = [.. _peersWithPendingRetentionWrites];
        _peersWithPendingRetentionWrites.Clear();

        foreach (SlotHandle handle in pending)
        {
            if (_peers.TryResolve(handle) is not PeerSlot slot)
            {
                continue;
            }

            slot.HasPendingRetentionWrite = false;
            FlushRetention(handle, slot);
        }
    }

    /// <summary>
    /// Writes as much of <paramref name="handle"/>'s unsent retained bytes
    /// (<see cref="RetentionArena.GetUnsent"/>) to the socket as it will currently take, marking each
    /// portion sent (<see cref="RetentionArena.MarkSent"/>) the instant it is handed off. Retention
    /// itself — the room a further <see cref="Send"/> has to work with — is released only once the
    /// peer's own delivery-counter record confirms it (<see cref="RetentionArena.ReleaseThroughMessageCount"/>,
    /// issue #113/#25); this method never frees any of that, it only tracks what has and has not yet
    /// reached the transport, which is a different question with a different answer (see
    /// <see cref="RetentionArena"/>'s class remarks on its two cursors). Marking sent rather than
    /// released is also what keeps a later Pump's flush from ever re-handing the same bytes to the
    /// socket a second time.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="RetentionArena.GetUnsent"/> can return two wrap segments. A short write partway
    /// through <c>first</c> stops here and re-arms without touching <c>second</c> at all —
    /// marking sent only what was actually accepted keeps the ring's sent total honest with what the
    /// socket has truly taken. Only once <c>first</c> is entirely on the wire is <c>second</c>
    /// attempted, with the same short-write re-arm applied to it in turn.
    /// </para>
    /// <para>
    /// A no-op for a Peer already <see cref="PeerSlot.IsInterrupted"/> (issue #119): its socket is
    /// disposed, so there is nowhere for these bytes to go until a Resume — landing a write on that
    /// socket is not merely wasted work, it is a use of a disposed <see cref="Socket"/>. Reachable even
    /// though <see cref="InterruptPeer"/> already removes this handle from
    /// <see cref="_peersWithPendingRetentionWrites"/>: a <see cref="Close"/> called against an
    /// Interrupted Peer re-adds it via <see cref="AppendGoodbyeOrDefer"/>, which knows nothing about
    /// transport state. What becomes of that owed Goodbye is #120/#121's concern, not this guard's —
    /// this only keeps the attempt from touching a dead socket.
    /// </para>
    /// <para>
    /// Also a no-op while <see cref="PeerSlot.AwaitingResumeCounter"/> (issue #127, D3): the dialling
    /// side of a Resume considers itself resumed the moment its own Resume record is queued, but a
    /// Resume carries no field for what the far end's read position is, so this end does not yet know
    /// where a rewind would even start from. Resending before that arrives risks resending bytes the
    /// far end already has. <see cref="DeliverCompleteMessages"/>'s <c>DeliveryCounterReceived</c> arm
    /// clears the flag the moment the far end's own first post-resume counter answers that question.
    /// </para>
    /// <para>
    /// Also a no-op while a control record's short-send remainder is still draining
    /// (<see cref="PeerSlot.PendingHandshakeBytesToSend"/> or
    /// <see cref="PeerSlot.PendingDeliveryCounterBytesToSend"/>, issue #326): retention bytes written now
    /// would land inside that record on the wire and corrupt both. Nothing is lost by returning:
    /// <see cref="OnPeerWritable"/> marks this Peer pending again once both remainders have drained.
    /// </para>
    /// <para>
    /// When <see cref="EmitDeliveryCounters"/> has marked this Peer's counter to ride this write
    /// (<see cref="PeerSlot.DeliveryCounterRidesRetentionWrite"/>, issue #132), the record leads the
    /// gather write (<see cref="FlushRetentionWithDeliveryCounter"/>) and this method's own one-run-at-
    /// a-time path is not used. The mark is consumed either way — a ride declined here, because the
    /// ring turned out to have nothing unsent after all, falls back to the standalone send rather than
    /// being carried into a later Pump against a stale count.
    /// </para>
    /// </remarks>
    private void FlushRetention(SlotHandle handle, PeerSlot slot)
    {
        bool ridingDeliveryCounter = slot.DeliveryCounterRidesRetentionWrite;
        slot.DeliveryCounterRidesRetentionWrite = false;

        if (slot.IsInterrupted
            || slot.AwaitingResumeCounter
            || slot.PendingHandshakeBytesToSend is { Length: > 0 }
            || slot.PendingDeliveryCounterBytesToSend is { Length: > 0 })
        {
            return;
        }

        (ArraySegment<byte> first, ArraySegment<byte> second) = _retention.GetUnsentSegments(handle.Index);
        if (first.Count == 0)
        {
            if (ridingDeliveryCounter)
            {
                // Nothing to ride after all: the marker said a write was due, but everything this Peer
                // had retained was already on the wire. The record still owes the far end its count, so
                // it goes out on its own — one write, exactly what a purely receiving Peer pays.
                SendDeliveryCounterOrArm(handle, slot);
            }

            TryCloseAfterRetentionDrained(handle, slot);
            return;
        }

        if (ridingDeliveryCounter)
        {
            FlushRetentionWithDeliveryCounter(handle, slot, first, second);
            return;
        }

        TcpLinkSendResult firstResult = slot.Link.Send(first.AsSpan());
        slot.Counters.RecordSocketWrite();
        if (!firstResult.IsSuccess)
        {
            InterruptOrCloseOnTransportFailure(handle, slot);
            return;
        }

        if (firstResult.BytesSent > 0)
        {
            _retention.MarkSent(handle.Index, firstResult.BytesSent);
            MarkSendActivity(slot);
        }

        if (firstResult.BytesSent < first.Count)
        {
            _dispatcher.ArmWritable(slot.Link.Socket);
            return;
        }

        if (second.Count == 0)
        {
            TryCloseAfterRetentionDrained(handle, slot);
            return;
        }

        TcpLinkSendResult secondResult = slot.Link.Send(second.AsSpan());
        slot.Counters.RecordSocketWrite();
        if (!secondResult.IsSuccess)
        {
            InterruptOrCloseOnTransportFailure(handle, slot);
            return;
        }

        if (secondResult.BytesSent > 0)
        {
            _retention.MarkSent(handle.Index, secondResult.BytesSent);
            MarkSendActivity(slot);
        }

        if (secondResult.BytesSent < second.Count)
        {
            _dispatcher.ArmWritable(slot.Link.Socket);
            return;
        }

        TryCloseAfterRetentionDrained(handle, slot);
    }

    /// <summary>
    /// The one write that carries both this Peer's delivery-counter record and its unsent retention
    /// bytes (issue #132): the record leads, <paramref name="first"/> and any wrapped
    /// <paramref name="second"/> follow, and the socket is offered all of them in a single gather send
    /// (<see cref="TcpLink.Send(IList{ArraySegment{byte}})"/>). This is what makes #25's stated cost
    /// true — a Peer with two-way traffic pays no record and no syscall for the counter beyond the
    /// write its own outbound Messages were already going to take.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A gather send reports one count spanning the segments in order, so a short write is split at the
    /// record's own boundary. A count short of the record's length means the record itself is only
    /// partly out and no retention byte moved: the remainder is armed exactly as a standalone counter
    /// send would arm it (<see cref="PeerSlot.PendingDeliveryCounterBytesToSend"/>), and
    /// <see cref="OnPeerWritable"/> drains it and re-marks retention pending — the same recovery both
    /// paths already had. Anything beyond the record's length is retention bytes and is marked sent as
    /// one run: <see cref="RetentionArena.MarkSent"/> counts oldest-first across the wrap, so one call
    /// covers what the unridden path spends two on.
    /// </para>
    /// <para>
    /// Ordering on the wire is unchanged from the standalone send — the counter still precedes this
    /// Pump's Messages, because it is the first segment rather than an earlier write. That is what
    /// keeps <see cref="EmitDeliveryCounters"/>'s ordering argument, and every test written against it,
    /// true of this path as well.
    /// </para>
    /// </remarks>
    private void FlushRetentionWithDeliveryCounter(
        SlotHandle handle,
        PeerSlot slot,
        ArraySegment<byte> first,
        ArraySegment<byte> second)
    {
        ArraySegment<byte> record = new(slot.DeliveryCounterRecord);
        ArraySegment<byte>[] segments;
        if (second.Count == 0)
        {
            _counterAndOneRun[0] = record;
            _counterAndOneRun[1] = first;
            segments = _counterAndOneRun;
        }
        else
        {
            _counterAndBothRuns[0] = record;
            _counterAndBothRuns[1] = first;
            _counterAndBothRuns[2] = second;
            segments = _counterAndBothRuns;
        }

        TcpLinkSendResult result = slot.Link.Send(segments);
        slot.Counters.RecordSocketWrite();
        if (!result.IsSuccess)
        {
            InterruptOrCloseOnTransportFailure(handle, slot);
            return;
        }

        if (result.BytesSent > 0)
        {
            MarkSendActivity(slot);
        }

        if (result.BytesSent < record.Count)
        {
            slot.PendingDeliveryCounterBytesToSend = slot.DeliveryCounterRecord.AsMemory(result.BytesSent);
            _dispatcher.ArmWritable(slot.Link.Socket);
            return;
        }

        slot.PendingDeliveryCounterBytesToSend = null;

        int retentionSent = result.BytesSent - record.Count;
        if (retentionSent > 0)
        {
            _retention.MarkSent(handle.Index, retentionSent);
        }

        if (retentionSent < first.Count + second.Count)
        {
            _dispatcher.ArmWritable(slot.Link.Socket);
            return;
        }

        TryCloseAfterRetentionDrained(handle, slot);
    }

    /// <summary>
    /// The single authority for "should this Peer close now": true only once a Goodbye — its own, from
    /// <see cref="Close"/>, or the remote's, observed while something was still outstanding
    /// (<see cref="PeerSlot.PendingCloseReasonAfterRetentionDrains"/>) — has been recorded, this end's
    /// own outbound retention for it has fully flushed (<see cref="RetentionIsDrained"/>), and nothing
    /// this end already reassembled from it remains undelivered to the consumer
    /// (<see cref="HasUndeliveredInboundWork"/>, issue #23 review finding — a Goodbye does not outrun
    /// the Messages queued ahead of it). Called both from every point in <see cref="FlushRetention"/>
    /// that finds nothing left to send, and once per Pump for every Peer still waiting
    /// (<see cref="RetryPeersPendingClose"/>) in case only the inbound side was still outstanding; a
    /// no-op when nothing is pending, which is the ordinary case for a Peer that was never told to
    /// close.
    /// </summary>
    private void TryCloseAfterRetentionDrained(SlotHandle handle, PeerSlot slot)
    {
        if (slot.PendingCloseReasonAfterRetentionDrains is DisconnectReason reason
            && RetentionIsDrained(handle)
            && !HasUndeliveredInboundWork(handle))
        {
            ClosePeer(handle, reason);
        }
    }

    /// <summary>
    /// Whether <paramref name="handle"/> has nothing left in its own outbound retention to flush — this
    /// end's own half of what <see cref="TryCloseAfterRetentionDrained"/> requires before closing.
    /// Deliberately asks whether everything has been *sent*, not whether the peer has *confirmed* it
    /// (<see cref="RetentionArena.GetUnsent"/>, not released-through-count): this end's own Goodbye
    /// sentinel is never confirmed by a peer's Message-received counter at all (issue #113 — see
    /// <see cref="RetentionArena"/>'s remarks), so a drained check that waited on confirmation would
    /// never fire once a Goodbye was the last thing queued. Says nothing about inbound work still owed
    /// to the consumer; see <see cref="HasUndeliveredInboundWork"/> for that half.
    /// </summary>
    private bool RetentionIsDrained(SlotHandle handle)
    {
        (ReadOnlyMemory<byte> first, ReadOnlyMemory<byte> second) = _retention.GetUnsent(handle.Index);
        return first.Length == 0 && second.Length == 0;
    }

    /// <summary>
    /// Whether the consumer still has something outstanding to receive from <paramref name="handle"/>:
    /// either a Message this end already reassembled and queued as
    /// <see cref="SessionEventKind.MessageReceived"/> in the current Frame's undrained events, or bytes
    /// still buffered in its receive share awaiting arena room (<see cref="_peersWithBufferedMessages"/>,
    /// #81). Checked live against <see cref="_pendingEvents"/> rather than any separate counter: since
    /// <see cref="Pump"/> clears that list first thing, a Message from an earlier Frame is — by the
    /// library's own memory-validity contract — already considered delivered, so this naturally reports
    /// false for anything older than the Frame in progress without this end needing to track whether
    /// the consumer literally called <c>Drain</c>.
    /// </summary>
    private bool HasUndeliveredInboundWork(SlotHandle handle)
    {
        if (_peersWithBufferedMessages.Contains(handle))
        {
            return true;
        }

        PeerId peer = ToPeerId(handle);
        foreach (PendingEvent pending in _pendingEvents)
        {
            if (pending.Kind == SessionEventKind.MessageReceived && pending.Peer == peer)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Shifts whatever bytes remain after a just-consumed Message or Goodbye (<paramref name="messageEnd"/>)
    /// down to the front of <paramref name="slot"/>'s receive buffer, so the next parse in the same loop
    /// starts at offset zero.
    /// </summary>
    private static void CompactReceiveBuffer(PeerSlot slot, int messageEnd)
    {
        int remaining = slot.ReceiveBytesFilled - messageEnd;
        if (remaining > 0)
        {
            slot.ReceiveBuffer.AsSpan(messageEnd, remaining).CopyTo(slot.ReceiveBuffer.AsSpan(0, remaining));
        }

        slot.ReceiveBytesFilled = remaining;
    }

    /// <summary>
    /// Reads whatever is ready on a Peer's transport. Before <see cref="HandshakeState.Established"/>
    /// this accumulates into the Peer's own handshake buffer until a full
    /// <see cref="StreamHandshakeCodec.RecordSize"/> has arrived; once Established, reads instead feed
    /// the Peer's persistent receive buffer and Message reassembly (<see cref="OnEstablishedPeerReadable"/>).
    /// </summary>
    /// <remarks>
    /// A full record already filled but not yet acted on (this end's Refusal is still draining a short
    /// send, deferring <c>ClosePeer</c>) must not be read again: <see cref="PeerSlot.HandshakeBytesFilled"/>
    /// sitting at <see cref="StreamHandshakeCodec.RecordSize"/> would otherwise hand
    /// <c>Receive</c> a zero-length span and re-enter <see cref="OnHandshakeRecordFilled"/> on the same
    /// buffer for whatever readability fires next (#98). This Peer's own writable/close path owns what
    /// happens next once a record is full; a further read is not this state's job.
    /// </remarks>
    private void OnPeerReadable(SlotHandle handle)
    {
        if (_peers.TryResolve(handle) is not PeerSlot slot)
        {
            return;
        }

        if (slot.State == HandshakeState.Established)
        {
            OnEstablishedPeerReadable(handle, slot);
            return;
        }

        if (slot.HandshakeBytesFilled >= StreamHandshakeCodec.RecordSize)
        {
            return;
        }

        TcpLinkReceiveResult result = slot.Link.Receive(slot.HandshakeBuffer.AsSpan(slot.HandshakeBytesFilled));
        switch (result.Outcome)
        {
            case TcpLinkReceiveOutcome.Delivered:
                slot.HandshakeBytesFilled += result.Payload.Length;
                if (slot.HandshakeBytesFilled == StreamHandshakeCodec.RecordSize)
                {
                    OnHandshakeRecordFilled(handle, slot);
                }

                break;
            case TcpLinkReceiveOutcome.EndOfStream:
            case TcpLinkReceiveOutcome.Failed:
                ClosePeer(handle, DisconnectReason.TransportFailed);
                break;
        }
    }

    /// <summary>
    /// Reads whatever is ready on an established Peer's transport into its persistent per-Peer receive
    /// buffer — never the frame arena directly, since a partial Message's tail must survive the arena
    /// being wiped every Pump (Scope 3.4.1) — then delivers every whole Message the buffer now holds
    /// (<see cref="DeliverCompleteMessages"/>). The read is skipped, not failed, when the buffer is
    /// already full of a Message body still legally in flight: the escape that lets a Peer always
    /// deliver one whole Message is #81's job, not this one's — this slice only refuses to read out of
    /// bounds. An oversized or zero-length prefix never reaches this state: <see cref="DeliverCompleteMessages"/>
    /// closes the Session the moment the four-byte prefix that names it has arrived, long before the
    /// buffer could fill.
    /// </summary>
    /// <remarks>
    /// <see cref="TcpLinkReceiveOutcome.EndOfStream"/> and <see cref="TcpLinkReceiveOutcome.Failed"/>
    /// are deliberately not folded into one branch here, unlike the handshake path above: a pending
    /// socket error (<c>Failed</c>) — a reset — moves this Peer to <c>Interrupted</c>
    /// (<see cref="InterruptOrCloseOnTransportFailure"/>, issue #119), arriving through this same read
    /// path exactly as PR #73's readiness dispatcher promises and needing nothing extra to detect. A
    /// graceful end of stream (<c>EndOfStream</c>) is classified by <see cref="ClassifyEndOfStream"/>
    /// instead, because whether it is ordinary or a truncation depends on what this Peer's receive
    /// buffer was still holding — a mid-Message truncation is still a genuine
    /// <see cref="DisconnectReason.ProtocolViolation"/> close, never merely an interruption, but a
    /// clean FIN with nothing buffered and no Goodbye already accounted for is exactly the "FIN is not
    /// a Goodbye" case ADR 0010 names: Interrupted, not closed, and never raised at all once a Goodbye
    /// already decided this Peer's ending (<see cref="PeerSlot.PendingCloseReasonAfterRetentionDrains"/>).
    /// </remarks>
    private void OnEstablishedPeerReadable(SlotHandle handle, PeerSlot slot)
    {
        int room = slot.ReceiveBuffer.Length - slot.ReceiveBytesFilled;
        if (room == 0)
        {
            return;
        }

        TcpLinkReceiveResult result = slot.Link.Receive(slot.ReceiveBuffer.AsSpan(slot.ReceiveBytesFilled, room));
        switch (result.Outcome)
        {
            case TcpLinkReceiveOutcome.Delivered:
                slot.ReceiveBytesFilled += result.Payload.Length;
                MarkReceiveActivity(slot);
                DeliverCompleteMessages(handle, slot);
                break;
            case TcpLinkReceiveOutcome.EndOfStream:
                if (ClassifyEndOfStream(slot) is DisconnectReason reason)
                {
                    ClosePeer(handle, reason);
                }
                else if (slot.PendingCloseReasonAfterRetentionDrains is null)
                {
                    InterruptPeer(handle, slot);
                }

                break;
            case TcpLinkReceiveOutcome.Failed:
                InterruptOrCloseOnTransportFailure(handle, slot);
                break;
        }
    }

    /// <summary>
    /// Repeatedly classifies the front of a Peer's receive buffer against the framing table
    /// (<see cref="MessageFramingLegality"/>). A complete, legal Message is copied into the shared
    /// frame arena and queued as a <see cref="SessionEventKind.MessageReceived"/> event, shifting the
    /// remainder of the buffer down to offset zero. An oversized or zero-length prefix, or bytes
    /// arriving behind an already-received Goodbye (<see cref="MessageFramingOutcome.DataAfterGoodbye"/>,
    /// issue #93 — a Peer that announced it is leaving and then keeps sending), ends the Session
    /// immediately with <see cref="DisconnectReason.ProtocolViolation"/> — the whole reason
    /// <c>MaxMessageSize</c> is announced as a receive limit is so an oversized claim never becomes an
    /// allocation. Stops — leaving whatever is left in place for a later Pump — the moment the front of
    /// the buffer holds less than a whole Message, or an arena reservation cannot be made this Frame. A
    /// Goodbye control record at the front (<see cref="MessageFramingOutcome.GoodbyeReceived"/>, issue
    /// #23) closes the Peer directly only if nothing is outstanding on either side: this end's own
    /// outbound retention for it already empty, and nothing this end already reassembled from it still
    /// queued undelivered (<see cref="HasUndeliveredInboundWork"/> — a Message parsed earlier in this
    /// same loop, ahead of the Goodbye in the same read, counts even though it already reached
    /// <see cref="SessionEventKind.MessageReceived"/> first). Otherwise marks it
    /// <see cref="SessionEventKind.PeerClosing"/> and defers the close to whenever both sides drain
    /// (<see cref="TryCloseAfterRetentionDrained"/>) — never both a <c>PeerClosing</c> event and an
    /// immediate close for the same Goodbye. A delivery-counter sentinel with fewer than its full u64
    /// body arrived (<see cref="MessageFramingOutcome.AwaitingDeliveryCounterBody"/>, issue #109) waits
    /// exactly like an incomplete Message; a whole delivery-counter record
    /// (<see cref="MessageFramingOutcome.DeliveryCounterReceived"/>, issue #113) releases this Peer's own
    /// outbound retention through the confirmed count
    /// (<see cref="RetentionArena.ReleaseThroughMessageCount"/>) — or, if the count claims Messages this
    /// end never sent, ends the Session with <see cref="DisconnectReason.ProtocolViolation"/> exactly as
    /// the other named violations below do.
    /// </summary>
    private void DeliverCompleteMessages(SlotHandle handle, PeerSlot slot)
    {
        while (true)
        {
            ReadOnlySpan<byte> filled = slot.ReceiveBuffer.AsSpan(0, slot.ReceiveBytesFilled);
            MessageFramingOutcome outcome = MessageFramingLegality.Classify(
                filled, _options.MaxMessageSize, slot.RemoteGoodbyeReceived, out uint length, out int messageEnd);

            switch (outcome)
            {
                case MessageFramingOutcome.AwaitingPrefix:
                case MessageFramingOutcome.AwaitingMessage:
                // A delivery-counter sentinel has arrived but not yet its full u64 body (issue #109) —
                // an ordinary incomplete state, waited out exactly like the two above rather than acted
                // on early.
                case MessageFramingOutcome.AwaitingDeliveryCounterBody:
                    return;

                case MessageFramingOutcome.Ready:
                {
                    ReadOnlySpan<byte> message = slot.ReceiveBuffer.AsSpan(MessageFramingCodec.LengthPrefixSize, (int)length);
                    if (!TryReserveMessage(handle, message, out int payloadOffset))
                    {
                        // Arena or per-Peer share exhausted for this Frame. The Message stays
                        // buffered, whole, at the front of ReceiveBuffer — nothing is consumed. Reads
                        // are level-triggered on new socket activity, so if the far end sends nothing
                        // further this Peer would otherwise never be revisited; recording the handle
                        // here is what lets RetryPeersWithBufferedMessages give it a fresh share on
                        // the next Pump regardless (#81).
                        _peersWithBufferedMessages.Add(handle);
                        return;
                    }

                    _pendingEvents.Add(new PendingEvent
                    {
                        Kind = SessionEventKind.MessageReceived,
                        Peer = ToPeerId(handle),
                        PayloadOffset = payloadOffset,
                        PayloadLength = (int)length,
                    });
                    slot.Counters.RecordMessageReceived();

                    // Marks this Peer due for EmitDeliveryCounters once this Frame's reads settle —
                    // issue #116/#25's second emission trigger: once per Pump in which the received
                    // count actually advanced, rather than once per Message (a Pump delivering several
                    // Messages still only reaches this line several times, but the set absorbs the
                    // duplicates for free).
                    _peersWithAdvancedReceivedCount.Add(handle);

                    CompactReceiveBuffer(slot, messageEnd);
                    continue;
                }

                case MessageFramingOutcome.GoodbyeReceived:
                {
                    CompactReceiveBuffer(slot, messageEnd);
                    slot.RemoteGoodbyeReceived = true;

                    if (slot.PendingCloseReasonAfterRetentionDrains is null)
                    {
                        if (!HasUndeliveredInboundWork(handle) && RetentionIsDrained(handle))
                        {
                            ClosePeer(handle, DisconnectReason.PeerGoodbye);
                            return;
                        }

                        slot.PendingCloseReasonAfterRetentionDrains = DisconnectReason.PeerGoodbye;
                        _peersPendingClose.Add(handle);
                        _pendingEvents.Add(new PendingEvent
                        {
                            Kind = SessionEventKind.PeerClosing,
                            Peer = ToPeerId(handle),
                        });
                    }

                    continue;
                }

                // A separate arm from the discard arm below rather than folded into it: length is always
                // 0 here (Classify returns before the prefix is even read), so there is no claimed
                // length to report — the peer's fault is contradicting its own Goodbye by sending
                // anything further, not a bad prefix (issue #106).
                case MessageFramingOutcome.DataAfterGoodbye:
                    if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
                    {
                        _diagnostics.Log(DiagnosticRecord.ForDataAfterGoodbye(ToPeerId(handle)));
                    }

                    ClosePeer(handle, DisconnectReason.ProtocolViolation);
                    return;

                case MessageFramingOutcome.DeliveryCounterReceived:
                {
                    ReadOnlySpan<byte> body = slot.ReceiveBuffer.AsSpan(
                        MessageFramingCodec.LengthPrefixSize, MessageFramingCodec.DeliveryCounterBodySize);
                    ulong confirmedThroughMessage = MessageFramingCodec.ReadDeliveryCounter(body);
                    CompactReceiveBuffer(slot, messageEnd);

                    // No offset is needed between the peer's Message count and the arena's own
                    // appended-Message count even though a locally queued Goodbye also goes through
                    // TryAppend: a Session profile refuses further Sends the instant its own Goodbye is
                    // queued, so the Goodbye's boundary entry always sits strictly after every Message's,
                    // and a legitimate confirmedThroughMessage — which counts only Messages, never a
                    // control record — can never reach it (see RetentionArena's remarks). Refused means
                    // the peer claims Messages this end never sent — a protocol violation, not something
                    // to clamp.
                    MessageReleaseOutcome releaseOutcome =
                        _retention.ReleaseThroughMessageCount(handle.Index, confirmedThroughMessage);
                    if (releaseOutcome == MessageReleaseOutcome.Refused)
                    {
                        if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
                        {
                            _diagnostics.Log(DiagnosticRecord.ForDeliveryCounterRefused(
                                ToPeerId(handle),
                                (long)confirmedThroughMessage,
                                (long)_retention.AppendedMessageCount(handle.Index)));
                        }

                        ClosePeer(handle, DisconnectReason.ProtocolViolation);
                        return;
                    }

                    // The dialling side's reciprocal rewind (issue #127, D3): a Resume record carries
                    // only what the dialling side reports, so this end's own read position reaches the
                    // dialling side for the first time on whichever delivery-counter record follows the
                    // resume — this one, if AwaitingResumeCounter is still set. Rewinding here, rather
                    // than waiting for a Resume of its own, is what lets the dialling side resend only
                    // what this end never actually received.
                    if (slot.AwaitingResumeCounter)
                    {
                        RetentionRewindOutcome rewindOutcome = _retention.RewindSentToMessageCount(
                            handle.Index, confirmedThroughMessage, out ulong bytesRewound);
                        slot.AwaitingResumeCounter = false;

                        if (rewindOutcome != RetentionRewindOutcome.Rewound)
                        {
                            ClosePeer(handle, DisconnectReasonForRewindRefusal(rewindOutcome));
                            return;
                        }

                        if (bytesRewound > 0)
                        {
                            slot.HasPendingRetentionWrite = true;
                            _peersWithPendingRetentionWrites.Add(handle);
                        }

                        // Issue #128: this is where the dialling side's resume actually completes — the
                        // Rewound outcome above is the first point this end knows its reciprocal rewind
                        // succeeded, so it is also the first point PeerResumed can be raised without a
                        // risk of a still-pending refusal (D5) retracting it. ResumeDialledPeer itself
                        // cleared IsInterrupted and re-established the transport earlier, but Resumed —
                        // and the Resumes counter — waits for this confirmation, the same way ResumeHeldPeer
                        // gates its own PeerResumed on RewindSentToMessageCount returning Rewound.
                        slot.Counters.RecordResume();
                        _pendingEvents.Add(new PendingEvent { Kind = SessionEventKind.PeerResumed, Peer = ToPeerId(handle) });
                    }

                    continue;
                }

                // The discard arm refuses rather than permits (the RawLinkLegality/TryReserve house
                // rule): OversizedPrefix and ZeroLengthPrefix are named explicitly, and any outcome
                // this switch does not otherwise recognise falls into the same arm rather than being
                // silently read as legal.
                case MessageFramingOutcome.OversizedPrefix:
                case MessageFramingOutcome.ZeroLengthPrefix:
                default:
                    if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
                    {
                        _diagnostics.Log(DiagnosticRecord.ForMalformedFraming(ToPeerId(handle), length, _options.MaxMessageSize));
                    }

                    ClosePeer(handle, DisconnectReason.ProtocolViolation);
                    return;
            }
        }
    }

    /// <summary>
    /// Gives every Peer whose receive buffer still holds a whole, undelivered Message — one that lost
    /// the per-Peer read-share or arena race on an earlier Pump (see <see cref="DeliverCompleteMessages"/>)
    /// — a fresh chance now that this Frame's arena and shares have just been reset. Runs once, at the
    /// start of every Pump, before the readiness dispatcher: without it, a Peer whose only remaining
    /// data already sits in this Reactor's own buffer — nothing left ready on the socket — would never
    /// be read again, since the dispatcher only re-fires when the far end sends more, and a Peer that
    /// never does would hold that Message forever. A Peer redelivered here that also has fresh bytes
    /// waiting on the wire is not double-serviced: <see cref="DeliverCompleteMessages"/> simply
    /// continues from wherever this call leaves its buffer.
    /// </summary>
    private void RetryPeersWithBufferedMessages()
    {
        if (_peersWithBufferedMessages.Count == 0)
        {
            return;
        }

        // Snapshot and clear first: DeliverCompleteMessages repopulates this set for any Peer that
        // stalls again, and iterating the field directly while it may be mutated underneath this loop
        // would be unsound.
        SlotHandle[] pending = [.. _peersWithBufferedMessages];
        _peersWithBufferedMessages.Clear();

        foreach (SlotHandle handle in pending)
        {
            if (_peers.TryResolve(handle) is PeerSlot slot)
            {
                DeliverCompleteMessages(handle, slot);
            }
        }
    }

    /// <summary>
    /// Gives every Peer waiting on <see cref="TryCloseAfterRetentionDrained"/> a fresh chance now that
    /// this Frame's just-cleared <see cref="_pendingEvents"/> may have let
    /// <see cref="HasUndeliveredInboundWork"/> turn false: a Peer that entered
    /// <see cref="SessionEventKind.PeerClosing"/> because a Message was still queued for the consumer
    /// has nothing left to trigger <see cref="FlushRetention"/>'s own close check once its own outbound
    /// retention was already drained at the time — the only thing still pending was delivery, and
    /// delivery has no socket event of its own. Runs once, at the very start of every Pump, right after
    /// the clear that is what makes a previous Frame's Messages count as delivered. Unlike
    /// <see cref="RetryPeersWithBufferedMessages"/>, this does not snapshot-and-clear:
    /// <see cref="_peersPendingClose"/> is a durable membership list a handle leaves only via
    /// <see cref="ClosePeer"/>, not a one-shot retry queue, so it is safe — and necessary — to just walk
    /// a snapshot of it every time without ever clearing it out from under itself.
    /// </summary>
    private void RetryPeersPendingClose()
    {
        if (_peersPendingClose.Count == 0)
        {
            return;
        }

        SlotHandle[] pending = [.. _peersPendingClose];
        foreach (SlotHandle handle in pending)
        {
            if (_peers.TryResolve(handle) is PeerSlot slot)
            {
                TryCloseAfterRetentionDrained(handle, slot);
            }
            else
            {
                _peersPendingClose.Remove(handle);
            }
        }
    }

    /// <summary>
    /// The <see cref="DisconnectReason"/> for an established Peer's transport reporting a graceful end
    /// of stream, or <see langword="null"/> if this FIN closes nothing. Anything buffered can only be
    /// an incomplete length prefix or an incomplete Message body: <see cref="DeliverCompleteMessages"/>
    /// already drains every whole, legal Message and closes on an oversized or zero-length prefix the
    /// instant its four bytes arrive, so nonzero leftover bytes here are always a mid-prefix or
    /// mid-Message truncation — <see cref="DisconnectReason.ProtocolViolation"/>, the malformed-framing
    /// table's other two rows. Nothing buffered means no Message was in flight when it ended — but a
    /// FIN is not a Goodbye (issue #23): without one, an otherwise-ordinary transport ending is not a
    /// close this library reports at all, so <see langword="null"/> here sends the caller to
    /// <see cref="InterruptPeer"/> instead (issue #119) rather than ending the Session outright.
    /// </summary>
    private static DisconnectReason? ClassifyEndOfStream(PeerSlot slot) =>
        slot.ReceiveBytesFilled == 0 ? null : DisconnectReason.ProtocolViolation;

    /// <summary>
    /// Reserves <paramref name="message"/> in the shared frame arena under this Peer's own
    /// share-holder identity (<see cref="SlotHandle.Index"/>, per ADR 0014), logging
    /// <see cref="DiagnosticId.ArenaExhausted"/> only for the outcome that names an actual shortfall.
    /// </summary>
    private bool TryReserveMessage(SlotHandle handle, ReadOnlySpan<byte> message, out int payloadOffset)
    {
        payloadOffset = _arena.BytesUsed;
        ArenaReservation reservation = _arena.Reserve(handle.Index, message);
        switch (reservation.Outcome)
        {
            case ArenaReservationOutcome.Committed:
                return true;
            case ArenaReservationOutcome.ShareExhausted:
                return false;
            case ArenaReservationOutcome.ArenaExhausted:
                if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
                {
                    _diagnostics.Log(DiagnosticRecord.ForArenaExhausted(ToPeerId(handle), reservation.BytesRequested, reservation.BytesFree));
                }

                return false;
        }

        return false;
    }

    /// <summary>
    /// Dispatches a Peer's now-complete handshake record by its message type: a Hello, the ordinary
    /// case, a Refusal — the shape the far end's reply takes when it declines instead of establishing
    /// (subtask 3.3) — or a Resume, a re-dialling end's claim on a Session this end may still be
    /// holding (subtask 4.4). A record naming none of the three is malformed and closes the Peer, per
    /// the house rule that a discard arm refuses rather than guesses.
    /// </summary>
    /// <remarks>
    /// A Resume never reaches here on the accepting side: a connection this Reactor accepted is held
    /// outside <see cref="_peers"/> until its first record has been read, and a Resume is matched onto
    /// the Peer it names — or dropped — before any Slot exists to reach this method with
    /// (<see cref="OnPendingAdmissionRecordFilled"/>, issue #123). What can arrive here is a Resume sent
    /// to a Peer <em>this</em> end dialled, which is a far end claiming a Session in the direction only
    /// the dialling side may claim one (issue #125): that closes the Peer, exactly as an unrecognised
    /// type does.
    /// </remarks>
    private void OnHandshakeRecordFilled(SlotHandle handle, PeerSlot slot)
    {
        if (!StreamHandshakeCodec.TryReadMessageType(slot.HandshakeBuffer, out StreamHandshakeMessageType type))
        {
            ClosePeer(handle, DisconnectReason.TransportFailed);
            return;
        }

        switch (type)
        {
            case StreamHandshakeMessageType.Hello:
                OnHelloRecordReceived(handle, slot);
                break;
            case StreamHandshakeMessageType.Refusal:
                OnRefusalRecordReceived(handle, slot);
                break;
            case StreamHandshakeMessageType.Resume:
            default:
                ClosePeer(handle, DisconnectReason.TransportFailed);
                break;
        }
    }

    /// <summary>
    /// Parses a Peer's Hello record. A version this Reactor does not speak refuses the Peer in-protocol
    /// rather than establishing it (Scope: exact match, no negotiation). Otherwise the Session
    /// establishes, Linger is negotiated to the minimum of the two ends' announced values (ADR 0011),
    /// and <see cref="SessionEventKind.PeerConnected"/> is queued.
    /// </summary>
    private void OnHelloRecordReceived(SlotHandle handle, PeerSlot slot)
    {
        Span<byte> remoteResumeSecret = stackalloc byte[StreamSessionProtocol.ResumeSecretLength];
        if (!StreamHandshakeCodec.TryRead(
                slot.HandshakeBuffer,
                out uint version,
                out uint remoteMaxMessageSize,
                out ulong remoteSendIdleIntervalMilliseconds,
                out ulong remoteLingerMilliseconds,
                remoteResumeSecret))
        {
            ClosePeer(handle, DisconnectReason.TransportFailed);
            return;
        }

        if (version != StreamSessionProtocol.Version)
        {
            RefuseVersionAndClose(handle, slot, version);
            return;
        }

        TimeSpan remoteLinger = TimeSpan.FromMilliseconds(remoteLingerMilliseconds);
        TimeSpan effectiveLinger = _options.Linger < remoteLinger ? _options.Linger : remoteLinger;

        slot.RemoteVersion = version;
        slot.RemoteMaxMessageSize = remoteMaxMessageSize;
        slot.RemoteSendIdleInterval = TimeSpan.FromMilliseconds(remoteSendIdleIntervalMilliseconds);
        slot.RemoteLinger = remoteLinger;
        slot.RemoteResumeSecret = remoteResumeSecret.ToArray();
        slot.EffectiveLinger = effectiveLinger;
        slot.State = HandshakeState.Established;

        // Both idle deadlines (issues #114 and #117) start counting from establishment, not from
        // default(long) — which would read as an interval already elapsed and either fire a heartbeat
        // or declare the Peer lost on the very next Pump regardless of how fresh the Session actually
        // is. The Hello that just arrived is itself received activity, so LastReceiveTimestamp is
        // stamped here rather than left to wait for the first post-establishment read.
        long now = _options.Reactor.TimeProvider.GetTimestamp();
        slot.LastSendTimestamp = now;
        slot.LastReceiveTimestamp = now;

        PeerId peer = ToPeerId(handle);
        _pendingEvents.Add(new PendingEvent { Kind = SessionEventKind.PeerConnected, Peer = peer });

        if (_diagnostics.IsEnabled(DiagnosticLevel.Info))
        {
            _diagnostics.Log(DiagnosticRecord.ForLingerNegotiated(
                peer, (long)_options.Linger.TotalMilliseconds, (long)effectiveLinger.TotalMilliseconds));
        }
    }

    /// <summary>
    /// Ends a Peer the far end refused instead of establishing, translating the wire
    /// <see cref="RefusalReason"/> to a <see cref="DisconnectReason"/>. A reason this profile does not
    /// recognise is treated as malformed rather than mapped to some default disconnection — the house
    /// rule that a discard arm refuses.
    /// </summary>
    private void OnRefusalRecordReceived(SlotHandle handle, PeerSlot slot)
    {
        if (!StreamHandshakeCodec.TryReadRefusal(slot.HandshakeBuffer, out RefusalReason reason, out _, out _))
        {
            ClosePeer(handle, DisconnectReason.TransportFailed);
            return;
        }

        if (RefusalReasonMap.DisconnectReasonFor(reason) is not DisconnectReason disconnectReason)
        {
            ClosePeer(handle, DisconnectReason.TransportFailed);
            return;
        }

        ClosePeer(handle, disconnectReason);
    }

    /// <summary>
    /// Reports the Frame's declined Admissions once, at its end, rather than once per declining. A
    /// capacity refusal is not reported here: it is an in-protocol Refusal that ends the connection,
    /// never a Deferral left for a later Pump, and the two counters this method feeds must stay
    /// untouched by it (see <see cref="RefuseForCapacity"/>). That holds whether the Slot the dial was
    /// refused for holds an established Peer or is reserved by an Interrupted one — a reservation
    /// changes which dials are refused, never how a refusal is counted (issue #107).
    /// </summary>
    private void ReportDeclinedAdmissions()
    {
        if (_capDeferralsThisFrame == 0)
        {
            return;
        }

        for (int i = 0; i < _capDeferralsThisFrame; i++)
        {
            _counters.RecordAdmissionDeferral();
        }

        if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
        {
            _diagnostics.Log(DiagnosticRecord.ForAdmissionCapReached(_admissionCap, _capDeferralsThisFrame));
        }
    }

    /// <summary>
    /// Moves an established Peer to Interrupted (issue #119): unregisters and disposes its dead
    /// socket, then stops — unlike <see cref="ClosePeer"/>, the Slot is never removed from
    /// <see cref="_peers"/>, so <see cref="PeerId"/> (generation included) stays live,
    /// <see cref="CountersOf"/> keeps answering for it, and its retention ring and any inbound
    /// Messages still buffered are held exactly as they were rather than reset or discarded. Any
    /// short-sent control record's remainder is dropped with the socket it was begun on (issue #326).
    /// The pending-retention-write marker is cleared only from the bookkeeping set that would
    /// otherwise hand <see cref="FlushPendingRetentionWrites"/> a disposed socket on this Frame — a
    /// consumer's own <see cref="Send"/> queuing more retention afterwards is #120's concern, not this
    /// one's. Also stamps <see cref="PeerSlot.InterruptedTimestamp"/>, which is what starts this Peer's
    /// effective Linger counting down (issue #121) — closing it is <see cref="EnforceIdleTimers"/>'s
    /// job on a later Pump, not this method's. A no-op if this Peer is already Interrupted, since every
    /// call site that reaches here can race a receive-idle expiry against a socket-level failure
    /// discovered the same Frame.
    /// </summary>
    private void InterruptPeer(SlotHandle handle, PeerSlot slot)
    {
        if (slot.IsInterrupted)
        {
            return;
        }

        _dispatcher.Unregister(slot.Link.Socket);
        slot.Link.Dispose();
        slot.IsInterrupted = true;
        slot.InterruptedTimestamp = _options.Reactor.TimeProvider.GetTimestamp();

        // D4 (issue #127): drop whatever un-counted inbound bytes were sitting in this Peer's receive
        // buffer — a partial length prefix, a partial Message body, a partial delivery-counter body, or
        // a whole Message the frame arena had no room to reserve yet (_peersWithBufferedMessages). None
        // of that has reached PeerCounterSet.MessagesReceived, since only a Message actually handed to
        // the frame arena counts (DeliverCompleteMessages' Ready arm) — so none of it is anything the
        // peer's own delivery counter could ever have claimed as received, and the peer will resend all
        // of it from its own rewind point once this Peer resumes. Keeping it here would double-deliver
        // it instead. A Message already reserved into the frame arena — counted, queued as
        // MessageReceived, merely still undrained — is untouched: issue #119's own promise that
        // undelivered inbound Messages survive an interruption unchanged.
        slot.ReceiveBytesFilled = 0;
        _peersWithBufferedMessages.Remove(handle);
        _peersWithPendingRetentionWrites.Remove(handle);

        // Issue #326: a short-sent record's remainder is the tail of a record begun on the socket just
        // disposed, so it can never be finished on a later connection, and nothing arms that connection
        // to drain it. Left in place, FlushRetention's remainder guard would block this Peer's
        // retention forever after a dialling-side Resume, and SendDeliveryCounterOrArm would never send
        // another counter. A dropped counter loses nothing: counts are cumulative, and each Resume path
        // tells the far end this end's received count on its own.
        slot.PendingHandshakeBytesToSend = null;
        slot.QueuedHandshakeSend = null;
        slot.PendingDeliveryCounterBytesToSend = null;
        slot.DeliveryCounterRidesRetentionWrite = false;

        if (slot.DialedEndpoint is not null && !slot.LocalGoodbyeSent && !slot.RemoteGoodbyeReceived)
        {
            BeginRedialSchedule(handle, slot);
        }

        _pendingEvents.Add(new PendingEvent
        {
            Kind = SessionEventKind.PeerInterrupted,
            Peer = ToPeerId(handle),
        });
    }

    /// <summary>
    /// Derives this Peer's re-dial schedule (<see cref="RedialSchedule"/>, issue #111) from the Pump
    /// interval just observed this Frame (<see cref="ObservedPumpInterval"/>, issue #112) and this
    /// Peer's own effective Linger, draws the first delay, and tracks the Peer for
    /// <see cref="ProcessRedials"/> to act on once that delay elapses. The jitter source is this
    /// Reactor's own <c>_entropy</c> generator — the same one <see cref="NextResumeSecret"/> already
    /// draws from — rather than a fresh one per Peer, since nothing about the schedule's own contract
    /// needs Peer-local entropy state. Called only from <see cref="InterruptPeer"/>, which has already
    /// checked <see cref="PeerSlot.DialedEndpoint"/> and both Goodbye flags.
    /// </summary>
    /// <remarks>
    /// An observed interval of exactly zero — a loop pumping faster than its own clock can resolve —
    /// floors the schedule at one tick rather than at anything larger: the next Pump is the floor, and
    /// that Pump has already arrived. It is the reading before any second Pump (<see cref="TimeSpan.MaxValue"/>,
    /// see <see cref="PumpIntervalObserver.Interval"/>) that defers to the Linger ceiling instead.
    /// </remarks>
    private void BeginRedialSchedule(SlotHandle handle, PeerSlot slot)
    {
        // A zero interval is a cadence too fast to measure, not an absent one: two Pumps did happen and
        // the clock could not separate them. So the floor derived from it is the smallest wait there
        // is, never the largest. Issue #112's principle is that the next Pump is the floor, and for a
        // loop pumping faster than its own clock's resolution the next Pump is already here. One tick
        // is the smallest positive value RedialSchedule's constructor accepts, since it refuses zero
        // outright. This is the opposite case to "no cadence observed yet" (TimeSpan.MaxValue), where
        // no second Pump has happened at all and there is nothing yet to floor.
        TimeSpan pumpInterval = ObservedPumpInterval > TimeSpan.Zero ? ObservedPumpInterval : TimeSpan.FromTicks(1);
        slot.RedialSchedule = new RedialSchedule(pumpInterval, slot.EffectiveLinger, _entropy);
        slot.RedialWaitStartTimestamp = slot.InterruptedTimestamp;
        slot.RedialDelay = slot.RedialSchedule.NextDelay();
        slot.RedialAttemptCount = 0;
        _peersWithRedialScheduled.Add(handle);
    }

    /// <summary>
    /// Gives every Interrupted Peer this Reactor is re-dialling (<c>_peersWithRedialScheduled</c>, issue
    /// #125) its turn this Frame: a Peer with an attempt already in flight has that attempt polled
    /// (<see cref="PollRedialAttempt"/>); one waiting out its current delay begins a fresh dial the
    /// instant that delay has elapsed, to the same <see cref="PeerSlot.DialedEndpoint"/> this Reactor
    /// originally dialled — there is no rebinding and no address discovery. A Peer no longer tracked
    /// here at all — closed by <see cref="EnforceIdleTimers"/>'s Linger check earlier this same Pump, or
    /// already resumed — is dropped from the set rather than acted on, which is what makes Linger the
    /// only ceiling on this activity: there is no separate attempt-count limit anywhere in this method.
    /// </summary>
    private void ProcessRedials()
    {
        if (_peersWithRedialScheduled.Count == 0)
        {
            return;
        }

        SlotHandle[] scheduled = [.. _peersWithRedialScheduled];
        foreach (SlotHandle handle in scheduled)
        {
            if (_peers.TryResolve(handle) is not PeerSlot slot || !slot.IsInterrupted || slot.RedialSchedule is null)
            {
                _peersWithRedialScheduled.Remove(handle);
                continue;
            }

            if (slot.RedialLink is TcpLink link)
            {
                PollRedialAttempt(handle, slot, link);
                continue;
            }

            if (_options.Reactor.TimeProvider.GetElapsedTime(slot.RedialWaitStartTimestamp) < slot.RedialDelay)
            {
                continue;
            }

            // DialedEndpoint is guaranteed non-null here: only BeginRedialSchedule adds a handle to
            // _peersWithRedialScheduled, and it does so only after InterruptPeer has already checked it.
            slot.RedialLink = TcpLink.Dial(slot.DialedEndpoint!, _options.SendBufferSize);
            slot.RedialAttemptCount++;

            if (_diagnostics.IsEnabled(DiagnosticLevel.Info))
            {
                _diagnostics.Log(DiagnosticRecord.ForRedialAttempted(
                    ToPeerId(handle), slot.RedialAttemptCount, (long)slot.RedialDelay.TotalMilliseconds));
            }
        }
    }

    /// <summary>
    /// Polls one Peer's in-flight re-dial attempt (<see cref="TcpLink.PollConnect"/>). A resolved
    /// connect hands the transport off to the resume match on the listening side
    /// (<see cref="ResumeDialledPeer"/>) — this end's Resume record is what actually makes that claim,
    /// sent the instant the transport exists. A resolved failure — the platform <see cref="SocketError"/>
    /// this attempt's <see cref="TcpConnectPollResult.Error"/> carries — never closes this Peer
    /// (<see cref="FailRedialAttempt"/>): it stays Interrupted and simply waits out the schedule's next
    /// delay, which is what leaves Linger, not an attempt count, as the only thing that ever closes it.
    /// </summary>
    private void PollRedialAttempt(SlotHandle handle, PeerSlot slot, TcpLink link)
    {
        TcpConnectPollResult result = link.PollConnect();
        switch (result.Outcome)
        {
            case TcpConnectPollOutcome.Pending:
                break;

            case TcpConnectPollOutcome.Connected:
                ResumeDialledPeer(handle, slot, link);
                break;

            case TcpConnectPollOutcome.Failed:
                FailRedialAttempt(handle, slot, result.Error);
                break;
        }
    }

    /// <summary>
    /// Retires one failed re-dial attempt's transport and draws the schedule's next delay
    /// (<see cref="RedialSchedule.NextDelay"/>), timed from this failure rather than from the attempt
    /// that preceded it — the doubling schedule counts attempts, not wall-clock windows. The Peer stays
    /// Interrupted and tracked in <c>_peersWithRedialScheduled</c>; only <see cref="EnforceIdleTimers"/>'s
    /// Linger check ever closes it from here.
    /// </summary>
    private void FailRedialAttempt(SlotHandle handle, PeerSlot slot, SocketError error)
    {
        slot.RedialLink?.Dispose();
        slot.RedialLink = null;
        slot.RedialWaitStartTimestamp = _options.Reactor.TimeProvider.GetTimestamp();
        slot.RedialDelay = slot.RedialSchedule!.NextDelay();

        if (_diagnostics.IsEnabled(DiagnosticLevel.Info))
        {
            _diagnostics.Log(DiagnosticRecord.ForRedialFailed(ToPeerId(handle), error, slot.RedialAttemptCount));
        }
    }

    /// <summary>
    /// Puts a re-dial's freshly connected transport under the Peer it belongs to and immediately claims
    /// the Session with a Resume record — this end's own resume secret is not what the far end needs;
    /// <see cref="PeerSlot.RemoteResumeSecret"/> is, since that is the secret the far end issued and will
    /// require back (<see cref="StreamHandshakeCodec.WriteResume"/>, issue #115). No reply record is
    /// expected: a Resume has no acknowledgement in this protocol, exactly as <see cref="ResumeHeldPeer"/>
    /// sends nothing back to a matched Resume on the listening side, so this Peer is considered resumed
    /// — <see cref="PeerSlot.IsInterrupted"/> cleared, both idle timers restarted from now — the moment
    /// the record is queued rather than waiting on anything further from the wire.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Sets <see cref="PeerSlot.AwaitingResumeCounter"/> (issue #127, D3): this Resume record carries
    /// only this end's own claim, never the far end's read position, so this end does not yet know
    /// where its own rewind would start from. <see cref="FlushRetention"/>'s own guard on that flag
    /// keeps this Peer from resending anything until the far end's first post-resume delivery-counter
    /// record answers that (<see cref="DeliverCompleteMessages"/>'s <c>DeliveryCounterReceived</c> arm).
    /// </para>
    /// <para>
    /// <see cref="SessionEventKind.PeerResumed"/> and the <see cref="PeerCounters.Resumes"/> counter are
    /// <b>not</b> raised here (issue #128): this method re-establishes the transport but cannot yet rule
    /// out a refusal (D5) — that only resolves once the reciprocal rewind above runs. Both are raised
    /// from the same <c>DeliveryCounterReceived</c> arm, the first point this end knows the resume is
    /// good.
    /// </para>
    /// </remarks>
    private void ResumeDialledPeer(SlotHandle handle, PeerSlot slot, TcpLink link)
    {
        slot.Link = link;
        slot.RedialLink = null;
        slot.RedialSchedule = null;
        _peersWithRedialScheduled.Remove(handle);
        slot.IsInterrupted = false;
        slot.InterruptedTimestamp = 0;
        slot.AwaitingResumeCounter = true;

        long now = _options.Reactor.TimeProvider.GetTimestamp();
        slot.LastSendTimestamp = now;
        slot.LastReceiveTimestamp = now;

        RegisterPeerTransport(handle, slot);
        if (slot.HasPendingRetentionWrite)
        {
            _peersWithPendingRetentionWrites.Add(handle);
        }

        byte[] resume = new byte[StreamHandshakeCodec.RecordSize];
        StreamHandshakeCodec.WriteResume(resume, slot.RemoteResumeSecret, slot.Counters.MessagesReceived);
        SendRecordOrArm(handle, slot, resume, closeReasonAfterSend: null);
    }

    /// <summary>
    /// The shared decision behind every established-Peer transport failure this Reactor discovers on
    /// its own side — a reset or failed read, a failed write, or the receive-idle deadline (issue
    /// #119): <see cref="InterruptPeer"/> normally, except when a Goodbye already decided this Peer's
    /// fate (<see cref="PeerSlot.PendingCloseReasonAfterRetentionDrains"/> already set), in which case
    /// the dead transport simply completes that ending — a write or read failing after this end's own
    /// Goodbye or the far end's was already recorded is not a fresh interruption, it is that ending's
    /// own socket finally giving out, and Closing must never be followed by Interrupted for the same
    /// Peer (issue #119's acceptance criterion). Pre-establishment failures — a handshake still in
    /// flight — never reach here at all: nothing was ever reported as connected, so there is no
    /// established Peer for a consumer to see interrupted, and those keep the plain
    /// <see cref="ClosePeer"/> behaviour this method exists to leave unchanged.
    /// </summary>
    private void InterruptOrCloseOnTransportFailure(SlotHandle handle, PeerSlot slot)
    {
        if (slot.State != HandshakeState.Established)
        {
            ClosePeer(handle, DisconnectReason.TransportFailed);
            return;
        }

        if (slot.PendingCloseReasonAfterRetentionDrains is DisconnectReason reason)
        {
            ClosePeer(handle, reason);
            return;
        }

        InterruptPeer(handle, slot);
    }

    private void ClosePeer(SlotHandle handle, DisconnectReason reason)
    {
        if (_peers.Remove(handle) is not PeerSlot slot)
        {
            return;
        }

        _dispatcher.Unregister(slot.Link.Socket);
        slot.Link.Dispose();
        slot.RedialLink?.Dispose();
        _peersWithBufferedMessages.Remove(handle);
        _peersWithPendingRetentionWrites.Remove(handle);
        _peersPendingClose.Remove(handle);
        _peersWithGoodbyeOwed.Remove(handle);
        _peersWithAdvancedReceivedCount.Remove(handle);
        _peersWithRedialScheduled.Remove(handle);
        _retention.ResetShareHolder(handle.Index);
        _pendingEvents.Add(new PendingEvent
        {
            Kind = SessionEventKind.PeerClosed,
            Peer = ToPeerId(handle),
            Reason = reason,
        });
    }

    /// <summary>A fresh secret for a later Resume. Established during the handshake and stored, never sent again once the handshake is over.</summary>
    private byte[] NextResumeSecret()
    {
        byte[] secret = new byte[StreamSessionProtocol.ResumeSecretLength];
        _entropy.GetBytes(secret);
        return secret;
    }

    private void EnsureUsable() => _lifecycle.EnsureUsable(this);

    private void EnsureNotInsideAPump() => _lifecycle.EnsureNotInsideAPump();

    private void EnsureCallingThreadIsConstructingThread() => _lifecycle.EnsureCallingThreadIsConstructingThread();

    private static PeerId ToPeerId(SlotHandle handle) => handle.ToPeerId();

    private static SlotHandle ToHandle(PeerId peer) => peer.ToHandle();

    /// <summary>How far through the handshake one Peer has got.</summary>
    private enum HandshakeState
    {
        /// <summary>A dial is waiting for its TCP connect to resolve.</summary>
        Connecting,

        /// <summary>The transport exists and this end has sent its own record; waiting for the other end's.</summary>
        AwaitingPeerHandshake,

        /// <summary>Both ends' handshake records have been exchanged and the Session exists.</summary>
        Established,
    }

    /// <summary>The state one occupied capacity Slot holds.</summary>
    private sealed class PeerSlot
    {
        /// <summary>
        /// The connected (or connecting) transport this Peer runs over. Settable rather than
        /// <c>init</c>-only because a Resume replaces it (<see cref="ResumeHeldPeer"/>, issue #123):
        /// the Session outlives the transport it happened to be running over, which is the one thing a
        /// Resume is for.
        /// </summary>
        public required TcpLink Link { get; set; }

        /// <summary>The secret this Reactor will require to be presented before it resumes this Session.</summary>
        public required byte[] LocalResumeSecret { get; init; }

        /// <summary>How far through the handshake this Peer has got.</summary>
        public required HandshakeState State { get; set; }

        /// <summary>The Slot this Peer occupies, filled in once the table has admitted it.</summary>
        public SlotHandle Handle { get; set; }

        /// <summary>This Peer's own counters (ADR 0013), living as long as its Slot does.</summary>
        public PeerCounterSet Counters { get; } = new();

        /// <summary>
        /// Whatever remains of this end's own handshake record after a short send, or
        /// <see langword="null"/> once it has all been sent.
        /// </summary>
        public ReadOnlyMemory<byte>? PendingHandshakeBytesToSend { get; set; }

        /// <summary>The bytes of the remote's handshake record received so far, accumulated across Pumps.</summary>
        public byte[] HandshakeBuffer { get; } = new byte[StreamHandshakeCodec.RecordSize];

        /// <summary>How many bytes of <see cref="HandshakeBuffer"/> are filled so far.</summary>
        public int HandshakeBytesFilled { get; set; }

        /// <summary>
        /// The persistent per-Peer receive buffer a partial Message's tail survives in across Pumps —
        /// never the frame arena, which is wiped every Pump. Sized at construction from
        /// <see cref="StreamSessionOptions.ResolvedReceiveBufferSize"/>: at least one incomplete
        /// maximum Message plus its length prefix.
        /// </summary>
        public required byte[] ReceiveBuffer { get; init; }

        /// <summary>
        /// This Peer's own fixed <see cref="MessageFramingCodec.DeliveryCounterRecordSize"/>-byte scratch
        /// buffer, rewritten in place each time <see cref="SendDeliveryCounterOrArm"/> emits (issue #116).
        /// Allocated once with the slot, like <see cref="ReceiveBuffer"/>, rather than per emission: the
        /// per-Pump trigger fires on every Pump a receiving Peer's count advances, so building a fresh
        /// record each time would put an allocation on the steady-state receive path — the one thing this
        /// Reactor's preallocate-everything discipline exists to avoid. Safe to reuse in place because
        /// <see cref="SendDeliveryCounterOrArm"/> refuses to build a new record at all while
        /// <see cref="PendingDeliveryCounterBytesToSend"/> — a remainder pointing into this very buffer —
        /// is still draining.
        /// </summary>
        public required byte[] DeliveryCounterRecord { get; init; }

        /// <summary>How many bytes of <see cref="ReceiveBuffer"/> are filled so far, from the front.</summary>
        public int ReceiveBytesFilled { get; set; }

        /// <summary>The protocol version the remote's handshake record offered.</summary>
        public uint RemoteVersion { get; set; }

        /// <summary>The largest Message the remote said it will accept.</summary>
        public uint RemoteMaxMessageSize { get; set; }

        /// <summary>The send-idle interval the remote announced.</summary>
        public TimeSpan RemoteSendIdleInterval { get; set; }

        /// <summary>The Linger the remote announced. The minimum of the two ends' Lingers governs (ADR 0011).</summary>
        public TimeSpan RemoteLinger { get; set; }

        /// <summary>
        /// The negotiated Linger — the minimum of this end's own <see cref="StreamSessionOptions.Linger"/>
        /// and <see cref="RemoteLinger"/> — set once the Session establishes. Immutable for the
        /// Session's lifetime from that point on, exactly as every other negotiated parameter is.
        /// </summary>
        public TimeSpan EffectiveLinger { get; set; }

        /// <summary>The secret the remote will require before it resumes this Session. Stored, unused until Resume lands.</summary>
        public byte[] RemoteResumeSecret { get; set; } = [];

        /// <summary>
        /// Set when this Peer is being closed once its pending send finishes flushing — a Refusal
        /// record — rather than kept open. <see langword="null"/> for an ordinary Hello send, which
        /// closes nothing on completion.
        /// </summary>
        public DisconnectReason? PendingCloseReason { get; set; }

        /// <summary>
        /// A second handshake record — always a Refusal — that arrived while
        /// <see cref="PendingHandshakeBytesToSend"/> was still draining a short-sent first one (#98),
        /// queued here rather than overwriting the still-pending remainder or interleaving early.
        /// <see cref="OnPeerWritable"/> sends it, via <c>SendRecordOrArm</c>, the moment the first
        /// record's remainder is fully on the wire.
        /// </summary>
        public (byte[] Record, DisconnectReason? CloseReasonAfterSend)? QueuedHandshakeSend { get; set; }

        /// <summary>
        /// Whatever remains of this end's own delivery-counter record after a short send, or
        /// <see langword="null"/> once it has all been sent — the same shape
        /// <see cref="PendingHandshakeBytesToSend"/> and <see cref="RefusalSlot.PendingBytesToSend"/>
        /// already use for a small fixed-size record sent directly to the socket (issue #116). Never
        /// routed through <see cref="RetentionArena"/>: see <see cref="EmitDeliveryCounters"/>'s remarks
        /// for why a recurring control record cannot safely ride that ring the way the one-shot Goodbye
        /// sentinel does.
        /// </summary>
        public ReadOnlyMemory<byte>? PendingDeliveryCounterBytesToSend { get; set; }

        /// <summary>
        /// Set by <see cref="EmitDeliveryCounters"/> when this Peer's counter record — already written
        /// into <see cref="DeliveryCounterRecord"/> — is to lead this Pump's retention write rather than
        /// take a write of its own (issue #132), and consumed by <see cref="FlushRetention"/> the moment
        /// it visits this Peer. Never outlives the Pump that set it: the two run back to back inside a
        /// single <see cref="Pump"/>, and the flush clears it whether or not the ride turns out to be
        /// possible, so a later Pump can never put a stale count on the wire.
        /// </summary>
        public bool DeliveryCounterRidesRetentionWrite { get; set; }

        /// <summary>
        /// Set by <see cref="Send"/> when it appends to this Peer's retention ring, and again by
        /// <see cref="OnPeerWritable"/> when a short retention write's own re-arm lands — cleared by
        /// <see cref="FlushPendingRetentionWrites"/> the moment it visits this Peer. This is the single
        /// signal that routes every retention write through that one call site (#90, 3.6.2): a
        /// <see cref="Send"/> never writes to the socket itself, it only sets this.
        /// </summary>
        public bool HasPendingRetentionWrite { get; set; }

        /// <summary>
        /// Set the moment <see cref="Close"/> accepts responsibility for this end's Goodbye — before
        /// the append is attempted, and it stays set even when that append is deferred for want of
        /// retention room, because from the caller's point of view the ending is already decided and
        /// the Goodbye is owed either way. Idempotent-guard and Send-refusal flag in one: a second
        /// <see cref="Close"/> call is a no-op, and <see cref="Send"/> refuses once this is set
        /// (issue #23). Note this says the Goodbye is owed, not that it has reached retention, let
        /// alone the wire — <see cref="PendingCloseReasonAfterRetentionDrains"/> is the record that
        /// waits on the append actually committing.
        /// </summary>
        public bool LocalGoodbyeSent { get; set; }

        /// <summary>
        /// Set by <see cref="DeliverCompleteMessages"/> the moment an inbound Goodbye is classified for
        /// this Peer (<see cref="MessageFramingOutcome.GoodbyeReceived"/>) — never by anything on this
        /// end's own outbound side. Deliberately a distinct fact from <see cref="LocalGoodbyeSent"/> and
        /// <see cref="PendingCloseReasonAfterRetentionDrains"/>: both of those can be set by this end's
        /// own <see cref="Close"/>, and a local Close must never make the far end's still-in-flight
        /// Messages a violation — they were sent legitimately, before that far end had any way to know
        /// this end was leaving. Only an inbound Goodbye — this Peer announcing its own departure — means
        /// anything it sends afterward is announcing one thing and doing another. Once true, every
        /// further byte at the front of <see cref="ReceiveBuffer"/> classifies as
        /// <see cref="MessageFramingOutcome.DataAfterGoodbye"/> (issue #93) and ends the Session with
        /// <see cref="DisconnectReason.ProtocolViolation"/> — the fix for a Peer that could otherwise
        /// hold its capacity Slot open indefinitely after announcing it was leaving.
        /// </summary>
        public bool RemoteGoodbyeReceived { get; set; }

        /// <summary>
        /// The reason <see cref="ClosePeer"/> is eventually called with, once both this end's own
        /// outbound retention for the Peer is drained and nothing this end already reassembled from it
        /// remains undelivered to the consumer (<c>TryCloseAfterRetentionDrained</c>). Written by two
        /// places, and the first of them to run wins: <see cref="AppendGoodbyeOrDefer"/> records
        /// <see cref="DisconnectReason.LocalClose"/> once this end's own Goodbye has actually been
        /// appended to retention — not eagerly when <see cref="Close"/> is called, which would let this
        /// end close locally before the Goodbye it announced ever reached the wire — and
        /// <see cref="DeliverCompleteMessages"/> records <see cref="DisconnectReason.PeerGoodbye"/> when
        /// an inbound Goodbye finds something still outstanding on either side. Two guards keep that
        /// first-writer-wins rule rather than one: <see cref="Close"/>'s early return when this is
        /// already non-null (which also skips announcing a second Goodbye, not merely coalescing the
        /// reason), and <see cref="AppendGoodbyeOrDefer"/>'s own <c>??=</c>, which covers the
        /// interleaving that never passes through that early return — a deferred append committing on
        /// a later Pump, after an inbound Goodbye has already recorded its reason. Either way this end
        /// is on its way out, and the first reason recorded is the truest account of why.
        /// </summary>
        public DisconnectReason? PendingCloseReasonAfterRetentionDrains { get; set; }

        /// <summary>
        /// The <see cref="TimeProvider.GetTimestamp"/> value at this end's most recent outbound write to
        /// this Peer's wire — a Message or Goodbye byte actually handed to the socket
        /// (<see cref="FlushRetention"/>), or a delivery-counter record sent either as this end's
        /// send-idle heartbeat (<see cref="EnforceIdleTimers"/>, issue #114) or the received-count
        /// trigger (<see cref="SendDeliveryCounterOrArm"/>, issue #116) — whichever happened last.
        /// Initialised the instant this Peer establishes (<see cref="OnHelloRecordReceived"/>), so the
        /// send-idle deadline counts from establishment rather than from
        /// <see langword="default"/>'s epoch, which would otherwise read as an already-elapsed interval
        /// and fire a heartbeat the very first Pump after connecting. Deliberately named for what this
        /// end wrote, not what it merely queued into retention: the send-idle timer measures silence on
        /// the wire, not silence in the application (issue #114's own wording), so a <see cref="Send"/>
        /// that queues a Message but is never followed by a <see cref="Pump"/> must not reset it.
        /// </summary>
        public long LastSendTimestamp { get; set; }

        /// <summary>
        /// The <see cref="TimeProvider.GetTimestamp"/> value at this end's most recent inbound bytes
        /// from this Peer's wire (<see cref="MarkReceiveActivity"/>, issue #117) — any run of bytes the
        /// OS handed back on a read, including a bare heartbeat's delivery-counter record carrying no
        /// Messages, since over TCP only receiving is evidence the peer's transport is still there.
        /// Initialised the instant this Peer establishes (<see cref="OnHelloRecordReceived"/>) from the
        /// Hello record that just arrived, so the receive-idle deadline
        /// (<see cref="HasExceededReceiveIdleDeadline"/>) counts from establishment rather than from
        /// <see langword="default"/>'s epoch, which would otherwise read as an already-elapsed interval
        /// and declare the Peer lost the very first Pump after connecting.
        /// </summary>
        public long LastReceiveTimestamp { get; set; }

        /// <summary>
        /// Set by <see cref="InterruptPeer"/> the instant this Peer's transport is gone (issue #119):
        /// its socket has already been unregistered and disposed, but the Slot itself, this end's
        /// counters, retention ring and any undelivered inbound Messages all stay exactly as they
        /// were, so <see cref="EnforceIdleTimers"/> and <see cref="FlushPendingRetentionWrites"/> must
        /// never touch <see cref="Link"/> again for a Peer with this set. Cleared only by a Resume
        /// matching onto this Slot (<see cref="ResumeHeldPeer"/>, issue #123) or by
        /// <see cref="ClosePeer"/> removing the Slot outright — Linger expiry (issue #121) is what
        /// eventually calls that when no Resume arrives first.
        /// </summary>
        public bool IsInterrupted { get; set; }

        /// <summary>
        /// Set by <see cref="ResumeDialledPeer"/> the moment the dialling side's own Resume record is
        /// queued (issue #127, D3): that record carries only this end's own claim, never the far end's
        /// read position, so this end does not yet know where its own rewind would even start from.
        /// While set, <see cref="FlushRetention"/>'s own guard on this flag refuses to resend anything
        /// for this Peer — resending blind could hand the far end bytes it already has. Cleared by
        /// <see cref="DeliverCompleteMessages"/>'s <c>DeliveryCounterReceived</c> arm the moment the far
        /// end's first post-resume delivery-counter record answers that question, which is also what
        /// drives this end's own reciprocal rewind.
        /// </summary>
        public bool AwaitingResumeCounter { get; set; }

        /// <summary>
        /// The <see cref="TimeProvider.GetTimestamp"/> value at the instant <see cref="InterruptPeer"/>
        /// set <see cref="IsInterrupted"/> (issue #121) — the moment this Peer's effective Linger began
        /// counting down. Meaningless while <see cref="IsInterrupted"/> is <see langword="false"/>: a
        /// Resume replacing <see cref="Link"/> clears this alongside <see cref="IsInterrupted"/>
        /// (<see cref="ResumeHeldPeer"/>, issue #123) rather than leaving a stale value behind for a
        /// Linger deadline that no longer applies.
        /// </summary>
        public long InterruptedTimestamp { get; set; }

        /// <summary>
        /// The endpoint <see cref="Connect"/> dialled to open this Session, or <see langword="null"/>
        /// for a Peer this Reactor accepted instead. Set once, at construction, and never cleared or
        /// replaced by a later Resume — it is what makes issue #125's re-dial possible at all: only the
        /// side that dialled has an address to retry, so <see cref="InterruptPeer"/> reads this to decide
        /// whether this Peer's transport loss is even eligible for one. A re-dial always targets exactly
        /// this endpoint — there is no rebinding and no address discovery.
        /// </summary>
        public IPEndPoint? DialedEndpoint { get; init; }

        /// <summary>
        /// This Peer's re-dial schedule (<see cref="Session.RedialSchedule"/>, issue #111), or
        /// <see langword="null"/> while this Peer is not currently being re-dialled — either it has
        /// never been Interrupted, its transport loss was ineligible (issue #125:
        /// <see cref="DialedEndpoint"/> unset or a Goodbye had passed), or a Resume already matched and
        /// cleared it. Constructed once per interruption by <see cref="BeginRedialSchedule"/>, from the
        /// Pump interval observed at that moment and this Peer's own effective Linger, and consulted only
        /// by <see cref="ProcessRedials"/> and <see cref="FailRedialAttempt"/>.
        /// </summary>
        public RedialSchedule? RedialSchedule { get; set; }

        /// <summary>
        /// The transport of a re-dial attempt currently in flight — connecting, not yet resolved — or
        /// <see langword="null"/> while this Peer is between attempts, waiting out
        /// <see cref="RedialDelay"/>. Distinct from <see cref="Link"/>, which stays the disposed socket
        /// from the transport loss until a re-dial actually resolves (<see cref="ResumeDialledPeer"/>):
        /// an attempt that fails must never disturb <see cref="Link"/>, since nothing about a failed
        /// attempt changes what this Peer is holding.
        /// </summary>
        public TcpLink? RedialLink { get; set; }

        /// <summary>
        /// The <see cref="TimeProvider.GetTimestamp"/> value <see cref="RedialDelay"/> is measured from
        /// — either the moment this Peer became Interrupted (its first delay,
        /// <see cref="BeginRedialSchedule"/>) or the moment its most recent attempt resolved as failed
        /// (<see cref="FailRedialAttempt"/>). <see cref="ProcessRedials"/> begins the next attempt once
        /// <see cref="TimeProvider.GetElapsedTime(long)"/> against this reaches <see cref="RedialDelay"/>.
        /// </summary>
        public long RedialWaitStartTimestamp { get; set; }

        /// <summary>
        /// How long this Peer's re-dial schedule says to wait, from <see cref="RedialWaitStartTimestamp"/>,
        /// before its next attempt — the delay <see cref="Session.RedialSchedule.NextDelay"/> most
        /// recently drew, already jittered.
        /// </summary>
        public TimeSpan RedialDelay { get; set; }

        /// <summary>
        /// How many re-dial attempts have begun since this Peer's schedule was last started
        /// (<see cref="BeginRedialSchedule"/>, which resets it to 0). Incremented by
        /// <see cref="ProcessRedials"/> immediately before each dial, and reported as
        /// <see cref="DiagnosticId.RedialAttempted"/>'s and <see cref="DiagnosticId.RedialFailed"/>'s
        /// attempt number. This is a diagnostics-only count — nothing reads it to decide whether to try
        /// again, and it must never become one: Linger alone bounds re-dialling (ADR 0011).
        /// </summary>
        public long RedialAttemptCount { get; set; }
    }

    /// <summary>
    /// A just-accepted connection held outside <see cref="_peers"/> until its first handshake record
    /// has arrived in full (issue #123). It has no Slot and no <see cref="PeerId"/> because it is not
    /// yet known to need one: a Resume is claiming the Slot its own Interrupted Peer is already
    /// reserving, and only the record says which of the two this is.
    /// </summary>
    private sealed class PendingAdmission
    {
        /// <summary>The accepted transport, held here until its first handshake record decides what it is.</summary>
        public required TcpLink Link { get; init; }

        /// <summary>The bytes of that first record received so far, accumulated across Pumps.</summary>
        public byte[] Record { get; } = new byte[StreamHandshakeCodec.RecordSize];

        /// <summary>How many bytes of <see cref="Record"/> are filled so far.</summary>
        public int BytesFilled { get; set; }
    }

    /// <summary>
    /// A just-accepted connection being refused for capacity, tracked outside <see cref="_peers"/>
    /// entirely: refusing it is precisely what happens because this Reactor has no capacity Slot to
    /// give it.
    /// </summary>
    private sealed class RefusalSlot
    {
        /// <summary>The accepted transport this refusal is being sent over, then closed.</summary>
        public required TcpLink Link { get; init; }

        /// <summary>
        /// Whatever remains of the Refusal record after a short send, or <see langword="null"/> once it
        /// has all been sent.
        /// </summary>
        public ReadOnlyMemory<byte>? PendingBytesToSend { get; set; }
    }
}
