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
    private readonly StreamSessionOptions _options;
    private readonly ReadinessDispatcher _dispatcher;
    private readonly DiagnosticGateway _diagnostics;
    private readonly ReactorCounterSet _counters;
    private readonly FrameArena _arena;
    private readonly RetentionArena _retention;
    private readonly ShareHolderTable<PeerSlot> _peers;
    private readonly RandomNumberGenerator _entropy;
    private readonly List<TcpListenLink> _listenLinks = [];
    private readonly List<SlotHandle> _connectingPeers = [];
    private readonly List<PendingEvent> _pendingEvents = [];
    private readonly List<RefusalSlot> _refusals = [];
    private readonly HashSet<SlotHandle> _peersWithBufferedMessages = [];
    private readonly HashSet<SlotHandle> _peersWithPendingRetentionWrites = [];
    private readonly int _constructingThreadId;
    private readonly int _admissionCap;
    private int _admissionsThisFrame;
    private int _capDeferralsThisFrame;
    private bool _inPump;
    private bool _disposed;

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
        _constructingThreadId = Environment.CurrentManagedThreadId;
        _dispatcher = new ReadinessDispatcher();
        _counters = new ReactorCounterSet();
        _diagnostics = new DiagnosticGateway(options.Reactor.Sink, options.Reactor.MinimumLevel, _counters);
        _arena = new FrameArena(arenaSize, options.Reactor.Capacity);
        _retention = new RetentionArena(options.RetentionBytes, options.Reactor.Capacity);
        _peers = new ShareHolderTable<PeerSlot>(options.Reactor.Capacity);
        _entropy = options.Entropy;
        _admissionCap = options.Reactor.ResolvedAdmissionCap;

        try
        {
            foreach (IPEndPoint endpoint in options.Endpoints)
            {
                BindAndRegister(endpoint);
            }
        }
        catch
        {
            DisposeLinks();
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

    /// <inheritdoc cref="ISessionEventSource.PendingEventCount"/>
    int ISessionEventSource.PendingEventCount => _pendingEvents.Count;

    /// <summary>
    /// One synchronous advance: wipes the previous Frame's events, then admits ready connections,
    /// advances every in-flight dial and reads whatever bound socket is ready right now, running the
    /// handshake for everything that arrived, then flushes every Peer's queued retention bytes exactly
    /// once (<see cref="FlushPendingRetentionWrites"/>, #90/3.6.2) regardless of how many
    /// <see cref="Send"/> calls queued them. Throws <see cref="InvalidOperationException"/> if called
    /// from any thread other than the one that constructed this Reactor, or from inside a Pump already
    /// in progress.
    /// </summary>
    public void Pump()
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();

        _inPump = true;
        try
        {
            _pendingEvents.Clear();
            _arena.Reset();
            _admissionsThisFrame = 0;
            _capDeferralsThisFrame = 0;
            RetryPeersWithBufferedMessages();
            _dispatcher.Pump();
            FlushPendingRetentionWrites();
            ProcessConnectingPeers();
            ReportDeclinedAdmissions();
        }
        finally
        {
            _inPump = false;
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

        TcpLink link = TcpLink.Dial(destination);
        PeerSlot slot = new()
        {
            Link = link,
            LocalResumeSecret = NextResumeSecret(),
            State = HandshakeState.Connecting,
            ReceiveBuffer = new byte[_options.ResolvedReceiveBufferSize],
        };

        SlotHandle handle = _peers.TryAdd(slot) ?? throw new InvalidOperationException(
            $"This Reactor's capacity of {_options.Reactor.Capacity} Peers is fully occupied — close a Peer before dialling another.");

        slot.Handle = handle;
        _connectingPeers.Add(handle);

        return ToPeerId(handle);
    }

    /// <summary>
    /// Announces this end is ending the Session with <paramref name="peer"/> on purpose: appends a
    /// Goodbye control record to whatever this Peer's retention region already holds, so it is the
    /// last thing flushed and everything queued before it is delivered first, then forbids any further
    /// <see cref="Send"/> to it. The <see cref="SessionEventKind.PeerClosed"/> carrying
    /// <see cref="DisconnectReason.LocalClose"/> is queued once that flush fully drains — immediately,
    /// on a later Pump, if nothing was queued when <see cref="Close"/> was called, or after however
    /// many Pumps a short write takes to carry everything already retained plus the Goodbye itself.
    /// Idempotent: a stale <see cref="PeerId"/>, or a Peer already closing, is a no-op — a Peer closing
    /// under a caller, or a caller that closes the same Peer twice, is an ordinary race rather than a
    /// programmer error.
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

        slot.PendingCloseReasonAfterRetentionDrains = DisconnectReason.LocalClose;

        Span<byte> goodbye = stackalloc byte[MessageFramingCodec.LengthPrefixSize];
        MessageFramingCodec.WriteGoodbye(goodbye);
        switch (_retention.TryAppend(handle.Index, goodbye))
        {
            case RetentionOutcome.Committed:
                slot.HasPendingRetentionWrite = true;
                _peersWithPendingRetentionWrites.Add(handle);
                break;

            case RetentionOutcome.Exhausted:
                // No room left for the Goodbye sentinel behind whatever Sends already queued. The Peer
                // still stops accepting further Sends (LocalGoodbyeSent above), but nothing announces
                // the ending on the wire until retention frees up — Close does not retry on the
                // caller's behalf (#81's RetryPeersWithBufferedMessages is for Messages, not this).
                break;
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
    /// call puts nothing on the wire.
    /// </summary>
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

        if (slot.LocalGoodbyeSent)
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
        if (_disposed)
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

        _refusals.Clear();
        _connectingPeers.Clear();
        _pendingEvents.Clear();
        _peersWithBufferedMessages.Clear();
        _peersWithPendingRetentionWrites.Clear();
        _disposed = true;
        DisposeLinks();
    }

    /// <inheritdoc cref="ISessionEventSource.GetPendingEvent"/>
    SessionEvent ISessionEventSource.GetPendingEvent(int index)
    {
        PendingEvent pending = _pendingEvents[index];
        return pending.Kind switch
        {
            SessionEventKind.PeerConnected => SessionEvent.ForPeerConnected(pending.Peer),
            SessionEventKind.PeerClosed => SessionEvent.ForPeerClosed(pending.Peer, pending.Reason),
            SessionEventKind.MessageReceived => SessionEvent.ForMessageReceived(
                pending.Peer, _arena.GetCommitted(pending.PayloadOffset, pending.PayloadLength)),
            SessionEventKind.PeerClosing => SessionEvent.ForPeerClosing(pending.Peer),
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
                case TcpAcceptOutcome.Accepted:
                    _admissionsThisFrame++;
                    AdmitOrRefuse(result.Socket!);
                    break;
            }
        }
    }

    /// <summary>
    /// Registers a socket a listening Link just accepted as a new Peer's transport and immediately
    /// sends this end's handshake record — or, if every capacity Slot is occupied, refuses it
    /// in-protocol instead of ever creating one. Checked here rather than before accepting: the
    /// admission cap alone bounds how many of these accept-then-decide cycles one Frame performs
    /// (Scope: "a burst of a thousand inbound connections is a thousand accept-refuse-close cycles").
    /// </summary>
    private void AdmitOrRefuse(Socket acceptedSocket)
    {
        TcpLink link = TcpLink.FromAccepted(acceptedSocket);
        if (!_peers.HasFreeSlot)
        {
            RefuseForCapacity(link);
            return;
        }

        PeerSlot slot = new()
        {
            Link = link,
            LocalResumeSecret = NextResumeSecret(),
            State = HandshakeState.AwaitingPeerHandshake,
            ReceiveBuffer = new byte[_options.ResolvedReceiveBufferSize],
        };

        // `HasFreeSlot` was just checked and this Reactor is single-threaded (ADR 0002), so `TryAdd`
        // cannot fail here — the guard is defensive rather than a case this branch expects to take.
        if (_peers.TryAdd(slot) is not SlotHandle handle)
        {
            link.Dispose();
            return;
        }

        slot.Handle = handle;
        RegisterPeerTransport(handle, slot);
        SendHandshakeRecord(handle, slot);
    }

    /// <summary>
    /// Refuses a just-accepted connection because every capacity Slot is occupied, without ever giving
    /// it one: a Refusal record naming this Reactor's capacity is sent and the connection is then
    /// closed. Never counted as an Admission Deferral or Discard — a refusal says something on the wire
    /// and ends the connection, which is a different outcome from silently leaving declined work for a
    /// later Pump.
    /// </summary>
    private void RefuseForCapacity(TcpLink link)
    {
        Source remote = Source.FromIPEndPoint(link.RemoteEndPoint);
        if (_diagnostics.IsEnabled(DiagnosticLevel.Info))
        {
            _diagnostics.Log(DiagnosticRecord.ForPeerRefusedCapacity(remote, _peers.Capacity));
        }

        RefusalSlot refusal = new() { Link = link };
        _refusals.Add(refusal);
        _dispatcher.Register(link.Socket, () => DrainRefusal(refusal), () => OnRefusalWritable(refusal));

        // The dialler's own Hello may already be sitting in the receive buffer by the time this
        // connection is accepted — draining it now, before the Refusal is sent, is what keeps the
        // close below clean rather than a reset (unread inbound data at close time is what forces one).
        DrainRefusal(refusal);

        byte[] record = new byte[StreamHandshakeCodec.RecordSize];
        StreamHandshakeCodec.WriteRefusal(record, RefusalReason.Capacity, (uint)_peers.Capacity, 0);
        SendRefusalRecord(refusal, record);
    }

    /// <summary>Sends a Refusal record, arming for a short send exactly as a Hello record does.</summary>
    private void SendRefusalRecord(RefusalSlot refusal, byte[] record)
    {
        TcpLinkSendResult result = refusal.Link.Send(record);
        if (!result.IsSuccess)
        {
            CloseRefusal(refusal);
            return;
        }

        if (result.BytesSent < record.Length)
        {
            refusal.PendingBytesToSend = record.AsMemory(result.BytesSent);
            _dispatcher.ArmWritable(refusal.Link.Socket);
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

        TcpLinkSendResult result = refusal.Link.Send(pending.Span);
        if (!result.IsSuccess)
        {
            CloseRefusal(refusal);
            return;
        }

        refusal.PendingBytesToSend = result.BytesSent < pending.Length ? pending[result.BytesSent..] : null;
        if (refusal.PendingBytesToSend is null)
        {
            CloseRefusal(refusal);
        }
        else
        {
            _dispatcher.ArmWritable(refusal.Link.Socket);
        }
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
    private void SendRecordOrArm(SlotHandle handle, PeerSlot slot, byte[] record, DisconnectReason? closeReasonAfterSend)
    {
        TcpLinkSendResult result = slot.Link.Send(record);
        if (!result.IsSuccess)
        {
            ClosePeer(handle, closeReasonAfterSend ?? DisconnectReason.TransportFailed);
            return;
        }

        if (result.BytesSent < record.Length)
        {
            slot.PendingHandshakeBytesToSend = record.AsMemory(result.BytesSent);
            slot.PendingCloseReason = closeReasonAfterSend;
            _dispatcher.ArmWritable(slot.Link.Socket);
            return;
        }

        if (closeReasonAfterSend is DisconnectReason reason)
        {
            ClosePeer(handle, reason);
        }
    }

    /// <summary>
    /// Flushes whatever remains of a handshake record a short send left behind, closes if a reason is
    /// pending on it, then — with the handshake bytes clear — marks this Peer's retention pending
    /// again, so a short retention write's own re-arm (<see cref="FlushRetention"/>) is picked up by
    /// this Frame's explicit flush step (<see cref="FlushPendingRetentionWrites"/>) rather than writing
    /// to the socket directly here. This is what keeps every retention write routed through exactly one
    /// call site: a socket that is both freshly re-armed from a short write and independently marked
    /// pending by a same-Frame <see cref="Send"/> still gets flushed exactly once.
    /// </summary>
    private void OnPeerWritable(SlotHandle handle)
    {
        if (_peers.TryResolve(handle) is not PeerSlot slot)
        {
            return;
        }

        if (slot.PendingHandshakeBytesToSend is { Length: > 0 } pending)
        {
            TcpLinkSendResult result = slot.Link.Send(pending.Span);
            if (!result.IsSuccess)
            {
                ClosePeer(handle, slot.PendingCloseReason ?? DisconnectReason.TransportFailed);
                return;
            }

            slot.PendingHandshakeBytesToSend = result.BytesSent < pending.Length ? pending[result.BytesSent..] : null;
            if (slot.PendingHandshakeBytesToSend is not null)
            {
                _dispatcher.ArmWritable(slot.Link.Socket);
                return;
            }

            if (slot.PendingCloseReason is DisconnectReason reason)
            {
                ClosePeer(handle, reason);
                return;
            }
        }

        slot.HasPendingRetentionWrite = true;
        _peersWithPendingRetentionWrites.Add(handle);
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
    /// pass over everything <see cref="RetentionArena.GetReadable"/> currently reports, and a Peer with
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
    /// Writes as much of <paramref name="handle"/>'s unread retained bytes
    /// (<see cref="RetentionArena.GetReadable"/>) to the socket as it will currently take, releasing
    /// each portion (<see cref="RetentionArena.Release"/>) the instant it is handed off. This is a
    /// local flush only (#86's scope boundary, ADR 0011/#21/#82): bytes are released once the OS
    /// socket buffer has accepted them, not once the peer has acknowledged them — the peer-ack
    /// mechanism that will eventually move the release trigger is #25's job, not this one's.
    /// </summary>
    /// <remarks>
    /// <see cref="RetentionArena.GetReadable"/> can return two wrap segments. A short write partway
    /// through <paramref name="first"/> stops here and re-arms without touching <c>second</c> at all —
    /// releasing only what was actually accepted keeps the ring's released total honest with what the
    /// socket has truly taken. Only once <c>first</c> is entirely on the wire is <c>second</c>
    /// attempted, with the same short-write re-arm applied to it in turn.
    /// </remarks>
    private void FlushRetention(SlotHandle handle, PeerSlot slot)
    {
        (ReadOnlyMemory<byte> first, ReadOnlyMemory<byte> second) = _retention.GetReadable(handle.Index);
        if (first.Length == 0)
        {
            TryCloseAfterRetentionDrained(handle, slot);
            return;
        }

        TcpLinkSendResult firstResult = slot.Link.Send(first.Span);
        if (!firstResult.IsSuccess)
        {
            ClosePeer(handle, DisconnectReason.TransportFailed);
            return;
        }

        if (firstResult.BytesSent > 0)
        {
            _retention.Release(handle.Index, firstResult.BytesSent);
        }

        if (firstResult.BytesSent < first.Length)
        {
            _dispatcher.ArmWritable(slot.Link.Socket);
            return;
        }

        if (second.Length == 0)
        {
            TryCloseAfterRetentionDrained(handle, slot);
            return;
        }

        TcpLinkSendResult secondResult = slot.Link.Send(second.Span);
        if (!secondResult.IsSuccess)
        {
            ClosePeer(handle, DisconnectReason.TransportFailed);
            return;
        }

        if (secondResult.BytesSent > 0)
        {
            _retention.Release(handle.Index, secondResult.BytesSent);
        }

        if (secondResult.BytesSent < second.Length)
        {
            _dispatcher.ArmWritable(slot.Link.Socket);
            return;
        }

        TryCloseAfterRetentionDrained(handle, slot);
    }

    /// <summary>
    /// Closes a Peer whose Goodbye — its own, from <see cref="Close"/>, or the remote's, observed while
    /// this end's retention for it was still non-empty — has finished flushing
    /// (<see cref="PeerSlot.PendingCloseReasonAfterRetentionDrains"/>). Called from every point in
    /// <see cref="FlushRetention"/> that finds nothing left to send; a no-op when nothing is pending,
    /// which is the ordinary case for a Peer that was never told to close.
    /// </summary>
    private void TryCloseAfterRetentionDrained(SlotHandle handle, PeerSlot slot)
    {
        if (slot.PendingCloseReasonAfterRetentionDrains is DisconnectReason reason)
        {
            ClosePeer(handle, reason);
        }
    }

    /// <summary>Whether <paramref name="handle"/> has nothing left in its own outbound retention to flush.</summary>
    private bool RetentionIsDrained(SlotHandle handle)
    {
        (ReadOnlyMemory<byte> first, ReadOnlyMemory<byte> second) = _retention.GetReadable(handle.Index);
        return first.Length == 0 && second.Length == 0;
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
    /// socket error (<c>Failed</c>) is always <see cref="DisconnectReason.TransportFailed"/>, the
    /// malformed-framing table's transport-failure row, arriving through this same read path exactly
    /// as PR #73's readiness dispatcher promises and needing nothing extra to detect. A graceful end of
    /// stream (<c>EndOfStream</c>) is classified by <see cref="ClassifyEndOfStream"/> instead, because
    /// whether it is ordinary or a truncation depends on what this Peer's receive buffer was still
    /// holding.
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
                DeliverCompleteMessages(handle, slot);
                break;
            case TcpLinkReceiveOutcome.EndOfStream:
                if (ClassifyEndOfStream(slot) is DisconnectReason reason)
                {
                    ClosePeer(handle, reason);
                }

                break;
            case TcpLinkReceiveOutcome.Failed:
                ClosePeer(handle, DisconnectReason.TransportFailed);
                break;
        }
    }

    /// <summary>
    /// Repeatedly classifies the front of a Peer's receive buffer against the malformed-framing table
    /// (<see cref="MessageFramingLegality"/>). A complete, legal Message is copied into the shared
    /// frame arena and queued as a <see cref="SessionEventKind.MessageReceived"/> event, shifting the
    /// remainder of the buffer down to offset zero. An oversized or zero-length prefix ends the
    /// Session immediately with <see cref="DisconnectReason.ProtocolViolation"/> — the whole reason
    /// <c>MaxMessageSize</c> is announced as a receive limit is so this never becomes an allocation.
    /// Stops — leaving whatever is left in place for a later Pump — the moment the front of the buffer
    /// holds less than a whole Message, or an arena reservation cannot be made this Frame. A Goodbye
    /// control record at the front (<see cref="MessageFramingOutcome.GoodbyeReceived"/>, issue #23)
    /// closes the Peer directly if this end's own retention for it is already empty, or otherwise marks
    /// it <see cref="SessionEventKind.PeerClosing"/> and defers the close to whenever that retention
    /// next drains (<see cref="FlushRetention"/>) — never both a <c>PeerClosing</c> event and an
    /// immediate close for the same Goodbye.
    /// </summary>
    private void DeliverCompleteMessages(SlotHandle handle, PeerSlot slot)
    {
        while (true)
        {
            ReadOnlySpan<byte> filled = slot.ReceiveBuffer.AsSpan(0, slot.ReceiveBytesFilled);
            MessageFramingOutcome outcome = MessageFramingLegality.Classify(
                filled, _options.MaxMessageSize, out uint length, out int messageEnd);

            switch (outcome)
            {
                case MessageFramingOutcome.AwaitingPrefix:
                case MessageFramingOutcome.AwaitingMessage:
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

                    CompactReceiveBuffer(slot, messageEnd);
                    continue;
                }

                case MessageFramingOutcome.GoodbyeReceived:
                {
                    CompactReceiveBuffer(slot, messageEnd);

                    if (RetentionIsDrained(handle))
                    {
                        ClosePeer(handle, DisconnectReason.PeerGoodbye);
                        return;
                    }

                    if (slot.PendingCloseReasonAfterRetentionDrains is null)
                    {
                        slot.PendingCloseReasonAfterRetentionDrains = DisconnectReason.PeerGoodbye;
                        _pendingEvents.Add(new PendingEvent
                        {
                            Kind = SessionEventKind.PeerClosing,
                            Peer = ToPeerId(handle),
                        });
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
    /// The <see cref="DisconnectReason"/> for an established Peer's transport reporting a graceful end
    /// of stream, or <see langword="null"/> if this FIN closes nothing. Anything buffered can only be
    /// an incomplete length prefix or an incomplete Message body: <see cref="DeliverCompleteMessages"/>
    /// already drains every whole, legal Message and closes on an oversized or zero-length prefix the
    /// instant its four bytes arrive, so nonzero leftover bytes here are always a mid-prefix or
    /// mid-Message truncation — <see cref="DisconnectReason.ProtocolViolation"/>, the malformed-framing
    /// table's other two rows. Nothing buffered means no Message was in flight when it ended — but a
    /// FIN is not a Goodbye (issue #23): without one, an otherwise-ordinary transport ending is not an
    /// ending this library reports at all, so the Peer stays open for whatever's still on the wire
    /// ahead of it or a Goodbye yet to arrive.
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
    /// case, or a Refusal — the shape the far end's reply takes when it declines instead of
    /// establishing (subtask 3.3). A record naming neither is malformed and closes the Peer, per the
    /// house rule that a discard arm refuses rather than guesses.
    /// </summary>
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
    /// untouched by it (see <see cref="RefuseForCapacity"/>).
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

    private void ClosePeer(SlotHandle handle, DisconnectReason reason)
    {
        if (_peers.Remove(handle) is not PeerSlot slot)
        {
            return;
        }

        _dispatcher.Unregister(slot.Link.Socket);
        slot.Link.Dispose();
        _peersWithBufferedMessages.Remove(handle);
        _peersWithPendingRetentionWrites.Remove(handle);
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

    private static PeerId ToPeerId(SlotHandle handle) => new(handle.Origin, handle.Index, handle.Generation);

    private static SlotHandle ToHandle(PeerId peer) => new(peer.Origin, peer.Index, peer.Generation);

    private void DisposeLinks()
    {
        foreach (TcpListenLink link in _listenLinks)
        {
            _dispatcher.Unregister(link.Socket);
            link.Dispose();
        }

        _listenLinks.Clear();
    }

    private void EnsureUsable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        EnsureNotInsideAPump();
    }

    /// <summary>
    /// Rejects re-entry (ADR 0005). The only caller this library hands control to mid-Pump is a
    /// Diagnostic Sink, so this is what stops a Sink acting on the Reactor it is reporting on.
    /// </summary>
    private void EnsureNotInsideAPump()
    {
        if (_inPump)
        {
            throw new InvalidOperationException(
                "This Reactor is already inside Pump — a Diagnostic Sink may not call back into the Reactor it is reporting on. Copy what the record carries and act on it after Pump returns.");
        }
    }

    private void EnsureCallingThreadIsConstructingThread()
    {
        int callingThreadId = Environment.CurrentManagedThreadId;
        if (callingThreadId != _constructingThreadId)
        {
            throw new InvalidOperationException(
                $"Called from thread {callingThreadId}, but this Reactor was constructed on thread {_constructingThreadId} — a Reactor may only be driven from the thread that constructed it.");
        }
    }

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
        /// <summary>The connected (or connecting) transport this Peer runs over.</summary>
        public required TcpLink Link { get; init; }

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
        /// Set by <see cref="Send"/> when it appends to this Peer's retention ring, and again by
        /// <see cref="OnPeerWritable"/> when a short retention write's own re-arm lands — cleared by
        /// <see cref="FlushPendingRetentionWrites"/> the moment it visits this Peer. This is the single
        /// signal that routes every retention write through that one call site (#90, 3.6.2): a
        /// <see cref="Send"/> never writes to the socket itself, it only sets this.
        /// </summary>
        public bool HasPendingRetentionWrite { get; set; }

        /// <summary>
        /// Set once <see cref="Close"/> has appended this end's Goodbye to its own retention ring.
        /// Idempotent-guard and Send-refusal flag in one: a second <see cref="Close"/> call is a no-op,
        /// and <see cref="Send"/> refuses once this is set (issue #23).
        /// </summary>
        public bool LocalGoodbyeSent { get; set; }

        /// <summary>
        /// Set by <see cref="Close"/> or by an inbound Goodbye that found this end's own retention for
        /// the Peer still non-empty: the reason <see cref="ClosePeer"/> is called with the moment
        /// <see cref="FlushRetention"/> next finds that retention fully drained
        /// (<c>TryCloseAfterRetentionDrained</c>). Whichever of the two sets it first wins — a
        /// <c>??=</c>, never overwritten — since either way this end is on its way out and the first
        /// reason recorded is the truest account of why.
        /// </summary>
        public DisconnectReason? PendingCloseReasonAfterRetentionDrains { get; set; }
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

    private readonly record struct PendingEvent
    {
        public SessionEventKind Kind { get; init; }

        public PeerId Peer { get; init; }

        public DisconnectReason Reason { get; init; }

        /// <summary>
        /// The committed Message's offset into the frame arena. Populated only when <see cref="Kind"/>
        /// is <see cref="SessionEventKind.MessageReceived"/>.
        /// </summary>
        public int PayloadOffset { get; init; }

        /// <summary>
        /// The committed Message's length. Populated only when <see cref="Kind"/> is
        /// <see cref="SessionEventKind.MessageReceived"/>.
        /// </summary>
        public int PayloadLength { get; init; }
    }
}
