using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using ReactorSockets.Core;
using ReactorSockets.Session;
using ReactorSockets.Transports;

namespace ReactorSockets;

/// <summary>
/// The datagram Session profile (ADR 0009): Sessions established over UDP, addressed as Peers, driven
/// by a single synchronous <see cref="Pump"/> from the consumer's own loop. This slice builds the
/// handshake and everything that guards it — the stateless cookie, the amplification bound, version
/// and capacity refusals, session identity and demultiplexing. It does not yet carry data; the packet
/// records, the four sequence Spaces, fragmentation and the acknowledgement machinery land in the
/// slices that follow, and <see cref="SessionEventKind"/> grows with them.
/// </summary>
/// <remarks>
/// <para>
/// <b>Endpoints are declared, not registered.</b> Unlike <see cref="RawReactor"/>, which hands back a
/// <see cref="LinkId"/> for every socket a consumer registers, this Reactor takes its endpoints on
/// <see cref="DatagramSessionOptions.Endpoints"/> at construction and never rebinds (ADR 0014).
/// Binding returns no identity because there is nothing to address: a consumer addresses Peers, and a
/// Peer is not a socket. A Reactor that declares no endpoints binds nothing until its first
/// <see cref="Connect"/>, which binds one wildcard, port-zero socket in the destination's address
/// family and appends it to <see cref="BoundEndpoints"/>.
/// </para>
/// <para>
/// <b>Capacity counts Peers.</b> The bound sockets are not share-holders and hold no read share
/// (ADR 0014): a Peer's Messages arrive through whichever socket happens to carry them, so charging
/// the arena per socket would give one Peer on a single-socket Reactor the whole arena and sixty-four
/// Peers a sixty-fourth each — exactly inverted. The read share is the arena divided by
/// <see cref="ReactorOptions.Capacity"/>, and it belongs to the Peer.
/// </para>
/// <para>
/// <b>What a stranger can make this Reactor do.</b> Very little, and that is the design. Anything an
/// unauthenticated remote can generate at will goes to the Diagnostic Sink and never to the event
/// stream, so a stranger cannot write into the consumer's protocol. Every reply to an unverified
/// sender is exactly one control datagram of
/// <see cref="DatagramSessionProtocol.ControlDatagramSize"/> bytes, answering exactly one control
/// datagram of the same size, so bytes sent never exceed bytes received and this Reactor cannot be
/// used to amplify a flood at a spoofed third party. A bare Hello is answered with a Cookie whatever
/// it claimed — never a Refusal — so nothing about this Reactor's version or occupancy is learnable
/// without first proving the ability to receive at the address claimed.
/// </para>
/// <para>
/// <b>The wire session id is not an authenticator.</b> It is a receiver-assigned random 32-bit
/// demultiplexing token: unguessable enough that an off-path attacker cannot address a Session it was
/// never introduced to, and nothing more. This library does not encrypt, does not authenticate the
/// data plane, and an on-path attacker who can read a datagram can forge one. Consumers needing
/// confidentiality or integrity must layer it themselves.
/// </para>
/// </remarks>
public sealed class DatagramSessionReactor : IDisposable
{
    /// <summary>
    /// The largest a UDP datagram can be, and this Reactor's scratch buffer size — at which
    /// truncation is unreachable, so a datagram is never silently shortened into a different one.
    /// </summary>
    private const int MaxUdpDatagramSize = 65_507;

    private readonly DatagramSessionOptions _options;
    private readonly ReadinessDispatcher _dispatcher;
    private readonly FrameArena _arena;
    private readonly DiagnosticGateway _diagnostics;
    private readonly ReactorCounterSet _counters;
    private readonly ShareHolderTable<PeerSlot> _peers;
    private readonly CookieMinter _cookies;
    private readonly RandomNumberGenerator _entropy;
    private readonly List<UdpLink> _links = [];
    private readonly List<PendingEvent> _pendingEvents = [];

    /// <summary>Live Sessions by the session id <em>this</em> Reactor assigned, which is what a remote stamps on datagrams sent here.</summary>
    private readonly Dictionary<uint, SlotHandle> _peersBySessionId = [];

    /// <summary>Dials awaiting a Cookie or an Accept, by the client nonce this Reactor chose for each.</summary>
    private readonly Dictionary<ulong, SlotHandle> _dialsByNonce = [];

    /// <summary>
    /// Handshakes already accepted, by the source and nonce that produced them, so that a
    /// retransmitted Hello-with-cookie re-sends the same Accept instead of allocating a second Peer.
    /// Without it, a cookie valid for its epoch could be replayed to fill every capacity Slot from one
    /// address.
    /// </summary>
    private readonly Dictionary<(Source Remote, ulong Nonce), SlotHandle> _acceptedHandshakes = [];

    private readonly byte[] _scratchBuffer = new byte[MaxUdpDatagramSize];
    private readonly byte[] _sendBuffer = new byte[DatagramSessionProtocol.ControlDatagramSize];
    private readonly int _constructingThreadId;
    private readonly int _admissionCap;
    private int _admissionsThisFrame;
    private int _capDiscardsThisFrame;
    private int _capacityDiscardsThisFrame;
    private bool _inPump;
    private bool _disposed;

    /// <summary>
    /// Constructs a Reactor bound to the calling thread (ADR 0002) — every future mutating call must
    /// come from this same thread or it throws — and binds every declared endpoint in the order
    /// supplied. A bind that fails throws here, naming the endpoint: binding is part of what this
    /// Reactor is, so there is no half-bound Reactor to hand back and no
    /// <see cref="DiagnosticId"/> member for the failure.
    /// </summary>
    /// <param name="options">
    /// Every knob is validated before anything is allocated or bound, so a combination that cannot
    /// work throws at construction and nowhere else.
    /// </param>
    public DatagramSessionReactor(DatagramSessionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        _options = options;
        _constructingThreadId = Environment.CurrentManagedThreadId;
        _dispatcher = new ReadinessDispatcher();
        _arena = new FrameArena(options.ResolveArenaSize(), options.Reactor.Capacity);
        _counters = new ReactorCounterSet();
        _diagnostics = new DiagnosticGateway(options.Reactor.Sink, options.Reactor.MinimumLevel, _counters);
        _peers = new ShareHolderTable<PeerSlot>(options.Reactor.Capacity);
        _entropy = options.Entropy;
        _cookies = new CookieMinter(options.Reactor.TimeProvider, _entropy);
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
    /// The endpoints this Reactor is bound to right now, in the order they were declared, with any
    /// socket bound by a dial appended. A snapshot: it answers what is bound, and there is no member
    /// that binds another.
    /// </summary>
    public IReadOnlyList<IPEndPoint> BoundEndpoints
    {
        get
        {
            EnsureUsable();

            IPEndPoint[] snapshot = new IPEndPoint[_links.Count];
            for (int index = 0; index < _links.Count; index++)
            {
                snapshot[index] = _links[index].LocalEndPoint;
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

    internal int PendingEventCount => _pendingEvents.Count;

    /// <summary>
    /// One synchronous advance: wipes the previous Frame's events and arena, then reads whatever bound
    /// socket is ready right now, running the handshake for everything that arrived. Throws
    /// <see cref="InvalidOperationException"/> if called from any thread other than the one that
    /// constructed this Reactor, or from inside a Pump already in progress.
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
            _capDiscardsThisFrame = 0;
            _capacityDiscardsThisFrame = 0;
            _dispatcher.Pump();
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
    /// round trip. <see cref="SessionEventKind.PeerConnected"/> or
    /// <see cref="SessionEventKind.PeerClosed"/> resolves it on a later Pump.
    /// </summary>
    /// <remarks>
    /// The Hello is sent once. Retransmitting it when a reply never comes is liveness, which belongs
    /// with the heartbeat and the missed-heartbeat count in the slice that implements them; until
    /// then, a dial whose Hello or Cookie is lost simply stays pending until the consumer closes it.
    /// </remarks>
    public PeerId Connect(IPEndPoint destination)
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();
        ArgumentNullException.ThrowIfNull(destination);

        UdpLink link = LinkForFamily(destination.AddressFamily);
        Source remote = Source.FromIPEndPoint(destination);

        PeerSlot slot = new()
        {
            Remote = remote,
            Link = link,
            ClientNonce = NextNonce(),
            LocalSessionId = NextSessionId(),
            LocalResumeSecret = NextResumeSecret(),
            State = HandshakeState.AwaitingCookie,
        };

        SlotHandle handle = _peers.TryAdd(slot) ?? throw new InvalidOperationException(
            $"This Reactor's capacity of {_options.Reactor.Capacity} Peers is fully occupied — close a Peer before dialling another.");

        slot.Handle = handle;
        _dialsByNonce[slot.ClientNonce] = handle;

        ControlDatagramCodec.WriteHello(_sendBuffer, DatagramSessionProtocol.Version, slot.ClientNonce);
        SendControl(link, remote);

        return ToPeerId(handle);
    }

    /// <summary>
    /// Ends the Session with <paramref name="peer"/> and queues a
    /// <see cref="SessionEventKind.PeerClosed"/> carrying <see cref="DisconnectReason.LocalClose"/>.
    /// Idempotent: a stale <see cref="PeerId"/> is a no-op, because a Peer closing under a caller is an
    /// ordinary race rather than a programmer error.
    /// </summary>
    /// <remarks>
    /// This slice closes locally and says nothing to the remote. The Goodbye exchange, and the Linger
    /// that lets it be retransmitted, belong to the slice that implements them; until then a closed
    /// Peer is discovered on the far side by its silence.
    /// </remarks>
    public void Close(PeerId peer)
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();

        ClosePeer(ToHandle(peer), DisconnectReason.LocalClose);
    }

    /// <summary>
    /// Releases every socket this Reactor bound. A Reactor owns every socket it bound, so a consumer
    /// that drops one without disposing it leaks those sockets until each <see cref="Socket"/>'s
    /// finalizer runs. No <see cref="SessionEventKind.PeerClosed"/> events are queued: nothing can
    /// drain a disposed Reactor. Idempotent; every other member throws
    /// <see cref="ObjectDisposedException"/> afterwards.
    /// </summary>
    /// <remarks>
    /// <see cref="DatagramSessionOptions.Entropy"/> is deliberately not disposed. It may be a source
    /// the consumer holds and uses elsewhere, and a Reactor disposing a value it was merely handed
    /// would be reaching outside what it owns.
    /// </remarks>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        EnsureCallingThreadIsConstructingThread();

        _disposed = true;
        _peers.DrainAll();
        _peersBySessionId.Clear();
        _dialsByNonce.Clear();
        _acceptedHandshakes.Clear();
        _pendingEvents.Clear();
        DisposeLinks();
    }

    internal SessionEvent GetPendingEvent(int index)
    {
        PendingEvent pending = _pendingEvents[index];
        return pending.Kind switch
        {
            SessionEventKind.PeerConnected => SessionEvent.ForPeerConnected(pending.Peer),
            SessionEventKind.PeerClosed => SessionEvent.ForPeerClosed(pending.Peer, pending.Reason),
            _ => throw new InvalidOperationException($"{pending.Kind} is not a recognised SessionEventKind."),
        };
    }

    /// <summary>
    /// Binds one socket, registers it for readiness dispatch and appends it to the bound-endpoint
    /// snapshot. The binding rules are unchanged from the Raw profile's (ADR 0014): exclusive, no
    /// dual-mode, no hostname, no configure callback.
    /// </summary>
    private UdpLink BindAndRegister(IPEndPoint endpoint)
    {
        UdpLink link = UdpLink.Bind(endpoint);
        _links.Add(link);
        _dispatcher.Register(link.Socket, () => OnLinkReadable(link));
        return link;
    }

    /// <summary>
    /// The socket a dial to <paramref name="family"/> sends from: an already-bound one in the same
    /// family if there is one, otherwise a wildcard, port-zero socket bound now. A Reactor that
    /// declared no endpoints is the ordinary case for this — a client dials and never listens — and
    /// binding lazily is what lets it bind in the family it turns out to need rather than guessing at
    /// construction (ADR 0014).
    /// </summary>
    private UdpLink LinkForFamily(AddressFamily family)
    {
        foreach (UdpLink candidate in _links)
        {
            if (candidate.LocalEndPoint.AddressFamily == family)
            {
                return candidate;
            }
        }

        IPAddress wildcard = family == AddressFamily.InterNetworkV6 ? IPAddress.IPv6Any : IPAddress.Any;
        return BindAndRegister(new IPEndPoint(wildcard, 0));
    }

    /// <summary>
    /// Reads everything queued on a ready socket, rather than the Raw profile's one read per Pump. The
    /// difference is deliberate: work here is bounded by the admission cap, which counts the HMACs a
    /// Pump will compute, and a datagram beyond that cap must still be taken from the queue so it can
    /// be counted as a Discard. Leaving it in the OS buffer would make it a deferral, and a deferral
    /// is a promise that a later Pump will find the work still there — a promise UDP cannot keep.
    /// </summary>
    /// <remarks>
    /// The loop ends on the socket's own answer that nothing is left —
    /// <see cref="UdpLinkReceiveOutcome.WouldBlock"/> — rather than on <c>Socket.Available</c>, which
    /// counts queued datagrams and so reports zero while queued ICMP errors still stand in front of
    /// them. Terminating on it would abandon the read at the first error and starve every live Peer
    /// behind it, which is the thing the Unreachable arm below exists to prevent.
    /// </remarks>
    private void OnLinkReadable(UdpLink link)
    {
        while (true)
        {
            UdpLinkReceiveResult result = link.Receive(_scratchBuffer);
            switch (result.Outcome)
            {
                case UdpLinkReceiveOutcome.Delivered:
                    _counters.RecordDatagramReceived();
                    ProcessDatagram(link, result.Source, result.Payload);
                    break;

                case UdpLinkReceiveOutcome.Truncated:
                    _counters.RecordDatagramReceived();
                    _counters.RecordUnknownSourceDiscard();
                    _diagnostics.Log(DiagnosticRecord.ForDatagramTruncated(result.Source, _scratchBuffer.Length));
                    break;

                // An ICMP error queued against this socket by an earlier send, and on Linux the reason
                // this method can be reached with nothing readable at all. There is no Peer to charge
                // it to — the address it names may belong to a Session, a dial, or nothing — so it is
                // consumed and dropped here, and a Peer that has genuinely gone is noticed by the
                // liveness deadline in the slice that implements it. Dropping it does not end the
                // Frame's reading: a Reactor that has just answered a thousand Hellos from ports that
                // no longer exist has a thousand of these queued ahead of the datagrams behind them,
                // and abandoning the read on the first would starve every live Peer behind it.
                case UdpLinkReceiveOutcome.Unreachable:
                    continue;

                // WouldBlock — the queue is drained, errors and all — or a Failed read there is no
                // point continuing past. Either way this Link is done for the Frame.
                default:
                    return;
            }
        }
    }

    /// <summary>
    /// The top of the parsing: is this datagram control or data? A session id of
    /// <see cref="DatagramSessionProtocol.ControlSession"/> means the body is a control type and a
    /// body; anything else names a Session and is looked up. Nothing here throws — every byte is a
    /// value a stranger chose.
    /// </summary>
    private void ProcessDatagram(UdpLink link, Source source, ReadOnlySpan<byte> datagram)
    {
        if (!ControlDatagramCodec.TryReadSessionId(datagram, out uint session))
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        if (session != DatagramSessionProtocol.ControlSession)
        {
            ProcessDataDatagram(source, session, datagram);
            return;
        }

        if (!ControlDatagramCodec.TryReadControlType(datagram, out DatagramControlType type))
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        switch (type)
        {
            case DatagramControlType.Hello:
                OnHello(link, source, datagram);
                break;

            case DatagramControlType.HelloWithCookie:
                OnHelloWithCookie(link, source, datagram);
                break;

            case DatagramControlType.Cookie:
                OnCookie(link, source, datagram);
                break;

            case DatagramControlType.Accept:
                OnAccept(source, datagram);
                break;

            case DatagramControlType.Refusal:
                OnRefusal(source, datagram);
                break;

            // The discard arm refuses. TryReadControlType has already rejected every type this
            // profile does not process, so reaching here means a type was added to the roster without
            // a decision made here — which is discarded rather than guessed at.
            default:
                DiscardMalformed(source, datagram.Length);
                break;
        }
    }

    /// <summary>
    /// Routes a data datagram to the Session its id names. A session id naming nothing is discarded and
    /// counted rather than treated as an introduction — a Session is created by the handshake and by
    /// nothing else. The source address is deliberately not checked against the one the Session was
    /// established from: a Peer behind a NAT that rebinds, or a device that changes network, keeps its
    /// Session, which is the whole reason the identity is a token rather than an address.
    /// </summary>
    private void ProcessDataDatagram(Source source, uint session, ReadOnlySpan<byte> datagram)
    {
        if (!_peersBySessionId.TryGetValue(session, out SlotHandle handle) || _peers.TryResolve(handle) is null)
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        // The record path — packet records, the four Spaces, fragmentation and acknowledgement — is
        // the next slice's. A datagram naming a live Session is dropped here rather than counted as
        // unattributable, because it was attributable: there is simply nothing yet to do with it.
    }

    /// <summary>
    /// Answers a bare Hello with a Cookie, and with nothing else, whatever the Hello claimed. No
    /// version check, no capacity check and no refusal happens here: replying with anything considered
    /// would let a sender who has not proved they can receive at the address they claim both extract
    /// information about this Reactor and aim that reply at a third party.
    /// </summary>
    private void OnHello(UdpLink link, Source source, ReadOnlySpan<byte> datagram)
    {
        if (!TryChargeAdmission())
        {
            return;
        }

        if (!ControlDatagramCodec.TryReadHello(datagram, out _, out ulong clientNonce))
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        Span<byte> cookie = stackalloc byte[DatagramSessionProtocol.CookieLength];
        _cookies.Mint(source, clientNonce, cookie, out long epoch);

        ControlDatagramCodec.WriteCookie(_sendBuffer, clientNonce, epoch, cookie);
        SendControl(link, source);
    }

    /// <summary>
    /// Admits, refuses or discards a Hello-with-cookie. The order is fixed and each step is a gate on
    /// the next: an invalid cookie is answered with silence, a valid cookie with the wrong version is
    /// refused whether or not there is capacity, and only a valid cookie carrying the right version
    /// ever reaches the capacity check.
    /// </summary>
    private void OnHelloWithCookie(UdpLink link, Source source, ReadOnlySpan<byte> datagram)
    {
        if (!TryChargeAdmission())
        {
            return;
        }

        Span<byte> cookie = stackalloc byte[DatagramSessionProtocol.CookieLength];
        Span<byte> remoteResumeSecret = stackalloc byte[DatagramSessionProtocol.ResumeSecretLength];
        if (!ControlDatagramCodec.TryReadHelloWithCookie(
                datagram,
                out uint version,
                out ulong clientNonce,
                out long epoch,
                cookie,
                out uint remoteSessionId,
                out uint remoteMaxMessageSize,
                remoteResumeSecret))
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        if (!_cookies.Verify(source, clientNonce, epoch, cookie))
        {
            _counters.RecordUnknownSourceDiscard();
            _diagnostics.Log(DiagnosticRecord.ForCookieRejected(source, epoch, _cookies.CurrentEpoch));
            return;
        }

        if (version != DatagramSessionProtocol.Version)
        {
            _diagnostics.Log(DiagnosticRecord.ForVersionRefused(source, version, DatagramSessionProtocol.Version));
            ControlDatagramCodec.WriteRefusal(
                _sendBuffer, clientNonce, RefusalReason.Version, version, DatagramSessionProtocol.Version);
            SendControl(link, source);
            return;
        }

        // A retransmitted Hello-with-cookie is answered with the same Accept rather than admitted
        // twice. Without this, one address holding one valid cookie could fill every capacity Slot.
        if (_acceptedHandshakes.TryGetValue((source, clientNonce), out SlotHandle existing)
            && _peers.TryResolve(existing) is PeerSlot established)
        {
            SendAccept(link, established);
            return;
        }

        if (_peers.TryAdd(NewAcceptedPeer(source, link, remoteSessionId, remoteMaxMessageSize, remoteResumeSecret, clientNonce))
            is not SlotHandle handle)
        {
            _counters.RecordAdmissionDiscard();
            _capacityDiscardsThisFrame++;
            ControlDatagramCodec.WriteRefusal(_sendBuffer, clientNonce, RefusalReason.Capacity, 0, 0);
            SendControl(link, source);
            return;
        }

        // `!` is not needed here: TryAdd returned the handle for the slot just constructed, and
        // TryResolve on a handle returned this instant cannot fail.
        PeerSlot slot = _peers.TryResolve(handle) ?? throw new InvalidOperationException(
            "A Peer Slot admitted this instant did not resolve — the share-holder table's generation accounting is broken.");

        slot.Handle = handle;
        _peersBySessionId[slot.LocalSessionId] = handle;
        _acceptedHandshakes[(source, clientNonce)] = handle;
        _pendingEvents.Add(new PendingEvent { Kind = SessionEventKind.PeerConnected, Peer = ToPeerId(handle) });
        SendAccept(link, slot);
    }

    /// <summary>
    /// Completes the dialling half of the round trip: echoes the cookie back in a Hello-with-cookie
    /// carrying everything a Session needs. A Cookie that matches no dial this Reactor made is
    /// discarded — it is either a stray or an attempt to make this Reactor speak to a stranger.
    /// </summary>
    private void OnCookie(UdpLink link, Source source, ReadOnlySpan<byte> datagram)
    {
        Span<byte> cookie = stackalloc byte[DatagramSessionProtocol.CookieLength];
        if (!ControlDatagramCodec.TryReadCookie(datagram, out ulong clientNonce, out long epoch, cookie))
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        if (ResolveDial(source, clientNonce, HandshakeState.AwaitingCookie) is not PeerSlot slot)
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        slot.State = HandshakeState.AwaitingAccept;
        ControlDatagramCodec.WriteHelloWithCookie(
            _sendBuffer,
            DatagramSessionProtocol.Version,
            clientNonce,
            epoch,
            cookie,
            slot.LocalSessionId,
            (uint)_options.MaxMessageSize,
            slot.LocalResumeSecret);
        SendControl(link, source);
    }

    /// <summary>
    /// Establishes the dialling end's Session. Each end announces the id it wants stamped on datagrams
    /// sent <em>to it</em>, so the two ids differ and neither end has to agree with the other about
    /// which is which. The fragment size is not announced: it is each sender's own path property, and
    /// a receiver that reassembles by offset and length never needs to know it.
    /// </summary>
    private void OnAccept(Source source, ReadOnlySpan<byte> datagram)
    {
        Span<byte> remoteResumeSecret = stackalloc byte[DatagramSessionProtocol.ResumeSecretLength];
        if (!ControlDatagramCodec.TryReadAccept(
                datagram,
                out ulong clientNonce,
                out uint remoteSessionId,
                out uint remoteMaxMessageSize,
                remoteResumeSecret))
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        if (ResolveDial(source, clientNonce, HandshakeState.AwaitingAccept) is not PeerSlot slot)
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        slot.RemoteSessionId = remoteSessionId;
        slot.RemoteMaxMessageSize = remoteMaxMessageSize;
        slot.RemoteResumeSecret = remoteResumeSecret.ToArray();
        slot.State = HandshakeState.Established;

        _dialsByNonce.Remove(clientNonce);
        _peersBySessionId[slot.LocalSessionId] = slot.Handle;
        _pendingEvents.Add(new PendingEvent { Kind = SessionEventKind.PeerConnected, Peer = ToPeerId(slot.Handle) });
    }

    /// <summary>
    /// Ends a dial the far end declined, naming which of the two reasons applied. A Refusal that
    /// matches no dial is discarded: were it not, anyone able to guess a nonce could tear down another
    /// party's handshake.
    /// </summary>
    private void OnRefusal(Source source, ReadOnlySpan<byte> datagram)
    {
        if (!ControlDatagramCodec.TryReadRefusal(datagram, out ulong clientNonce, out RefusalReason reason, out _, out _))
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        if (ResolveDial(source, clientNonce, HandshakeState.AwaitingAccept) is not PeerSlot slot)
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        if (RefusalReasonMap.DisconnectReasonFor(reason) is not DisconnectReason disconnectReason)
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        ClosePeer(slot.Handle, disconnectReason);
    }

    /// <summary>
    /// The dial a control datagram claims to answer, or <see langword="null"/> when it answers none —
    /// checking the nonce, the state the dial is actually in, and that the reply came from the address
    /// dialled. All three must hold: a nonce alone would let a reply from anywhere redirect a dial.
    /// </summary>
    private PeerSlot? ResolveDial(Source source, ulong clientNonce, HandshakeState expectedState)
    {
        if (!_dialsByNonce.TryGetValue(clientNonce, out SlotHandle handle)
            || _peers.TryResolve(handle) is not PeerSlot slot
            || slot.State != expectedState
            || slot.Remote != source)
        {
            return null;
        }

        return slot;
    }

    private PeerSlot NewAcceptedPeer(
        Source source,
        UdpLink link,
        uint remoteSessionId,
        uint remoteMaxMessageSize,
        ReadOnlySpan<byte> remoteResumeSecret,
        ulong clientNonce) =>
        new()
        {
            Remote = source,
            Link = link,
            ClientNonce = clientNonce,
            LocalSessionId = NextSessionId(),
            LocalResumeSecret = NextResumeSecret(),
            RemoteSessionId = remoteSessionId,
            RemoteMaxMessageSize = remoteMaxMessageSize,
            RemoteResumeSecret = remoteResumeSecret.ToArray(),
            State = HandshakeState.Established,
        };

    private void SendAccept(UdpLink link, PeerSlot slot)
    {
        ControlDatagramCodec.WriteAccept(
            _sendBuffer,
            slot.ClientNonce,
            slot.LocalSessionId,
            (uint)_options.MaxMessageSize,
            slot.LocalResumeSecret);
        SendControl(link, slot.Remote);
    }

    /// <summary>
    /// Spends one unit of the per-Pump admission budget, or Discards. The cap bounds work rather than
    /// state — one HMAC minted or verified is one unit, and both halves of the handshake cost one —
    /// while the cookie is what bounds state. Beyond the cap the datagram is gone, because UDP gives
    /// it nowhere to wait: this profile Discards where the others defer, and the count is exact
    /// because the Reactor had the work in hand when it let it go.
    /// </summary>
    private bool TryChargeAdmission()
    {
        if (_admissionsThisFrame >= _admissionCap)
        {
            _counters.RecordAdmissionDiscard();
            _capDiscardsThisFrame++;
            return false;
        }

        _admissionsThisFrame++;
        return true;
    }

    /// <summary>
    /// Reports the Frame's declined Admissions once each, at its end, rather than once per declining —
    /// a flood would otherwise produce one record per datagram, which is the volume the cap exists to
    /// bound.
    /// </summary>
    private void ReportDeclinedAdmissions()
    {
        if (_capDiscardsThisFrame > 0)
        {
            _diagnostics.Log(DiagnosticRecord.ForAdmissionCapReached(_admissionCap, _capDiscardsThisFrame));
        }

        if (_capacityDiscardsThisFrame > 0)
        {
            _diagnostics.Log(DiagnosticRecord.ForCapacityExhausted(_options.Reactor.Capacity, _capacityDiscardsThisFrame));
        }
    }

    private void DiscardMalformed(Source source, int bytesReceived)
    {
        _counters.RecordUnknownSourceDiscard();
        _diagnostics.Log(DiagnosticRecord.ForDatagramMalformed(source, bytesReceived));
    }

    private void SendControl(UdpLink link, Source destination)
    {
        UdpLinkSendResult result = link.SendTo(destination, _sendBuffer);
        if (result.IsSuccess)
        {
            _counters.RecordDatagramSent();
        }
    }

    private void ClosePeer(SlotHandle handle, DisconnectReason reason)
    {
        if (_peers.Remove(handle) is not PeerSlot slot)
        {
            return;
        }

        _peersBySessionId.Remove(slot.LocalSessionId);
        _dialsByNonce.Remove(slot.ClientNonce);
        _acceptedHandshakes.Remove((slot.Remote, slot.ClientNonce));
        _pendingEvents.Add(new PendingEvent
        {
            Kind = SessionEventKind.PeerClosed,
            Peer = ToPeerId(handle),
            Reason = reason,
        });
    }

    /// <summary>
    /// A session id no live Session already holds, never
    /// <see cref="DatagramSessionProtocol.ControlSession"/>. Drawn from
    /// <see cref="DatagramSessionOptions.Entropy"/> so that it is unguessable to anyone not on the
    /// path, which is what stops an off-path attacker addressing a Session it was never introduced to.
    /// The loop terminates because at most <see cref="ReactorOptions.Capacity"/> of 2^32 values are
    /// taken.
    /// </summary>
    private uint NextSessionId()
    {
        Span<byte> bytes = stackalloc byte[sizeof(uint)];
        while (true)
        {
            _entropy.GetBytes(bytes);
            uint candidate = BitConverter.ToUInt32(bytes);
            if (candidate != DatagramSessionProtocol.ControlSession && !_peersBySessionId.ContainsKey(candidate))
            {
                return candidate;
            }
        }
    }

    private ulong NextNonce()
    {
        Span<byte> bytes = stackalloc byte[sizeof(ulong)];
        _entropy.GetBytes(bytes);
        return BitConverter.ToUInt64(bytes);
    }

    /// <summary>
    /// A fresh secret for a later Resume. Established during the handshake and stored, never used as a
    /// routing field and never sent again once the handshake is over: a value that both identifies and
    /// authorises is a value an eavesdropper who sees one datagram can replay.
    /// </summary>
    private byte[] NextResumeSecret()
    {
        byte[] secret = new byte[DatagramSessionProtocol.ResumeSecretLength];
        _entropy.GetBytes(secret);
        return secret;
    }

    private static PeerId ToPeerId(SlotHandle handle) => new(handle.Origin, handle.Index, handle.Generation);

    private static SlotHandle ToHandle(PeerId peer) => new(peer.Origin, peer.Index, peer.Generation);

    private void DisposeLinks()
    {
        foreach (UdpLink link in _links)
        {
            _dispatcher.Unregister(link.Socket);
            link.Dispose();
        }

        _links.Clear();
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
        /// <summary>A dial has sent its Hello and is waiting for the Cookie that answers it.</summary>
        AwaitingCookie,

        /// <summary>A dial has echoed the cookie and is waiting for an Accept or a Refusal.</summary>
        AwaitingAccept,

        /// <summary>The Session exists and both ends know each other's session id.</summary>
        Established,
    }

    /// <summary>
    /// The state one occupied capacity Slot holds. The two session ids are deliberately separate:
    /// each end assigns the value it wants stamped on datagrams sent to it, so neither has to agree
    /// with the other about a single shared number, and neither can be made to adopt one it did not
    /// choose.
    /// </summary>
    private sealed class PeerSlot
    {
        /// <summary>The address this Peer was last known at — a claim, never an authenticator.</summary>
        public required Source Remote { get; init; }

        /// <summary>The bound socket this Peer's datagrams are sent from.</summary>
        public required UdpLink Link { get; init; }

        /// <summary>The nonce that ties every datagram of this handshake to this Peer and no other.</summary>
        public required ulong ClientNonce { get; init; }

        /// <summary>The id this Reactor assigned, which the remote stamps on datagrams sent here.</summary>
        public required uint LocalSessionId { get; init; }

        /// <summary>The secret this Reactor will require to be presented before it resumes this Session.</summary>
        public required byte[] LocalResumeSecret { get; init; }

        /// <summary>The id the remote assigned, which this Reactor stamps on datagrams sent there.</summary>
        public uint RemoteSessionId { get; set; }

        /// <summary>The largest Message the remote said it will accept.</summary>
        public uint RemoteMaxMessageSize { get; set; }

        /// <summary>The secret the remote will require before it resumes this Session. Stored, unused until Resume lands.</summary>
        public byte[] RemoteResumeSecret { get; set; } = [];

        /// <summary>The Slot this Peer occupies, filled in once the table has admitted it.</summary>
        public SlotHandle Handle { get; set; }

        /// <summary>How far through the handshake this Peer has got.</summary>
        public required HandshakeState State { get; set; }
    }

    private readonly record struct PendingEvent
    {
        public SessionEventKind Kind { get; init; }

        public PeerId Peer { get; init; }

        public DisconnectReason Reason { get; init; }
    }
}
