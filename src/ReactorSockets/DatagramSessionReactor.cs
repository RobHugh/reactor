using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using ReactorSockets.Core;
using ReactorSockets.Session;
using ReactorSockets.Transports;

namespace ReactorSockets;

/// <summary>
/// The datagram Session profile (ADR 0009): Sessions established over UDP, addressed as Peers, driven
/// by a single synchronous <see cref="Pump"/> from the consumer's own loop. It carries the handshake
/// and everything that guards it — the stateless cookie, the amplification bound, version and
/// capacity refusals, session identity and demultiplexing — and the data plane behind it: packet
/// records in four sequence Spaces, one per <see cref="DeliveryMode"/>, fragmentation of reliable
/// Messages, acknowledgement and retransmission, heartbeats, Goodbye, interruption and Resume.
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
public sealed class DatagramSessionReactor : IDisposable, ISessionEventSource
{
    /// <summary>
    /// The largest a UDP datagram can be, and this Reactor's scratch buffer size — at which
    /// truncation is unreachable, so a datagram is never silently shortened into a different one.
    /// </summary>
    private const int MaxUdpDatagramSize = 65_507;

    private readonly SessionReactorLifecycle _lifecycle = new();
    private readonly DatagramSessionOptions _options;
    private readonly ReadinessDispatcher _dispatcher;
    private readonly FrameArena _arena;
    private readonly ReceiveArena _receiveArena;
    private readonly DiagnosticGateway _diagnostics;
    private readonly ReactorCounterSet _counters;
    private readonly ShareHolderTable<PeerSlot> _peers;

    /// <summary>
    /// One <see cref="ReceiveWindowSpaces"/> per capacity Slot, preallocated at construction and never
    /// grown or replaced (issue #180): each Slot's four windows are built once over the receive
    /// arena's admission runs for that Slot index and outlive whichever Peer occupies the Slot.
    /// <see cref="ClassifyRecord"/> resolves into this by <see cref="SlotHandle.Index"/> rather than
    /// through <c>PeerSlot</c>, since a Slot's windows exist before any Peer is admitted into it.
    /// </summary>
    private readonly ReceiveWindowSpaces[] _receiveWindows;

    /// <summary>
    /// This Reactor's one send-side retention pool (issue #43, #206), built once at construction from
    /// the options' capacity, reliable Space count, retention slot count and fragment size, and
    /// resolved by <see cref="SlotHandle.Index"/> the same way <see cref="_receiveWindows"/> is —
    /// a Slot's retention exists before any Peer is admitted into it and outlives whichever Peer
    /// occupies the Slot, so nothing is allocated on admission and nothing is freed on disconnect.
    /// </summary>
    private readonly RetentionPool _retention;

    private readonly CookieMinter _cookies;
    private readonly RandomNumberGenerator _entropy;
    private readonly PumpIntervalObserver _pumpIntervalObserver;
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

    /// <summary>
    /// Closed-session records (ADR 0017, second amendment 2026-09-29): one ring entry per capacity Slot,
    /// written by <see cref="RememberClosedSession"/> when a Peer closes while the remote may still be
    /// waiting for the ack of its Goodbye. Holds no capacity Slot. When the ring is full the oldest entry
    /// is overwritten, so a burst of closes larger than Capacity within one Linger can lose the oldest
    /// re-ack, which degrades that sender to <see cref="DisconnectReason.LingerExpired"/> as before.
    /// </summary>
    private readonly ClosedSession[] _closedSessions;

    /// <summary>Ring index in <see cref="_closedSessions"/> by the closed session's local id. Sized once, never grows.</summary>
    private readonly Dictionary<uint, int> _closedSessionIndex;

    /// <summary>The ring position the next <see cref="RememberClosedSession"/> writes to.</summary>
    private int _closedSessionNext;

    private readonly byte[] _scratchBuffer = new byte[MaxUdpDatagramSize];
    private readonly byte[] _sendBuffer = new byte[DatagramSessionProtocol.ControlDatagramSize];

    /// <summary>
    /// The scratch buffer <see cref="FlushPeer"/> packs one Peer's data Datagram into (issue #216),
    /// separate from <see cref="_scratchBuffer"/> — which is read-side and reused within one
    /// <see cref="OnLinkReadable"/> call — and from <see cref="_sendBuffer"/>, which is sized for a
    /// control datagram only and far too small for a coalesced data one. Sized at
    /// <see cref="MaxUdpDatagramSize"/> like <see cref="_scratchBuffer"/> so it can hold whatever a
    /// Pump packs into it; reused across every Peer flushed by one Pump rather than allocated per Peer.
    /// </summary>
    private readonly byte[] _outboundDatagramBuffer = new byte[MaxUdpDatagramSize];

    /// <summary>
    /// <see cref="FragmentSetsAreWellFormed"/>'s scratch, sized once at construction from
    /// <see cref="MaxUdpDatagramSize"/> — the largest datagram this Reactor ever reads — divided by the
    /// smallest a Fragment record can be (<see cref="DatagramSessionProtocol.FragmentRecordSize"/>), so
    /// no datagram this Reactor accepts can ever hold more Fragments than these arrays hold entries for
    /// (issue #256, DoS follow-up). A group's identity is the pair (mode, first sequence), never first
    /// sequence alone (issue #47): <see cref="ReceiveWindowSpaces"/>'s own doc states the reason — each
    /// <see cref="PacketRecordMode"/> is an independent Space, so a sequence admitted under one mode says
    /// nothing about the same sequence under another, and two groups in different modes can never
    /// conflict, however their first sequences or declared ranges happen to line up. One entry per
    /// Fragment actually found in the datagram being parsed: <see cref="_fragmentGroupKey"/> packs each
    /// Fragment's mode into its high bits and its first sequence into its low bits
    /// (<see cref="PackModeAndFirstSequence"/>), and is sorted together with
    /// <see cref="_fragmentGroupIndexAndCount"/> by that combined key so every Fragment of one (mode,
    /// first sequence) group becomes one contiguous run in O(n log n) instead of re-walking the datagram
    /// once per Fragment — sorting by mode first is what keeps one mode's runs from ever interleaving
    /// with another's. <see cref="_distinctGroupFirstSequenceAndCount"/> then holds one entry per
    /// distinct group (mode, first sequence and declared count all packed together), grouped by mode in
    /// the same order the runs above were closed in; the O(g log g) sweep that finds a declared-range
    /// overlap is run once per mode's own contiguous slice of that array via
    /// <see cref="_distinctGroupSortKey"/> — each group's declared range start relative to an anchor
    /// derived fresh for that mode's slice — so a comparison across two different modes' groups never
    /// happens at all.
    /// Four arrays of <see cref="MaxFragmentRecordsPerDatagram"/> entries costs about 140 KB per Reactor,
    /// paid once at construction and never again: the point of sizing them off a bound fixed at
    /// construction is that a peer's choice of datagram can never move it.
    /// Reused across every datagram parsed — never cleared, since every read is bounded by a count
    /// computed fresh for that datagram, and never resized, since the bound above is fixed at
    /// construction. Single-threaded like every other field this Reactor's Pump touches (ADR 0002).
    /// </summary>
    /// <remarks>
    /// <see cref="OneSequenceSpace"/> sits beside these because the sweep they feed measures distances
    /// around the <c>u32</c> sequence circle, and 2^32 does not fit in the <see cref="uint"/> that circle
    /// is counted in.
    /// </remarks>
    private const ulong OneSequenceSpace = 1UL << 32;

    private const int MaxFragmentRecordsPerDatagram =
        (MaxUdpDatagramSize - DatagramSessionProtocol.DatagramHeaderSize) / DatagramSessionProtocol.FragmentRecordSize;

    private readonly ulong[] _fragmentGroupKey = new ulong[MaxFragmentRecordsPerDatagram];
    private readonly uint[] _fragmentGroupIndexAndCount = new uint[MaxFragmentRecordsPerDatagram];
    private readonly ulong[] _distinctGroupFirstSequenceAndCount = new ulong[MaxFragmentRecordsPerDatagram];
    private readonly int[] _distinctGroupSortKey = new int[MaxFragmentRecordsPerDatagram];

    private readonly int _admissionCap;
    private int _admissionsThisFrame;
    private int _capDiscardsThisFrame;
    private int _capacityDiscardsThisFrame;

    /// <summary>
    /// The largest declared <c>fragmentCount</c> a Fragment's set may legally claim: this Reactor's own
    /// receive window, <see cref="DatagramSessionOptions.ResolvedReceiveWindowFragments"/>, not
    /// <see cref="DatagramSessionOptions.ReceiveWindowFloor"/> (issue #282, reworked under #264). The
    /// floor is computed from <em>this end's own</em> <see cref="DatagramSessionOptions.ResolvedFragmentSize"/>,
    /// but fragment size is unilateral and per-direction (ADR 0004) — a conforming peer with a smaller
    /// fragment size legally cuts the same, within-<see cref="DatagramSessionOptions.MaxMessageSize"/>
    /// Message into more Fragments than that floor, and bounding by the floor would close that peer as
    /// a protocol violator for a set it was always entitled to send. The window is the right bound
    /// instead of a looser one: it is what a Fragment set actually occupies while it is held, so a
    /// declared count larger than the window can never be reassembled — there is no slot for every
    /// piece to sit in at once — and refusing it refuses exactly the sets that were never legal,
    /// whoever's fragment size produced them. <see cref="DatagramSessionOptions.Validate"/> already
    /// requires the window to be at least the floor, so this bound is never tighter than the old one: a
    /// set at exactly the announced limit is still admitted, and at default options — where the window
    /// resolves to the floor — nothing observable changes. Where an operator deliberately widens
    /// <see cref="DatagramSessionOptions.ReceiveWindowFragments"/> past the floor, a declared count
    /// between the floor and the window is admitted, so such a set can reassemble past
    /// <see cref="DatagramSessionOptions.MaxMessageSize"/>. That is the accepted trade for never closing
    /// a conforming peer: this check bounds what is held, and <see cref="RefuseOverLimit"/> refuses to
    /// deliver the assembled Message (issue #425). Computed once
    /// here, at construction, rather than read from <see cref="_options"/> per datagram — the refusal
    /// path this bounds must allocate nothing and do no more arithmetic than a single comparison.
    /// </summary>
    private readonly int _maxLegalFragmentCount;

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
        _dispatcher = new ReadinessDispatcher();
        _arena = new FrameArena(options.ResolveArenaSize(), options.Reactor.Capacity);
        _receiveArena = new ReceiveArena(
            options.Reactor.Capacity,
            DatagramSessionOptions.ReliableSpaceCount,
            DatagramSessionOptions.SpaceCount,
            options.ResolvedReceiveWindowFragments,
            options.ResolvedFragmentSize);
        _counters = new ReactorCounterSet();
        _diagnostics = new DiagnosticGateway(options.Reactor.Sink, options.Reactor.MinimumLevel, _counters);
        _peers = new ShareHolderTable<PeerSlot>(options.Reactor.Capacity, ReactorOrigin.Next());
        _entropy = options.Entropy;
        _cookies = new CookieMinter(options.Reactor.TimeProvider, _entropy);
        _admissionCap = options.Reactor.ResolvedAdmissionCap;
        _maxLegalFragmentCount = options.ResolvedReceiveWindowFragments;
        _pumpIntervalObserver = new PumpIntervalObserver(options.Reactor.TimeProvider);

        _closedSessions = new ClosedSession[options.Reactor.Capacity];
        _closedSessionIndex = new Dictionary<uint, int>(options.Reactor.Capacity);

        _receiveWindows = new ReceiveWindowSpaces[options.Reactor.Capacity];
        for (int i = 0; i < _receiveWindows.Length; i++)
        {
            _receiveWindows[i] = new ReceiveWindowSpaces(_receiveArena, i);
        }

        _retention = new RetentionPool(
            options.Reactor.Capacity,
            DatagramSessionOptions.ReliableSpaceCount,
            options.RetentionSlots,
            options.ResolvedFragmentSize);

        try
        {
            foreach (IPEndPoint endpoint in options.Endpoints)
            {
                BindAndRegister(endpoint);
            }
        }
        catch
        {
            _links.DisposeAndClear(_dispatcher, link => link.Socket);
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

    /// <summary>
    /// The interval this Reactor's own Pump calls are currently observed to arrive at
    /// (<see cref="PumpIntervalObserver"/>), read fresh from every Pump. This is the derived floor the
    /// adaptive RTO (issue #191, slice #42) clamps against: a timeout shorter than the gap between
    /// Pumps cannot be acted on. Internal because nothing outside this Reactor derives a bound from it
    /// yet — this ticket only wires the measurement in.
    /// </summary>
    internal TimeSpan ObservedPumpInterval => _pumpIntervalObserver.Interval;

    /// <summary>
    /// This Reactor's <see cref="DatagramSessionOptions.SendIdleInterval"/> as announced on
    /// Hello-with-cookie and Accept, in whole milliseconds rounded up. Rounding up keeps a
    /// sub-millisecond interval from going out as zero, which the peer rejects as malformed, and never
    /// shortens the deadline the peer derives from it.
    /// </summary>
    private ulong AnnouncedSendIdleIntervalMilliseconds => (ulong)Math.Ceiling(_options.SendIdleInterval.TotalMilliseconds);

    /// <summary>
    /// This Reactor's <see cref="DatagramSessionOptions.Linger"/> as announced on Hello-with-cookie and
    /// Accept, in whole milliseconds rounded up, for the same reason
    /// <see cref="AnnouncedSendIdleIntervalMilliseconds"/> rounds up: a sub-millisecond Linger going out
    /// as zero would be rejected as malformed, and rounding up never shortens the value the far end
    /// negotiates the effective (minimum) Linger from (issue #363). This is also this end's own operand
    /// of the minimum in <see cref="PeerSlot.EffectiveLinger"/> — rounded here once, on both the dialling
    /// (<see cref="OnAccept"/>) and accepting (<see cref="NewAcceptedPeer"/>) paths — so both ends
    /// negotiate from identical whole-millisecond values and land on the same effective Linger, whatever
    /// fractional-millisecond <see cref="DatagramSessionOptions.Linger"/> either end configured. The
    /// stream profile mirrors this with its own equivalent (issue #55 follow-up).
    /// </summary>
    private ulong AnnouncedLingerMilliseconds => (ulong)Math.Ceiling(_options.Linger.TotalMilliseconds);

    /// <inheritdoc cref="ISessionEventSource.PendingEventCount"/>
    int ISessionEventSource.PendingEventCount => _pendingEvents.Count;

    /// <summary>
    /// One synchronous advance: records this call's arrival against the last one
    /// (<see cref="ObservedPumpInterval"/>, issue #191) before anything else, then wipes the previous
    /// Frame's events and arena, then reads whatever bound socket is ready right now, running the
    /// handshake for everything that arrived. Throws <see cref="InvalidOperationException"/> if called
    /// from any thread other than the one that constructed this Reactor, or from inside a Pump already
    /// in progress.
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
            _capDiscardsThisFrame = 0;
            _capacityDiscardsThisFrame = 0;
            MarkDrainedClosingPeersCloseDue();
            _dispatcher.Pump();
            CloseAckedGoodbyePeers();
            RetryPendingFragmentDeliveries();
            ReportExpiredRetransmissions();
            EnforceReceiveIdleTimer();
            EnforceLingerExpiry();
            QueueDeferredGoodbyes();
            EnforceSendIdleTimer();
            FlushOutboundDatagrams();
            ClosePeersWithCloseDue();
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
    /// round trip. <see cref="SessionEventKind.PeerConnected"/> or
    /// <see cref="SessionEventKind.PeerClosed"/> resolves it on a later Pump.
    /// </summary>
    /// <remarks>
    /// The Hello is sent once, and no handshake control datagram is ever retransmitted. The heartbeat
    /// and the missed-heartbeat deadline apply only to an Established Peer, so a dial whose Hello,
    /// Cookie or Accept is lost has no timeout: it stays pending, holding its capacity Slot, until the
    /// consumer closes it.
    /// </remarks>
    public PeerId Connect(IPEndPoint destination)
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();
        ArgumentNullException.ThrowIfNull(destination);

        UdpLink link = LinkForFamily(destination.AddressFamily);
        Source remote = Source.FromIPEndPoint(destination);
        RetransmissionTimer retransmitTimer = new();

        PeerSlot slot = new()
        {
            Remote = remote,
            Link = link,
            ClientNonce = NextNonce(),
            LocalSessionId = NextSessionId(),
            LocalResumeSecret = NextResumeSecret(),
            State = HandshakeState.AwaitingCookie,
            AckTracker = NewAckTracker(retransmitTimer),
            RetransmitTimer = retransmitTimer,
            StallSignature = new(_options.Reactor.TimeProvider),
            FragmentSize = _options.ResolvedFragmentSize,
            OrderedDelivery = new(_options.OrderedHoldSlots, _options.ResolvedFragmentSize),
        };

        SlotHandle handle = _peers.TryAdd(slot) ?? throw new InvalidOperationException(
            $"This Reactor's capacity of {_options.Reactor.Capacity} Peers is fully occupied — close a Peer before dialling another.");

        slot.Handle = handle;
        _receiveWindows[handle.Index].Reset();
        _retention.ResetShareHolder(handle.Index);
        _dialsByNonce[slot.ClientNonce] = handle;

        ControlDatagramCodec.WriteHello(_sendBuffer, DatagramSessionProtocol.Version, slot.ClientNonce);
        SendControl(link, remote);

        return ToPeerId(handle);
    }

    /// <summary>
    /// Ends the Session with <paramref name="peer"/>. On an Established, non-Interrupted Peer this queues a
    /// Goodbye in the Peer's <c>ReliableOrdered</c> Space and puts the Peer in Closing (issue #397): later
    /// <see cref="Send"/> calls return <see cref="SendOutcome.Closed"/> and no
    /// <see cref="SessionEventKind.PeerClosed"/> is raised yet. On any other Peer (dialling or Interrupted)
    /// it queues a <see cref="SessionEventKind.PeerClosed"/> carrying <see cref="DisconnectReason.LocalClose"/>
    /// at once. Idempotent: a stale <see cref="PeerId"/> is a no-op, because a Peer closing under a caller
    /// is an ordinary race rather than a programmer error, and a second call on a Closing Peer queues no
    /// second Goodbye.
    /// </summary>
    /// <remarks>
    /// A Closing Peer ends with <see cref="DisconnectReason.LocalClose"/> once the Goodbye and every
    /// <c>ReliableOrdered</c> record before it are acknowledged, or <see cref="DisconnectReason.LingerExpired"/> once its effective Linger has elapsed since this
    /// call with the Goodbye still unacknowledged. It is never Interrupted and never sends a Resume.
    /// When both ends Close at once, each ends with <see cref="DisconnectReason.LocalClose"/>: a Closing Peer
    /// that receives the remote's Goodbye acknowledges it and raises neither
    /// <see cref="SessionEventKind.PeerClosing"/> nor <see cref="DisconnectReason.PeerGoodbye"/>.
    /// Accepted gap: the Goodbye is ordered only within <c>ReliableOrdered</c>, so a
    /// <c>ReliableUnordered</c> Message sent before this call can still arrive after the Goodbye, and
    /// the remote, already closed or Closing, does not deliver it.
    /// </remarks>
    public void Close(PeerId peer)
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();

        SlotHandle handle = ToHandle(peer);
        if (_peers.TryResolve(handle) is PeerSlot { State: HandshakeState.Established, IsInterrupted: false } slot)
        {
            if (!slot.IsClosing)
            {
                slot.LocalClose = LocalCloseState.GoodbyeDeferred;
                slot.ClosingTimestamp = _options.Reactor.TimeProvider.GetTimestamp();
                QueueGoodbye(slot);
            }

            return;
        }

        ClosePeer(handle, DisconnectReason.LocalClose);
    }

    /// <summary>
    /// A point-in-time snapshot of one Peer's counters (ADR 0013). Throws if the Peer is not open,
    /// matching <see cref="StreamSessionReactor.CountersOf(PeerId)"/>'s shape for the stream profile —
    /// the datagram profile had no per-Peer counter reader until this member (subtask 6.2.1, issue
    /// #168). <see cref="PeerCounters.OutOfWindowPackets"/> and <see cref="PeerCounters.DuplicatePackets"/>
    /// now reflect what the four per-Space receive windows have refused (issue #172);
    /// <see cref="PeerCounters.UnreliableLossPackets"/> reflects gaps either unreliable Space's window
    /// has slid past (issues #173, #176).
    /// <see cref="PeerCounters.MessagesReceived"/> counts whole Messages handed to the frame arena for
    /// delivery, and not a Message the arena refused. <see cref="PeerCounters.Resumes"/> counts the
    /// Peer's resumes: once per <see cref="SessionEventKind.PeerResumed"/> raised for it, on the
    /// accepting end when a matching Resume moves it out of Interrupted and on the dialling end when it
    /// hears its session again after sending a Resume, and never reset by one (ADR 0013).
    /// <see cref="PeerCounters.RetentionHighWaterBytes"/> reads the true per-Peer figure — summed
    /// across both reliable Spaces — from <see cref="_retention"/> (subtask 6.5.6, issue #205).
    /// </summary>
    public PeerCounters CountersOf(PeerId peer)
    {
        EnsureUsable();

        SlotHandle handle = ToHandle(peer);
        PeerSlot slot = _peers.TryResolve(handle)
            ?? throw new ArgumentException($"Peer {peer} is not open.", nameof(peer));

        return slot.Counters.Snapshot(retentionHighWaterBytes: _retention.HighWaterBytes(handle.Index));
    }

    /// <summary>
    /// Sends <paramref name="message"/> to <paramref name="peer"/> under <paramref name="mode"/> (ADR
    /// 0009, issues #207, #215, #216). A Message too wide for its mode to carry is refused before any
    /// work is done on it: no sequence is allocated and no retention is taken, so a rejected Send leaves
    /// no trace on the Session. How wide is too wide depends on the mode and is decided by
    /// <see cref="FragmentPlan.Resolve"/>, the one place the rule is stated: an unreliable mode is capped
    /// at a single Datagram and refused as <see cref="SendOutcome.MessageTooLarge"/>, while a reliable
    /// mode fragments below a cap and is refused above it as <see cref="SendOutcome.RemoteMessageTooLarge"/>
    /// (ADR 0004). That reliable cap is <paramref name="peer"/>'s own announced receive limit, captured
    /// on its Slot at handshake, not this Reactor's own <see cref="DatagramSessionOptions.MaxMessageSize"/>
    /// (issue #284): <c>Send</c> is therefore not purely local to this Reactor's configuration, because
    /// the same payload can be accepted for one Peer and refused for another whose handshake announced a
    /// smaller limit. That gate sits behind the <paramref name="peer"/> lookup rather than in front of it,
    /// because both caps it measures against, fragment size and remote max message size alike, are the
    /// Peer's own captured values (issue #248); a Peer this Reactor is not holding open returns
    /// <see cref="SendOutcome.Closed"/> whatever the Message's width. Past
    /// that gate, a reliable mode allocates the next sequence in its own Space and retains the Message
    /// whole there, returning <see cref="SendOutcome.RetentionExhausted"/> once that Space has no slot
    /// left — and that refusal spends nothing either: the sequence is claimed only once retention has
    /// committed, so a refused reliable Send leaves the Space's next sequence exactly where it was
    /// (issue #244), rather than opening a gap the receiver's window would wait on for a record that
    /// is never sent; <see cref="DeliveryMode.Unreliable"/> and <see cref="DeliveryMode.UnreliableSequenced"/>
    /// still allocate a sequence — every mode gets one, the same as every other
    /// (<see cref="OutboundSequenceSpaces"/>) — but retain nothing, because nothing unreliable is ever
    /// retransmitted (issue #43). A Message this call accepts is queued on <paramref name="peer"/>'s
    /// Slot (<see cref="PeerSlot.PendingSends"/>) and leaves as one packet record inside the next
    /// <see cref="Pump"/>'s single coalesced Datagram to that Peer, in Send order
    /// (<see cref="FlushOutboundDatagrams"/>, issue #216) — nothing reaches the wire before that Pump
    /// runs, and a <c>Send</c> with no following Pump puts nothing on the wire at all. That next Pump is
    /// only the first one this record is <em>eligible</em> to leave in: if this Peer already has more
    /// queued than one Datagram holds, a record queued behind the boundary waits for however many
    /// further Pumps it takes to reach the front, still in the order it was Sent (issue #230).
    /// </summary>
    /// <remarks>
    /// <b><see cref="SendOutcome.RetentionExhausted"/> is reachable from one <c>Send</c> call</b> (issue
    /// #247, ADR 0004's "one Message can fill the send window alone"): the retention budget is sized in
    /// Fragments, not Messages, so a single Message cut into more Fragments than its Space has free
    /// slots for exhausts that Space by itself, with nothing else needing to have sent anything first.
    /// The refusal is whole, not partial — this checks the Space's free-slot count against the Fragment
    /// count the Message needs <em>before</em> retaining or sequencing a single Fragment, so a refused
    /// Send leaves retention holding no Fragment of that Message and leaves the Space's next sequence
    /// exactly where <see cref="Send"/> found it, the same "spend nothing on a refusal" contract issue
    /// #244 established for a whole small Message. A half-retained Message would be unrepairable: the
    /// receiver's reassembly waits forever for a set that was never cut in full. A smaller Message that
    /// fits the Space's free slots still succeeds afterward — the refusal costs the Space nothing.
    /// <para>
    /// Sending to a <paramref name="peer"/> this Reactor is not currently holding open — unknown, or
    /// already closed — returns <see cref="SendOutcome.Closed"/> rather than throwing, unlike
    /// <see cref="CountersOf"/>: a Peer closing under a caller is an ordinary race, the same reason
    /// <see cref="Close"/> tolerates a stale <see cref="PeerId"/>.
    /// </para>
    /// <para>
    /// Sending to a <paramref name="peer"/> whose handshake has not reached
    /// <see cref="HandshakeState.Established"/> returns <see cref="SendOutcome.NotEstablished"/> under
    /// every mode (issue #332), checked before any width gate. Before the handshake the Peer has no
    /// remote-announced <c>MaxMessageSize</c>, so a reliable Send would otherwise be misreported as
    /// <see cref="SendOutcome.RemoteMessageTooLarge"/>.
    /// </para>
    /// </remarks>
    /// <param name="peer">The Peer to send to.</param>
    /// <param name="mode">Which of the four delivery modes carries this Message; see <c>docs/delivery-modes.md</c>.</param>
    /// <param name="message">
    /// The Message payload, unframed. The bytes are copied before this call returns — into
    /// <see cref="_retention"/> for a reliable mode, into the queued record for an unreliable one — so
    /// the caller owns its buffer again the instant <c>Send</c> returns and may overwrite or reuse it
    /// without changing what is eventually put on the wire (issue #244). A Message wider than
    /// <see cref="DatagramSessionOptions.ResolvedFragmentSize"/> cannot fit in one Datagram, and for
    /// <see cref="DeliveryMode.Unreliable"/> and <see cref="DeliveryMode.UnreliableSequenced"/> that cap
    /// is reported back as <see cref="SendOutcome.MessageTooLarge"/> rather than thrown, and it is
    /// permanent — no option lifts it: you may only fragment what you can retransmit (ADR 0004), because
    /// fragmenting what cannot be repaired multiplies the failure rate by the number of pieces — a
    /// ten-Fragment unreliable Message on a path losing 1% of Datagrams arrives about 90% of the time,
    /// having spent ten Datagrams of bandwidth on a coin flip (<c>docs/delivery-modes.md</c>). For
    /// <see cref="DeliveryMode.ReliableOrdered"/> and <see cref="DeliveryMode.ReliableUnordered"/> the
    /// single-Datagram cap does not apply: a Message up to <paramref name="peer"/>'s own announced
    /// <c>MaxMessageSize</c> (issue #284; see the summary above) is cut into
    /// <c>ceil(length / fragmentSize)</c> Fragments (issue #245), each retained and sequenced as its own
    /// ordinary reliable packet, and only a Message wider than that remote limit is refused with
    /// <see cref="SendOutcome.RemoteMessageTooLarge"/>. That <c>fragmentSize</c> is this Peer's own
    /// <see cref="PeerSlot.FragmentSize"/>, captured from <see cref="DatagramSessionOptions.ResolvedFragmentSize"/>
    /// once when the Peer was admitted (issue #248) — not re-read from options on this or any later
    /// call. Fragment size is unilateral, per-direction, and decided once, at <c>Send</c> (ADR 0004): a
    /// retransmit re-sends bytes already cut rather than re-deciding anything, so two Messages sent to
    /// the same Peer at different times, and every retransmit of either, are always cut with the same
    /// size. It is never announced in the handshake, because a receiver reassembles a Fragment set by
    /// <c>fragmentIndex</c> and <c>fragmentCount</c> alone and never needs to know the sender's choice.
    /// </param>
    public SendOutcome Send(PeerId peer, DeliveryMode mode, ReadOnlySpan<byte> message)
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();

        PacketRecordMode recordMode = ToRecordMode(mode);
        bool isReliable = recordMode is PacketRecordMode.ReliableOrdered or PacketRecordMode.ReliableUnordered;

        SlotHandle handle = ToHandle(peer);
        if (_peers.TryResolve(handle) is not PeerSlot slot)
        {
            return SendOutcome.Closed;
        }

        if (slot.RemoteGoodbyeDelivered)
        {
            return SendOutcome.Closed;
        }

        if (slot.State != HandshakeState.Established)
        {
            return SendOutcome.NotEstablished;
        }

        if (slot.IsClosing)
        {
            return SendOutcome.Closed;
        }

        // The fragment size used to cut this Message is this Peer's own, captured on the Slot at
        // admission (issue #248, ADR 0004) rather than read from _options here — fragment size is
        // decided once, per Peer, and Send only ever consults what was already decided. The reliable
        // cap handed to FragmentPlan.Resolve is likewise this Peer's own remote-announced MaxMessageSize
        // (issue #284; see this method's XML doc for why a reliable Send is capped by the far end's
        // limit rather than this Reactor's own). RemoteMaxMessageSize is a uint off the wire while
        // FragmentPlan.Resolve takes an int message length, so a value above int.MaxValue is clamped
        // rather than cast unchecked; a Message that large will never exist in practice, so the clamp
        // only ever widens the cap past a width nothing here could reach anyway. One call to
        // FragmentPlan.Resolve states the too-large rule exactly once: unreliable modes refuse above one
        // Fragment's worth, reliable modes refuse above that remote cap.
        int remoteMaxMessageSize = slot.RemoteMaxMessageSize > int.MaxValue ? int.MaxValue : (int)slot.RemoteMaxMessageSize;
        FragmentPlanResult plan = FragmentPlan.Resolve(message.Length, slot.FragmentSize, mode, remoteMaxMessageSize);
        if (plan.IsMessageTooLarge)
        {
            // Which limit refused it follows from the mode alone (issue #284): an unreliable mode can
            // only have been refused for exceeding the local one-Datagram cap (slot.FragmentSize), while
            // a reliable mode can only have been refused for exceeding remoteMaxMessageSize, the peer's
            // own announced limit — FragmentPlan.Resolve never applies the other cap to the other kind
            // of mode.
            return isReliable ? SendOutcome.RemoteMessageTooLarge : SendOutcome.MessageTooLarge;
        }

        if (!isReliable)
        {
            slot.PendingSends.Add(new PendingOutboundRecord
            {
                Mode = recordMode,
                Sequence = slot.OutboundSequences.Allocate(recordMode),
                UnreliablePayload = message.ToArray(),
            });
            return SendOutcome.Sent;
        }

        int spaceIndex = ReliableSpaceIndex(recordMode);
        int fragmentSize = slot.FragmentSize;
        int fragmentCount = plan.FragmentCount;

        // A Message's Fragments are committed as one unit (issue #247): the retention budget is sized
        // in Fragments, not Messages, so a Message wide enough to need more Fragments than the Space has
        // free slots for must be refused whole rather than left half-retained — the receiver can never
        // reassemble a set that was never cut in full. Checked before anything is touched, because
        // neither this pool nor OutboundSequenceSpaces can roll a partial commit back (issue #244): the
        // slot count and the sequence space are both left exactly where they were on a refusal.
        if (_retention.FreeSlotCount(handle.Index, spaceIndex) < fragmentCount)
        {
            return SendOutcome.RetentionExhausted;
        }

        if (fragmentCount == 1)
        {
            // The sequence is read before retention is attempted and only spent once retention commits.
            // TryAcquire refuses an exhausted Space without touching the pool, so a refusal here has to
            // leave the Space where it found it too: a burned sequence is a hole the receiver's window
            // waits on for a record that will never be sent (issue #244), and a Space has no rewind.
            uint sequence = slot.OutboundSequences.Peek(recordMode);
            RetentionSlotOutcome outcome = _retention.TryAcquire(handle.Index, spaceIndex, sequence, message);

            if (outcome == RetentionSlotOutcome.Committed)
            {
                slot.OutboundSequences.Allocate(recordMode);
                slot.PendingSends.Add(new PendingOutboundRecord { Mode = recordMode, Sequence = sequence });
            }

            return outcome switch
            {
                RetentionSlotOutcome.Committed => SendOutcome.Sent,
                RetentionSlotOutcome.Exhausted => SendOutcome.RetentionExhausted,
                _ => throw new InvalidOperationException($"Unrecognised {nameof(RetentionSlotOutcome)}: {outcome}."),
            };
        }

        // Cutting a Message into Fragments (issue #245, ADR 0004): each Fragment is retained and
        // sequenced exactly as a whole small Message would be above, one at a time in order, so their
        // sequence numbers fall out contiguous and ascending as a consequence of allocating from the
        // same Space with nothing else able to interleave. Reassembly's firstSeq = seq - fragmentIndex
        // depends on that contiguity, so it is asserted below, once the set is built, by
        // FragmentGroup.AssertContiguousSet (issue #319) rather than merely trusted.
        //
        // The FreeSlotCount check above already guarantees every TryAcquire below commits: nothing else
        // can shrink this Space's free slots between that check and this loop, because Send only ever
        // runs on the constructing thread (EnsureCallingThreadIsConstructingThread). A commit failing
        // here would mean the two disagreed, which is a bug in this method, not a runtime outcome to
        // report through SendOutcome (issue #247).
#if DEBUG
        uint[] debugSequencesInFragmentIndexOrder = new uint[fragmentCount];
#endif

        for (int fragmentIndex = 0; fragmentIndex < fragmentCount; fragmentIndex++)
        {
            int offset = fragmentIndex * fragmentSize;
            int length = Math.Min(fragmentSize, message.Length - offset);
            ReadOnlySpan<byte> fragmentPayload = message.Slice(offset, length);

            uint sequence = slot.OutboundSequences.Peek(recordMode);

            // The Fragment's shape — that it is one, its index, its count — is handed to retention here
            // and nowhere else (issue #250): TryRead hands it straight back to a retransmit, so the
            // shape is decided once and never rebuilt from a PendingOutboundRecord.
            RetentionSlotOutcome outcome = _retention.TryAcquire(
                handle.Index, spaceIndex, sequence, fragmentPayload, isFragment: true, (ushort)fragmentIndex, (ushort)fragmentCount);

            Debug.Assert(
                outcome == RetentionSlotOutcome.Committed,
                "the FreeSlotCount check above already reserved enough slots for every Fragment in this Message.");

            slot.OutboundSequences.Allocate(recordMode);
            slot.PendingSends.Add(new PendingOutboundRecord { Mode = recordMode, Sequence = sequence });

#if DEBUG
            debugSequencesInFragmentIndexOrder[fragmentIndex] = sequence;
#endif
        }

#if DEBUG
        FragmentGroup.AssertContiguousSet(debugSequencesInFragmentIndexOrder, (ushort)fragmentCount);
#endif

        return SendOutcome.Sent;
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
        if (_lifecycle.IsDisposed)
        {
            return;
        }

        EnsureCallingThreadIsConstructingThread();

        _lifecycle.IsDisposed = true;
        _peers.DrainAll();
        _peersBySessionId.Clear();
        _closedSessionIndex.Clear();
        _dialsByNonce.Clear();
        _acceptedHandshakes.Clear();
        _pendingEvents.Clear();
        _links.DisposeAndClear(_dispatcher, link => link.Socket);
    }

    /// <inheritdoc cref="ISessionEventSource.GetPendingEvent"/>
    SessionEvent ISessionEventSource.GetPendingEvent(int index)
    {
        PendingEvent pending = _pendingEvents[index];
        if (pending.Kind.TryCreateCommonSessionEvent(pending.Peer, pending.Reason, out SessionEvent sessionEvent))
        {
            return sessionEvent;
        }

        // MessageReceived is the one kind here the shared half cannot build, because it alone needs
        // the arena the payload was committed to (subtask 6.6.6, issue #219). The span is handed out
        // aliasing the arena directly, so it is valid for this Frame only — the same contract the
        // stream profile's own MessageReceived carries.
        return pending.Kind switch
        {
            SessionEventKind.MessageReceived => SessionEvent.ForMessageReceived(
                pending.Peer, _arena.GetCommitted(pending.PayloadOffset, pending.PayloadLength)),
            SessionEventKind.PeerClosing => SessionEvent.ForPeerClosing(pending.Peer),
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
                    _counters.RecordUnknownSourceDatagram();
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
            ProcessDataDatagram(link, source, session, datagram);
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

            case DatagramControlType.Resume:
                OnResume(link, source, datagram);
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
    /// Session, which is the whole reason the identity is a token rather than an address. A session id
    /// naming a recently closed Session goes to <see cref="TryReAckClosedSession"/> first (ADR 0017,
    /// second amendment) and is discarded only if that declines.
    /// </summary>
    private void ProcessDataDatagram(UdpLink link, Source source, uint session, ReadOnlySpan<byte> datagram)
    {
        if (!_peersBySessionId.TryGetValue(session, out SlotHandle handle) || _peers.TryResolve(handle) is not PeerSlot slot)
        {
            if (!TryReAckClosedSession(link, source, session, datagram))
            {
                DiscardMalformed(source, datagram.Length);
            }

            return;
        }

        ParseDataDatagram(source, slot, datagram);
    }

    /// <summary>
    /// Reads the 20-byte header, then packet records until the datagram is exhausted (issue #163), and
    /// routes every record that parses into <paramref name="slot"/>'s Space for its mode (issue #172).
    /// A datagram is a container, not a Message: one bad record — a short header, a bad flags byte, a
    /// spare bit set, a length running past the end, or truncation inside a record — discards the
    /// whole datagram rather than whatever records parsed before it, because a partial admission would
    /// let a forged tail poison an otherwise legitimate send. A record with the fragment bit set is not
    /// by itself bad, so long as it is in one of the two reliable modes: <see cref="PacketRecordCodec.TryRead"/>
    /// reads the Fragment shape same as the unfragmented one, and this method routes it into its Space
    /// the same way — <see cref="ClassifyRecord"/>
    /// is what diverts an admitted Fragment away from delivery and into its Space's receive-window slot
    /// instead (issue #257). The fragment bit in an unreliable mode <em>is</em> by itself bad and fails
    /// the read outright, the same discard as any other malformed record, since ADR 0004 caps those two
    /// modes at a single Datagram and no reassembly path exists for them (issue #273); the first pass
    /// below names that refusal on the Diagnostic stream as well, because it says the sender fragments
    /// in modes this profile does not rather than that rubbish arrived. Parsing therefore runs in
    /// three passes over the same bytes: the first confirms every record is well-formed without
    /// touching any Space; the second, once <see cref="FragmentSetsAreWellFormed"/> also stands, walks
    /// every record again to refuse — with a close, not a discard — a declared <c>fragmentCount</c>
    /// larger than this Reactor's own receive window, <see cref="DatagramSessionOptions.ResolvedReceiveWindowFragments"/>,
    /// could ever hold (issue #282, reworked under #264); only once neither of those passes objects does
    /// the third classify each record in turn — so an early record is never classified on the strength
    /// of a datagram whose tail turns out to be forged, or whose Fragment set claims more Fragments
    /// than this Reactor could ever hold at once.
    /// The header's two ack blocks are applied to <paramref name="slot"/>'s
    /// <see cref="PeerSlot.AckTracker"/> for the two reliable Spaces only (subtask 6.3.6, issue #188)
    /// once that same proof stands — a short or malformed datagram applies neither block, exactly as it
    /// admits no record. Both blocks are applied at the same effective RTO, read once from
    /// <paramref name="slot"/>'s own <see cref="PeerSlot.RetransmitTimer"/> and this Reactor's
    /// <see cref="ObservedPumpInterval"/> (subtask 6.4.7, issue #197), so a horizon slide inside either
    /// call arms at the same value. Nothing here throws, allocates or reads past <paramref name="datagram"/>. No
    /// Session event is produced and nothing is delivered: what a Space does with an admitted record is
    /// #173 and #6.6's, so this method's whole job ends at "classified, and counted if refused".
    /// </summary>
    private void ParseDataDatagram(Source source, PeerSlot slot, ReadOnlySpan<byte> datagram)
    {
        if (!DatagramHeaderCodec.TryReadHeader(
                datagram,
                out _,
                out uint reliableOrderedAckBase,
                out uint reliableOrderedAckBits,
                out uint reliableUnorderedAckBase,
                out uint reliableUnorderedAckBits))
        {
            DiscardMalformedForSession(source, datagram.Length);
            return;
        }

        ReadOnlySpan<byte> body = datagram[DatagramSessionProtocol.DatagramHeaderSize..];

        ReadOnlySpan<byte> remainder = body;
        while (remainder.Length > 0)
        {
            if (!PacketRecordCodec.TryRead(remainder, out _, out _, out _, out _, out _, out _, out _, out int bytesConsumed))
            {
                // One refusal out of the several this read can reach is worth naming separately
                // (issue #273): a Fragment in an unreliable mode is well-formed at every level but
                // ADR 0004's rule, so it says the sender fragments in modes this profile does not,
                // rather than that rubbish arrived. The malformed discard below still fires — this
                // adds which rule broke, it does not replace the outcome.
                if (PacketRecordLegality.RefusedAsUnreliableFragment(remainder)
                    && _diagnostics.IsEnabled(DiagnosticLevel.Warn))
                {
                    _diagnostics.Log(DiagnosticRecord.ForUnreliableFragment(
                        source, remainder[0] & DatagramSessionProtocol.ModeMask));
                }

                DiscardMalformedForSession(source, datagram.Length);
                return;
            }

            remainder = remainder[bytesConsumed..];
        }

        // Every record on its own proved legal above (issue #253), but a Fragment's index and count are
        // only half the story: this checks the claimed sets those individually-legal Fragments form
        // (issue #256), still before any Space is touched, so a forged set is refused exactly like a
        // malformed record rather than reaching classification.
        if (!FragmentSetsAreWellFormed(body))
        {
            DiscardMalformedForSession(source, datagram.Length);
            return;
        }

        // The well-formedness proof above is what earns this datagram the right to keep slot's
        // receive-idle deadline alive (issue #351, PeerSlot.LastReceiveTimestamp): every record parsed
        // and any Fragment sets they claim are internally consistent. A datagram refused below for
        // declaring more Fragments than this Reactor's own receive window could ever hold does not
        // matter either way — slot is closed by that refusal, so nothing reads this timestamp again.
        slot.LastReceiveTimestamp = _options.Reactor.TimeProvider.GetTimestamp();

        // A declared fragmentCount larger than this Reactor's own receive window is refused with a
        // close, not a discard (issue #282, reworked under #264) — the record is individually legal and
        // its set internally consistent (both already proved above), but the set it claims could never
        // be held: the window is what a Fragment set actually occupies while it waits, and there is no
        // slot for every piece of a set that size to sit in at once, whatever fragment size a
        // conforming peer used to cut it. This is its own pass over body, ahead of the classification loop below, so that a
        // datagram carrying other, legal records ahead of the offending Fragment still closes the Peer
        // having classified none of them — no receive-window slot or Fragment-group bookkeeping is
        // created for any record of this datagram once the check fires. Only ReliableOrdered and
        // ReliableUnordered ever carry the fragment shape (ADR 0004) — a Fragment in an unreliable mode
        // already failed the read in the first pass above (issue #273), so the mode test here and
        // ClassifyRecord's matching one now restate a guarantee rather than establish it, and stay as
        // the local statement of which modes this arm is about.
        remainder = body;
        while (remainder.Length > 0)
        {
            PacketRecordCodec.TryRead(
                remainder, out PacketRecordMode oversizedMode, out _, out _,
                out bool oversizedIsFragment, out _, out ushort oversizedFragmentCount, out _, out int oversizedBytesConsumed);

            if (oversizedIsFragment
                && oversizedMode is PacketRecordMode.ReliableOrdered or PacketRecordMode.ReliableUnordered
                && oversizedFragmentCount > _maxLegalFragmentCount)
            {
                if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
                {
                    _diagnostics.Log(DiagnosticRecord.ForOversizedFragmentSet(
                        ToPeerId(slot.Handle), oversizedFragmentCount, _maxLegalFragmentCount));
                }

                ClosePeer(slot.Handle, DisconnectReason.ProtocolViolation);
                return;
            }

            remainder = remainder[oversizedBytesConsumed..];
        }

        // Fully parsed and not refused above: this is what "well-formed" means for leaving Interrupted
        // (issue #383). It sits after the oversized-set close so a datagram that closes the Peer never
        // resumes it first.
        ResumeInterruptedDialledPeerOnData(slot);

        // The proof above is what attributes this Datagram to slot's Peer: it names an established
        // session id (ProcessDataDatagram's own lookup) and every record in it parses. Only from this
        // point on is the whole Datagram's length, header included, counted against that Peer (issue
        // #231) — a malformed one, even if it named a real session, falls to
        // DiscardMalformedForSession's Reactor-scope figure instead, above, and never reaches here.
        slot.Counters.RecordBytesReceived((ulong)datagram.Length);

        TimeSpan effectiveRto = slot.RetransmitTimer.EffectiveTimeout(ObservedPumpInterval, slot.EffectiveLinger);
        slot.AckTracker.ApplyAck(PacketRecordMode.ReliableOrdered, reliableOrderedAckBase, reliableOrderedAckBits, effectiveRto);
        ReleaseRetentionForAck(slot, PacketRecordMode.ReliableOrdered);
        ApplyAckToStallSignature(slot);
        slot.AckTracker.ApplyAck(PacketRecordMode.ReliableUnordered, reliableUnorderedAckBase, reliableUnorderedAckBits, effectiveRto);
        ReleaseRetentionForAck(slot, PacketRecordMode.ReliableUnordered);
        ApplyAckToStallSignature(slot);
        ReconcileHorizonDeferrals(slot);

        remainder = body;
        while (remainder.Length > 0)
        {
            // Every record in this span already proved well-formed in the pass above, so this read
            // cannot fail — the return value is not re-checked. fragmentIndex and fragmentCount are
            // read here (issue #258): a held Fragment's group first sequence, and so its completeness,
            // is derived from them the moment it is held.
            PacketRecordCodec.TryRead(
                remainder, out PacketRecordMode mode, out uint sequence, out ushort length,
                out bool isFragment, out ushort fragmentIndex, out ushort fragmentCount, out bool isControl,
                out int bytesConsumed);

            // The payload is the tail of what this record consumed, so it is sliced from the record's
            // own span rather than re-derived from a header width this method would then have to know.
            ReadOnlySpan<byte> payload = remainder.Slice(bytesConsumed - length, length);
            ClassifyRecord(slot, mode, sequence, isFragment, isControl, fragmentIndex, fragmentCount, payload);
            remainder = remainder[bytesConsumed..];
        }
    }

    /// <summary>
    /// Checks every Fragment in <paramref name="body"/> as part of the set it claims to belong to, not
    /// only individually (issue #256; ADR 0009). Contiguity stands in for a message ID no wire byte
    /// carries, so two shapes of forgery break it: Fragments naming the same group — the pair of that
    /// group's mode and <see cref="FragmentGroup.FirstSequenceOf"/> — whose actually-present indices
    /// leave a gap, and two distinct claimed groups <em>in the same mode</em> whose <em>declared</em>
    /// ranges overlap — some one wire sequence that could be read as belonging to either, whether or not
    /// a Fragment has actually arrived there yet. Every record here already proved individually legal in
    /// the pass above (issue #253) — an index at or beyond its own count, a count of zero, or a count of
    /// one already discarded the datagram before this method is reached — so this is where a datagram
    /// whose individually-legal Fragments lie about which set each belongs to is refused instead.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>A group is identified by (mode, first sequence), not first sequence alone (issue #47).</b>
    /// <see cref="ReceiveWindowSpaces"/> keeps one independent <see cref="ReceiveWindow"/> per
    /// <see cref="PacketRecordMode"/> — its own doc states why: a sequence admitted under one mode says
    /// nothing about the same sequence under another. A <c>ReliableOrdered</c> group starting at sequence
    /// 100 and a <c>ReliableUnordered</c> group also starting at sequence 100 are unrelated groups in
    /// unrelated Spaces; folding them into one first-sequence-only namespace, as an earlier shape of this
    /// check did, sees two Fragments both claiming index 0 of what it wrongly believes is one group and
    /// discards a datagram no attacker forged. The same is true of the declared-range overlap sweep: two
    /// groups in different modes can never conflict, however their declared ranges happen to line up, so
    /// that sweep only ever compares two groups already known to share a mode.
    /// </para>
    /// <para>
    /// <b>Scope is this one datagram.</b> A group that arrives split across more than one datagram, or
    /// one a later datagram contradicts, needs state only reassembly owns (a future sub-slice); this
    /// method holds nothing across calls beyond its own preallocated scratch (below) and knows nothing
    /// beyond <paramref name="body"/>'s own bytes. A partial set — fewer Fragments than
    /// <see cref="PacketRecordCodec.TryRead"/>'s <c>fragmentCount</c> promises — is not a violation, and
    /// neither is a gap in the indices it does carry (issue #274): an honest sender's Fragments of one
    /// group may be spread across datagrams, and selective retransmission packs whichever of them
    /// expired, which is a subset chosen by what was acked rather than a run (see
    /// <see cref="TryCloseContiguousRun"/>'s own remarks). What each group's members must agree on is
    /// their declared count, and no index may appear twice.
    /// </para>
    /// <para>
    /// <b>Overlap is judged on each same-mode group's DECLARED range, not on what is merely observed.</b>
    /// Two Fragments in one mode — firstSeq 100 declaring count 5 (range 100..104), and firstSeq 103
    /// declaring count 2 (range 103..104) — can arrive with only sequences 100, 101 and 103 actually
    /// present, so their observed points (100-101 vs 103) never collide. Sequence 104 is still claimed by
    /// both groups: a later Fragment landing there is index 4 of the first group or index 1 of the second
    /// depending on which group is believed, and nothing observed so far can tell them apart. Judging the
    /// check on declared ranges is what catches this before either group is trusted.
    /// </para>
    /// <para>
    /// <b>Bounded cost, no per-datagram allocation (DoS follow-up to issue #256).</b> Re-deriving every
    /// Fragment's group by re-walking <paramref name="body"/> once per Fragment — the previous shape of
    /// this check — is O(n²) in the datagram's record count on a path a remote peer's datagram length
    /// alone controls: at the wire floor of 11 bytes per Fragment, a maximum-size 65,507-byte UDP
    /// datagram holds <see cref="MaxFragmentRecordsPerDatagram"/> (5,953) of them, and two O(n²) walks
    /// over that many records is tens of millions of <see cref="PacketRecordCodec.TryRead"/> calls for
    /// one datagram on the single-threaded Pump. Instead, one linear pass collects each Fragment's
    /// (mode, first sequence) key and index into <see cref="_fragmentGroupKey"/> and
    /// <see cref="_fragmentGroupIndexAndCount"/>, both sorted by that combined key
    /// (<see cref="Array.Sort(Array, Array, int, int)"/> over ulong/uint keys, no comparer allocated,
    /// mode occupying the key's high bits so one mode's entries never interleave with another's) so
    /// every Fragment of one (mode, first sequence) group becomes one contiguous
    /// run — O(n log n) rather than O(n) group lookups each costing O(n). Each run is checked for a
    /// disagreed count and a repeated index and, if it passes, contributes one entry to
    /// <see cref="_distinctGroupFirstSequenceAndCount"/> (<see cref="TryCloseContiguousRun"/>), which
    /// packs that group's mode alongside its first sequence and declared count. Because the runs above
    /// were closed in (mode, first sequence) order, the distinct groups already sit in contiguous
    /// per-mode slices; the declared-range overlap sweep (<see cref="NoTwoDeclaredRangesOverlap"/>) walks
    /// those slices one mode at a time and only ever sorts and sweeps within one slice
    /// (<see cref="_distinctGroupSortKey"/>) — an interval-scheduling sweep, O(g log g) overall in the
    /// number of distinct groups (the per-mode sweeps' costs sum to at most that bound) rather than the
    /// O(g²) an all-pairs comparison would cost, and a comparison between two different modes' groups
    /// never happens at all. Every array here is preallocated once at construction from
    /// <see cref="MaxUdpDatagramSize"/> (see the scratch fields' own remarks) and reused datagram after
    /// datagram: nothing is allocated, resized or cleared on this path, only bounded by a count freshly
    /// computed for the datagram being parsed.
    /// </para>
    /// <para>
    /// <b>Each per-mode sweep's sort key is relative to an anchor chosen fresh for that mode's slice,
    /// using the same unsigned wrap-aware subtraction <see cref="SequenceNumber.IsNewerThan"/> is built
    /// on.</b> This assumes, as every "newer than" decision on this data path already does, that the
    /// groups actually present in one datagram never span more than half the sequence space (2^31) — true
    /// of anything an honest sender produces, and the same boundary this whole design accepts elsewhere
    /// rather than treating as a hazard to defend against (see <see cref="SequenceNumber"/>'s own
    /// remarks). Each mode's own anchor keeps this assumption scoped to that mode's own groups, exactly as
    /// their sequence Spaces already are.
    /// </para>
    /// </remarks>
    private bool FragmentSetsAreWellFormed(ReadOnlySpan<byte> body)
    {
        int fragmentCount = 0;
        ReadOnlySpan<byte> remainder = body;
        while (remainder.Length > 0)
        {
            PacketRecordCodec.TryRead(
                remainder, out PacketRecordMode mode, out uint sequence, out _, out bool isFragment,
                out ushort index, out ushort count, out _, out int bytesConsumed);

            if (isFragment)
            {
                // Cannot happen: MaxFragmentRecordsPerDatagram is this Reactor's own scratch bound,
                // derived from the largest datagram it ever reads (MaxUdpDatagramSize) divided by the
                // smallest a Fragment record can be, so no datagram this method is ever called with can
                // produce more Fragments than the scratch below has room for.
                Debug.Assert(fragmentCount < MaxFragmentRecordsPerDatagram, "More Fragments than the scratch was sized for.");

                _fragmentGroupKey[fragmentCount] = PackModeAndFirstSequence(mode, FragmentGroup.FirstSequenceOf(sequence, index));
                _fragmentGroupIndexAndCount[fragmentCount] = PackIndexAndCount(index, count);
                fragmentCount++;
            }

            remainder = remainder[bytesConsumed..];
        }

        if (fragmentCount == 0)
        {
            return true;
        }

        Array.Sort(_fragmentGroupKey, _fragmentGroupIndexAndCount, 0, fragmentCount);

        int distinctGroupCount = 0;
        int runStart = 0;
        for (int i = 1; i <= fragmentCount; i++)
        {
            if (i == fragmentCount || _fragmentGroupKey[i] != _fragmentGroupKey[runStart])
            {
                if (!TryCloseContiguousRun(runStart, i, ref distinctGroupCount))
                {
                    return false;
                }

                runStart = i;
            }
        }

        return NoTwoDeclaredRangesOverlap(distinctGroupCount);
    }

    /// <summary>
    /// Packs a Fragment's group identity for <see cref="_fragmentGroupKey"/>: <paramref name="mode"/> in
    /// the key's high bits, <paramref name="firstSequence"/> in its low 32 bits. Mode dominates the sort
    /// order this key drives, so every Fragment of one mode sorts before every Fragment of the next —
    /// two groups in different modes can never end up in the same contiguous run.
    /// </summary>
    private static ulong PackModeAndFirstSequence(PacketRecordMode mode, uint firstSequence) =>
        ((ulong)(byte)mode << 32) | firstSequence;

    private static byte UnpackMode(ulong modeAndFirstSequence) => (byte)(modeAndFirstSequence >> 32);

    private static uint UnpackFirstSequenceFromGroupKey(ulong modeAndFirstSequence) => (uint)modeAndFirstSequence;

    /// <summary>Packs a Fragment's index and count into one <c>uint</c> for <see cref="_fragmentGroupIndexAndCount"/>.</summary>
    private static uint PackIndexAndCount(ushort index, ushort count) => ((uint)index << 16) | count;

    private static ushort UnpackIndex(uint indexAndCount) => (ushort)(indexAndCount >> 16);

    private static ushort UnpackCount(uint indexAndCount) => (ushort)indexAndCount;

    /// <summary>
    /// Closes the run <c>[runStart, runEnd)</c> of <see cref="_fragmentGroupKey"/> and
    /// <see cref="_fragmentGroupIndexAndCount"/> — every Fragment the datagram's walk found that shares
    /// one group's mode and first sequence, now contiguous in the sorted arrays (the run is what is
    /// contiguous here, not the indices it carries) — checking that every member agrees on its declared
    /// count and that no index appears twice. A group's Fragments disagreeing on the count they claim
    /// makes the set's shape ambiguous, and one index claimed by two Fragments makes its content
    /// ambiguous: neither describes a set an honest sender, which uses one count per Message and cuts
    /// each index once, could ever produce. On success, appends one entry to
    /// <see cref="_distinctGroupFirstSequenceAndCount"/> — mode, first sequence and declared count all
    /// packed together — for the per-mode overlap sweep that follows.
    /// </summary>
    /// <remarks>
    /// <b>The indices present need only be a subset of <c>[0, declaredCount)</c> with no duplicates, not
    /// a gap-free run (issue #274).</b> Requiring a run was right while the only way to see a gap was a
    /// forgery, but selective retransmission produces one honestly: a Message cut into Fragments 0, 1
    /// and 2 whose index 1 is acked while 0 and 2 both expire packs those two into one datagram, with no
    /// index 1 anywhere in it. What issue #256 rejects is ambiguity about which wire sequence belongs to
    /// which group, and that lives in the <em>claimed</em> set — the declared count each Fragment carries
    /// and the range it implies, checked here and in <see cref="NoTwoDeclaredRangesOverlap"/> — not in
    /// which pieces of a set happen to share one datagram. The subset's upper bound is already
    /// established before this pass: <see cref="PacketRecordCodec.TryRead"/> refuses any Fragment whose
    /// index is at or beyond its own count (issue #253) and <see cref="ParseDataDatagram"/> proves every
    /// record legal there first, so once the members agree on one declared count every index is inside
    /// it.
    /// </remarks>
    private bool TryCloseContiguousRun(int runStart, int runEnd, ref int distinctGroupCount)
    {
        ushort declaredCount = UnpackCount(_fragmentGroupIndexAndCount[runStart]);

        for (int i = runStart; i < runEnd; i++)
        {
            if (UnpackCount(_fragmentGroupIndexAndCount[i]) != declaredCount)
            {
                return false;
            }

            // Cannot happen: an index at or beyond its own count already failed the read (issue #253),
            // and every member of this run has just been proved to carry declaredCount as that count.
            Debug.Assert(
                UnpackIndex(_fragmentGroupIndexAndCount[i]) < declaredCount,
                "A Fragment whose index is outside its declared count reached the set check.");
        }

        // Sorting the run by its packed index-and-count puts any repeated index beside its twin, so one
        // further linear pass finds a duplicate — O(k log k) for a k-member group, nothing allocated, and
        // the sort cannot disturb this group's pairing with _fragmentGroupKey: every key in the run is
        // equal by construction, so reordering only the values leaves each pair intact.
        Array.Sort(_fragmentGroupIndexAndCount, runStart, runEnd - runStart);
        for (int i = runStart + 1; i < runEnd; i++)
        {
            if (UnpackIndex(_fragmentGroupIndexAndCount[i]) == UnpackIndex(_fragmentGroupIndexAndCount[i - 1]))
            {
                return false;
            }
        }

        byte mode = UnpackMode(_fragmentGroupKey[runStart]);
        uint firstSequence = UnpackFirstSequenceFromGroupKey(_fragmentGroupKey[runStart]);
        _distinctGroupFirstSequenceAndCount[distinctGroupCount] =
            PackGroupIdentity(mode, firstSequence, declaredCount);
        distinctGroupCount++;
        return true;
    }

    /// <summary>
    /// Packs one distinct group's identity for <see cref="_distinctGroupFirstSequenceAndCount"/>:
    /// <paramref name="mode"/> in bits 48-55, <paramref name="firstSequence"/> in bits 16-47, and
    /// <paramref name="declaredCount"/> in bits 0-15.
    /// </summary>
    private static ulong PackGroupIdentity(byte mode, uint firstSequence, ushort declaredCount) =>
        ((ulong)mode << 48) | ((ulong)firstSequence << 16) | declaredCount;

    private static byte UnpackGroupMode(ulong groupIdentity) => (byte)(groupIdentity >> 48);

    // Truncating to uint after the >>16 shift drops the mode bits above bit 31 along with it, leaving
    // exactly the first-sequence bits PackGroupIdentity placed at 16-47.
    private static uint UnpackFirstSequence(ulong groupIdentity) => (uint)(groupIdentity >> 16);

    private static ushort UnpackDeclaredCount(ulong groupIdentity) => (ushort)groupIdentity;

    /// <summary>
    /// Sweeps <see cref="_distinctGroupFirstSequenceAndCount"/>'s first <paramref name="groupCount"/>
    /// entries for a declared-range overlap — the second forgery #256 rejects, judged on each group's
    /// full <c>[firstSequence, firstSequence + declaredCount)</c> range rather than only the sequences a
    /// Fragment has actually claimed so far (see this class's <see cref="FragmentSetsAreWellFormed"/>
    /// remarks for why) — but only ever between two groups sharing one mode (issue #47): two groups in
    /// different modes live in different <see cref="ReceiveWindowSpaces"/> Spaces and can never conflict,
    /// whatever their declared ranges happen to be. Because <see cref="TryCloseContiguousRun"/> appends
    /// groups in the (mode, first sequence) order <see cref="FragmentSetsAreWellFormed"/> sorted
    /// Fragments into, this array already sits in contiguous per-mode slices; this method finds each
    /// slice's boundary in one linear pass and delegates the actual sweep, unchanged in shape from before
    /// this fix, to <see cref="NoTwoDeclaredRangesOverlapWithinOneMode"/> for each slice in turn.
    /// </summary>
    private bool NoTwoDeclaredRangesOverlap(int groupCount)
    {
        if (groupCount == 0)
        {
            return true;
        }

        int sliceStart = 0;
        for (int i = 1; i <= groupCount; i++)
        {
            if (i == groupCount || UnpackGroupMode(_distinctGroupFirstSequenceAndCount[i]) != UnpackGroupMode(_distinctGroupFirstSequenceAndCount[sliceStart]))
            {
                if (!NoTwoDeclaredRangesOverlapWithinOneMode(sliceStart, i))
                {
                    return false;
                }

                sliceStart = i;
            }
        }

        return true;
    }

    /// <summary>
    /// The declared-range overlap sweep proper, run over one mode's own contiguous slice
    /// <c>[sliceStart, sliceEnd)</c> of <see cref="_distinctGroupFirstSequenceAndCount"/> — every group in
    /// that slice is already known to share one mode, so comparing any two of them is meaningful. Sorts
    /// the slice by declared range start, relative to a wrap-aware anchor, then sweeps once tracking the
    /// furthest declared end seen so far within the slice — the standard O(k log k) interval-overlap check
    /// for a slice of <c>k</c> groups, correct for nested as well as merely adjacent ranges, in place of an
    /// O(k²) all-pairs comparison.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The anchor is derived, not assumed.</b> Ordering <c>u32</c> sequences that may straddle the wrap
    /// needs a point to measure from, and taking the slice's numerically smallest value is wrong: a hostile
    /// peer adds one decoy group to drag that point away, which splits a genuinely overlapping pair to
    /// opposite ends of the sorted slice, and the linear sweep then never compares them. The anchor is
    /// therefore the slice's <em>wrap-aware</em> minimum — the value immediately after the largest circular
    /// gap between consecutive first sequences, the wrap gap from the largest value back to the smallest
    /// included. Every offset measured from it is then a genuine distance forward around the circle, so the
    /// sort key is a consistent total order and the sweep compares the pairs it must.
    /// </para>
    /// <para>
    /// <b>A slice too wide to order is refused, not guessed at.</b> No wrap-aware order exists over
    /// sequences spanning more than half the space (2^31): past that, "forward around the circle" stops
    /// being well defined, which is the same boundary <see cref="SequenceNumber"/> names. An honest sender
    /// cannot produce such a datagram — a Peer's receive window is smaller than 2^31 by many orders of
    /// magnitude — so a slice whose circular span exceeds it is discarded exactly as any other violation
    /// here is. The assumption is enforced rather than relied upon, because on this path the span is a
    /// value the attacker chooses.
    /// </para>
    /// <para>
    /// Offsets are carried as <see cref="long"/> rather than <see cref="int"/>: a declared range's end is
    /// its start plus up to <see cref="ushort.MaxValue"/>, which a slice already spanning close to 2^31
    /// would push past <see cref="int.MaxValue"/> and wrap to a negative, defeating the very comparison the
    /// sweep exists to make.
    /// </para>
    /// </remarks>
    private bool NoTwoDeclaredRangesOverlapWithinOneMode(int sliceStart, int sliceEnd)
    {
        int sliceCount = sliceEnd - sliceStart;
        if (sliceCount <= 1)
        {
            return true;
        }

        // The slice arrived in ascending numeric first-sequence order, and its groups are distinct, so the
        // largest circular gap is found by one pass over adjacent pairs plus the wrap gap that closes the
        // circle from the last value back to the first.
        uint smallest = UnpackFirstSequence(_distinctGroupFirstSequenceAndCount[sliceStart]);
        uint largest = UnpackFirstSequence(_distinctGroupFirstSequenceAndCount[sliceEnd - 1]);

        ulong largestGap = OneSequenceSpace - largest + smallest;
        uint anchor = smallest;

        for (int i = sliceStart + 1; i < sliceEnd; i++)
        {
            uint previous = UnpackFirstSequence(_distinctGroupFirstSequenceAndCount[i - 1]);
            uint current = UnpackFirstSequence(_distinctGroupFirstSequenceAndCount[i]);
            ulong gap = current - previous;
            if (gap > largestGap)
            {
                largestGap = gap;
                anchor = current;
            }
        }

        // The circular span is whatever the largest gap does not cover. At half the space or wider, no
        // ordering of these groups is meaningful, so the datagram is refused rather than swept in an order
        // the peer chose. The bound is "at or wider", not merely "wider": a span of exactly 2^31 puts the
        // furthest group at an offset of 2^31 from the anchor, which is one past what the sort key below
        // can hold, and it would wrap to a negative and sort to the wrong end — the very reordering this
        // check exists to prevent.
        if (OneSequenceSpace - largestGap >= OneSequenceSpace / 2)
        {
            return false;
        }

        for (int i = sliceStart; i < sliceEnd; i++)
        {
            uint firstSequence = UnpackFirstSequence(_distinctGroupFirstSequenceAndCount[i]);
            _distinctGroupSortKey[i] = unchecked((int)(firstSequence - anchor));
        }

        Array.Sort(_distinctGroupSortKey, _distinctGroupFirstSequenceAndCount, sliceStart, sliceCount);

        long maxDeclaredEndSoFar = long.MinValue;
        for (int i = sliceStart; i < sliceEnd; i++)
        {
            uint firstSequence = UnpackFirstSequence(_distinctGroupFirstSequenceAndCount[i]);
            ushort declaredCount = UnpackDeclaredCount(_distinctGroupFirstSequenceAndCount[i]);

            long start = unchecked(firstSequence - anchor);
            long end = start + declaredCount - 1;

            if (i > sliceStart && start <= maxDeclaredEndSoFar)
            {
                return false;
            }

            maxDeclaredEndSoFar = Math.Max(maxDeclaredEndSoFar, end);
        }

        return true;
    }

    /// <summary>
    /// Folds however much <paramref name="slot"/>'s <see cref="PeerSlot.AckTracker"/> has deferred,
    /// since this method last read it, into <see cref="PeerSlot.Counters"/> (subtask 6.3.6, issue #188).
    /// The tracker exposes <see cref="OutstandingReliableSequences.HorizonDeferrals"/> as a monotonic
    /// total across both reliable Spaces, while <see cref="Core.PeerCounterSet.RecordHorizonDeferral"/>
    /// increments by one per call, so the two shapes are reconciled here rather than at either end: the
    /// difference between the total and <see cref="PeerSlot.ObservedHorizonDeferrals"/> is folded in one
    /// call at a time, and nothing is folded twice because the observed figure is advanced to match.
    /// Nothing on this ticket's live paths ever calls <see cref="OutstandingReliableSequences.RecordSent"/>,
    /// so the difference is always zero today; this is the reconciliation 6.4/6.5's outbound send will
    /// exercise once it does.
    /// </summary>
    private static void ReconcileHorizonDeferrals(PeerSlot slot)
    {
        ulong total = slot.AckTracker.HorizonDeferrals;
        ulong delta = total - slot.ObservedHorizonDeferrals;
        for (ulong i = 0; i < delta; i++)
        {
            slot.Counters.RecordHorizonDeferral();
        }

        slot.ObservedHorizonDeferrals = total;
    }

    /// <summary>
    /// Runs once per <see cref="Pump"/>, over every Established Peer's own
    /// <see cref="PeerSlot.AckTracker"/> (subtask 6.4.8, issue #198): sweeps each reliable Space for
    /// sequences whose retransmit deadline has passed, raises that Peer's
    /// <see cref="Core.PeerCounterSet.RecordPacketsRetransmitted"/> by however many
    /// <see cref="OutstandingReliableSequences.CollectExpired"/> reports, and — if either Space reported
    /// anything — backs off that Peer's <see cref="PeerSlot.RetransmitTimer"/> exactly once and reports
    /// it (subtask 6.4.9, issue #199). Nothing here resends anything — holding the record bytes is 6.5's
    /// retention and packing them into an outbound datagram is 6.7's coalescing; this ticket stops at
    /// the sequences having been named.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>At most one backoff per Peer per Pump.</b> A single <see cref="RetransmissionTimer"/> is
    /// shared by both of a Peer's reliable Spaces (<see cref="NewAckTracker"/>). The backoff decision
    /// used to live inside <see cref="OutstandingReliableSequences.CollectExpired"/> itself, called once
    /// per Space — so a Pump where both Spaces expired something backed off the shared timer twice,
    /// doubling the RTO twice and raising <see cref="RetransmissionTimer.ConsecutiveBackoffs"/> by two
    /// for what subtask 6.4.4 (issue #194) defines as one loss event, not two. This method now sweeps
    /// both Spaces first, decides the backoff once here at the Peer level from their combined count, and
    /// only then re-arms each Space's findings — <see cref="OutstandingReliableSequences.ReArmExpired"/>
    /// — at the single resulting <see cref="RetransmissionTimer.EffectiveTimeout"/>, matching how a
    /// horizon slide arms everything it newly covers from one shared moment and RTO.
    /// </para>
    /// <para>
    /// A single scan over <see cref="_peers"/> (<see cref="ShareHolderTable{T}.OccupantAt"/>), the same
    /// shape <see cref="StreamSessionReactor.EnforceIdleTimers"/> uses for its own per-Pump-per-Peer
    /// sweep: it costs at most <see cref="ShareHolderTable{T}.Capacity"/> array reads against
    /// already-allocated state, so it allocates nothing itself, and
    /// <see cref="OutstandingReliableSequences.CollectExpired"/> and
    /// <see cref="OutstandingReliableSequences.ReArmExpired"/> allocate nothing either — including on
    /// the steady path where nothing has expired. A Peer still mid-handshake has recorded nothing on
    /// either reliable Space yet (<see cref="OutstandingReliableSequences.RecordSent"/> is only ever
    /// called for a sent record), so restricting the scan to <see cref="HandshakeState.Established"/>
    /// Peers changes nothing observable; it only skips a scan certain to find nothing.
    /// </para>
    /// </remarks>
    private void ReportExpiredRetransmissions()
    {
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is not PeerSlot slot || slot.State != HandshakeState.Established)
            {
                continue;
            }

            // Each Space keeps its own scratch buffer, so holding both spans across the second call is
            // safe — they are not the same array.
            ReadOnlySpan<uint> reliableOrderedExpired = slot.AckTracker.CollectExpired(PacketRecordMode.ReliableOrdered);
            ReadOnlySpan<uint> reliableUnorderedExpired = slot.AckTracker.CollectExpired(PacketRecordMode.ReliableUnordered);
            int expiredCount = reliableOrderedExpired.Length + reliableUnorderedExpired.Length;

            if (expiredCount == 0)
            {
                continue;
            }

            slot.Counters.RecordPacketsRetransmitted((ulong)expiredCount);

            // Queued, not sent: the record joins whatever this Peer's PendingSends already holds and
            // rides the same Pump's flush, alongside records Sent this Frame. That is what makes
            // retransmission byte-identical at packet-record granularity and not at Datagram
            // granularity — the record is repacked into whatever container goes out next, and no
            // attempt is made to reproduce the Datagram it originally travelled in.
            QueueForRetransmission(slot, PacketRecordMode.ReliableOrdered, reliableOrderedExpired);
            QueueForRetransmission(slot, PacketRecordMode.ReliableUnordered, reliableUnorderedExpired);

            slot.RetransmitTimer.BackOff();

            // One elapsed retransmission round for this Peer (subtask 7.5.3.1, issue #294): once per
            // Peer per Pump that expired something, whatever expired, alongside the shared backoff
            // above — not once per expired Fragment. StallSignature itself decides whether this round
            // counts toward anything (it is a no-op unless a Fragment window is open and unacked).
            slot.StallSignature.RecordRetransmissionRound();

            // Acting on the stall signature (issue #302): the round just recorded above is the only
            // place RoundsSinceFirstUnackedFragment can advance, so it is the earliest and only point a
            // false IsFragmentSizeStall can newly turn true — no separate per-Pump sweep is needed.
            // Closing here, before the re-arm below, means the expired sequences QueueForRetransmission
            // just queued onto slot.PendingSends never reach FlushOutboundDatagrams: ClosePeer removes
            // the slot from every table that keeps it reachable, so nothing about this Peer survives to
            // be flushed, backed off, or traced. Removing the occupant at this index leaves the Slot
            // null (ShareHolderTable.Remove) rather than shifting later Slots, so continuing the loop at
            // index + 1 is exactly as safe as an ordinary skip.
            if (slot.StallSignature.IsFragmentSizeStall)
            {
                ClosePeer(slot.Handle, DisconnectReason.FragmentSizeUnviable);
                continue;
            }

            TimeSpan effectiveRto = slot.RetransmitTimer.EffectiveTimeout(ObservedPumpInterval, slot.EffectiveLinger);
            slot.AckTracker.ReArmExpired(PacketRecordMode.ReliableOrdered, effectiveRto);
            slot.AckTracker.ReArmExpired(PacketRecordMode.ReliableUnordered, effectiveRto);

            if (_diagnostics.IsEnabled(DiagnosticLevel.Trace))
            {
                _diagnostics.Log(DiagnosticRecord.ForRetransmissionTimerBackedOff(
                    ToPeerId(slot.Handle),
                    newRtoMilliseconds: (long)effectiveRto.TotalMilliseconds,
                    consecutiveBackoffs: slot.RetransmitTimer.ConsecutiveBackoffs));
            }
        }
    }

    /// <summary>
    /// Whether <paramref name="mode"/> is one of the two Spaces that retain, acknowledge and retransmit
    /// — the single place that question is asked, so the reliable half of the four modes is named once.
    /// </summary>
    private static bool IsReliable(PacketRecordMode mode) =>
        mode is PacketRecordMode.ReliableOrdered or PacketRecordMode.ReliableUnordered;

    /// <summary>
    /// Folds the <see cref="OutstandingReliableSequences.LastAckRelease"/> left behind by the
    /// <see cref="OutstandingReliableSequences.ApplyAck"/> call that just ran into
    /// <paramref name="slot"/>'s <see cref="PeerSlot.StallSignature"/> (subtask 7.5.3.1, issue #294):
    /// called once per <see cref="OutstandingReliableSequences.ApplyAck"/> call, immediately after it, so
    /// each Space's own release is read before the other Space's call overwrites
    /// <see cref="OutstandingReliableSequences.LastAckRelease"/>.
    /// </summary>
    private static void ApplyAckToStallSignature(PeerSlot slot)
    {
        (bool releasedFragment, bool releasedUnfragmentedRecord) = slot.AckTracker.LastAckRelease;
        if (releasedFragment)
        {
            slot.StallSignature.RecordFragmentAcked();
        }

        if (releasedUnfragmentedRecord)
        {
            slot.StallSignature.RecordUnfragmentedRecordAcked();
        }
    }

    /// <summary>
    /// Frees the retention slot of every sequence the <see cref="OutstandingReliableSequences.ApplyAck"/>
    /// call that just ran actually released on <paramref name="mode"/>'s reliable Space (issue #345):
    /// called once per <see cref="OutstandingReliableSequences.ApplyAck"/> call, immediately after it, so
    /// each Space's own release is read before the other Space's call overwrites
    /// <see cref="OutstandingReliableSequences.LastAckReleasedSequences"/> — the same idiom
    /// <see cref="ApplyAckToStallSignature"/> already follows for <see cref="OutstandingReliableSequences.LastAckRelease"/>.
    /// Without this, nothing ever freed a slot <see cref="_retention"/>'s <see cref="RetentionPool.TryAcquire"/>
    /// committed: a quiet Session's idle-triggered Pings and a Peer's reliable Sends would both hold
    /// their slots forever and every further reliable Send would eventually see
    /// <see cref="RetentionSlotOutcome.Exhausted"/>.
    /// </summary>
    /// <remarks>
    /// Allocates nothing: <see cref="OutstandingReliableSequences.LastAckReleasedSequences"/> is a reused
    /// scratch span and <c>foreach</c> over a <see cref="ReadOnlySpan{T}"/> does not allocate an
    /// enumerator, matching this whole ack path's no-allocation discipline.
    /// </remarks>
    private void ReleaseRetentionForAck(PeerSlot slot, PacketRecordMode mode)
    {
        int spaceIndex = ReliableSpaceIndex(mode);
        foreach (uint sequence in slot.AckTracker.LastAckReleasedSequences(mode))
        {
            _retention.Release(slot.Handle.Index, spaceIndex, sequence);

            if (slot.LocalClose == LocalCloseState.GoodbyeUnacked
                && mode == PacketRecordMode.ReliableOrdered
                && sequence == slot.GoodbyeSequence)
            {
                slot.LocalClose = LocalCloseState.GoodbyeAcked;
            }
        }
    }

    /// <summary>
    /// Closes every Closing Peer whose Goodbye has been acknowledged (<see cref="LocalCloseState.GoodbyeAcked"/>,
    /// issue #398) with <see cref="DisconnectReason.LocalClose"/>, once every <c>ReliableOrdered</c> record
    /// up to the Goodbye is acknowledged too: a selective ack of the Goodbye behind an unacknowledged earlier
    /// Message would otherwise free the Slot and stop that Message's retransmission, leaving the remote's
    /// Goodbye waiting behind a hole that never fills. Run right after the read pass, so an ack
    /// that arrived this Pump closes the Peer this Pump, and before <see cref="EnforceLingerExpiry"/>, so an
    /// ack arriving in the Pump Linger would also have run out wins and no second close follows.
    /// </summary>
    private void CloseAckedGoodbyePeers()
    {
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is PeerSlot { LocalClose: LocalCloseState.GoodbyeAcked } slot
                && !slot.AckTracker.HasOutstandingAtOrBelow(PacketRecordMode.ReliableOrdered, slot.GoodbyeSequence))
            {
                ClosePeer(slot.Handle, DisconnectReason.LocalClose);
            }
        }
    }

    /// <summary>
    /// Queues every sequence <see cref="ReportExpiredRetransmissions"/> found expired on one reliable
    /// Space back onto <paramref name="slot"/>'s <see cref="PeerSlot.PendingSends"/>, so the same
    /// Pump's <see cref="FlushOutboundDatagrams"/> packs it (subtask 6.6.8, issue #218). No payload
    /// copy is made or needed: a reliable record's bytes, and its record shape — whole or one Fragment
    /// of a cut Message, index and count included — are in <see cref="_retention"/> from the moment
    /// <see cref="Send"/> committed them, and <see cref="TryResolvePendingPayload"/> reads both back at
    /// flush time (issue #250). That is what makes the resent bytes identical to the original: this
    /// queue entry carries nothing of its own for <see cref="FlushPeer"/> to rebuild the shape from —
    /// <see cref="_retention"/> is the only place it is ever decided.
    /// </summary>
    /// <remarks>
    /// A sequence whose retention slot has already been released resolves to nothing at flush time and
    /// is skipped there rather than guarded against here. That is the right place for it: releasing a
    /// slot is what an arriving acknowledgement does, so a sequence acked between this queueing and
    /// the flush is one that no longer needs resending, and dropping it silently is the outcome
    /// wanted.
    /// </remarks>
    private static void QueueForRetransmission(PeerSlot slot, PacketRecordMode mode, ReadOnlySpan<uint> sequences)
    {
        foreach (uint sequence in sequences)
        {
            slot.PendingSends.Add(new PendingOutboundRecord { Mode = mode, Sequence = sequence, IsRetransmission = true });
        }
    }

    /// <summary>
    /// Puts every Established Peer's Sent-since-last-flush records on the wire, once per
    /// <see cref="Pump"/> (issue #216): a Peer with nothing queued in
    /// <see cref="PeerSlot.PendingSends"/> is skipped entirely, so a Pump following no <see cref="Send"/>
    /// puts nothing on the wire for that Peer. A Peer still mid-handshake has nothing to flush to
    /// either — <see cref="Send"/> can queue against it, but nothing here reaches it until it reaches
    /// <see cref="HandshakeState.Established"/>, the same restriction <see cref="ReportExpiredRetransmissions"/>
    /// already applies to its own per-Peer sweep.
    /// </summary>
    /// <remarks>
    /// A single scan over <see cref="_peers"/> (<see cref="ShareHolderTable{T}.OccupantAt"/>), the same
    /// shape <see cref="ReportExpiredRetransmissions"/> uses: at most <see cref="ShareHolderTable{T}.Capacity"/>
    /// array reads, and a Peer with an empty queue costs nothing more than that one read.
    /// <see cref="FlushPeer"/> itself trims <see cref="PeerSlot.PendingSends"/> down to whatever it
    /// could not pack this Pump (issue #230), so there is nothing left for this method to clear — a Peer
    /// whose queue drained completely is left with an empty list by that trim, which is exactly what
    /// makes the next Pump's <c>Count == 0</c> check above skip it and send no header-only Datagram.
    /// </remarks>
    private void FlushOutboundDatagrams()
    {
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is not PeerSlot slot || slot.State != HandshakeState.Established)
            {
                continue;
            }

            // A Peer whose Goodbye close is due is flushed even with nothing queued: its Slot is freed
            // right after, so this datagram is the last that could carry the ack of the Goodbye.
            // The same holds for a Closing Peer that just acknowledged the remote's Goodbye (AckDue): that
            // datagram is what tells the remote its Goodbye arrived.
            bool finalAckDue = slot.RemoteClose is RemoteCloseState.CloseDue or RemoteCloseState.AckDue;
            if (slot.PendingSends.Count == 0 && !finalAckDue)
            {
                continue;
            }

            FlushPeer(slot);

            if (slot.RemoteClose == RemoteCloseState.AckDue)
            {
                slot.RemoteClose = RemoteCloseState.Acked;
            }
        }
    }

    /// <summary>
    /// Whether <paramref name="slot"/> has gone silent past this end's receive-idle deadline (issue
    /// #352): elapsed time since <see cref="PeerSlot.LastReceiveTimestamp"/> at or past
    /// <see cref="PeerSlot.RemoteSendIdleInterval"/> times <see cref="DatagramSessionOptions.ResolvedMissedHeartbeats"/>.
    /// Mirrors <see cref="StreamSessionReactor.HasExceededReceiveIdleDeadline"/>.
    /// </summary>
    /// <remarks>
    /// This profile has an adaptive RTO already, unlike the stream profile, and the deadline is
    /// deliberately not derived from it (issue #359, ADR 0012). The RTO answers "when do I resend the
    /// probe", a fact about the current path that must keep adapting. The count answers "how many
    /// missed intervals before I give up", the consumer's own tolerance, which must not adapt with it.
    /// Deriving the deadline from the RTO would stretch that tolerance exactly when the path degrades,
    /// since backing off the RTO is what the estimator does when the path is bad, so the consumer
    /// would wait longest precisely when it most wanted to be told. It would also tie a consumer-facing
    /// promise to an estimator that Karn's algorithm is already discarding samples from. The Ping
    /// still inherits the adaptive RTO for its own retransmission (#343), so the path-adaptivity this
    /// profile has stays there instead of leaking into liveness. Keeping the deadline a plain count
    /// also means a consumer moving between the datagram and stream session profiles does not have to
    /// relearn how liveness works.
    ///
    /// Validating this is structural rather than arithmetic: <see cref="DatagramSessionOptions"/>
    /// rejects a <see cref="DatagramSessionOptions.MissedHeartbeats"/> count below 2 at construction,
    /// and a count of at least 2 already guarantees room for one probe and one retransmission, because
    /// the RTO floor is the observed Pump interval (<see cref="ObservedPumpInterval"/>) and the
    /// send-idle interval necessarily exceeds one Pump. No round-trip term needs to appear in the
    /// deadline itself. In consumer terms: a peer announcing a 10-second send-idle interval, at the
    /// default count of 3, is declared lost after 30 seconds of silence from it.
    ///
    /// See the tolerance test that degrades the path and confirms the deadline does not stretch with
    /// it, referred to in this codebase as <c>DatagramSessionLivenessToleranceTests</c>.
    /// </remarks>
    private bool HasExceededReceiveIdleDeadline(PeerSlot slot) =>
        // Divides instead of multiplying: a legal announced interval times a large MissedHeartbeats can
        // exceed what a TimeSpan holds, and that overflow would throw out of every Pump (issue #422).
        // Whole-tick division keeps the comparison exact, because the interval is a whole tick count.
        _options.Reactor.TimeProvider.GetElapsedTime(slot.LastReceiveTimestamp).Ticks / _options.ResolvedMissedHeartbeats
            >= slot.RemoteSendIdleInterval.Ticks;

    /// <summary>
    /// Moves every Established Peer that has exceeded <see cref="HasExceededReceiveIdleDeadline"/> to
    /// <see cref="PeerSlot.IsInterrupted"/> and queues its <see cref="SessionEventKind.PeerInterrupted"/>
    /// (issue #352), run once per <see cref="Pump"/> ahead of
    /// <see cref="EnforceSendIdleTimer"/> — the two timers are independent: this one reads only receive
    /// state, that one only send state, and neither call arms or disarms the other. No
    /// <see cref="DiagnosticGateway"/> record is logged for the transition, since it is already reported
    /// as an Event (ADR 0013). Also stamps <see cref="PeerSlot.InterruptedTimestamp"/> on the same Pump
    /// (issue #366) — the moment this Peer's effective Linger begins counting down (#55). Skips a Peer
    /// already <see cref="PeerSlot.IsInterrupted"/> (issue #353), so a deadline that stays exceeded after
    /// the transition never raises a second Event, and never re-stamps the timestamp, for it.
    /// </summary>
    private void EnforceReceiveIdleTimer()
    {
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is not PeerSlot slot || slot.State != HandshakeState.Established)
            {
                continue;
            }

            // A Closing Peer never goes Interrupted (issue #406): this end has already said Goodbye, so a
            // silent remote is not a path to win back with a Resume. Only the Goodbye's ack or Linger
            // measured from Close (EnforceLingerExpiry) ends it, and no PeerInterrupted is raised.
            if (slot.IsInterrupted || slot.IsClosing)
            {
                continue;
            }

            if (!HasExceededReceiveIdleDeadline(slot))
            {
                continue;
            }

            slot.IsInterrupted = true;
            slot.InterruptedTimestamp = _options.Reactor.TimeProvider.GetTimestamp();
            slot.LastResumeTimestamp = null;
            _pendingEvents.Add(new PendingEvent
            {
                Kind = SessionEventKind.PeerInterrupted,
                Peer = ToPeerId(slot.Handle),
            });
        }
    }

    /// <summary>
    /// Whether an <see cref="PeerSlot.IsInterrupted"/> Peer's effective Linger
    /// (<see cref="PeerSlot.EffectiveLinger"/>, negotiated at handshake and already reported via
    /// <c>LingerNegotiated</c>) has run out since <see cref="PeerSlot.InterruptedTimestamp"/> (issue
    /// #367, parent #55) — the datagram twin of <see cref="StreamSessionReactor.HasExceededLinger"/>.
    /// Linger is what closes an Interrupted Peer, not the transport loss that produced it — the loss
    /// already happened, potentially long before this check ever runs (<see cref="EnforceReceiveIdleTimer"/>
    /// stamped <see cref="PeerSlot.InterruptedTimestamp"/> on whichever earlier Pump crossed the
    /// receive-idle deadline, issue #366), so the deadline is measured from the moment this end stopped
    /// waiting on that transport rather than from anything about the transport itself.
    /// </summary>
    private bool HasLingerExpired(PeerSlot slot) =>
        _options.Reactor.TimeProvider.GetElapsedTime(slot.InterruptedTimestamp) >= slot.EffectiveLinger;

    /// <summary>
    /// Closes every Established, <see cref="PeerSlot.IsInterrupted"/> Peer whose <see cref="HasLingerExpired"/>
    /// with <see cref="DisconnectReason.LingerExpired"/> (issue #367, parent #55), run once per
    /// <see cref="Pump"/> right after <see cref="EnforceReceiveIdleTimer"/> — mirroring
    /// <see cref="StreamSessionReactor.EnforceIdleTimers"/>'s own ordering, where the Linger check for an
    /// already-Interrupted Peer runs in the same pass as, and right after, the receive-idle check that
    /// can newly interrupt one. A Peer <see cref="EnforceReceiveIdleTimer"/> interrupted this very Pump
    /// can in principle be seen by this pass in the same Pump — nothing here waits a Pump before
    /// checking it — but <see cref="HasLingerExpired"/> can never be true for it yet:
    /// <see cref="PeerSlot.InterruptedTimestamp"/> was just stamped to this Pump's own clock reading, so
    /// the elapsed time this pass reads is zero, and <see cref="DatagramSessionOptions"/> rejects a zero
    /// <see cref="PeerSlot.EffectiveLinger"/> at construction — <c>0 &gt;= EffectiveLinger</c> can only
    /// hold for an <see cref="PeerSlot.EffectiveLinger"/> that validation already refuses to admit. Runs
    /// ahead of <see cref="EnforceSendIdleTimer"/> so a Peer this pass closes is never offered a Ping
    /// into a Slot it no longer occupies. Also closes a Closing Peer (<see cref="PeerSlot.IsClosing"/>)
    /// whose Goodbye is still unacknowledged once its effective Linger has elapsed since
    /// <see cref="PeerSlot.ClosingTimestamp"/> (issue #399).
    /// </summary>
    /// <remarks>
    /// Iterates by index over every occupied Slot exactly as <see cref="EnforceReceiveIdleTimer"/> and
    /// <see cref="StreamSessionReactor.EnforceIdleTimers"/> do, and calls <see cref="ClosePeer"/> directly
    /// from inside that loop — safe for the same reason it is safe there: <see cref="ClosePeer"/> only
    /// ever removes the occupant at the handle's own index from <see cref="_peers"/>, never shifts or
    /// touches any other index, so the loop's remaining iterations see every other Slot exactly as they
    /// would have regardless of whether this iteration's Peer closed.
    /// </remarks>
    private void EnforceLingerExpiry()
    {
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is not PeerSlot slot || slot.State != HandshakeState.Established)
            {
                continue;
            }

            // A Closing Peer's Linger runs from Close, not from any interruption (issue #399).
            bool expired = slot.IsClosing
                ? _options.Reactor.TimeProvider.GetElapsedTime(slot.ClosingTimestamp) >= slot.EffectiveLinger
                : slot.IsInterrupted && HasLingerExpired(slot);
            if (!expired)
            {
                continue;
            }

            ClosePeer(slot.Handle, DisconnectReason.LingerExpired);
        }
    }

    /// <summary>
    /// Queues one Ping for every Established Peer that has sent no data datagram for
    /// <see cref="DatagramSessionOptions.SendIdleInterval"/> (issue #340, subtask 8.1.4), run once per
    /// <see cref="Pump"/> ahead of <see cref="FlushOutboundDatagrams"/> so a Ping this call queues rides
    /// the same Pump's flush out — a Peer this method queues against is never left waiting a whole
    /// further Pump just to reach the wire. A Ping is a Heartbeat, and a Heartbeat is not a keepalive:
    /// it proves the party behind this Session is still there to receive and eventually acknowledge
    /// application data, not merely that some NAT mapping between here and there is still open — a bare
    /// keepalive a middlebox itself could answer would prove nothing about the party ADR 0011's
    /// liveness deadline (#54) actually cares about.
    /// </summary>
    /// <remarks>
    /// Reads <see cref="PeerSlot.LastSendTimestamp"/> against this Reactor's own
    /// <see cref="ReactorOptions.TimeProvider"/>, never the ambient clock, matching every other timer in
    /// this Reactor. A Peer still mid-handshake is skipped exactly as <see cref="FlushOutboundDatagrams"/>
    /// skips one — there is no established Session yet for a Heartbeat to prove anything about. This loop
    /// deliberately does not read <see cref="PeerSlot.IsInterrupted"/> (issue #371, parent #55): an
    /// Interrupted datagram Peer's <see cref="PeerSlot.State"/> is still <see cref="HandshakeState.Established"/>,
    /// so it keeps being offered a Ping here exactly like any other Established Peer. A datagram Session
    /// has no re-dial schedule (#55) — there is nothing to re-dial, and the interrupted end can only
    /// resume by being heard from again — so it keeps probing the path with Pings, at the send-idle
    /// interval or as the adaptive retransmission of one still unacknowledged, for as long as Linger
    /// keeps the Peer alive.
    /// </remarks>
    private void EnforceSendIdleTimer()
    {
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is not PeerSlot slot || slot.State != HandshakeState.Established)
            {
                continue;
            }

            SendResumeIfDue(slot);

            if (_options.Reactor.TimeProvider.GetElapsedTime(slot.LastSendTimestamp) < _options.SendIdleInterval)
            {
                continue;
            }

            QueuePing(slot);
        }
    }

    /// <summary>
    /// Sends one <see cref="DatagramControlType.Resume"/> control datagram to <paramref name="slot"/>'s
    /// address when the Peer is a dialled, <see cref="PeerSlot.IsInterrupted"/> one and no Resume has
    /// left for it within the last <see cref="DatagramSessionOptions.SendIdleInterval"/> (issue #382,
    /// parent #56): the first goes out on the Pump that finds the Peer Interrupted, every later one a
    /// send-idle interval after the previous. Only the dialling end sends Resume, because the accepting
    /// end cannot know the dialler's new address. The datagram is routed by the accepting end's own
    /// session id (<see cref="PeerSlot.RemoteSessionId"/>) and authorised by the secret it issued
    /// (<see cref="PeerSlot.RemoteResumeSecret"/>), ADR 0009.
    /// </summary>
    /// <remarks>
    /// A Resume is deliberately not a data datagram, so it does not stamp
    /// <see cref="PeerSlot.LastSendTimestamp"/>: Pings keep exactly their own send-idle cadence, and the
    /// Resume cadence reads <see cref="PeerSlot.LastResumeTimestamp"/> instead. It is still charged to
    /// the Peer's <see cref="PeerCounters.SocketWrites"/> and <see cref="PeerCounters.BytesSent"/>, send
    /// success or not, as a data datagram is (ADR 0013).
    /// </remarks>
    private void SendResumeIfDue(PeerSlot slot)
    {
        if (!slot.IsDialled || !slot.IsInterrupted)
        {
            return;
        }

        long now = _options.Reactor.TimeProvider.GetTimestamp();
        if (slot.LastResumeTimestamp is long lastResume
            && _options.Reactor.TimeProvider.GetElapsedTime(lastResume, now) < _options.SendIdleInterval)
        {
            return;
        }

        ControlDatagramCodec.WriteResume(_sendBuffer, slot.RemoteSessionId, slot.RemoteResumeSecret);
        SendControl(slot.Link, slot.Remote);

        // Unlike a handshake control datagram, a Resume has a Peer to charge (ADR 0013).
        slot.Counters.RecordSocketWrite();
        slot.Counters.RecordBytesSent(DatagramSessionProtocol.ControlDatagramSize);
        slot.LastResumeTimestamp = now;
    }

    /// <summary>
    /// Takes an <see cref="PeerSlot.IsInterrupted"/> dialled Peer out of Interrupted on hearing its
    /// session again (issue #383, parent #56): clears the flag, queues
    /// <see cref="SessionEventKind.PeerResumed"/> with the same <see cref="PeerId"/> (generation
    /// included), calls <see cref="Core.PeerCounterSet.RecordResume"/> once and logs
    /// <see cref="DiagnosticId.ResumeMatched"/> with <c>Value0</c> 0 (a datagram Session rewinds nothing).
    /// Does nothing for an accepting-end Peer or a Peer that is not Interrupted.
    /// </summary>
    /// <remarks>
    /// Called once <see cref="ParseDataDatagram"/> has fully parsed a data datagram, the same well-formedness
    /// proof that stamps <see cref="PeerSlot.LastReceiveTimestamp"/>. An observer who can see the session id
    /// can forge such a datagram, so it can fake this exit on the dialling end. That is accepted: the exit
    /// grants no takeover (the accepting end still demands the resume secret, ADR 0009) and on-path
    /// attackers are out of scope. Counters are never reset by a Resume (ADR 0013).
    /// <para>
    /// No "has sent a Resume yet" check is needed (ADR 0015): the Pump that marks a dialled Peer
    /// Interrupted also sends its first Resume (<c>SendResumeIfDue</c> runs after the receive-idle
    /// check), and datagrams are read before either, so every datagram that reaches an Interrupted
    /// dialled Peer arrives after at least one Resume went out.
    /// </para>
    /// </remarks>
    private void ResumeInterruptedDialledPeerOnData(PeerSlot slot)
    {
        if (!slot.IsDialled || !slot.IsInterrupted)
        {
            return;
        }

        PeerId peer = ToPeerId(slot.Handle);
        LeaveInterrupted(slot, peer);
        if (_diagnostics.IsEnabled(DiagnosticLevel.Info))
        {
            _diagnostics.Log(DiagnosticRecord.ForResumeMatched(peer, bytesRewound: 0));
        }
    }

    /// <summary>
    /// Queues one seven-byte control record — the send-idle Ping (issue #340) — on
    /// <paramref name="slot"/>'s <see cref="PeerSlot.PendingSends"/>, exactly as an ordinary
    /// single-Fragment reliable <see cref="Send"/> queues one: the same <c>ReliableUnordered</c>
    /// sequence allocation and retention commit, with <see cref="RetentionPool.TryAcquire"/>'s
    /// <c>isControl</c> flag set so <see cref="TryResolvePendingPayload"/> — on this send and on every
    /// later resend <see cref="ReportExpiredRetransmissions"/> re-queues — packs it through
    /// <see cref="Session.DatagramRecordPacker.TryPackPingRecord"/> rather than as an ordinary
    /// zero-length Message. A Ping is always <c>ReliableUnordered</c> (ADR 0009's control-record shape,
    /// issue #338): ordering a Heartbeat against application data would only ever add a head-of-line
    /// dependency neither needs.
    /// </summary>
    /// <remarks>
    /// Spends nothing on a refusal, the same contract issue #244 established for <see cref="Send"/>: the
    /// sequence is only read, never allocated, until <see cref="RetentionPool.TryAcquire"/> commits, so a
    /// Peer whose <c>ReliableUnordered</c> Space has no free slot this Pump leaves its next sequence
    /// exactly where it was and is tried again — this same check, unconditionally, every Pump — the next
    /// time <see cref="EnforceSendIdleTimer"/> finds it still idle.
    /// </remarks>
    private void QueuePing(PeerSlot slot)
    {
        const PacketRecordMode mode = PacketRecordMode.ReliableUnordered;
        int spaceIndex = ReliableSpaceIndex(mode);

        uint sequence = slot.OutboundSequences.Peek(mode);
        RetentionSlotOutcome outcome = _retention.TryAcquire(
            slot.Handle.Index, spaceIndex, sequence, ReadOnlySpan<byte>.Empty, isControl: true);

        if (outcome != RetentionSlotOutcome.Committed)
        {
            return;
        }

        slot.OutboundSequences.Allocate(mode);
        slot.PendingSends.Add(new PendingOutboundRecord { Mode = mode, Sequence = sequence });
    }

    /// <summary>
    /// Queues this end's Goodbye — a zero-length control record in <paramref name="slot"/>'s
    /// <c>ReliableOrdered</c> Space (issue #397) — exactly as <see cref="QueuePing"/> queues a Ping: the next
    /// sequence, retained with the control flag so first send and every resend pack it as a Goodbye.
    /// Spends nothing when the Space's retention is full, leaving <see cref="PeerSlot.LocalClose"/> at
    /// <see cref="LocalCloseState.GoodbyeDeferred"/> for <see cref="QueueDeferredGoodbyes"/> to retry.
    /// </summary>
    private void QueueGoodbye(PeerSlot slot)
    {
        const PacketRecordMode mode = PacketRecordMode.ReliableOrdered;
        int spaceIndex = ReliableSpaceIndex(mode);

        uint sequence = slot.OutboundSequences.Peek(mode);
        RetentionSlotOutcome outcome = _retention.TryAcquire(
            slot.Handle.Index, spaceIndex, sequence, ReadOnlySpan<byte>.Empty, isControl: true);

        if (outcome != RetentionSlotOutcome.Committed)
        {
            return;
        }

        slot.OutboundSequences.Allocate(mode);
        slot.PendingSends.Add(new PendingOutboundRecord { Mode = mode, Sequence = sequence });
        slot.GoodbyeSequence = sequence;
        slot.LocalClose = LocalCloseState.GoodbyeUnacked;
    }

    /// <summary>
    /// Retries the Goodbye of every Closing Peer whose <see cref="Close"/> found its <c>ReliableOrdered</c>
    /// retention full (issue #397).
    /// </summary>
    private void QueueDeferredGoodbyes()
    {
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is PeerSlot { LocalClose: LocalCloseState.GoodbyeDeferred } slot)
            {
                QueueGoodbye(slot);
            }
        }
    }

    /// <summary>
    /// Packs as much as fits of <paramref name="slot"/>'s queue into one data Datagram, in Send order,
    /// and sends it (issue #216): the 20-byte header — this Peer's own <see cref="PeerSlot.RemoteSessionId"/>
    /// stamped as the session, since that is the id the far end asked to be stamped on datagrams sent to
    /// it — followed by both reliable Spaces' ack blocks read fresh from
    /// <see cref="_receiveWindows"/>, so an admission this same Pump's read pass just made is reflected
    /// in the Datagram this same Pump's flush sends. A reliable record's payload is read back from
    /// <see cref="_retention"/> and an unreliable one's from <see cref="PendingOutboundRecord.UnreliablePayload"/>
    /// (<see cref="TryResolvePendingPayload"/>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>A record already acked before its flush.</b> An ack read earlier this same Pump (issue #345)
    /// can release a sequence's retention slot before it reaches this flush — queued for its first send,
    /// or re-queued for retransmission by <see cref="ReportExpiredRetransmissions"/> — in which case
    /// <see cref="TryResolvePendingPayload"/> fails and the record below is consumed unsent rather than
    /// retried: resending an already-released sequence would be a duplicate the far end has already
    /// acknowledged, and no path on this ticket's live traffic wrote a payload to a released slot.
    /// </para>
    /// <para>
    /// <b>A record that does not fit waits for the next Pump</b> (issue #230). Packing stops at the
    /// first record <see cref="DatagramRecordPacker.TryPackRecord"/> refuses — this Datagram is full —
    /// and everything from there to the end of <see cref="PeerSlot.PendingSends"/>, in the order it was
    /// queued, is left on the Slot rather than discarded: <see cref="List{T}.RemoveRange"/> trims only
    /// the prefix this call actually consumed, so those records are exactly what the next
    /// <see cref="FlushOutboundDatagrams"/> sees at the front of the same queue. A Datagram is flushed
    /// once per Peer per Pump (the loop above never calls this twice in one Pump), so the remainder does
    /// not go out as a second Datagram this Pump — it rides the next one. A record whose payload failed
    /// to resolve — a reliable sequence already released from retention — is consumed rather than left
    /// queued: retrying it later cannot succeed either, since nothing repopulates a released slot.
    /// </para>
    /// <para>
    /// <b>Arming still happens only where the record actually reached a Datagram.</b> A record that did
    /// not fit is not sent, so it must not start its retransmit deadline — the loop below only reaches
    /// the <see cref="OutstandingReliableSequences.RecordSent"/> call for a record
    /// <see cref="DatagramRecordPacker.TryPackRecord"/> just accepted, never for the one that broke the
    /// loop, so a queued-but-unsent record is armed on whichever later Pump actually packs it (subtask
    /// 6.6.8, issue #218). Nothing is sent at all if nothing ended up packed, so a queue whose one
    /// record failed to resolve costs a wasted flush rather than a header-only Datagram with no records
    /// in it.
    /// </para>
    /// </remarks>
    private void FlushPeer(PeerSlot slot)
    {
        ((uint Base, uint Bits) reliableOrdered, (uint Base, uint Bits) reliableUnordered) =
            _receiveWindows[slot.Handle.Index].GenerateAckBlocks();

        DatagramRecordPacker packer = new(
            _outboundDatagramBuffer,
            slot.RemoteSessionId,
            reliableOrdered.Base,
            reliableOrdered.Bits,
            reliableUnordered.Base,
            reliableUnordered.Bits);

        int consumed = 0;
        foreach (PendingOutboundRecord pending in slot.PendingSends)
        {
            if (!TryResolvePendingPayload(
                slot, pending, out ReadOnlySpan<byte> payload, out bool isFragment, out ushort fragmentIndex,
                out ushort fragmentCount, out bool isControl))
            {
                consumed++;
                continue;
            }

            // A control record — the send-idle Ping (issue #340) — is packed through its own seven-byte
            // shape, never through TryPackRecord: retention marked it control at TryAcquire, and that
            // mark is what routes a first send and every later resend through the same packing call.
            // Retention's control mark cannot tell a Ping from a Goodbye, but their Spaces can: a Ping is
            // ReliableUnordered and a Goodbye is ReliableOrdered (issue #397).
            bool packed = isControl
                ? pending.Mode == PacketRecordMode.ReliableOrdered
                    ? packer.TryPackGoodbyeRecord(pending.Sequence)
                    : packer.TryPackPingRecord(pending.Sequence)
                : isFragment
                    ? packer.TryPackFragmentRecord(pending.Mode, pending.Sequence, fragmentIndex, fragmentCount, payload)
                    : packer.TryPackRecord(pending.Mode, pending.Sequence, payload);

            if (!packed)
            {
                break;
            }

            consumed++;

            // Arming happens here, at the moment the record actually reaches a Datagram, rather than in
            // Send: a sequence queued but not yet flushed has not been transmitted, and starting its
            // retransmit deadline before it leaves would spend part of that deadline on this end's own
            // queue (subtask 6.6.8, issue #218). A resend is skipped — ReArmExpired has already given it
            // this Pump's single backed-off deadline. The horizon gate lives inside RecordSent, so a
            // sequence too far ahead of the ack horizon still has its timer withheld rather than
            // started, exactly as #185 requires.
            if (!pending.IsRetransmission && IsReliable(pending.Mode))
            {
                slot.AckTracker.RecordSent(
                    pending.Mode,
                    pending.Sequence,
                    slot.RetransmitTimer.EffectiveTimeout(ObservedPumpInterval, slot.EffectiveLinger),
                    isFragment);
            }

            // StallSignature.RecordFragmentSent opens the unacked-Fragment window on the first Fragment
            // only (subtask 7.5.3.1, issue #294) but must see every reliable Fragment actually packed,
            // retransmits included — the tracker itself latches only the first send.
            if (isFragment && IsReliable(pending.Mode))
            {
                slot.StallSignature.RecordFragmentSent();
            }
        }

        slot.PendingSends.RemoveRange(0, consumed);

        // Nothing packed sends nothing, except for a Peer whose Goodbye close is due: it sends the header
        // and ack blocks alone, the one datagram that can still tell the remote its Goodbye arrived.
        if (packer.RecordsPacked == 0 && slot.RemoteClose is not (RemoteCloseState.CloseDue or RemoteCloseState.AckDue))
        {
            return;
        }

        UdpLinkSendResult result = slot.Link.SendTo(slot.Remote, _outboundDatagramBuffer.AsSpan(0, packer.BytesWritten));
        slot.Counters.RecordSocketWrite();
        slot.Counters.RecordBytesSent((ulong)packer.BytesWritten);
        if (result.IsSuccess)
        {
            _counters.RecordDatagramSent();
        }

        // A data datagram just left for this Peer — this flush packed at least one record, or the
        // RecordsPacked == 0 return above would already have exited — so the send-idle interval
        // restarts from now (issue #340): the Ping this same flush may just have packed counts as a
        // sent datagram exactly like any other, which is what keeps a Session that has nothing but
        // Pings to send from ever emitting a second one before the next interval elapses.
        slot.LastSendTimestamp = _options.Reactor.TimeProvider.GetTimestamp();
    }

    /// <summary>
    /// The bytes, and the record shape, one queued record's flush should pack: <paramref name="pending"/>'s
    /// own copy for an unreliable record, since nothing else retains it, and never a Fragment (ADR 0004
    /// — only reliable modes ever fragment); otherwise both are read back from <see cref="_retention"/>
    /// at the reliable Space <paramref name="pending"/>'s mode addresses, which is where <see cref="Send"/>
    /// recorded them and the only place a retransmit — <paramref name="pending"/> itself carries nothing
    /// of the shape — can recover them from (issue #250). Fails — nothing to pack — only if a reliable
    /// sequence was already released from retention between being queued and this flush: an ack read
    /// earlier the same Pump (issue #345) is the live path that does this today.
    /// </summary>
    /// <param name="slot">The Peer this record was queued against.</param>
    /// <param name="pending">The queued record whose bytes and shape are being resolved.</param>
    /// <param name="payload">The resolved record's payload bytes.</param>
    /// <param name="isFragment">Whether the record is one piece of a Message cut into Fragments.</param>
    /// <param name="fragmentIndex">This piece's position within its Message, meaningful only when <paramref name="isFragment"/> is <see langword="true"/>.</param>
    /// <param name="fragmentCount">The Message's total piece count, meaningful only when <paramref name="isFragment"/> is <see langword="true"/>.</param>
    /// <param name="isControl">
    /// Whether this record is a control record — an idle-triggered Ping (issue #340) — rather than a
    /// Message a consumer Sent, read back from <see cref="_retention"/> exactly as <paramref name="isFragment"/>
    /// is. Always <see langword="false"/> for an unreliable record: nothing but a reliable
    /// <c>ReliableUnordered</c> send-idle Ping is ever marked control.
    /// </param>
    private bool TryResolvePendingPayload(
        PeerSlot slot,
        PendingOutboundRecord pending,
        out ReadOnlySpan<byte> payload,
        out bool isFragment,
        out ushort fragmentIndex,
        out ushort fragmentCount,
        out bool isControl)
    {
        if (pending.UnreliablePayload is byte[] bytes)
        {
            payload = bytes;
            isFragment = false;
            fragmentIndex = 0;
            fragmentCount = 0;
            isControl = false;
            return true;
        }

        int spaceIndex = ReliableSpaceIndex(pending.Mode);
        return _retention.TryRead(
            slot.Handle.Index, spaceIndex, pending.Sequence, out payload, out isFragment, out fragmentIndex, out fragmentCount, out isControl);
    }

    /// <summary>
    /// Classifies one already-validated record against its mode's Space, counts what the window
    /// refused plus whatever the touched window's slide just retired un-admitted (issues #173, #176),
    /// and delivers what it admitted (subtask 6.6.5, issue #217).
    /// </summary>
    /// <remarks>
    /// <para>
    /// For three of the four Spaces, only an admitted record is delivered: a duplicate or an
    /// out-of-window record reaches neither the frame arena nor
    /// <see cref="PeerCounters.MessagesReceived"/>, because suppressing a duplicate is the window's
    /// whole purpose and counting one as received would make the suppression invisible in the figure a
    /// consumer reads to check it.
    /// </para>
    /// <para>
    /// <b>Plain <c>Unreliable</c> is the exception</b> (subtask 6.6.12, issue #222; ADR 0017). Its
    /// promise is that a late arrival is delivered and that duplicate suppression is bounded at 64
    /// sequences, and the receive window can keep neither: it refuses everything below its base
    /// outright, so a repeat delayed past 64 would be discarded rather than delivered, and its capacity
    /// is a consumer option rather than the protocol's 64. So for that Space the receive window still
    /// classifies — that is what counts loss from the gaps it slides past, and what its ack bookkeeping
    /// needs — while the delivery decision comes from
    /// <see cref="PeerSlot.UnreliableDuplicates"/> alone. The receive window's own refusals are not
    /// counted for this Space, since the 64-entry window is the authority on what was a duplicate here
    /// and counting both would count one repeat twice.
    /// </para>
    /// <para>
    /// <b>An admitted Fragment is diverted, not delivered</b> (issue #257, ADR 0004). A Fragment is one
    /// piece of a Message, not a Message, so an admitted one is held in its Space's receive-window slot
    /// instead — <see cref="HoldFragment"/> — rather than reaching <see cref="DeliverAdmittedRecord"/>:
    /// no frame-arena reservation, no pending <see cref="SessionEventKind.MessageReceived"/>, and
    /// <see cref="PeerCounters.MessagesReceived"/> does not rise. <see cref="HoldFragment"/> also
    /// derives the held Fragment's group first sequence and answers whether its group is now complete
    /// (issue #258); a complete group is handed to <see cref="TryDeliverOrDeferFragmentGroup"/>, which
    /// delivers it as one Message or defers it if delivery cannot happen yet (issue #259). Only
    /// <c>ReliableOrdered</c> and <c>ReliableUnordered</c> ever carry the fragment shape (ADR 0004);
    /// <paramref name="isFragment"/> is otherwise ignored exactly as it always was, so a stray fragment
    /// bit on an unreliable-mode record changes nothing this method does. By the time this method runs
    /// at all, <see cref="ParseDataDatagram"/>'s own pass has already refused any Fragment in this
    /// datagram whose declared <c>fragmentCount</c> claims more Fragments than this Reactor's own
    /// receive window, <see cref="DatagramSessionOptions.ResolvedReceiveWindowFragments"/>, could ever
    /// hold (issue #282, reworked under #264), so <paramref name="fragmentCount"/> here is always a legal
    /// claim, never one this method itself needs to bound.
    /// </para>
    /// <para>
    /// <b>An abandoned Fragment set is reported, never acted on</b> (issue #297). The
    /// <see cref="ReceiveWindowSpaces.Classify(PacketRecordMode, uint, out int)"/> call above may have
    /// just clobbered the ring slot of an incomplete Fragment set that can now never complete — the
    /// window itself has no <see cref="PeerId"/> to subject a Diagnostic to, so this method reads
    /// <see cref="ReceiveWindowSpaces.LastAbandonedFragmentSet"/> right after that call and logs
    /// <see cref="DiagnosticRecord.ForFragmentSetAbandoned"/> from it. This is accounting only: it fires
    /// on ordinary loss as readily as on anything more serious, so nothing here closes, disconnects, or
    /// otherwise changes Session state — that is issue #270's job, off a different signal.
    /// </para>
    /// <para>
    /// <b>An admitted control record — a Ping — is acknowledged and never delivered</b> (issue #339).
    /// <paramref name="isControl"/> comes from <see cref="PacketRecordCodec.TryRead"/>, which reports it
    /// only for a record that is legally a Ping: <c>ReliableUnordered</c>, unfragmented, zero length.
    /// Admission runs exactly as it does for any other record — the same <see
    /// cref="ReceiveWindowSpaces.Classify(PacketRecordMode, uint, out int)"/> call above, the same
    /// duplicate and out-of-window counting, the same ack block once <see
    /// cref="ReceiveWindowSpaces.GenerateAckBlocks"/> next runs — so a Ping at sequence N is covered by
    /// the ordinary ack block exactly as a Message at N would be, and a duplicate Ping raises <see
    /// cref="PeerCounters.DuplicatePackets"/> exactly as a duplicate Message does. What differs is what
    /// happens once admitted: neither <see cref="DeliverAdmittedRecord"/> nor <see cref="HoldFragment"/>
    /// is reached for it, so no <see cref="SessionEventKind.MessageReceived"/> is queued and <see
    /// cref="PeerCounters.MessagesReceived"/> does not rise.
    /// </para>
    /// </remarks>
    private void ClassifyRecord(
        PeerSlot slot, PacketRecordMode mode, uint sequence, bool isFragment, bool isControl,
        ushort fragmentIndex, ushort fragmentCount, ReadOnlySpan<byte> payload)
    {
        // A Goodbye earlier in this same Datagram may have closed the Peer directly (issue #400); its
        // Slot can already belong to no one, and nothing after the Goodbye is admitted.
        if (_peers.TryResolve(slot.Handle) is null)
        {
            return;
        }

        ReceiveWindowSpaces windows = _receiveWindows[slot.Handle.Index];

        // Left unadmitted, so no ack covers it and the far end's retransmission brings it back once the
        // waiting groups have been delivered and the slot is free (issue #423).
        if (IsReliable(mode) && windows.MustWaitForWaitingGroups(mode, sequence))
        {
            return;
        }

        if (RefusesForHoldBudget(slot, windows, mode, sequence, isFragment, payload.Length))
        {
            slot.Counters.RecordOutOfWindowPacket();
            return;
        }

        if (RefusesForArenaRoom(slot, windows, mode, sequence, isFragment, isControl, payload.Length))
        {
            return;
        }

        ReceiveWindowOutcome outcome = windows.Classify(mode, sequence, out int ringSlot);

        // The moment a Fragment set's slot is discarded is the last moment anyone can say how much of
        // it had arrived (issue #297): windows.Classify just clobbered such a slot if this is set. This
        // is accounting only — it says what was lost and not why, and fires on ordinary loss too, which
        // is exactly why it may never close, disconnect, or otherwise change Session state itself.
        if (windows.LastAbandonedFragmentSet.Occurred && _diagnostics.IsEnabled(DiagnosticLevel.Warn))
        {
            _diagnostics.Log(DiagnosticRecord.ForFragmentSetAbandoned(
                ToPeerId(slot.Handle),
                windows.LastAbandonedFragmentSet.Held,
                windows.LastAbandonedFragmentSet.Expected));
        }

        switch (outcome)
        {
            case ReceiveWindowOutcome.OutOfWindow:
                slot.Counters.RecordOutOfWindowPacket();
                break;

            case ReceiveWindowOutcome.Duplicate:
                slot.Counters.RecordDuplicatePacket();
                break;
        }

        // Receiver-side Closing (issue #401): the record was still admitted above, so it is acked, but a
        // Peer whose remote has said Goodbye accepts no further Messages from it.
        bool acceptsMessages = !slot.RemoteGoodbyeDelivered;

        if (acceptsMessages && mode == PacketRecordMode.Unreliable)
        {
            DeliverUnreliableRecord(slot, sequence, payload, alreadyCountedAsRefused: outcome != ReceiveWindowOutcome.Admitted);
        }
        else if (acceptsMessages && outcome == ReceiveWindowOutcome.Admitted)
        {
            if (isControl)
            {
                // Admitted into its Space's receive window exactly like any other record above, so the
                // ordinary ack block covers it and a duplicate counts like any other duplicate, but it
                // carries no payload for a consumer to receive, so MessagesReceived does not rise.
                // A ReliableUnordered one is a Ping (#339) and nothing more happens. A ReliableOrdered
                // one is a Goodbye (#400): it takes its turn in send order like a Message would, and
                // acts only when delivered. The lowest sequence is kept so a second, misbehaving
                // Goodbye can never displace an earlier one.
                if (mode == PacketRecordMode.ReliableOrdered)
                {
                    if (slot.RemoteGoodbyeSequence is not uint pending || SequenceNumber.IsNewerThan(pending, sequence))
                    {
                        slot.RemoteGoodbyeSequence = sequence;
                    }

                    DeliverAdmittedRecord(slot, mode, sequence, payload);
                }
            }
            else if (isFragment && mode is PacketRecordMode.ReliableOrdered or PacketRecordMode.ReliableUnordered)
            {
                bool groupComplete = HoldFragment(slot, mode, sequence, ringSlot, fragmentIndex, fragmentCount, payload);
                if (groupComplete)
                {
                    uint groupFirstSequence = FragmentGroup.FirstSequenceOf(sequence, fragmentIndex);
                    TryDeliverOrDeferFragmentGroup(slot, mode, groupFirstSequence, fragmentCount);
                }
            }
            else
            {
                DeliverAdmittedRecord(slot, mode, sequence, payload);
            }
        }

        // No mode test guards this (#176): a window reports a nonzero LastSlideLoss only if it slid,
        // and only the two unreliable Spaces' windows are built to slide, so this counts both of them
        // and neither reliable Space without restating which Spaces slide. ReceiveWindowSpaces'
        // constructor stays the one place that decides that, so the two cannot drift apart.
        if (windows.LastSlideLoss > 0)
        {
            slot.Counters.RecordUnreliableLoss(windows.LastSlideLoss);
        }
    }

    /// <summary>
    /// Decides, before the receive window sees it, whether an ordinary <c>ReliableOrdered</c> record
    /// would go past what <see cref="PeerSlot.OrderedDelivery"/> may hold (issue #424). A refused record
    /// is left unadmitted, so no ack covers it and the far end's ordinary retransmission brings it back
    /// once the send-order head has moved. A head that never moves stops this Space and no other.
    /// </summary>
    /// <remarks>
    /// A Fragment is never held by the queue, so only the receive window bounds it. A Peer whose Goodbye
    /// has been delivered holds nothing further, so what it sends is still admitted and acknowledged. A
    /// sequence the window would not admit anyway is left for <see cref="ReceiveWindowSpaces.Classify(PacketRecordMode, uint, out int)"/>
    /// to count as usual.
    /// </remarks>
    /// <returns><see langword="true"/> when the record was refused and must not be classified.</returns>
    private static bool RefusesForHoldBudget(
        PeerSlot slot, ReceiveWindowSpaces windows, PacketRecordMode mode, uint sequence, bool isFragment, int payloadLength) =>
        mode == PacketRecordMode.ReliableOrdered
        && !isFragment
        && !slot.RemoteGoodbyeDelivered
        && windows.WouldAdmit(mode, sequence)
        && !slot.OrderedDelivery.CanTake(sequence, payloadLength);

    /// <summary>
    /// Decides, before the receive window sees it, whether a whole reliable Message that would be
    /// delivered the moment it is admitted must be turned away because the frame arena has no room for
    /// it this Frame. A refused record is left unadmitted, so no ack covers it and the far end's
    /// ordinary retransmission brings it back on a later Pump. Admitting it first and failing the
    /// reservation afterwards would lose it: the ack would stop the retransmissions, and any copy
    /// already in flight would arrive below the window base.
    /// </summary>
    /// <remarks>
    /// Only a record that would reach <see cref="DeliverToConsumer"/> straight from this datagram is
    /// checked: a <c>ReliableUnordered</c> Message, or a <c>ReliableOrdered</c> one at the head of send
    /// order. A <c>ReliableOrdered</c> record behind a gap is copied into
    /// <see cref="PeerSlot.OrderedDelivery"/> and retried there, and a Fragment set is retried through
    /// <see cref="PendingFragmentGroups"/>, so neither needs this end to refuse it. Nothing reserves arena
    /// room between this check and the delivery that follows it, so a record this passes is delivered.
    /// A record over <see cref="DatagramSessionOptions.MaxMessageSize"/> is never refused here: it will
    /// be discarded rather than reserved, and an arena at its floor could never make room for it, so
    /// refusing it would stop its Space for good.
    /// </remarks>
    /// <returns><see langword="true"/> when the record was refused and must not be classified.</returns>
    private bool RefusesForArenaRoom(
        PeerSlot slot, ReceiveWindowSpaces windows, PacketRecordMode mode, uint sequence,
        bool isFragment, bool isControl, int payloadLength)
    {
        bool deliversOnAdmission = mode switch
        {
            PacketRecordMode.ReliableUnordered => true,
            PacketRecordMode.ReliableOrdered => sequence == slot.OrderedDelivery.NextSequence,
            _ => false,
        };

        if (!deliversOnAdmission || isFragment || isControl || slot.RemoteGoodbyeDelivered
            || payloadLength > _options.MaxMessageSize || !windows.WouldAdmit(mode, sequence))
        {
            return false;
        }

        return !AccountForReservation(slot, _arena.Check(slot.Handle.Index, payloadLength));
    }

    /// <summary>
    /// The plain <c>Unreliable</c> Space's delivery decision (subtask 6.6.12, issue #222; ADR 0017):
    /// deliver unless this Peer's own 64-entry <see cref="UnreliableDuplicateWindow"/> has already seen
    /// <paramref name="sequence"/>. A repeat inside those 64 sequences is suppressed and counted as a
    /// duplicate; a repeat delayed past them is delivered, because suppression here is bounded rather
    /// than absolute and pretending otherwise would promise something a 64-entry window cannot keep.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The receive window's own verdict does not decide delivery here. That window refuses everything
    /// below its base, which for this Space would discard exactly the late arrival the mode promises to
    /// deliver, and its capacity is a consumer option rather than the protocol's 64. It still runs and
    /// is still counted, for the loss its slides retire and the refusals a consumer reads off
    /// <see cref="PeerCounters.OutOfWindowPackets"/>; it simply is not what says whether a Message
    /// reaches the consumer.
    /// </para>
    /// <para>
    /// <paramref name="alreadyCountedAsRefused"/> is what keeps one repeat from counting twice. A
    /// record the receive window already refused has raised a counter of its own, so suppressing it
    /// here raises nothing further; only a record that window admitted, and that this one recognises,
    /// is a duplicate this Space would otherwise never have counted.
    /// </para>
    /// </remarks>
    private void DeliverUnreliableRecord(
        PeerSlot slot, uint sequence, ReadOnlySpan<byte> payload, bool alreadyCountedAsRefused)
    {
        if (!slot.UnreliableDuplicates.TryAdmit(sequence))
        {
            if (!alreadyCountedAsRefused)
            {
                slot.Counters.RecordDuplicatePacket();
            }

            return;
        }

        DeliverAdmittedRecord(slot, PacketRecordMode.Unreliable, sequence, payload);
    }

    /// <summary>
    /// Diverts an admitted Fragment away from delivery and into its Space's receive-window slot for
    /// <paramref name="ringSlot"/> instead (issue #257, ADR 0004): copies its payload into the
    /// fixed-size slot the receive arena preallocated for that ring position, and records the payload's
    /// byte length on the window so a later Pump can tell a held Fragment apart from an empty slot.
    /// Nothing is reserved in the frame arena, no pending <see cref="SessionEventKind.MessageReceived"/>
    /// is queued, and <see cref="PeerCounters.MessagesReceived"/> does not rise — a Fragment is a piece
    /// of a Message, not a Message, so none of the three steps <see cref="DeliverToConsumer"/> takes
    /// apply to it. Once the length is recorded, this derives the held piece's group first sequence
    /// (issue #258), records that identity on the window too (issue #297) — so the set can still be
    /// named if its slot is later discarded before it completes — and answers whether every piece of
    /// that group is now held.
    /// </summary>
    /// <remarks>
    /// <para>
    /// There is no separate reassembly buffer to copy into instead (ADR 0004): the receive window this
    /// Fragment was already admitted into <em>is</em> the reassembly storage, by design rather than by
    /// omission. A dedicated buffer would be new attack surface — a sender flooding partial Fragment
    /// sets would fill memory taken solely for that purpose — while this design lets such a sender only
    /// ever fill the same receive window an honest sender's traffic already bounds and this Reactor
    /// already defends. <paramref name="ringSlot"/> comes from
    /// <see cref="ReceiveWindowSpaces.Classify(PacketRecordMode, uint, out int)"/>'s own call in
    /// <see cref="ClassifyRecord"/>, which is why this method takes the slot rather than re-resolving it
    /// from <paramref name="mode"/> and a sequence — the window's <see cref="ReceiveWindow.Base"/> may
    /// already have advanced past that sequence by the time this call is made.
    /// </para>
    /// <para>
    /// <b>Survives an interruption at no cost (issue #262).</b> A held Fragment's slot is ordinary
    /// per-Peer <see cref="PeerSlot"/> and receive-arena state — the same state a lost and rebuilt
    /// transport leaves untouched, because this profile identifies a Session by its wire token rather
    /// than by the address or link a datagram arrived on (see <see cref="ProcessDataDatagram"/>'s
    /// remarks). A half-arrived group's pieces are therefore still exactly here, still counted against
    /// this Peer's own receive window, no matter how long the gap between one Fragment and the next:
    /// nothing reads this state as part of resuming a Session, and nothing needs to, because nothing
    /// about it was ever tied to the transport that went away. This is the general shape any resume of
    /// any kind must respect — <see cref="StreamSessionReactor"/>'s own Resume (issue #128) makes the
    /// same promise for its profile, stream framing rather than Fragments: resuming fixes <em>where
    /// delivery continues from</em>, at Message granularity, and never discards what a receive window is
    /// already holding piece by piece underneath that.
    /// </para>
    /// </remarks>
    /// <param name="slot">The Peer this Fragment arrived on.</param>
    /// <param name="mode">The Fragment's delivery mode — always <c>ReliableOrdered</c> or <c>ReliableUnordered</c> (ADR 0004).</param>
    /// <param name="sequence">The Fragment's own sequence — see <see cref="FragmentGroup.FirstSequenceOf"/>.</param>
    /// <param name="ringSlot">The ring position the admitting <see cref="ReceiveWindowSpaces.Classify(PacketRecordMode, uint, out int)"/> call reported.</param>
    /// <param name="fragmentIndex">The Fragment's zero-based position within its group.</param>
    /// <param name="fragmentCount">The group's declared Fragment count.</param>
    /// <param name="payload">The Fragment's payload bytes.</param>
    /// <returns>
    /// Whether the Fragment's group is complete after this piece was held (issue #258) —
    /// <see langword="false"/> whenever the payload did not fit its slot, since that leaves the slot's
    /// length at zero and the piece effectively lost. Delivering a complete group is issue #259's, so
    /// today's only caller discards this.
    /// </returns>
    private bool HoldFragment(
        PeerSlot slot, PacketRecordMode mode, uint sequence, int ringSlot,
        ushort fragmentIndex, ushort fragmentCount, ReadOnlySpan<byte> payload)
    {
        int spaceIndex = ReliableSpaceIndex(mode);
        Span<byte> fragmentSlot = _receiveArena.GetFragmentSlot(slot.Handle.Index, spaceIndex, ringSlot);

        if (payload.Length > fragmentSlot.Length)
        {
            // Cannot happen from a peer whose own FragmentSize matches this Reactor's (ADR 0004's
            // ordinary case), but fragment size is unilateral and per-direction, so nothing on the
            // wire proves the two sides agree. Refusing to write past the slot's fixed width — rather
            // than throwing — leaves the sequence admitted (the window already advanced past it) but
            // this slot's length at zero, indistinguishable from an empty one: the Fragment set this
            // piece belonged to simply never completes, exactly as if the piece had been lost.
            return false;
        }

        payload.CopyTo(fragmentSlot);
        ReceiveWindowSpaces windows = _receiveWindows[slot.Handle.Index];
        windows.RecordFragmentLength(mode, ringSlot, (ushort)payload.Length);

        uint groupFirstSequence = FragmentGroup.FirstSequenceOf(sequence, fragmentIndex);
        windows.RecordFragmentGroup(mode, ringSlot, groupFirstSequence, fragmentCount);
        return windows.IsGroupComplete(mode, groupFirstSequence, fragmentCount);
    }

    /// <summary>
    /// Decides how a just-completed Fragment group reaches the consumer (issue #259). For
    /// <c>ReliableUnordered</c> there is no ordering rule to satisfy, so this attempts delivery
    /// immediately and, if the frame arena had no room, defers it on the Space's <see cref="PendingFragmentGroups"/>
    /// for <see cref="RetryPendingFragmentDeliveries"/> to retry on a later Pump. For
    /// <c>ReliableOrdered</c> the group must additionally wait its turn in this Peer's
    /// <see cref="PeerSlot.OrderedDelivery"/> queue — the same send-order rule an unfragmented record
    /// already obeys (subtask 6.6.10, issue #220) — so this records the group as
    /// the Space's <see cref="PendingFragmentGroups"/> and asks <see cref="DrainOrderedDelivery"/>
    /// to deliver it the moment that queue's <see cref="OrderedDeliveryQueue.NextSequence"/> reaches
    /// the group's first sequence, whether that is true already or only after some other sequence
    /// still ahead of it in that Space arrives.
    /// </summary>
    /// <summary>
    /// The completed-but-undelivered Fragment groups of <paramref name="slot"/>'s
    /// <paramref name="mode"/> Space (issue #259). Resolved through <see cref="_receiveWindows"/> by
    /// <see cref="SlotHandle.Index"/>, the same way this Peer's windows themselves are, so a waiting
    /// group and the held pieces it names live in one place and are reset together when a Slot is
    /// reused.
    /// </summary>
    private PendingFragmentGroups PendingGroupsOf(PeerSlot slot, PacketRecordMode mode) =>
        _receiveWindows[slot.Handle.Index].PendingGroupsFor(mode);

    private void TryDeliverOrDeferFragmentGroup(PeerSlot slot, PacketRecordMode mode, uint groupFirstSequence, ushort fragmentCount)
    {
        if (mode == PacketRecordMode.ReliableUnordered)
        {
            if (!TryDeliverFragmentGroup(slot, mode, groupFirstSequence, fragmentCount))
            {
                PendingGroupsOf(slot, mode).Add(groupFirstSequence, fragmentCount);
            }

            return;
        }

        PendingGroupsOf(slot, mode).Add(groupFirstSequence, fragmentCount);
        DrainOrderedDelivery(slot);
    }

    /// <summary>
    /// Copies a completed Fragment group's held pieces, in fragment index order, into one contiguous
    /// frame arena reservation and queues it as one pending <see cref="SessionEventKind.MessageReceived"/>
    /// (issue #259) — the same three steps <see cref="DeliverToConsumer"/> takes for an unfragmented
    /// record, taken once for the whole group rather than once per piece, because the consumer is owed
    /// one whole Message and not <paramref name="fragmentCount"/> partial ones. Releases the group's
    /// window slots (<see cref="ReceiveWindowSpaces.ReleaseGroup"/>) only on success: a reservation
    /// that fails leaves every piece exactly where <see cref="HoldFragment"/> put it, nothing is
    /// counted as received, and the caller is responsible for retrying — see
    /// the Space's <see cref="PendingFragmentGroups"/>, the Space's <see cref="PendingFragmentGroups"/>
    /// and <see cref="RetryPendingFragmentDeliveries"/>.
    /// </summary>
    /// <remarks>
    /// One reservation for the whole group, not one per piece, is deliberate: a consumer that Sent one
    /// Message is owed one <see cref="SessionEventKind.MessageReceived"/>, not a run of them that
    /// happens to reassemble into the same bytes if concatenated. The per-share-holder escape
    /// <see cref="FrameArena.Reserve(int, int)"/> already grants a share-holder with nothing else
    /// reserved this Frame — a Peer that has delivered nothing yet — applies here exactly as it does
    /// for a single record: a reassembled Message at <see cref="DatagramSessionOptions.MaxMessageSize"/>
    /// can exceed this Peer's fair share of the frame arena and still be delivered whole, which is what
    /// makes such a Message reachable at all rather than permanently stuck behind a share bound sized
    /// for ordinary traffic.
    /// <para>
    /// A group that assembles past <see cref="DatagramSessionOptions.MaxMessageSize"/> is discarded here,
    /// when its turn comes, rather than when it completes (issue #425). Its slots are released exactly as
    /// a delivery releases them and its sequences are finished with, so a <c>ReliableOrdered</c> Space
    /// moves past it in send order.
    /// </para>
    /// </remarks>
    /// <returns>
    /// <see langword="true"/> if the group was delivered or discarded, so it is no longer waiting;
    /// <see langword="false"/> if the reservation failed.
    /// </returns>
    private bool TryDeliverFragmentGroup(PeerSlot slot, PacketRecordMode mode, uint groupFirstSequence, ushort fragmentCount)
    {
        ReceiveWindowSpaces windows = _receiveWindows[slot.Handle.Index];
        int spaceIndex = ReliableSpaceIndex(mode);

        int totalLength = 0;
        for (int i = 0; i < fragmentCount; i++)
        {
            uint sequence = unchecked(groupFirstSequence + (uint)i);
            if (!windows.TryGetRingSlot(mode, sequence, out int ringSlot))
            {
                // A group HoldFragment/IsGroupComplete just reported complete should always resolve
                // every one of its own pieces; if window state ever moved out from under it before
                // this ran, there is nothing safe to deliver — treat it as not ready rather than throw
                // or deliver a partial Message, and let a later Pump's retry re-derive the answer.
                return false;
            }

            totalLength += windows.GetFragmentLength(mode, ringSlot);
        }

        if (RefuseOverLimit(slot, totalLength))
        {
            windows.ReleaseGroup(mode, groupFirstSequence, fragmentCount);
            return true;
        }

        int payloadOffset = _arena.BytesUsed;
        ArenaReservation reservation = _arena.Reserve(slot.Handle.Index, totalLength);
        if (!AccountForReservation(slot, reservation))
        {
            return false;
        }

        Span<byte> destination = reservation.Payload;
        int destinationOffset = 0;
        for (int i = 0; i < fragmentCount; i++)
        {
            uint sequence = unchecked(groupFirstSequence + (uint)i);
            windows.TryGetRingSlot(mode, sequence, out int ringSlot);
            int length = windows.GetFragmentLength(mode, ringSlot);
            ReadOnlySpan<byte> fragmentPayload = _receiveArena.GetFragmentSlot(slot.Handle.Index, spaceIndex, ringSlot)[..length];
            fragmentPayload.CopyTo(destination.Slice(destinationOffset, length));
            destinationOffset += length;
        }

        windows.ReleaseGroup(mode, groupFirstSequence, fragmentCount);

        _pendingEvents.Add(new PendingEvent
        {
            Kind = SessionEventKind.MessageReceived,
            Peer = ToPeerId(slot.Handle),
            PayloadOffset = payloadOffset,
            PayloadLength = totalLength,
        });

        slot.Counters.RecordMessageReceived();
        return true;
    }

    /// <summary>
    /// Drains this Peer's <c>ReliableOrdered</c> send-order queue — every payload
    /// <see cref="OrderedDeliveryQueue.TryPeekNext"/> now yields, in order — and then attempts
    /// the Space's <see cref="PendingFragmentGroups"/> if that drain reached its first sequence
    /// (issue #259). The two are interleaved rather than run once each, because delivering a group
    /// advances the queue past the whole span it occupies
    /// (<see cref="OrderedDeliveryQueue.SkipTo"/>), which can itself unblock a further run of ordinary
    /// records that was held behind the group's own gap; the loop keeps alternating until a full pass
    /// delivers nothing further.
    /// </summary>
    private void DrainOrderedDelivery(PeerSlot slot)
    {
        while (true)
        {
            // Nothing behind a delivered Goodbye is ever delivered (issue #400).
            if (slot.RemoteGoodbyeDelivered)
            {
                return;
            }

            uint heldSequence = slot.OrderedDelivery.NextSequence;
            while (slot.OrderedDelivery.TryPeekNext(out ReadOnlyMemory<byte> held))
            {
                if (slot.RemoteGoodbyeSequence == heldSequence)
                {
                    slot.OrderedDelivery.ReleaseNext();
                    OnGoodbyeDelivered(slot);
                    return;
                }

                // A held record is released only once the arena has taken it. One the arena refuses
                // stays first in line, and RetryPendingFragmentDeliveries tries it again next Pump: it
                // was admitted and acknowledged when it arrived, so no retransmission will bring it back.
                if (!DeliverToConsumer(slot, held.Span))
                {
                    return;
                }

                slot.OrderedDelivery.ReleaseNext();
                heldSequence = slot.OrderedDelivery.NextSequence;
            }

            if (!TryFlushPendingOrderedFragmentGroup(slot))
            {
                return;
            }
        }
    }

    /// <summary>
    /// Delivers the Space's <see cref="PendingFragmentGroups"/> if it is both present and its Space's
    /// ordering queue has reached exactly its first sequence — the group's turn, in the same send-order
    /// rule an unfragmented <c>ReliableOrdered</c> record already obeys (issue #259). A pending group
    /// whose turn has not yet arrived, because some earlier sequence in the Space is still missing, is
    /// left exactly as it is: its pieces stay held in the receive window and nothing here disturbs them.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> if a pending group was both due and successfully delivered — the signal
    /// <see cref="DrainOrderedDelivery"/> uses to keep looping, since delivering a group can unblock
    /// more of the ordinary queue behind it; <see langword="false"/> otherwise, whether because nothing
    /// was pending, it was not yet due, or its reservation failed and it remains pending for a later
    /// Pump.
    /// </returns>
    private bool TryFlushPendingOrderedFragmentGroup(PeerSlot slot)
    {
        PendingFragmentGroups pending = PendingGroupsOf(slot, PacketRecordMode.ReliableOrdered);
        uint firstSequence = slot.OrderedDelivery.NextSequence;
        if (!pending.TryGetDueAt(firstSequence, out ushort fragmentCount))
        {
            return false;
        }

        if (!TryDeliverFragmentGroup(slot, PacketRecordMode.ReliableOrdered, firstSequence, fragmentCount))
        {
            return false;
        }

        pending.Remove(firstSequence);
        slot.OrderedDelivery.SkipTo(unchecked(firstSequence + fragmentCount));
        return true;
    }

    /// <summary>
    /// Retries every open Peer's Fragment group still waiting to be delivered (issue #259): one whose
    /// frame-arena reservation failed the moment it completed, in either reliable Space. Also drains
    /// each Peer's <c>ReliableOrdered</c> queue, whose head may be a held record the arena refused on
    /// an earlier Pump. Driven once
    /// per Pump, right after the datagram-processing pass that both completes new groups and can
    /// advance a <c>ReliableOrdered</c> group past a gap — this Reactor's own send path never frees
    /// receive-side room, so only a fresh Frame's wiped arena (<see cref="FrameArena.Reset"/>, at the
    /// top of the next <see cref="Pump"/>) can turn a previously failed reservation into one that
    /// succeeds. Allocates nothing: it walks the fixed-size Slot table by index exactly as the stream
    /// profile's own per-Pump sweep does, rather than tracking a separate list of Peers with something
    /// pending.
    /// </summary>
    private void RetryPendingFragmentDeliveries()
    {
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is not PeerSlot slot)
            {
                continue;
            }

            DrainOrderedDelivery(slot);

            // Walks the dense prefix backwards, so a delivered group's removal — which moves the last
            // entry down into the freed position — can never skip an entry this pass has not yet tried.
            PendingFragmentGroups unordered = PendingGroupsOf(slot, PacketRecordMode.ReliableUnordered);
            for (int pending = unordered.Count - 1; pending >= 0; pending--)
            {
                (uint firstSequence, ushort fragmentCount) = unordered.At(pending);
                if (TryDeliverFragmentGroup(slot, PacketRecordMode.ReliableUnordered, firstSequence, fragmentCount))
                {
                    unordered.Remove(firstSequence);
                }
            }
        }
    }

    /// <summary>
    /// Copies an admitted record's payload into the shared frame arena under this Peer's own
    /// share-holder identity (ADR 0014), queues it as a pending
    /// <see cref="SessionEventKind.MessageReceived"/>, and raises this Peer's
    /// <see cref="PeerCounters.MessagesReceived"/> — the same three steps, in the same order, that
    /// <c>StreamSessionReactor.DeliverCompleteMessages</c> takes, so the two profiles report a received
    /// Message identically (subtask 6.6.5, issue #217).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>A Message the arena had no room for is not counted as received.</b> The counter moves only
    /// where the bytes did, which is what makes a share exhaustion visible: a Frame in which records
    /// plainly arrived but this figure did not move is a Message the arena refused or one over
    /// <see cref="DatagramSessionOptions.MaxMessageSize"/> (<see cref="DiagnosticId.OversizedMessage"/>),
    /// not one the network lost. An unreliable record the arena refuses is gone. A reliable one is never refused here: one
    /// delivered straight from its datagram was checked for room before it was admitted (see
    /// <see cref="RefusesForArenaRoom"/>) and is otherwise left unacknowledged for the far end to
    /// retransmit, and a <c>ReliableOrdered</c> record released from behind a gap stays held in its
    /// queue until a later Pump's arena takes it.
    /// </para>
    /// <para>
    /// <b>The one rule applied here is <c>UnreliableSequenced</c>'s</b> (subtask 6.6.11, issue #221):
    /// a record is delivered only if it is strictly newer than the highest that Space has already
    /// delivered. Admission and delivery part company for this mode alone — a record reordered behind a
    /// newer one is a perfectly ordinary arrival as far as the receive window is concerned, and
    /// discarding it is the whole reason the mode exists. There is deliberately no tolerance window for
    /// recovering such a record: the rule is stated in a sentence with no exceptions — you never
    /// receive an older update after a newer one — and a tolerance would break that sentence to
    /// recover exactly what the mode is for discarding.
    /// </para>
    /// <para>
    /// <b>Plain <c>Unreliable</c>'s rule is bounded duplicate suppression</b> (subtask 6.6.12, issue
    /// #222; ADR 0017): a record the Peer's own <see cref="UnreliableDuplicateWindow"/> has already
    /// seen is suppressed, and one delayed past that window's 64 sequences is delivered. That window
    /// is deliberately not the receive window — 64 is a protocol constant, while the receive window's
    /// capacity is a consumer option — so the two are asked separately and a suppression here is
    /// counted as a duplicate exactly as one the receive window caught is.
    /// </para>
    /// <para>
    /// <b><c>ReliableOrdered</c>'s rule is send order</b> (subtask 6.6.10, issue #220): the record goes
    /// through this Peer's <see cref="PeerSlot.OrderedDelivery"/> queue, which holds it whenever an
    /// earlier sequence in that Space has not arrived and releases the whole contiguous run behind the
    /// gap the moment it fills. Held bytes reach the consumer only when they drain, so a Message waiting
    /// on a hole is not counted as received until it does. The queue is per-Peer and per-Space, so a
    /// hole here stalls nothing in the other three Spaces — that is what the four-Space decision buys.
    /// <c>ReliableUnordered</c> takes no queue at all and is delivered exactly as the window admitted it.
    /// </para>
    /// </remarks>
    private void DeliverAdmittedRecord(PeerSlot slot, PacketRecordMode mode, uint sequence, ReadOnlySpan<byte> payload)
    {
        if (mode == PacketRecordMode.ReliableOrdered)
        {
            if (!slot.OrderedDelivery.Admit(sequence, payload))
            {
                // Held behind a gap, or a duplicate of a sequence already released. Either way nothing
                // reaches the consumer for this record now; a held one drains on the Pump that fills
                // the hole ahead of it.
                return;
            }

            if (!DeliverOrderedRecord(slot, sequence, payload))
            {
                DrainOrderedDelivery(slot);
            }

            return;
        }

        if (mode == PacketRecordMode.UnreliableSequenced)
        {
            if (slot.HighestDeliveredUnreliableSequenced is uint highest
                && !SequenceNumber.IsNewerThan(sequence, highest))
            {
                return;
            }

            // Advanced before the delivery attempt, not after it: a record this Space has decided is
            // the newest stays the newest whether or not the arena had room for it. Advancing only on a
            // successful reservation would let a Message refused for room be superseded by an older one
            // that happened to fit, which is the one thing the mode promises cannot happen. A record
            // over MaxMessageSize advances it for the same reason.
            slot.HighestDeliveredUnreliableSequenced = sequence;
        }

        DeliverToConsumer(slot, payload);
    }

    /// <summary>
    /// Releases one <c>ReliableOrdered</c> record that has reached its turn in send order: a Message goes
    /// to <see cref="DeliverToConsumer"/>, and the Goodbye recorded in
    /// <see cref="PeerSlot.RemoteGoodbyeSequence"/> goes to <see cref="OnGoodbyeDelivered"/> instead.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when the record was the Goodbye, in which case the caller must not
    /// deliver anything further to this Peer.
    /// </returns>
    private bool DeliverOrderedRecord(PeerSlot slot, uint sequence, ReadOnlySpan<byte> payload)
    {
        if (slot.RemoteGoodbyeSequence == sequence)
        {
            OnGoodbyeDelivered(slot);
            return true;
        }

        DeliverToConsumer(slot, payload);
        return false;
    }

    /// <summary>
    /// The remote's Goodbye has reached its turn in send order (issues #400, #401), so every Message
    /// sent before it has been delivered. If none of them is still undelivered — see
    /// <see cref="HasUndeliveredMessages"/> — the Peer closes at once with
    /// <see cref="DisconnectReason.PeerGoodbye"/> and no <see cref="SessionEventKind.PeerClosing"/> is
    /// raised. Otherwise the Peer enters receiver-side Closing: <see cref="SessionEventKind.PeerClosing"/>
    /// is queued after those Messages, and <see cref="MarkDrainedClosingPeersCloseDue"/> and
    /// <see cref="ClosePeersWithCloseDue"/> raise <see cref="SessionEventKind.PeerClosed"/> on a later Pump. This mirrors the stream profile's
    /// Goodbye handling.
    /// </summary>
    private void OnGoodbyeDelivered(PeerSlot slot)
    {
        slot.RemoteGoodbyeSequence = null;

        // Both ends closing at once: this end's own Close is already in progress, so the remote's Goodbye is
        // acknowledged and nothing more. The reason stays LocalClose (or LingerExpired), whichever end's
        // Goodbye reached the wire first.
        if (slot.IsClosing)
        {
            slot.RemoteClose = RemoteCloseState.AckDue;
            return;
        }

        if (!HasUndeliveredMessages(slot))
        {
            // Closed after this Pump's flush, not here: the read pass is still running and the ack of the
            // Goodbye (issue #398) has to reach the wire before the Slot that owns its ack state is freed.
            slot.RemoteClose = RemoteCloseState.CloseDue;
            return;
        }

        slot.RemoteClose = RemoteCloseState.Closing;
        _pendingEvents.Add(new PendingEvent
        {
            Kind = SessionEventKind.PeerClosing,
            Peer = ToPeerId(slot.Handle),
        });
    }

    /// <summary>
    /// Whether <paramref name="slot"/>'s Peer still has Messages the consumer has not been handed:
    /// a <see cref="SessionEventKind.MessageReceived"/> queued for it this Frame (the consumer has not
    /// drained this Frame yet, since <see cref="Pump"/> clears undrained events at its start), or a
    /// completed <c>ReliableUnordered</c> Fragment group deferred for lack of arena room. Deferred
    /// <c>ReliableOrdered</c> groups are not counted: a Goodbye reaching its turn means none is ahead of it.
    /// </summary>
    private bool HasUndeliveredMessages(PeerSlot slot)
    {
        if (PendingGroupsOf(slot, PacketRecordMode.ReliableUnordered).Count > 0)
        {
            return true;
        }

        PeerId peer = ToPeerId(slot.Handle);
        for (int i = 0; i < _pendingEvents.Count; i++)
        {
            if (_pendingEvents[i].Kind == SessionEventKind.MessageReceived && _pendingEvents[i].Peer == peer)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Drain-then-close, step one: moves every Peer in receiver-side Closing
    /// (<see cref="RemoteCloseState.Closing"/>) that has nothing deferred left to
    /// <see cref="RemoteCloseState.CloseDue"/>, so <see cref="ClosePeersWithCloseDue"/> closes it with
    /// <see cref="DisconnectReason.PeerGoodbye"/> at the end of this Pump. Run at the start of each Pump,
    /// after the previous Frame's events are cleared, so a Peer that entered Closing this Frame stays
    /// Closing until the consumer has been able to drain its Messages and
    /// <see cref="SessionEventKind.PeerClosing"/>. A Peer with nothing to drain is already
    /// <see cref="RemoteCloseState.CloseDue"/> in the Pump its Goodbye arrived.
    /// </summary>
    private void MarkDrainedClosingPeersCloseDue()
    {
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is PeerSlot { RemoteClose: RemoteCloseState.Closing } slot
                && PendingGroupsOf(slot, PacketRecordMode.ReliableUnordered).Count == 0)
            {
                slot.RemoteClose = RemoteCloseState.CloseDue;
            }
        }
    }

    /// <summary>
    /// Closes every Peer that is <see cref="RemoteCloseState.CloseDue"/> with
    /// <see cref="DisconnectReason.PeerGoodbye"/>, after <see cref="FlushOutboundDatagrams"/> (issue #398).
    /// <see cref="FlushPeer"/> sent such a Peer one final datagram even with nothing queued, carrying the
    /// ack of the Goodbye, so the sender learns it was received before the Slot that owns that ack state
    /// is freed. <see cref="SessionEventKind.PeerClosed"/> follows in the same Pump with no
    /// <see cref="SessionEventKind.PeerClosing"/> on the direct path.
    /// </summary>
    private void ClosePeersWithCloseDue()
    {
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is PeerSlot { RemoteClose: RemoteCloseState.CloseDue } slot)
            {
                ClosePeer(slot.Handle, DisconnectReason.PeerGoodbye);
            }
        }
    }

    /// <summary>
    /// Answers whether <paramref name="reservation"/> committed, and charges whichever outcome it had:
    /// a commit raises the Arena high-water, a spent read share is counted on the Peer, and an Arena
    /// with no room is a <see cref="DiagnosticId.ArenaExhausted"/> record (issue #61).
    /// </summary>
    private bool AccountForReservation(PeerSlot slot, ArenaReservation reservation)
    {
        switch (reservation.Outcome)
        {
            case ArenaReservationOutcome.Committed:
                _counters.ObserveArenaUsage(_arena.BytesUsed);
                return true;
            case ArenaReservationOutcome.ShareExhausted:
                slot.Counters.RecordReadShareExhaustion();
                return false;
            case ArenaReservationOutcome.ArenaExhausted:
                if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
                {
                    _diagnostics.Log(DiagnosticRecord.ForArenaExhausted(
                        ToPeerId(slot.Handle),
                        reservation.BytesRequested,
                        reservation.BytesFree));
                }

                return false;
        }

        return false;
    }

    /// <summary>
    /// The last step every mode's delivery rule ends at: copy the payload into the shared frame arena
    /// under this Peer's own share-holder identity (ADR 0014), queue it as a pending
    /// <see cref="SessionEventKind.MessageReceived"/>, and raise this Peer's
    /// <see cref="PeerCounters.MessagesReceived"/>. Split out from
    /// <see cref="DeliverAdmittedRecord"/> so a <c>ReliableOrdered</c> run released from behind a gap
    /// reaches the consumer through exactly the same three steps, in the same order, as a record
    /// delivered the moment it arrived (subtask 6.6.10, issue #220).
    /// </summary>
    /// <remarks>
    /// A Message the arena had no room for is not counted as received; the reasoning is on
    /// <see cref="DeliverAdmittedRecord"/>.
    /// </remarks>
    /// <returns>
    /// <see langword="false"/> when the arena refused the Message, so a caller still holding it can
    /// keep it for a later Pump. A Message discarded for being over the limit returns
    /// <see langword="true"/>: no later Pump would change the answer.
    /// </returns>
    private bool DeliverToConsumer(PeerSlot slot, ReadOnlySpan<byte> payload)
    {
        if (RefuseOverLimit(slot, payload.Length))
        {
            return true;
        }

        int payloadOffset = _arena.BytesUsed;
        ArenaReservation reservation = _arena.Reserve(slot.Handle.Index, payload);
        if (!AccountForReservation(slot, reservation))
        {
            return false;
        }

        _pendingEvents.Add(new PendingEvent
        {
            Kind = SessionEventKind.MessageReceived,
            Peer = ToPeerId(slot.Handle),
            PayloadOffset = payloadOffset,
            PayloadLength = payload.Length,
        });

        slot.Counters.RecordMessageReceived();
        return true;
    }

    /// <summary>
    /// Applies this Reactor's own <see cref="DatagramSessionOptions.MaxMessageSize"/> to a completed
    /// Message (issue #425), reporting one over it with <see cref="DiagnosticId.OversizedMessage"/>. The
    /// receive window bounds what is held, not what is assembled, and a whole record is bounded only by
    /// the Datagram it came in, so this is what makes the announced limit true of what a consumer receives.
    /// </summary>
    /// <remarks>
    /// The Session stays open. A reliable Send above the limit is refused at the sending end, so only a
    /// peer ignoring the announcement reaches this with a reliable Message. The unreliable modes are
    /// capped by one Datagram at that end and not by the announcement, so a conforming peer can reach
    /// this with one of those, which is not a violation worth closing over.
    /// </remarks>
    /// <returns><see langword="true"/> when the Message is over the limit and must not be delivered.</returns>
    private bool RefuseOverLimit(PeerSlot slot, int messageLength)
    {
        if (messageLength <= _options.MaxMessageSize)
        {
            return false;
        }

        if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
        {
            _diagnostics.Log(DiagnosticRecord.ForOversizedMessage(ToPeerId(slot.Handle), messageLength, _options.MaxMessageSize));
        }

        return true;
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
                out ulong remoteSendIdleIntervalMilliseconds,
                out ulong remoteLingerMilliseconds,
                remoteResumeSecret))
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        if (!_cookies.Verify(source, clientNonce, epoch, cookie))
        {
            _counters.RecordUnknownSourceDatagram();
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
            SendAccept(link, established, source);
            return;
        }

        TimeSpan remoteSendIdleInterval = TimeSpan.FromMilliseconds(remoteSendIdleIntervalMilliseconds);
        TimeSpan remoteLinger = TimeSpan.FromMilliseconds(remoteLingerMilliseconds);
        TimeSpan announcedLinger = TimeSpan.FromMilliseconds(AnnouncedLingerMilliseconds);
        if (_peers.TryAdd(NewAcceptedPeer(
                source, link, remoteSessionId, remoteMaxMessageSize, remoteSendIdleInterval, remoteLinger, announcedLinger, remoteResumeSecret, clientNonce))
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
        _receiveWindows[handle.Index].Reset();
        _retention.ResetShareHolder(handle.Index);
        _peersBySessionId[slot.LocalSessionId] = handle;
        _acceptedHandshakes[(source, clientNonce)] = handle;
        PeerId peer = ToPeerId(handle);
        _pendingEvents.Add(new PendingEvent { Kind = SessionEventKind.PeerConnected, Peer = peer });

        if (_diagnostics.IsEnabled(DiagnosticLevel.Info))
        {
            _diagnostics.Log(DiagnosticRecord.ForLingerNegotiated(
                peer, (long)AnnouncedLingerMilliseconds, (long)slot.EffectiveLinger.TotalMilliseconds));
        }

        SendAccept(link, slot, source);
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
        slot.HelloWithCookieSentTimestamp = _options.Reactor.TimeProvider.GetTimestamp();
        ControlDatagramCodec.WriteHelloWithCookie(
            _sendBuffer,
            DatagramSessionProtocol.Version,
            clientNonce,
            epoch,
            cookie,
            slot.LocalSessionId,
            (uint)_options.MaxMessageSize,
            AnnouncedSendIdleIntervalMilliseconds,
            AnnouncedLingerMilliseconds,
            slot.LocalResumeSecret);
        SendControl(link, source);
    }

    /// <summary>
    /// Establishes the dialling end's Session. Each end announces the id it wants stamped on datagrams
    /// sent <em>to it</em>, so the two ids differ and neither end has to agree with the other about
    /// which is which. The fragment size is not announced: it is each sender's own path property, and
    /// a receiver that reassembles by offset and length never needs to know it.
    /// </summary>
    /// <remarks>
    /// Also seeds the Peer's round-trip estimate with the time since its Hello-with-cookie was sent
    /// (<see cref="PeerSlot.HelloWithCookieSentTimestamp"/>), so the first reliable record has a real
    /// retransmission timeout rather than one equal to the Linger (ADR 0018). A zero-length measurement is
    /// not sampled.
    /// </remarks>
    private void OnAccept(Source source, ReadOnlySpan<byte> datagram)
    {
        Span<byte> remoteResumeSecret = stackalloc byte[DatagramSessionProtocol.ResumeSecretLength];
        if (!ControlDatagramCodec.TryReadAccept(
                datagram,
                out ulong clientNonce,
                out uint remoteSessionId,
                out uint remoteMaxMessageSize,
                out ulong remoteSendIdleIntervalMilliseconds,
                out ulong remoteLingerMilliseconds,
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

        // ControlDatagramCodec.TryReadAccept rejects a zero-announced maxMessageSize, send-idle
        // interval or Linger at parse (issue #287, issue #348, issue #362), so all three values are
        // already trusted by the time this Peer reaches Established — an internal invariant to assert
        // rather than input to re-validate, mirroring NewAcceptedPeer's asserts on the accepting path.
        Debug.Assert(
            remoteMaxMessageSize != 0,
            "ControlDatagramCodec.TryReadAccept rejects a zero-announced maxMessageSize before this is called.");
        Debug.Assert(
            remoteSendIdleIntervalMilliseconds != 0,
            "ControlDatagramCodec.TryReadAccept rejects a zero-announced send-idle interval before this is called.");
        Debug.Assert(
            remoteLingerMilliseconds != 0,
            "ControlDatagramCodec.TryReadAccept rejects a zero-announced Linger before this is called.");

        TimeSpan remoteLinger = TimeSpan.FromMilliseconds(remoteLingerMilliseconds);
        TimeSpan announcedLinger = TimeSpan.FromMilliseconds(AnnouncedLingerMilliseconds);

        slot.RemoteSessionId = remoteSessionId;
        slot.RemoteMaxMessageSize = remoteMaxMessageSize;
        slot.RemoteSendIdleInterval = TimeSpan.FromMilliseconds(remoteSendIdleIntervalMilliseconds);
        slot.RemoteLinger = remoteLinger;
        slot.EffectiveLinger = announcedLinger < remoteLinger ? announcedLinger : remoteLinger;
        slot.RemoteResumeSecret = remoteResumeSecret.ToArray();
        slot.State = HandshakeState.Established;

        // Both idle-timer timestamps start counting from the moment this Peer exists, not from
        // default(long) — which would read as an interval already elapsed. The Accept that just
        // arrived is itself received activity, so LastReceiveTimestamp is stamped here rather than
        // left to wait for the first post-establishment data datagram (issue #351), mirroring
        // StreamSessionReactor.OnHelloRecordReceived's same reasoning for the stream profile.
        long now = _options.Reactor.TimeProvider.GetTimestamp();
        slot.LastSendTimestamp = now;
        slot.LastReceiveTimestamp = now;

        // Seed the round-trip estimate from Hello-with-cookie -> Accept. Unseeded, the RTO equals the
        // Linger, so the Peer's first reliable record (a Goodbye included) could be resent only once
        // per Linger and never inside it. The handshake resends nothing, so this leg is never ambiguous.
        // A leg that measures zero ticks is below the clock's resolution (a frozen clock, never a real
        // network) and says nothing about the round trip, so it leaves the estimator unseeded.
        TimeSpan handshakeRoundTrip = _options.Reactor.TimeProvider.GetElapsedTime(slot.HelloWithCookieSentTimestamp, now);
        if (handshakeRoundTrip > TimeSpan.Zero)
        {
            slot.RetransmitTimer.Estimator.Sample(handshakeRoundTrip);
        }

        _dialsByNonce.Remove(clientNonce);
        _peersBySessionId[slot.LocalSessionId] = slot.Handle;
        PeerId peer = ToPeerId(slot.Handle);
        _pendingEvents.Add(new PendingEvent { Kind = SessionEventKind.PeerConnected, Peer = peer });

        if (_diagnostics.IsEnabled(DiagnosticLevel.Info))
        {
            _diagnostics.Log(DiagnosticRecord.ForLingerNegotiated(
                peer, (long)AnnouncedLingerMilliseconds, (long)slot.EffectiveLinger.TotalMilliseconds));
        }
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
    /// Routes a Resume by the session id in its body. A datagram that does not read as a Resume is
    /// discarded like any other malformed control datagram. A target that names no Peer this Reactor
    /// holds, or names a dialled Peer (<see cref="PeerSlot.IsDialled"/>, never a Resume target), logs
    /// <see cref="DiagnosticId.ResumeUnmatched"/> against the Source with <c>Value0</c> 0
    /// (no reservation was checked) and <c>Value1</c> 6 (the Resume record type), and changes nothing:
    /// no event is raised and no Peer is touched. The session id only routes (ADR 0009): a target that
    /// names a Peer must also present that Peer's <see cref="PeerSlot.LocalResumeSecret"/>, compared in
    /// constant time, and a mismatch logs <see cref="DiagnosticId.ResumeUnmatched"/> with <c>Value0</c> 1
    /// and leaves the Peer exactly as it was. A matching secret aimed at a Peer in Closing, either side
    /// (<see cref="PeerSlot.IsClosing"/> or <see cref="PeerSlot.RemoteGoodbyeDelivered"/>), is refused
    /// the same way with <c>Value0</c> 1.
    /// </summary>
    private void OnResume(UdpLink link, Source source, ReadOnlySpan<byte> datagram)
    {
        Span<byte> presentedSecret = stackalloc byte[DatagramSessionProtocol.ResumeSecretLength];
        if (!ControlDatagramCodec.TryReadResume(datagram, out uint targetSessionId, presentedSecret))
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        // A dialled Peer is never a Resume target: only the dialling end sends Resume, since only it can
        // know its own new address. Its session id and secret both travel in its HelloWithCookie, so
        // matching them here would let an observer of the handshake move where the dialler sends.
        if (!_peersBySessionId.TryGetValue(targetSessionId, out SlotHandle handle)
            || _peers.TryResolve(handle) is not PeerSlot slot
            || slot.IsDialled)
        {
            LogResumeUnmatched(source, candidatesChecked: 0);
            return;
        }

        // Constant time, so how long the comparison takes does not say how many leading bytes of a
        // guess were right.
        if (!CryptographicOperations.FixedTimeEquals(presentedSecret, slot.LocalResumeSecret))
        {
            LogResumeUnmatched(source, candidatesChecked: 1);
            return;
        }

        // A Goodbye, sent (IsClosing) or received (RemoteGoodbyeDelivered), forbids a Resume: the Peer is
        // winding down, so moving its address or resuming it would be a silent success. The secret matched,
        // so this counts as one candidate checked, and nothing about the Peer changes.
        if (slot.IsClosing || slot.RemoteGoodbyeDelivered)
        {
            LogResumeUnmatched(source, candidatesChecked: 1);
            return;
        }

        ResumeInterruptedPeer(slot, link, source);
    }

    /// <summary>
    /// Points every later datagram to <paramref name="slot"/> at <paramref name="source"/>, the address
    /// a matched Resume arrived from, and sends them from <paramref name="link"/>, the bound socket it
    /// arrived on: that socket demonstrably reaches the new source, which the Peer's earlier socket may
    /// not (a different address family, or a different bound endpoint). Only a matched Resume calls
    /// this. The accepted-handshake map is keyed by <see cref="PeerSlot.HandshakeSource"/>, which does
    /// not move, so a replayed handshake from the original address still finds this Peer and
    /// <see cref="ClosePeer"/> still removes the entry it inserted.
    /// </summary>
    private static void MoveSendAddress(PeerSlot slot, UdpLink link, Source source)
    {
        slot.Remote = source;
        slot.Link = link;
    }

    /// <summary>
    /// The matched-Resume branch. For an Established Peer of any liveness it points sends at the
    /// Resume's source (<see cref="MoveSendAddress"/>), charges the Resume to
    /// <see cref="PeerCounters.BytesReceived"/> (ADR 0013), stamps <see cref="PeerSlot.LastReceiveTimestamp"/>
    /// so the receive-idle deadline restarts from the Resume, and logs
    /// <see cref="DiagnosticId.ResumeMatched"/> with <c>Value0</c> 0, since nothing is rewound on this
    /// profile: retransmission already re-sends anything unacknowledged. Sequence state, ack state,
    /// receive windows and held Fragments are not touched. If the Peer is
    /// <see cref="PeerSlot.IsInterrupted"/> it also leaves Interrupted, queues
    /// <see cref="SessionEventKind.PeerResumed"/> for the same <see cref="PeerId"/> it had before,
    /// generation included, and counts it once in <see cref="PeerCounters.Resumes"/>. Linger stops counting because the Peer is no longer Interrupted, and a later
    /// interruption stamps <see cref="PeerSlot.InterruptedTimestamp"/> afresh. A Peer this end does not
    /// consider Interrupted (the dialling end can notice an interruption first) raises no event and
    /// leaves <see cref="PeerCounters.Resumes"/> unchanged.
    /// </summary>
    private void ResumeInterruptedPeer(PeerSlot slot, UdpLink link, Source source)
    {
        if (slot.State != HandshakeState.Established)
        {
            return;
        }

        MoveSendAddress(slot, link, source);
        slot.Counters.RecordBytesReceived(DatagramSessionProtocol.ControlDatagramSize);

        // The Resume is hearing from the Peer. Pump reads datagrams before it runs the timers, so a
        // stale timestamp here would have EnforceReceiveIdleTimer interrupt the Peer again on this very Pump.
        slot.LastReceiveTimestamp = _options.Reactor.TimeProvider.GetTimestamp();

        PeerId peer = ToPeerId(slot.Handle);
        if (slot.IsInterrupted)
        {
            LeaveInterrupted(slot, peer);
        }

        if (_diagnostics.IsEnabled(DiagnosticLevel.Info))
        {
            _diagnostics.Log(DiagnosticRecord.ForResumeMatched(peer, bytesRewound: 0));
        }
    }

    /// <summary>
    /// Takes an <see cref="PeerSlot.IsInterrupted"/> Peer out of Interrupted, the steps both ends share
    /// (issues #380 and #383): clears the flag, counts one <see cref="PeerCounters.Resumes"/> and queues
    /// <see cref="SessionEventKind.PeerResumed"/> for <paramref name="peer"/>, the same
    /// <see cref="PeerId"/> it had before, generation included.
    /// </summary>
    private void LeaveInterrupted(PeerSlot slot, PeerId peer)
    {
        slot.IsInterrupted = false;
        slot.Counters.RecordResume();
        _pendingEvents.Add(new PendingEvent { Kind = SessionEventKind.PeerResumed, Peer = peer });
    }

    /// <summary>
    /// Logs <see cref="DiagnosticId.ResumeUnmatched"/> for a Resume that was not matched onto a Peer,
    /// with <c>Value1</c> the Resume record type (<see cref="DatagramControlType.Resume"/>).
    /// </summary>
    private void LogResumeUnmatched(Source source, long candidatesChecked)
    {
        if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
        {
            _diagnostics.Log(DiagnosticRecord.ForResumeUnmatched(source, candidatesChecked, (long)DatagramControlType.Resume));
        }
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
        TimeSpan remoteSendIdleInterval,
        TimeSpan remoteLinger,
        TimeSpan announcedLinger,
        ReadOnlySpan<byte> remoteResumeSecret,
        ulong clientNonce)
    {
        // ControlDatagramCodec.TryReadHelloWithCookie rejects a zero-announced maxMessageSize,
        // send-idle interval or Linger at parse (issue #287, issue #347, issue #361), so by the time a
        // Peer is built here, straight into Established, all three values are already trusted — an
        // internal invariant to assert rather than input to re-validate.
        Debug.Assert(
            remoteMaxMessageSize != 0,
            "ControlDatagramCodec.TryReadHelloWithCookie rejects a zero-announced maxMessageSize before this is called.");
        Debug.Assert(
            remoteSendIdleInterval > TimeSpan.Zero,
            "ControlDatagramCodec.TryReadHelloWithCookie rejects a zero-announced send-idle interval before this is called.");
        Debug.Assert(
            remoteLinger > TimeSpan.Zero,
            "ControlDatagramCodec.TryReadHelloWithCookie rejects a zero-announced Linger before this is called.");

        RetransmissionTimer retransmitTimer = new();
        long now = _options.Reactor.TimeProvider.GetTimestamp();
        TimeSpan effectiveLinger = announcedLinger < remoteLinger ? announcedLinger : remoteLinger;
        return new()
        {
            Remote = source,
            HandshakeSource = source,
            Link = link,
            ClientNonce = clientNonce,
            LocalSessionId = NextSessionId(),
            LocalResumeSecret = NextResumeSecret(),
            RemoteSessionId = remoteSessionId,
            RemoteMaxMessageSize = remoteMaxMessageSize,
            RemoteSendIdleInterval = remoteSendIdleInterval,
            RemoteLinger = remoteLinger,
            EffectiveLinger = effectiveLinger,
            RemoteResumeSecret = remoteResumeSecret.ToArray(),
            State = HandshakeState.Established,
            AckTracker = NewAckTracker(retransmitTimer),
            RetransmitTimer = retransmitTimer,
            StallSignature = new(_options.Reactor.TimeProvider),
            FragmentSize = _options.ResolvedFragmentSize,
            OrderedDelivery = new(_options.OrderedHoldSlots, _options.ResolvedFragmentSize),
            LastSendTimestamp = now,
            LastReceiveTimestamp = now,
        };
    }

    /// <summary>
    /// Builds a fresh outstanding-sequence tracker for a Peer newly admitted into a Slot (subtask
    /// 6.3.6, issue #188; subtask 6.4.7, issue #197), timed from this Reactor's own
    /// <see cref="ReactorOptions.TimeProvider"/> and backed by <paramref name="retransmitTimer"/> — the
    /// same instance stored on the Slot's own <see cref="PeerSlot.RetransmitTimer"/>, so the tracker's
    /// estimator samples and the Slot's own effective-RTO reads are two views of one timer, not two that
    /// happen to agree.
    /// </summary>
    private OutstandingReliableSequences NewAckTracker(RetransmissionTimer retransmitTimer) =>
        new(_options.Reactor.TimeProvider, retransmitTimer);

    /// <summary>
    /// Answers a Hello-with-cookie with the Peer's Accept, to <paramref name="destination"/>, the
    /// address that Hello came from. Not <see cref="PeerSlot.Remote"/>, which a matched Resume may
    /// have moved since: a replay from the original address is answered there.
    /// </summary>
    private void SendAccept(UdpLink link, PeerSlot slot, Source destination)
    {
        ControlDatagramCodec.WriteAccept(
            _sendBuffer,
            slot.ClientNonce,
            slot.LocalSessionId,
            (uint)_options.MaxMessageSize,
            AnnouncedSendIdleIntervalMilliseconds,
            AnnouncedLingerMilliseconds,
            slot.LocalResumeSecret);
        SendControl(link, destination);
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

    /// <summary>
    /// Discards a datagram nothing in which could be attributed to a Peer — a control datagram that
    /// did not read, a control reply answering no dial, or a session id naming no Session — and counts
    /// it on <see cref="ReactorCounters.UnknownSourceDatagrams"/>. A data datagram that failed to parse
    /// <em>after</em> its session id resolved goes to <see cref="DiscardMalformedForSession"/>
    /// instead (issue #275).
    /// </summary>
    private void DiscardMalformed(Source source, int bytesReceived)
    {
        _counters.RecordUnknownSourceDatagram();
        _diagnostics.Log(DiagnosticRecord.ForDatagramMalformed(source, bytesReceived));
    }

    /// <summary>
    /// Discards a data datagram that named a live Session and then failed to parse, counting it on
    /// <see cref="ReactorCounters.MalformedSessionDatagrams"/> rather than
    /// <see cref="ReactorCounters.UnknownSourceDatagrams"/> (issue #275): the id resolved, which is the
    /// only reason parsing was reached at all, so this is not traffic that named nobody. The
    /// Diagnostic is the same <see cref="DiagnosticId.DatagramMalformed"/> and stays Source-subjected
    /// — a body that did not parse is no proof the sender is the Peer whose id it carried, so neither
    /// the record nor the figure is charged to that Peer.
    /// </summary>
    private void DiscardMalformedForSession(Source source, int bytesReceived)
    {
        _counters.RecordMalformedSessionDatagram();
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
        RememberClosedSession(slot, reason);
        _dialsByNonce.Remove(slot.ClientNonce);
        if (slot.HandshakeSource is Source handshakeSource)
        {
            _acceptedHandshakes.Remove((handshakeSource, slot.ClientNonce));
        }
        _pendingEvents.Add(new PendingEvent
        {
            Kind = SessionEventKind.PeerClosed,
            Peer = ToPeerId(handle),
            Reason = reason,
        });
    }

    /// <summary>
    /// Remembers a Peer that just closed on a Goodbye for one effective Linger, so a resend of the remote's
    /// Goodbye can still be acknowledged (ADR 0017, second amendment). Applies when the remote may still be
    /// waiting for the ack of its Goodbye: a close with <see cref="DisconnectReason.PeerGoodbye"/>, or a
    /// <see cref="DisconnectReason.LocalClose"/> of a Closing Peer, whose remote Goodbye may not have been
    /// delivered yet. Writes one <see cref="ClosedSession"/> into the ring, overwriting the oldest entry when
    /// it is full, and holds no capacity Slot. A Closing Peer whose remote Goodbye was delivered but not yet
    /// acknowledged on the wire (<see cref="RemoteCloseState.AckDue"/>) has that ack sent here, since the
    /// Pump's flush no longer reaches its Slot.
    /// </summary>
    private void RememberClosedSession(PeerSlot slot, DisconnectReason reason)
    {
        bool goodbyeClose = reason == DisconnectReason.PeerGoodbye
            || (reason == DisconnectReason.LocalClose && slot.IsClosing);
        if (slot.State != HandshakeState.Established || !goodbyeClose)
        {
            return;
        }

        int ring = _closedSessionNext;
        _closedSessionNext = (ring + 1) % _closedSessions.Length;

        uint overwritten = _closedSessions[ring].LocalSessionId;
        if (overwritten != 0 && _closedSessionIndex.TryGetValue(overwritten, out int overwrittenRing) && overwrittenRing == ring)
        {
            _closedSessionIndex.Remove(overwritten);
        }

        ((uint Base, uint Bits) reliableOrdered, (uint Base, uint Bits) reliableUnordered) =
            _receiveWindows[slot.Handle.Index].GenerateAckBlocks();

        _closedSessions[ring] = new ClosedSession
        {
            LocalSessionId = slot.LocalSessionId,
            Remote = slot.Remote,
            RemoteSessionId = slot.RemoteSessionId,
            ReliableOrderedBase = reliableOrdered.Base,
            ReliableOrderedBits = reliableOrdered.Bits,
            ReliableUnorderedBase = reliableUnordered.Base,
            ReliableUnorderedBits = reliableUnordered.Bits,
            ClosedTimestamp = _options.Reactor.TimeProvider.GetTimestamp(),
            Linger = slot.EffectiveLinger,
        };
        _closedSessionIndex[slot.LocalSessionId] = ring;

        if (slot.RemoteClose == RemoteCloseState.AckDue)
        {
            SendClosedSessionAck(slot.Link, in _closedSessions[ring]);
        }
    }

    /// <summary>
    /// Answers a data datagram naming a recently closed Session with one header-only datagram carrying the
    /// stored ack blocks, and reports whether it did (ADR 0017, second amendment). Declines, so the caller
    /// discards as for any unknown session, unless the id has a live record inside its Linger, the source is
    /// the stored remote endpoint, the datagram is well-formed, and it carries at least one ReliableOrdered
    /// record. The last condition stops two closed ends answering each other's replies. The reply is the
    /// 20-byte header alone, never larger than the datagram that triggered it, and raises no event, takes no
    /// Slot and touches no per-Peer counter. The stored blocks acknowledge exactly one thing beyond what the
    /// Peer had received: the remote's Goodbye, when it is the next <c>ReliableOrdered</c> sequence
    /// (<see cref="ClosedSession.AcknowledgeGoodbye"/>). A Message is never acknowledged, since nothing is
    /// delivered after the close, so its sender ends <see cref="DisconnectReason.LingerExpired"/> honestly.
    /// </summary>
    private bool TryReAckClosedSession(UdpLink link, Source source, uint session, ReadOnlySpan<byte> datagram)
    {
        if (!_closedSessionIndex.TryGetValue(session, out int ring))
        {
            return false;
        }

        ref ClosedSession closed = ref _closedSessions[ring];
        if (_options.Reactor.TimeProvider.GetElapsedTime(closed.ClosedTimestamp) >= closed.Linger)
        {
            _closedSessionIndex.Remove(session);
            closed = default;
            return false;
        }

        if (closed.Remote != source || !DatagramHeaderCodec.TryReadHeader(datagram, out _, out _, out _, out _, out _))
        {
            return false;
        }

        ReadOnlySpan<byte> body = datagram[DatagramSessionProtocol.DatagramHeaderSize..];
        bool carriesReliableRecord = false;
        ReadOnlySpan<byte> remainder = body;
        while (remainder.Length > 0)
        {
            if (!PacketRecordCodec.TryRead(remainder, out PacketRecordMode mode, out _, out _, out _, out _, out _, out _, out int bytesConsumed))
            {
                return false;
            }

            carriesReliableRecord |= mode == PacketRecordMode.ReliableOrdered;
            remainder = remainder[bytesConsumed..];
        }

        if (!carriesReliableRecord)
        {
            return false;
        }

        remainder = body;
        while (remainder.Length > 0)
        {
            PacketRecordCodec.TryRead(remainder, out PacketRecordMode mode, out uint sequence, out _, out _, out _, out _, out bool isControl, out int bytesConsumed);
            if (mode == PacketRecordMode.ReliableOrdered && isControl)
            {
                closed.AcknowledgeGoodbye(sequence);
            }

            remainder = remainder[bytesConsumed..];
        }

        SendClosedSessionAck(link, in closed);
        return true;
    }

    /// <summary>Writes and sends the header-only ack datagram for <paramref name="closed"/> to its remote endpoint.</summary>
    private void SendClosedSessionAck(UdpLink link, in ClosedSession closed)
    {
        DatagramHeaderCodec.WriteHeader(
            _outboundDatagramBuffer,
            closed.RemoteSessionId,
            closed.ReliableOrderedBase,
            closed.ReliableOrderedBits,
            closed.ReliableUnorderedBase,
            closed.ReliableUnorderedBits);

        UdpLinkSendResult result = link.SendTo(closed.Remote, _outboundDatagramBuffer.AsSpan(0, DatagramSessionProtocol.DatagramHeaderSize));
        if (result.IsSuccess)
        {
            _counters.RecordDatagramSent();
        }
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
            if (candidate != DatagramSessionProtocol.ControlSession
                && !_peersBySessionId.ContainsKey(candidate)
                && !_closedSessionIndex.ContainsKey(candidate))
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

    private void EnsureUsable() => _lifecycle.EnsureUsable(this);

    private void EnsureNotInsideAPump() => _lifecycle.EnsureNotInsideAPump();

    private void EnsureCallingThreadIsConstructingThread() => _lifecycle.EnsureCallingThreadIsConstructingThread();

    private static PeerId ToPeerId(SlotHandle handle) => handle.ToPeerId();

    private static SlotHandle ToHandle(PeerId peer) => peer.ToHandle();

    /// <summary>The wire mode <paramref name="mode"/> names, for <see cref="Send"/> to allocate and route by.</summary>
    private static PacketRecordMode ToRecordMode(DeliveryMode mode) => mode switch
    {
        DeliveryMode.Unreliable => PacketRecordMode.Unreliable,
        DeliveryMode.UnreliableSequenced => PacketRecordMode.UnreliableSequenced,
        DeliveryMode.ReliableUnordered => PacketRecordMode.ReliableUnordered,
        DeliveryMode.ReliableOrdered => PacketRecordMode.ReliableOrdered,
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown delivery mode."),
    };

    /// <summary>
    /// The reliable Space index <see cref="_retention"/> addresses <paramref name="mode"/> under —
    /// <c>ReliableOrdered</c> first, <c>ReliableUnordered</c> second, the same order
    /// <see cref="ReceiveWindowSpaces.GenerateAckBlocks"/> and <see cref="ParseDataDatagram"/>'s ack
    /// application already use, so the send and receive sides never address the two reliable Spaces
    /// differently.
    /// </summary>
    private static int ReliableSpaceIndex(PacketRecordMode mode) => mode switch
    {
        PacketRecordMode.ReliableOrdered => 0,
        PacketRecordMode.ReliableUnordered => 1,
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Not a reliable delivery mode."),
    };

    /// <summary>
    /// One record <see cref="Send"/> queued on a Peer's Slot, awaiting that Peer's next flush
    /// (<see cref="FlushOutboundDatagrams"/>, issue #216): the wire mode and sequence
    /// <see cref="Send"/> already allocated, plus — only for the two unreliable modes — a copy of the
    /// Message's bytes. A reliable record carries no copy of its own and no record shape of its own
    /// either: its bytes, and whether it is a whole Message or one Fragment of a cut one (with which
    /// index and count), already live in <see cref="_retention"/> from the moment <see cref="Send"/>
    /// committed them, and <see cref="TryResolvePendingPayload"/> reads both back from there at flush
    /// time instead of duplicating them here (issue #250) — a queue entry rebuilding the shape itself
    /// is exactly the bug that ticket fixed. Nothing retains an unreliable Send's bytes anywhere else —
    /// plain <see cref="DeliveryMode.Unreliable"/> and <see cref="DeliveryMode.UnreliableSequenced"/>
    /// take no retention slot at all (issue #43) and are never a Fragment (ADR 0004) — so this is the
    /// simplest place that can hold them until the Pump that finally needs them: one small copy per
    /// unreliable Send, released the moment that Pump's flush clears <see cref="PeerSlot.PendingSends"/>.
    /// </summary>
    private readonly struct PendingOutboundRecord
    {
        /// <summary>The wire mode this record was allocated under.</summary>
        public required PacketRecordMode Mode { get; init; }

        /// <summary>The sequence number <see cref="Send"/> allocated for this record.</summary>
        public required uint Sequence { get; init; }

        /// <summary>
        /// Whether this queue entry is a resend rather than a first transmission (subtask 6.6.8, issue
        /// #218). What it decides at flush time is one thing: a first transmission arms its retransmit
        /// deadline through <see cref="OutstandingReliableSequences.RecordSent"/>, while a resend has
        /// already been re-armed by <see cref="OutstandingReliableSequences.ReArmExpired"/> at the single
        /// RTO its Peer's one backoff for that Pump produced. Recording a resend as newly sent would
        /// overwrite that deadline with a second one and count the sequence as outstanding twice.
        /// </summary>
        public bool IsRetransmission { get; init; }

        /// <summary>
        /// A copy of the Message's bytes for an unreliable record; <see langword="null"/> for a
        /// reliable one, whose bytes live in <see cref="_retention"/> instead.
        /// </summary>
        public byte[]? UnreliablePayload { get; init; }
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

    /// <summary>Where this end's own <see cref="Close"/> has got a Peer (issues #397 to #399).</summary>
    private enum LocalCloseState
    {
        /// <summary><see cref="Close"/> has not been called on this Peer.</summary>
        None,

        /// <summary>
        /// <see cref="Close"/> was called but the <c>ReliableOrdered</c> retention was full, so no Goodbye is
        /// queued yet. <see cref="QueueDeferredGoodbyes"/> retries it each Pump.
        /// </summary>
        GoodbyeDeferred,

        /// <summary>The Goodbye is queued and retained, and no ack has released it yet.</summary>
        GoodbyeUnacked,

        /// <summary>
        /// An ack released the Goodbye from retention. <see cref="CloseAckedGoodbyePeers"/> closes the Peer
        /// once no earlier <c>ReliableOrdered</c> record is outstanding either.
        /// </summary>
        GoodbyeAcked,
    }

    /// <summary>Where the remote's Goodbye has got a Peer (issues #400, #401).</summary>
    private enum RemoteCloseState
    {
        /// <summary>The remote's Goodbye has not been delivered in order.</summary>
        None,

        /// <summary>
        /// The Goodbye was delivered while Messages were still undelivered, so <see cref="SessionEventKind.PeerClosing"/>
        /// was raised. <see cref="MarkDrainedClosingPeersCloseDue"/> makes it <see cref="CloseDue"/> on a later Pump.
        /// </summary>
        Closing,

        /// <summary>
        /// The Goodbye was delivered with nothing undelivered. <see cref="ClosePeersWithCloseDue"/> closes the
        /// Peer after this Pump's flush, so the flush can carry the ack of the Goodbye.
        /// </summary>
        CloseDue,

        /// <summary>
        /// The Goodbye was delivered while this end's own <see cref="Close"/> was in progress. No
        /// <see cref="SessionEventKind.PeerClosing"/> or <see cref="DisconnectReason.PeerGoodbye"/> follows: the Peer
        /// keeps waiting for its own Goodbye's ack or its Linger. <see cref="FlushOutboundDatagrams"/> sends the
        /// ack of the remote Goodbye this Pump even with no records, then moves the Peer to <see cref="Acked"/>.
        /// </summary>
        AckDue,

        /// <summary>The ack of a remote Goodbye delivered during this end's own Close has been flushed.</summary>
        Acked,
    }

    /// <summary>
    /// What a Peer that closed on a Goodbye leaves behind for one effective Linger (ADR 0017, second
    /// amendment), so a resent Goodbye can still be acknowledged. A fixed-size value in
    /// <see cref="_closedSessions"/>, one entry per capacity Slot, never allocated per datagram. Both ack
    /// blocks are the final <see cref="ReceiveWindowSpaces.GenerateAckBlocks"/> values, extended by
    /// <see cref="AcknowledgeGoodbye"/> only when the remote's Goodbye arrives next in order.
    /// </summary>
    private struct ClosedSession
    {
        /// <summary>The session id this Reactor assigned, which the remote stamps on its datagrams. Zero marks an unused entry.</summary>
        public uint LocalSessionId;

        /// <summary>The only endpoint a reply is sent to.</summary>
        public Source Remote;

        /// <summary>The session id to stamp on the reply.</summary>
        public uint RemoteSessionId;

        public uint ReliableOrderedBase;

        public uint ReliableOrderedBits;

        public uint ReliableUnorderedBase;

        public uint ReliableUnorderedBits;

        /// <summary>When the Peer closed; the entry expires <see cref="Linger"/> later.</summary>
        public long ClosedTimestamp;

        /// <summary>The Peer's effective Linger.</summary>
        public TimeSpan Linger;

        /// <summary>
        /// Acknowledges the remote's Goodbye at <paramref name="sequence"/>, the only thing this record may
        /// acknowledge beyond its stored state: nothing is delivered once the Session is closed, so a Message
        /// must never be acked. It counts only when it is exactly the next <c>ReliableOrdered</c> sequence
        /// (<see cref="ReliableOrderedBase"/>), meaning every ordered record before it was delivered and the
        /// ack truthfully says its Goodbye reached its turn. A Goodbye behind a hole, or one already below the
        /// base, changes nothing. Uses the base-plus-32-bits shape of <see cref="ReceiveWindow.DescribeAsAckBlock"/>.
        /// </summary>
        public void AcknowledgeGoodbye(uint sequence)
        {
            if (sequence != ReliableOrderedBase)
            {
                return;
            }

            bool nextAlreadyReceived;
            do
            {
                ReliableOrderedBase = unchecked(ReliableOrderedBase + 1);
                nextAlreadyReceived = (ReliableOrderedBits & 1) != 0;
                ReliableOrderedBits >>= 1;
            }
            while (nextAlreadyReceived);
        }
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
        public required Source Remote { get; set; }

        /// <summary>
        /// The address and nonce an accepted Peer's handshake arrived from, which key
        /// <see cref="_acceptedHandshakes"/>. Unlike <see cref="Remote"/> it never moves, so the entry
        /// can always be found and removed when the Peer closes. <see langword="null"/> for a dialled Peer.
        /// </summary>
        public Source? HandshakeSource { get; init; }

        /// <summary>The bound socket this Peer's datagrams are sent from. Moves with <see cref="Remote"/> on a matched Resume.</summary>
        public required UdpLink Link { get; set; }

        /// <summary>The nonce that ties every datagram of this handshake to this Peer and no other.</summary>
        public required ulong ClientNonce { get; init; }

        /// <summary>
        /// <see cref="TimeProvider.GetTimestamp"/> at the moment a dialled Peer sent its
        /// Hello-with-cookie. <see cref="OnAccept"/> measures the Accept's arrival against it to seed
        /// <see cref="RetransmitTimer"/>'s estimator. The handshake never resends, so the request is
        /// sent exactly once and the sample is never ambiguous under Karn's rule. Unused on an accepted Peer.
        /// </summary>
        public long HelloWithCookieSentTimestamp { get; set; }

        /// <summary>The id this Reactor assigned, which the remote stamps on datagrams sent here.</summary>
        public required uint LocalSessionId { get; init; }

        /// <summary>The secret this Reactor will require to be presented before it resumes this Session.</summary>
        public required byte[] LocalResumeSecret { get; init; }

        /// <summary>The id the remote assigned, which this Reactor stamps on datagrams sent there.</summary>
        public uint RemoteSessionId { get; set; }

        /// <summary>
        /// The largest Message the remote said it will accept, announced during the handshake
        /// (issue #284). <see cref="DatagramSessionReactor.Send"/> refuses a reliable Message wider than
        /// this with <see cref="SendOutcome.RemoteMessageTooLarge"/> rather than sending bytes the far
        /// end has already said it cannot hold (issue #287) — which is why an unset value here would not
        /// refuse nothing, it would refuse everything: every reliable Send would exceed a limit of zero.
        /// Both Established paths, <see cref="OnHelloWithCookie"/> (via <see cref="NewAcceptedPeer"/>)
        /// and <see cref="OnAccept"/>, therefore reject a zero announcement off the wire before a Peer
        /// ever reaches Established, and each asserts or re-asserts the resulting non-zero invariant
        /// rather than defaulting a missing value to something permissive or something that refuses. The
        /// Datagram profile's wire-level Resume (issues #52, #56) carries no parameters: what looks like
        /// "surviving an interruption" here is a Session identified by its wire token rather than by
        /// address, so an outage, and the Resume that ends it, never tear down the
        /// <see cref="PeerSlot"/>. This field is never re-read or re-established — it simply was never
        /// lost, and a Resume only moves the send address.
        /// </summary>
        public uint RemoteMaxMessageSize { get; set; }

        /// <summary>
        /// The send-idle interval the peer itself announced during the handshake (issue #349) — never
        /// this end's own <see cref="DatagramSessionOptions.SendIdleInterval"/>. Set on both Established
        /// paths, <see cref="DatagramSessionReactor.NewAcceptedPeer"/> (from the Hello-with-cookie field,
        /// issue #347) and <see cref="DatagramSessionReactor.OnAccept"/> (from the Accept field, issue #348), from
        /// the value <see cref="ControlDatagramCodec.TryReadHelloWithCookie"/> and
        /// <see cref="ControlDatagramCodec.TryReadAccept"/> already reject as zero at parse, so it is
        /// always positive once a Peer reaches Established. The stream profile's
        /// <see cref="StreamSessionReactor.PeerSlot.RemoteSendIdleInterval"/> is the model this mirrors.
        /// <see cref="DatagramSessionReactor.HasExceededReceiveIdleDeadline"/> multiplies it by the
        /// missed-heartbeat count to get this Peer's receive-idle deadline.
        /// </summary>
        public TimeSpan RemoteSendIdleInterval { get; set; }

        /// <summary>
        /// The Linger the peer itself announced during the handshake (issue #361, issue #362) — never
        /// this end's own <see cref="DatagramSessionOptions.Linger"/>. Set on both Established paths,
        /// <see cref="DatagramSessionReactor.NewAcceptedPeer"/> (from the Hello-with-cookie field) and
        /// <see cref="DatagramSessionReactor.OnAccept"/> (from the Accept field), from the value
        /// <see cref="ControlDatagramCodec.TryReadHelloWithCookie"/> and
        /// <see cref="ControlDatagramCodec.TryReadAccept"/> already reject as zero at parse. The minimum
        /// of this end's own <see cref="DatagramSessionReactor.AnnouncedLingerMilliseconds"/> (already whole milliseconds) and
        /// this governs (<see cref="EffectiveLinger"/>, ADR 0012 adopting ADR 0011's shape). The stream
        /// profile's <see cref="StreamSessionReactor.PeerSlot.RemoteLinger"/> is the model this mirrors.
        /// </summary>
        public TimeSpan RemoteLinger { get; set; }

        /// <summary>
        /// The negotiated Linger — the minimum of this end's own <see cref="DatagramSessionReactor.AnnouncedLingerMilliseconds"/>
        /// (this end's <see cref="DatagramSessionOptions.Linger"/> rounded up to whole milliseconds once,
        /// the same value announced on the wire) and <see cref="RemoteLinger"/> — set once the Session
        /// establishes, on both Established paths, exactly where <see cref="RemoteLinger"/> is. Both
        /// operands are already whole milliseconds, so the minimum is exact: no further rounding happens
        /// here. Immutable for the Session's lifetime from that point on, exactly as every other
        /// negotiated parameter is. This is the RTO's ceiling (subtask 6.4, issue #365): every
        /// <see cref="RetransmissionTimer.EffectiveTimeout"/> call in <see cref="DatagramSessionReactor"/>
        /// clamps against this value, never against this end's own
        /// <see cref="DatagramSessionOptions.Linger"/>, so the ceiling and Linger expiry (issue #367)
        /// read the same effective value. The Diagnostic is issue #364; expiry is issue #367.
        /// </summary>
        public TimeSpan EffectiveLinger { get; set; }

        /// <summary>
        /// This Peer's own fragment size, resolved from <see cref="DatagramSessionOptions.ResolvedFragmentSize"/>
        /// once at admission and stored here rather than read afresh from options on every
        /// <see cref="DatagramSessionReactor.Send"/> (issue #248). Fragment size is unilateral and
        /// per-direction (ADR 0004): it is decided once, when this Peer opens, and nothing consulted
        /// later — a config change, a handshake reply, a Resume — can move it for a Session already in
        /// flight. Every Fragment a Message is cut into and every retransmit of one uses this same
        /// value for that Peer's whole lifetime.
        /// </summary>
        public required int FragmentSize { get; init; }

        /// <summary>The secret the remote issued for resuming this Session. The dialling end sends it in each Resume (<see cref="DatagramSessionReactor.SendResumeIfDue"/>). The accepting end stores it but never reads it: it authorises a Resume against <see cref="PeerSlot.LocalResumeSecret"/>, the secret it issued.</summary>
        public byte[] RemoteResumeSecret { get; set; } = [];

        /// <summary>The Slot this Peer occupies, filled in once the table has admitted it.</summary>
        public SlotHandle Handle { get; set; }

        /// <summary>How far through the handshake this Peer has got.</summary>
        public required HandshakeState State { get; set; }

        /// <summary>
        /// The <see cref="TimeProvider.GetTimestamp"/> reading of this Peer's most recent data
        /// datagram sent — set the moment this Peer reaches <see cref="HandshakeState.Established"/>,
        /// on both the dialling path (<see cref="DatagramSessionReactor.OnAccept"/>) and the accepting
        /// one (<see cref="DatagramSessionReactor.NewAcceptedPeer"/>), and restarted by
        /// <see cref="DatagramSessionReactor.FlushPeer"/> every time a data Datagram actually reaches
        /// the wire for this Peer (issue #340). The handshake's own control datagrams never touch this:
        /// they are not data datagrams, and a Session that took several retransmitted Hellos to connect
        /// should not start its idle clock earlier than the moment it actually became a Session.
        /// </summary>
        public long LastSendTimestamp { get; set; }

        /// <summary>
        /// The <see cref="TimeProvider.GetTimestamp"/> reading of the most recent moment this Peer's
        /// wire proved it was still there (issue #351) — set the instant this Peer reaches
        /// <see cref="HandshakeState.Established"/>, on both the dialling path
        /// (<see cref="DatagramSessionReactor.OnAccept"/>) and the accepting one
        /// (<see cref="DatagramSessionReactor.NewAcceptedPeer"/>), exactly where
        /// <see cref="LastSendTimestamp"/> is set, so the receive-idle deadline starts from the moment
        /// the Session exists rather than from <see langword="default"/>(<see cref="long"/>), which
        /// would read as an interval already elapsed.
        /// </summary>
        /// <remarks>
        /// Updated only once <see cref="DatagramSessionReactor.ParseDataDatagram"/>'s well-formedness
        /// pass has accepted the whole datagram — every record parses, and any Fragment sets it
        /// declares are internally consistent — never merely because a datagram named this Peer's
        /// session id. Anything carrying a session id could be forged or corrupted (session ids are
        /// not secrets), so a datagram that fails that proof must not keep a dead Peer's receive-idle
        /// deadline alive; a malformed or truncated datagram routed here by session id alone touches
        /// neither this field nor anything else about the Peer. A well-formed datagram later refused
        /// with a close (for example, an oversized fragment count) does not matter either way, because
        /// the Peer is closed by the time anything would have read this field again.
        /// </remarks>
        public long LastReceiveTimestamp { get; set; }

        /// <summary>
        /// Whether this Peer's receive-idle deadline (issue #352, <see cref="HasExceededReceiveIdleDeadline"/>)
        /// has fired: the Peer stays open when it is set — <see cref="_peers"/> still holds its Slot and
        /// <see cref="CountersOf"/> keeps answering, and nothing here resets any queue or retention.
        /// <see langword="false"/> for a fresh Slot. Re-checked before a later Pump would otherwise raise
        /// another <see cref="SessionEventKind.PeerInterrupted"/> for a Peer already Interrupted (issue
        /// #353).
        /// </summary>
        /// <remarks>
        /// What Interrupted changes about this Peer, now that parent #55's sub-issues have landed:
        /// <see cref="Send"/> keeps succeeding, queuing and retaining exactly as it does for an
        /// established Peer, until the Space's retention fills, at which point it returns
        /// <see cref="SendOutcome.RetentionExhausted"/> while the Peer stays Interrupted rather than
        /// closing (issue #369) — the same rule, for the same reasons, as the stream profile. The
        /// Interrupted end keeps sending Pings, at the send-idle interval or as the adaptive
        /// retransmission of one still unacknowledged, since <see cref="EnforceSendIdleTimer"/>
        /// deliberately does not read this flag (issue #371) — a datagram Session has no re-dial
        /// schedule, so probing the path is the only way an Interrupted end can ever be heard from
        /// again. The dialling end also sends a Resume each send-idle interval
        /// (<see cref="SendResumeIfDue"/>, issue #382). And this Peer's <see cref="EffectiveLinger"/>, elapsed since
        /// <see cref="InterruptedTimestamp"/>, is what ends it: <c>HasLingerExpired</c> closes it with
        /// <see cref="DisconnectReason.LingerExpired"/> once that wait runs out (issue #367), mirroring
        /// <see cref="StreamSessionReactor.HasExceededLinger"/>. Leaving Interrupted through an actual
        /// Resume is the other way out. On the accepting end a Resume matched by session id and secret
        /// clears the flag (<see cref="ResumeInterruptedPeer"/>). On the dialling end, hearing
        /// its session again after it has sent a Resume clears it
        /// (<see cref="ResumeInterruptedDialledPeerOnData"/>, issue #383). Either end then raises
        /// <see cref="SessionEventKind.PeerResumed"/>, and a later interruption stamps
        /// <see cref="InterruptedTimestamp"/> afresh.
        /// </remarks>
        public bool IsInterrupted { get; set; }

        /// <summary>
        /// How far this end's own <see cref="Close"/> has got (issue #397, parent #57). Anything but
        /// <see cref="LocalCloseState.None"/> means the Peer is Closing: <see cref="Send"/> returns
        /// <see cref="SendOutcome.Closed"/>, and the Peer stays open until the Goodbye is acknowledged or
        /// Linger runs out.
        /// </summary>
        public LocalCloseState LocalClose { get; set; }

        /// <summary>Whether <see cref="Close"/> has put this Peer in Closing.</summary>
        public bool IsClosing => LocalClose != LocalCloseState.None;

        /// <summary>
        /// The <see cref="TimeProvider.GetTimestamp"/> value when <see cref="Close"/> put this Peer in
        /// Closing (issue #399): the instant its effective Linger begins counting down.
        /// </summary>
        public long ClosingTimestamp { get; set; }

        /// <summary>
        /// The <c>ReliableOrdered</c> sequence this end's Goodbye was queued at, valid once
        /// <see cref="LocalClose"/> has passed <see cref="LocalCloseState.GoodbyeDeferred"/> (issue #398).
        /// </summary>
        public uint GoodbyeSequence { get; set; }

        /// <summary>
        /// How far the remote's Goodbye has got (issues #398, #400, #401). Anything but
        /// <see cref="RemoteCloseState.None"/> means the remote's Goodbye was delivered in order.
        /// </summary>
        public RemoteCloseState RemoteClose { get; set; }

        /// <summary>
        /// The <see cref="TimeProvider.GetTimestamp"/> value at the instant <see cref="EnforceReceiveIdleTimer"/>
        /// set <see cref="IsInterrupted"/> (issue #366) — the moment this Peer's effective Linger begins
        /// counting down (#55). Stamped on the same Pump as <see cref="IsInterrupted"/> and only then:
        /// the #353 guard that skips a Peer already Interrupted also skips this field, so it is never
        /// re-stamped while the interruption continues.
        /// </summary>
        public long InterruptedTimestamp { get; set; }

        /// <summary>
        /// Whether this end dialled this Peer (<see cref="Connect"/>) rather than accepted it. Only a
        /// dialled Peer sends Resume (issue #382): the accepting end cannot know the dialler's new address.
        /// Derived from <see cref="HandshakeSource"/>, which only an accepted Peer has (issue #393).
        /// </summary>
        public bool IsDialled => HandshakeSource is null;

        /// <summary>
        /// The <see cref="TimeProvider.GetTimestamp"/> value of this Peer's latest Resume datagram (issue
        /// #382), or <see langword="null"/> when none has left since <see cref="EnforceReceiveIdleTimer"/>
        /// last set <see cref="IsInterrupted"/>, which clears it. <c>SendResumeIfDue</c> paces the next
        /// Resume from it (issue #393).
        /// </summary>
        public long? LastResumeTimestamp { get; set; }

        /// <summary>
        /// This Peer's own mutable counters (ADR 0013), incremented on the hot path and read only
        /// through <see cref="CountersOf"/>. Mirrors <see cref="StreamSessionReactor"/>'s equivalent
        /// field. The parse loop's per-Space classification increments
        /// <see cref="Core.PeerCounterSet.OutOfWindowPackets"/>,
        /// <see cref="Core.PeerCounterSet.DuplicatePackets"/> and
        /// <see cref="Core.PeerCounterSet.UnreliableLossPackets"/> (issues #172, #173, #176); a freshly
        /// connected Peer reads zero for all three.
        /// </summary>
        public PeerCounterSet Counters { get; } = new();

        /// <summary>
        /// This Peer's four outbound sequence allocators (issue #40, #169), one per delivery mode.
        /// <see cref="DatagramSessionReactor.Send"/> (issue #207) is what allocates from it now — one
        /// sequence per Send, whatever the mode — though nothing built from that sequence reaches the
        /// wire until #45's coalescing flush exists.
        /// </summary>
        public OutboundSequenceSpaces OutboundSequences { get; } = new();

        /// <summary>
        /// Records this Peer has Sent since its last flush, in Send order (issue #216): appended to by
        /// <see cref="DatagramSessionReactor.Send"/> once a record is accepted, packed into one data
        /// Datagram by <see cref="FlushPeer"/> on the next <see cref="Pump"/> that finds this
        /// non-empty, and cleared immediately after by <see cref="FlushOutboundDatagrams"/>. A fresh
        /// Slot always starts with an empty queue — <see cref="Connect"/> and the accepted-Peer path in
        /// <see cref="OnHelloWithCookie"/> both construct a brand new <see cref="PeerSlot"/> rather than
        /// reusing one, so a Slot a later Peer occupies never inherits a previous occupant's queue.
        /// </summary>
        public List<PendingOutboundRecord> PendingSends { get; } = [];

        /// <summary>
        /// This Peer's own tracker of outstanding reliable sequences and their ack horizon (subtask
        /// 6.3.6, issue #188): built fresh alongside <see cref="OutboundSequences"/> at the same two
        /// admission sites and released with the Slot, so a Slot a later Peer reuses inherits none of
        /// the previous occupant's outstanding sequences or horizon. Time comes from this Reactor's own
        /// <see cref="ReactorOptions.TimeProvider"/>, never the ambient clock.
        /// </summary>
        public required OutstandingReliableSequences AckTracker { get; init; }

        /// <summary>
        /// This Peer's own adaptive retransmission timer (issue #194; subtask 6.4.7, issue #197): the
        /// same instance passed into <see cref="AckTracker"/> at construction, so a release
        /// <see cref="AckTracker"/> processes and an effective-RTO read from this property are two views
        /// of one timer rather than two that happen to agree. Built fresh alongside
        /// <see cref="OutboundSequences"/> and <see cref="AckTracker"/> at the same two admission sites
        /// and released with the Slot. Minimal state added by 6.4.7 so <see cref="AckTracker"/> is
        /// constructible at all; issue #198 owns the per-Pump expiry sweep that reads
        /// <see cref="RetransmissionTimer.BackOff"/>/<see cref="RetransmissionTimer.RecordDelivery"/>
        /// through it and should extend this property rather than add a second one.
        /// </summary>
        public required RetransmissionTimer RetransmitTimer { get; init; }

        /// <summary>
        /// This Peer's own fragment-size stall signature state (subtask 7.5.3.1, issue #294; parent
        /// #269): built fresh from this Reactor's own <see cref="ReactorOptions.TimeProvider"/> at the
        /// same two admission sites as <see cref="AckTracker"/> and <see cref="RetransmitTimer"/>, and
        /// released with the Slot, so a Slot a later Peer reuses inherits none of the previous
        /// occupant's stall state. Fed from the same flush, ack and retransmission-sweep paths that
        /// already run for <see cref="AckTracker"/> and <see cref="RetransmitTimer"/>; nothing new reads
        /// the clock or sweeps for it.
        /// </summary>
        public required FragmentStallSignature StallSignature { get; init; }

        /// <summary>
        /// The value of <see cref="OutstandingReliableSequences.HorizonDeferrals"/> last folded into
        /// <see cref="Counters"/> (subtask 6.3.6, issue #188). The tracker exposes a monotonic total
        /// rather than a per-event signal, while <see cref="Core.PeerCounterSet.RecordHorizonDeferral"/>
        /// increments by one per call, so reconciling the two means comparing the tracker's total
        /// against this and folding in only the difference, once, each time the tracker's state is
        /// consulted. Zero for a freshly built Slot, matching a tracker that has deferred nothing yet.
        /// </summary>
        public ulong ObservedHorizonDeferrals { get; set; }

        /// <summary>
        /// The highest sequence this Peer has delivered in its <c>UnreliableSequenced</c> Space, or
        /// <see langword="null"/> if it has delivered none (subtask 6.6.11, issue #221). What the mode's
        /// whole rule is stated against: a record is delivered only if it is strictly newer than this,
        /// so a Message superseded before it arrived is discarded rather than delivered late.
        /// </summary>
        /// <remarks>
        /// Nullable rather than a zero default, because zero is a real sequence a Session's first
        /// UnreliableSequenced Send uses: without the distinction that first Message would have to be
        /// strictly newer than itself to be delivered. Compared with
        /// <see cref="SequenceNumber.IsNewerThan"/> rather than <c>&gt;</c>, so the comparison stays
        /// correct across the <c>u32</c> wrap.
        /// </remarks>
        public uint? HighestDeliveredUnreliableSequenced { get; set; }

        /// <summary>
        /// This Peer's bounded duplicate suppression for the plain <c>Unreliable</c> Space (subtask
        /// 6.6.12, issue #222; ADR 0017): 64 sequences wide, a protocol constant rather than anything
        /// derived from this Reactor's options. Built with the Slot and released with it, so a Slot a
        /// later Peer occupies never inherits the previous occupant's suppression state.
        /// </summary>
        public UnreliableDuplicateWindow UnreliableDuplicates { get; } = new();

        /// <summary>
        /// This Peer's send-order release queue for its <c>ReliableOrdered</c> Space (subtask 6.6.10,
        /// issue #220). One queue, for that Space alone: the other three Spaces have none, which is what
        /// confines head-of-line blocking to this Space by construction rather than by convention. Built
        /// with the Slot and released with it, so a Slot a later Peer occupies never inherits the
        /// previous occupant's held run or its next expected sequence.
        /// </summary>
        public required OrderedDeliveryQueue OrderedDelivery { get; init; }

        /// <summary>
        /// The lowest <c>ReliableOrdered</c> sequence a received Goodbye record (issue #400) has arrived
        /// at and not yet been delivered, or <see langword="null"/> when none is waiting. The record sits
        /// in <see cref="OrderedDelivery"/> like any other, so it acts only once every record before it
        /// has been delivered; the ordered release path compares each released sequence against this to
        /// tell a Goodbye from a zero-length Message, which the queue itself cannot.
        /// </summary>
        public uint? RemoteGoodbyeSequence { get; set; }

        /// <summary>
        /// Whether the remote's Goodbye has been delivered in order and this end has entered receiver-side
        /// Closing (issues #400, #401): the Peer accepts no further Messages and
        /// <see cref="DatagramSessionReactor.Send"/> answers <see cref="SendOutcome.Closed"/>. Independent of
        /// <see cref="LocalClose"/>: this one records that the remote said Goodbye.
        /// </summary>
        public bool RemoteGoodbyeDelivered => RemoteClose != RemoteCloseState.None;
    }
}
