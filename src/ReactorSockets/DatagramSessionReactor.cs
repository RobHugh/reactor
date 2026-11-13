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
    /// The largest payload a UDP datagram can carry over IPv4, and the largest datagram this Reactor
    /// reads. An IPv4 datagram is never truncated. An IPv6 datagram can carry up to 65 527 bytes, so
    /// one of 65 508 to 65 527 bytes is reported as truncated and discarded. In both cases a datagram is
    /// never silently shortened into a different one.
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
    /// grown or replaced: each Slot's four windows are built once over the receive arena's admission
    /// runs for that Slot index and outlive whichever Peer occupies the Slot.
    /// <see cref="ClassifyRecord"/> resolves into this by <see cref="SlotHandle.Index"/> rather than
    /// through <c>PeerSlot</c>, since a Slot's windows exist before any Peer is admitted into it.
    /// </summary>
    private readonly ReceiveWindowSpaces[] _receiveWindows;

    /// <summary>
    /// This Reactor's one send-side retention pool, built once at construction from the options'
    /// capacity, reliable Space count, retention slot count and fragment size, and resolved by
    /// <see cref="SlotHandle.Index"/> the same way <see cref="_receiveWindows"/> is — a Slot's
    /// retention exists before any Peer is admitted into it and outlives whichever Peer occupies the
    /// Slot, so nothing is allocated on admission and nothing is freed on disconnect.
    /// </summary>
    private readonly RetentionPool _retention;

    /// <summary>
    /// Queued unreliable payload bytes for every Peer: one region of <see cref="_unreliableQueueBytesPerPeer"/>
    /// bytes per Slot index, allocated at construction. <see cref="Send"/> appends at
    /// <see cref="PeerSlot.UnreliableQueuedPayloadEnd"/>, and <see cref="CompactUnreliableQueue"/> moves what
    /// is still queued to the region's front after each flush.
    /// </summary>
    private readonly byte[] _unreliableQueue;

    /// <summary>One Peer's region of <see cref="_unreliableQueue"/>: <see cref="DatagramSessionOptions.ResolvedUnreliableQueueBytes"/>.</summary>
    private readonly int _unreliableQueueBytesPerPeer;

    /// <summary>
    /// <paramref name="slot"/>'s region of <see cref="_unreliableQueue"/>: <see cref="_unreliableQueueBytesPerPeer"/>
    /// bytes starting at its Slot index, so no two Peers share a byte.
    /// </summary>
    private Span<byte> UnreliableRegion(PeerSlot slot) =>
        _unreliableQueue.AsSpan(slot.Handle.Index * _unreliableQueueBytesPerPeer, _unreliableQueueBytesPerPeer);

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
    /// Sessions that closed on a Goodbye, remembered for one effective Linger so a resent Goodbye can
    /// still be acknowledged (ADR 0017, second amendment). Written by <see cref="RememberClosedSession"/>,
    /// read by <see cref="TryReAckClosedSession"/>. Holds no capacity Slot.
    /// </summary>
    private readonly ClosedSessionMemory _closedSessions;

    /// <summary>
    /// The read-side buffer. One byte longer than <see cref="MaxUdpDatagramSize"/> so a read that
    /// fills it is known to have been truncated (see <c>UdpLink.Receive</c>).
    /// </summary>
    private readonly byte[] _scratchBuffer = new byte[MaxUdpDatagramSize + 1];
    private readonly byte[] _sendBuffer = new byte[DatagramSessionProtocol.ControlDatagramSize];

    /// <summary>
    /// The scratch buffer <see cref="FlushOneDatagram"/> packs each data Datagram into, separate from
    /// <see cref="_scratchBuffer"/> — which is read-side and reused within one
    /// <see cref="OnLinkReadable"/> call — and from <see cref="_sendBuffer"/>, which is sized for a
    /// control datagram only and far too small for a coalesced data one. Sized at
    /// <see cref="MaxUdpDatagramSize"/>, the largest datagram one send can carry, so it can hold the largest
    /// data Datagram any Peer's fragment size allows; reused across every Peer flushed by one Pump rather than allocated per Peer.
    /// </summary>
    private readonly byte[] _outboundDatagramBuffer = new byte[MaxUdpDatagramSize];

    /// <summary>
    /// Refuses a data datagram whose individually legal Fragments lie about which set each belongs to
    /// (ADR 0009). Its scratch is sized once here and reused for every datagram parsed.
    /// </summary>
    private readonly FragmentSetValidator _fragmentSetValidator = new();

    private readonly int _maxDatagramsPerPump;
    private readonly int _admissionCap;
    private int _admissionsThisFrame;

    /// <summary>
    /// How many Pumps have started on this Reactor. A Peer's ack is due once it has been owed since an
    /// earlier Pump than the current one (<see cref="IsAckDue"/>).
    /// </summary>
    private long _pumpNumber;
    private int _capDiscardsThisFrame;
    private int _capacityDiscardsThisFrame;

    /// <summary>
    /// The largest declared <c>fragmentCount</c> a Fragment's set may legally claim: this Reactor's own
    /// receive window, <see cref="DatagramSessionOptions.ResolvedReceiveWindowFragments"/>, not
    /// <see cref="DatagramSessionOptions.ReceiveWindowFloor"/>. The floor is computed from <em>this
    /// end's own</em> <see cref="DatagramSessionOptions.ResolvedFragmentSize"/>, but a conforming peer
    /// with a smaller fragment size legally cuts the same,
    /// within-<see cref="DatagramSessionOptions.MaxMessageSize"/> Message into more Fragments than that
    /// floor, up to the window this end announced (ADR 0024). Bounding by the floor would close that
    /// peer as a protocol violator for a set it was always entitled to send. The window is the right bound instead of a
    /// looser one: it is what a Fragment set actually occupies while it is held, so a declared count
    /// larger than the window can never be reassembled — there is no slot for every piece to sit in at
    /// once — and refusing it refuses exactly the sets that were never legal, whoever's fragment size
    /// produced them. <see cref="DatagramSessionOptions.Validate"/> already requires the window to be
    /// at least the floor, so this bound is never tighter than the old one: a set at exactly the
    /// announced limit is still admitted, and at default options — where the window resolves to the
    /// floor — nothing observable changes. Where an operator deliberately widens
    /// <see cref="DatagramSessionOptions.ReceiveWindowFragments"/> past the floor, a declared count
    /// between the floor and the window is admitted, so such a set can reassemble past
    /// <see cref="DatagramSessionOptions.MaxMessageSize"/>. That is the accepted trade for never closing
    /// a conforming peer: this check bounds what is held, and <see cref="RefuseOverLimit"/> refuses to
    /// deliver the assembled Message. Computed once here, at construction, rather than read from
    /// <see cref="_options"/> per datagram — the refusal path this bounds must allocate nothing and do
    /// no more arithmetic than a single comparison.
    /// </summary>
    private readonly int _maxLegalFragmentCount;

    /// <summary>
    /// The widest Fragment payload this Reactor holds: its own resolved
    /// <see cref="DatagramSessionOptions.ResolvedFragmentSize"/>, the width of every receive-arena
    /// Fragment slot (ADR 0024). A reliable Fragment wider than this closes the Peer with
    /// <see cref="DisconnectReason.ProtocolViolation"/> in <see cref="ProcessDataDatagram"/>'s
    /// pre-classification pass. Captured at construction, as <see cref="_maxLegalFragmentCount"/> is.
    /// </summary>
    private readonly int _maxLegalFragmentWidth;

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
        _maxDatagramsPerPump = options.ResolvedMaxDatagramsPerPump;
        _maxLegalFragmentCount = options.ResolvedReceiveWindowFragments;
        _maxLegalFragmentWidth = options.ResolvedFragmentSize;
        _pumpIntervalObserver = new PumpIntervalObserver(options.Reactor.TimeProvider);

        _closedSessions = new ClosedSessionMemory(options.Reactor.Capacity, options.Reactor.TimeProvider);

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

        _unreliableQueueBytesPerPeer = options.ResolvedUnreliableQueueBytes;
        _unreliableQueue = new byte[checked((int)options.UnreliableQueueBudgetBytes)];

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
    /// adaptive RTO clamps against: a timeout shorter than the gap between Pumps cannot be acted on.
    /// Internal because nothing outside this Reactor derives a bound from it.
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
    /// negotiates the effective (minimum) Linger from. This is also this end's own operand of the
    /// minimum in <see cref="PeerSlot.EffectiveLinger"/> — rounded here once, on both the dialling
    /// (<see cref="OnAccept"/>) and accepting (<see cref="NewAcceptedPeer"/>) paths — so both ends
    /// negotiate from identical whole-millisecond values and land on the same effective Linger, whatever
    /// fractional-millisecond <see cref="DatagramSessionOptions.Linger"/> either end configured. The
    /// stream profile mirrors this with its own equivalent.
    /// </summary>
    private ulong AnnouncedLingerMilliseconds => (ulong)Math.Ceiling(_options.Linger.TotalMilliseconds);

    /// <inheritdoc cref="ISessionEventSource.PendingEventCount"/>
    int ISessionEventSource.PendingEventCount => _pendingEvents.Count;

    /// <summary>
    /// One synchronous advance: records this call's arrival against the last one
    /// (<see cref="ObservedPumpInterval"/>) before anything else, then wipes the previous Frame's
    /// events and arena, then reads whatever bound socket is ready right now, running the handshake for
    /// everything that arrived. Throws <see cref="InvalidOperationException"/> if called from any
    /// thread other than the one that constructed this Reactor, or from inside a Pump already in
    /// progress.
    /// </summary>
    public void Pump()
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();

        _lifecycle.InPump = true;
        try
        {
            _pumpIntervalObserver.Observe();
            _pumpNumber++;
            _pendingEvents.Clear();
            _arena.Reset();
            _admissionsThisFrame = 0;
            _capDiscardsThisFrame = 0;
            _capacityDiscardsThisFrame = 0;
            MarkDrainedClosingPeersCloseDue();
            _dispatcher.Pump();
            RetransmitPendingDials();
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
    /// While the dial is pending, a Pump resends its current flight: the Hello while it awaits a Cookie,
    /// and the Hello-with-cookie while it awaits an Accept. The delay starts at the observed Pump
    /// interval, doubles, stops at this Reactor's <see cref="DatagramSessionOptions.Linger"/> and is
    /// drawn with full jitter, the same schedule the stream profile re-dials on. A dial whose cookie is
    /// one epoch old restarts from a fresh Hello. A dial still pending one
    /// <see cref="DatagramSessionOptions.Linger"/> after this call ends with
    /// <see cref="DisconnectReason.TransportFailed"/> and frees its Slot. <see cref="Close"/> ends it
    /// sooner and stops the resends. Nothing is resent while the clock has not moved since the last send.
    /// </remarks>
    public PeerId Connect(IPEndPoint destination)
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();
        ArgumentNullException.ThrowIfNull(destination);

        UdpLink link = LinkForFamily(destination.AddressFamily);
        Source remote = Source.FromIPEndPoint(destination);
        RetransmissionTimer retransmitTimer = new();
        long now = _options.Reactor.TimeProvider.GetTimestamp();

        PeerSlot slot = new()
        {
            Remote = remote,
            DialStartTimestamp = now,
            HandshakeSentTimestamp = now,
            Link = link,
            ClientNonce = NextNonce(),
            LocalSessionId = NextSessionId(),
            LocalResumeSecret = NextResumeSecret(),
            Lifecycle = PeerLifecycle.Dialling(),
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
    /// Goodbye in the Peer's <c>ReliableOrdered</c> Space and puts the Peer in Closing: later
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
        if (_peers.TryResolve(handle) is PeerSlot slot)
        {
            if (slot.Lifecycle.IsClosing)
            {
                return;
            }

            if (slot.Lifecycle.TryBeginClose(_options.Reactor.TimeProvider.GetTimestamp()))
            {
                QueueGoodbye(slot);
                return;
            }
        }

        ClosePeer(handle, DisconnectReason.LocalClose);
    }

    /// <summary>
    /// A point-in-time snapshot of one Peer's counters (ADR 0013). Throws if the Peer is not open,
    /// matching <see cref="StreamSessionReactor.CountersOf(PeerId)"/>'s shape for the stream profile —
    /// the datagram profile had no per-Peer counter reader until this member.
    /// <see cref="PeerCounters.OutOfWindowPackets"/> and <see cref="PeerCounters.DuplicatePackets"/>
    /// now reflect what the four per-Space receive windows have refused;
    /// <see cref="PeerCounters.UnreliableLossPackets"/> reflects gaps either unreliable Space's window
    /// has slid past. <see cref="PeerCounters.MessagesReceived"/> counts whole Messages handed to the
    /// frame arena for delivery, and not a Message the arena refused.
    /// <see cref="PeerCounters.Resumes"/> counts the Peer's resumes: once per
    /// <see cref="SessionEventKind.PeerResumed"/> raised for it, and never reset by one (ADR 0013). The
    /// accepting end raises it when a matching Resume or a well-formed data Datagram from the exact address
    /// and port it sends to moves the Peer out of Interrupted. The dialling end raises it when it hears its
    /// session again after sending a Resume.
    /// <see cref="PeerCounters.RetentionHighWaterBytes"/> reads the true per-Peer figure — summed
    /// across both reliable Spaces — from <see cref="_retention"/>.
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
    /// 0009). A Message too wide for its mode to carry is refused before any work is done on it: no
    /// sequence is allocated and no retention is taken, so a rejected Send leaves no trace on the
    /// Session. How wide is too wide depends on the mode and is decided by
    /// <see cref="FragmentPlan.Resolve"/>, the one place the rule is stated: an unreliable mode is capped
    /// at a single Datagram and refused above it as <see cref="SendOutcome.MessageTooLarge"/>, a reliable
    /// mode fragments, and every mode is refused above a second cap as
    /// <see cref="SendOutcome.RemoteMessageTooLarge"/> (ADR 0004). That second cap is
    /// <paramref name="peer"/>'s own announced receive limit, captured on its Slot at handshake, not this
    /// Reactor's own <see cref="DatagramSessionOptions.MaxMessageSize"/>: <c>Send</c> is therefore not
    /// purely local to this Reactor's configuration, because the same payload can be accepted for one
    /// Peer and refused for another whose handshake announced a smaller limit. That gate sits behind
    /// the <paramref name="peer"/> lookup rather than in front of it, because both caps it measures
    /// against, fragment size and remote max message size alike, are the Peer's own captured values; a
    /// Peer this Reactor is not holding open returns <see cref="SendOutcome.Closed"/> whatever the
    /// Message's width. Past that gate, a reliable mode allocates the next sequence in its own Space
    /// and retains the Message whole there, returning <see cref="SendOutcome.RetentionExhausted"/> once
    /// that Space has no slot left — and that refusal spends nothing either: the sequence is claimed
    /// only once retention has committed, so a refused reliable Send leaves the Space's next sequence
    /// exactly where it was, rather than opening a gap the receiver's window would wait on for a record
    /// that is never sent; <see cref="DeliveryMode.Unreliable"/> and
    /// <see cref="DeliveryMode.UnreliableSequenced"/> still allocate a sequence — every mode gets one,
    /// the same as every other (<see cref="OutboundSequenceSpaces"/>) — but retain nothing, because
    /// nothing unreliable is ever retransmitted. A Message this call accepts is queued on
    /// <paramref name="peer"/>'s Slot (<see cref="PeerSlot.PendingSends"/>) and leaves as one packet
    /// record in a data Datagram written by a later <see cref="Pump"/>, in Send
    /// order (<see cref="FlushOutboundDatagrams"/>). Nothing reaches the wire before that Pump runs. One
    /// Pump writes a bounded number of Datagrams per Peer (<see cref="FlushPeer"/>), so a record queued
    /// behind a backlog waits for however many Pumps it takes to reach the front. An unreliable Message
    /// is charged its length plus a 7-byte record header against
    /// <see cref="DatagramSessionOptions.UnreliableQueueBytes"/> and refused with
    /// <see cref="SendOutcome.SendQueueFull"/> past it, with nothing copied and no sequence spent.
    /// </summary>
    /// <remarks>
    /// <b><see cref="SendOutcome.RetentionExhausted"/> is reachable from one <c>Send</c> call</b> (ADR
    /// 0004's "one Message can fill the send window alone"): the retention budget is sized in
    /// Fragments, not Messages, so a single Message cut into more Fragments than its Space has free
    /// slots for exhausts that Space by itself, with nothing else needing to have sent anything first.
    /// The refusal is whole, not partial — this checks the Space's free-slot count against the Fragment
    /// count the Message needs <em>before</em> retaining or sequencing a single Fragment, so a refused
    /// Send leaves retention holding no Fragment of that Message and leaves the Space's next sequence
    /// exactly where <see cref="Send"/> found it, the same "spend nothing on a refusal" contract a
    /// whole small Message keeps. A half-retained Message would be unrepairable: the receiver's
    /// reassembly waits forever for a set that was never cut in full. A smaller Message that fits the
    /// Space's free slots still succeeds afterward — the refusal costs the Space nothing.
    /// <para>
    /// Sending to a <paramref name="peer"/> this Reactor is not currently holding open — unknown, or
    /// already closed — returns <see cref="SendOutcome.Closed"/> rather than throwing, unlike
    /// <see cref="CountersOf"/>: a Peer closing under a caller is an ordinary race, the same reason
    /// <see cref="Close"/> tolerates a stale <see cref="PeerId"/>.
    /// </para>
    /// <para>
    /// Sending to a <paramref name="peer"/> whose handshake has not reached
    /// <see cref="PeerPhase.Open"/> returns <see cref="SendOutcome.NotEstablished"/> under
    /// every mode, checked before any width gate. Before the handshake the Peer has no remote-announced
    /// <c>MaxMessageSize</c>, so a Send would otherwise be misreported as
    /// <see cref="SendOutcome.RemoteMessageTooLarge"/>.
    /// </para>
    /// </remarks>
    /// <param name="peer">The Peer to send to.</param>
    /// <param name="mode">Which of the four delivery modes carries this Message; see <c>docs/delivery-modes.md</c>.</param>
    /// <param name="message">
    /// The Message payload, unframed. The bytes are copied before this call returns — into
    /// <see cref="_retention"/> for a reliable mode, into the queued record for an unreliable one — so
    /// the caller owns its buffer again the instant <c>Send</c> returns and may overwrite or reuse it
    /// without changing what is eventually put on the wire. A Message wider than this Peer's send
    /// Fragment size (<c>fragmentSize</c> below: the smaller of this Reactor's
    /// <see cref="DatagramSessionOptions.ResolvedFragmentSize"/> and the peer's announced Fragment size)
    /// cannot fit in one Datagram, and for
    /// <see cref="DeliveryMode.Unreliable"/> and <see cref="DeliveryMode.UnreliableSequenced"/> that cap
    /// is reported back as <see cref="SendOutcome.MessageTooLarge"/> rather than thrown, and it is
    /// permanent — no option lifts it: you may only fragment what you can retransmit (ADR 0004), because
    /// fragmenting what cannot be repaired multiplies the failure rate by the number of pieces — a
    /// ten-Fragment unreliable Message on a path losing 1% of Datagrams arrives about 90% of the time,
    /// having spent ten Datagrams of bandwidth on a coin flip (<c>docs/delivery-modes.md</c>). A Message
    /// within that cap but wider than <paramref name="peer"/>'s own announced <c>MaxMessageSize</c> is
    /// refused with <see cref="SendOutcome.RemoteMessageTooLarge"/>, because the far end would discard it
    /// on arrival; a Message over both caps reports <see cref="SendOutcome.MessageTooLarge"/>. For
    /// <see cref="DeliveryMode.ReliableOrdered"/> and <see cref="DeliveryMode.ReliableUnordered"/> the
    /// single-Datagram cap does not apply: a Message up to <paramref name="peer"/>'s own announced
    /// <c>MaxMessageSize</c> (see the summary above) is cut into <c>ceil(length / fragmentSize)</c>
    /// Fragments, each retained and sequenced as its own ordinary reliable packet, and a Message
    /// wider than that remote limit is refused with <see cref="SendOutcome.RemoteMessageTooLarge"/>.
    /// That <c>fragmentSize</c> is this Peer's <see cref="PeerSlot.FragmentSize"/>: the smaller of this
    /// Reactor's <see cref="DatagramSessionOptions.ResolvedFragmentSize"/> and the Fragment size the
    /// peer announced, fixed once when the Peer reached Established (ADR 0024) — not re-read on this or
    /// any later call, and unchanged by a Resume. A retransmit re-sends bytes already cut, so two
    /// Messages sent to the same Peer at different times, and every retransmit of either, are cut with
    /// the same size. A reliable Message that needs more Fragments at that size than the peer's announced
    /// receive window holds is refused with <see cref="SendOutcome.RemoteMessageTooLarge"/>, with
    /// nothing retained or sequenced.
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

        if (slot.Lifecycle.SendRefusal is SendOutcome refusal)
        {
            return refusal;
        }

        // Every mode uses this Peer's send Fragment size, fixed on the Slot when the Peer reached
        // Established (issue #248, ADR 0024) rather than read from _options here, so Send only ever
        // consults what was already decided. A reliable Message is cut at it, and an unreliable Message
        // wider than it is refused with MessageTooLarge. The cap handed to FragmentPlan.Resolve is this
        // Peer's own remote-announced MaxMessageSize (issues
        // #284, #449; see this method's XML doc for why a Send is capped by the far end's limit rather
        // than this Reactor's own). RemoteMaxMessageSize is a uint off the wire while
        // FragmentPlan.Resolve takes an int message length, so a value above int.MaxValue is clamped
        // rather than cast unchecked; a Message that large will never exist in practice, so the clamp
        // only ever widens the cap past a width nothing here could reach anyway. One call to
        // FragmentPlan.Resolve states the too-large rule exactly once: unreliable modes refuse above one
        // Fragment's worth, and every mode refuses above that remote cap.
        int remoteMaxMessageSize = slot.RemoteMaxMessageSize > int.MaxValue ? int.MaxValue : (int)slot.RemoteMaxMessageSize;
        int fragmentSize = slot.FragmentSize;
        FragmentPlanResult plan = FragmentPlan.Resolve(
            message.Length, fragmentSize, mode, slot.RemoteReceiveWindowFragments, remoteMaxMessageSize);
        if (plan.IsMessageTooLarge)
        {
            // Which limit refused it is the plan's to say (issue #449): an unreliable mode can be refused
            // for either cap, so the mode alone no longer tells them apart.
            return plan.IsOverRemoteLimit ? SendOutcome.RemoteMessageTooLarge : SendOutcome.MessageTooLarge;
        }

        if (!isReliable)
        {
            int charge = message.Length + DatagramSessionProtocol.UnfragmentedRecordSize;
            if (charge > _unreliableQueueBytesPerPeer - slot.UnreliableQueuedBytes)
            {
                return SendOutcome.SendQueueFull;
            }

            int offset = slot.UnreliableQueuedPayloadEnd;
            message.CopyTo(UnreliableRegion(slot).Slice(offset, message.Length));
            slot.UnreliableQueuedPayloadEnd = offset + message.Length;
            slot.UnreliableQueuedBytes += charge;
            slot.PendingSends.Add(new PendingOutboundRecord
            {
                Mode = recordMode,
                Sequence = slot.OutboundSequences.Allocate(recordMode),
                UnreliableOffset = offset,
                UnreliableLength = message.Length,
            });
            return SendOutcome.Sent;
        }

        int spaceIndex = ReliableSpaceIndex(recordMode);
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
        _closedSessions.ForgetAll();
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
    /// Reads a ready socket until it is empty or <see cref="DatagramSessionOptions.MaxDatagramsPerPump"/>
    /// reads have been spent, rather than the Raw profile's one read per Pump. Everything read is
    /// handled now, so a datagram beyond the admission cap is still counted as a Discard. Every read that
    /// returned something spends the limit, queued errors included, because each is work a remote party
    /// can cause. What the limit leaves waits in the socket's buffer, and the socket stays ready for a
    /// later Pump. If that buffer fills, the operating system drops what arrives next, which is normal
    /// for UDP (ADR 0012).
    /// </summary>
    /// <remarks>
    /// The loop ends early on the socket's own answer that nothing is left —
    /// <see cref="UdpLinkReceiveOutcome.WouldBlock"/> — rather than on <c>Socket.Available</c>, which
    /// counts queued datagrams and so reports zero while queued ICMP errors still stand in front of
    /// them. Terminating on it would abandon the read at the first error and starve every live Peer
    /// behind it, which is the thing the Unreachable arm below exists to prevent.
    /// </remarks>
    private void OnLinkReadable(UdpLink link)
    {
        for (int reads = 0; reads < _maxDatagramsPerPump; reads++)
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
                    _diagnostics.Log(DiagnosticRecord.ForDatagramTruncated(result.Source, MaxUdpDatagramSize));
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

        _diagnostics.Log(DiagnosticRecord.ForDatagramReadLimitReached(_maxDatagramsPerPump, _links.IndexOf(link)));
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

            case DatagramControlType.PathChallenge:
                OnPathChallenge(source, datagram);
                break;

            case DatagramControlType.PathResponse:
                OnPathResponse(link, source, datagram);
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
    /// second amendment) and is discarded only if that declines. A data Datagram from another address or
    /// port is delivered but neither moves the Peer's send address nor resumes an Interrupted accepted Peer.
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
    /// Reads the 20-byte header, then packet records until the datagram is exhausted, and routes every
    /// record that parses into <paramref name="slot"/>'s Space for its mode. A datagram is a container,
    /// not a Message: one bad record — a short header, a bad flags byte, a spare bit set, a length
    /// running past the end, or truncation inside a record — discards the whole datagram rather than
    /// whatever records parsed before it, because a partial admission would let a forged tail poison an
    /// otherwise legitimate send. A record with the fragment bit set is not by itself bad, so long as
    /// it is in one of the two reliable modes: <see cref="PacketRecordCodec.TryRead"/> reads the
    /// Fragment shape same as the unfragmented one, and this method routes it into its Space the same
    /// way — <see cref="ClassifyRecord"/> is what diverts an admitted Fragment away from delivery and
    /// into its Space's receive-window slot instead. The fragment bit in an unreliable mode <em>is</em>
    /// by itself bad and fails the read outright, the same discard as any other malformed record, since
    /// ADR 0004 caps those two modes at a single Datagram and no reassembly path exists for them; the
    /// first pass below names that refusal on the Diagnostic stream as well, because it says the sender
    /// fragments in modes this profile does not rather than that rubbish arrived. Parsing therefore
    /// runs in three passes over the same bytes: the first confirms every record is well-formed without
    /// touching any Space; the second, once <see cref="FragmentSetValidator.AreWellFormed"/> also stands, walks
    /// every record again to refuse — with a close, not a discard — a declared <c>fragmentCount</c>
    /// larger than this Reactor's own receive window, <see cref="DatagramSessionOptions.ResolvedReceiveWindowFragments"/>,
    /// could ever hold; only once neither of those passes objects does the third classify each record
    /// in turn — so an early record is never classified on the strength of a datagram whose tail turns
    /// out to be forged, or whose Fragment set claims more Fragments than this Reactor could ever hold
    /// at once. The header's two ack blocks are applied to <paramref name="slot"/>'s
    /// <see cref="PeerSlot.AckTracker"/> for the two reliable Spaces only once that same proof stands —
    /// a short or malformed datagram applies neither block, exactly as it admits no record. Both blocks
    /// are applied at the same effective RTO, read once from <paramref name="slot"/>'s own
    /// <see cref="PeerSlot.RetransmitTimer"/> and this Reactor's <see cref="ObservedPumpInterval"/>, so
    /// a horizon slide inside either call arms at the same value. Nothing here throws, allocates or
    /// reads past <paramref name="datagram"/>. No Session event is produced and nothing is delivered:
    /// what a Space does with an admitted record belongs to that Space's delivery path, so this
    /// method's whole job ends at "classified, and counted if refused".
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

        PacketRecordReader wellFormedScan = new(body);
        while (wellFormedScan.TryReadNext(out _))
        {
            // This pass proves only that every record parses; the passes below read the fields.
        }

        if (!wellFormedScan.IsAtEnd)
        {
            // One refusal out of the several this read can reach is worth naming separately
            // (issue #273): a Fragment in an unreliable mode is well-formed at every level but
            // ADR 0004's rule, so it says the sender fragments in modes this profile does not,
            // rather than that rubbish arrived. The malformed discard below still fires — this
            // adds which rule broke, it does not replace the outcome.
            ReadOnlySpan<byte> refused = wellFormedScan.Remaining;
            if (PacketRecordLegality.RefusedAsUnreliableFragment(refused)
                && _diagnostics.IsEnabled(DiagnosticLevel.Warn))
            {
                _diagnostics.Log(DiagnosticRecord.ForUnreliableFragment(
                    source, refused[0] & DatagramSessionProtocol.ModeMask));
            }

            DiscardMalformedForSession(source, datagram.Length);
            return;
        }

        // Every record on its own proved legal above (issue #253), but a Fragment's index and count are
        // only half the story: this checks the claimed sets those individually-legal Fragments form
        // (issue #256), still before any Space is touched, so a forged set is refused exactly like a
        // malformed record rather than reaching classification.
        if (!_fragmentSetValidator.AreWellFormed(body))
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

        // Two refusals share this pass, and both close the Peer rather than discard. A Fragment wider
        // than this Reactor's own FragmentSize (ADR 0024) could never fit a receive-arena slot, and only
        // a non-conforming peer sends one, since the handshake announces the size. A declared
        // fragmentCount larger than this Reactor's own receive window is refused with a
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
        PacketRecordReader oversizedScan = new(body);
        while (oversizedScan.TryReadNext(out PacketRecord record))
        {
            if (record.IsFragment
                && record.Mode is PacketRecordMode.ReliableOrdered or PacketRecordMode.ReliableUnordered)
            {
                if (record.FragmentCount > _maxLegalFragmentCount)
                {
                    if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
                    {
                        _diagnostics.Log(DiagnosticRecord.ForOversizedFragmentSet(
                            ToPeerId(slot.Handle), record.FragmentCount, _maxLegalFragmentCount));
                    }

                    ClosePeer(slot.Handle, DisconnectReason.ProtocolViolation);
                    return;
                }

                if (record.Payload.Length > _maxLegalFragmentWidth)
                {
                    if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
                    {
                        _diagnostics.Log(DiagnosticRecord.ForOversizedFragment(
                            ToPeerId(slot.Handle), record.Payload.Length, _maxLegalFragmentWidth));
                    }

                    ClosePeer(slot.Handle, DisconnectReason.ProtocolViolation);
                    return;
                }
            }
        }

        // Fully parsed and not refused above: this is what "well-formed" means for leaving Interrupted
        // (issue #383). It sits after the oversized-set close so a datagram that closes the Peer never
        // resumes it first.
        ResumeInterruptedPeerOnData(source, slot);

        // The proof above is what attributes this Datagram to slot's Peer: it names an established
        // session id (ProcessDataDatagram's own lookup) and every record in it parses. Only from this
        // point on is the whole Datagram's length, header included, counted against that Peer (issue
        // #231) — a malformed one, even if it named a real session, falls to
        // DiscardMalformedForSession's Reactor-scope figure instead, above, and never reaches here.
        slot.Counters.RecordBytesReceived((ulong)datagram.Length);

        TimeSpan effectiveRto = slot.RetransmitTimer.EffectiveTimeout(ObservedPumpInterval, slot.EffectiveLinger);
        AckRelease orderedRelease = slot.AckTracker.ApplyAck(
            PacketRecordMode.ReliableOrdered, reliableOrderedAckBase, reliableOrderedAckBits, effectiveRto);
        ReleaseRetentionForAck(slot, PacketRecordMode.ReliableOrdered, orderedRelease.Sequences);
        ApplyAckToStallSignature(slot, orderedRelease);
        AckRelease unorderedRelease = slot.AckTracker.ApplyAck(
            PacketRecordMode.ReliableUnordered, reliableUnorderedAckBase, reliableUnorderedAckBits, effectiveRto);
        ReleaseRetentionForAck(slot, PacketRecordMode.ReliableUnordered, unorderedRelease.Sequences);
        ApplyAckToStallSignature(slot, unorderedRelease);
        ReconcileHorizonDeferrals(slot);

        // Every record in body already proved well-formed in the first pass, so this walk reads all of
        // them. The fragment index and count are passed on here (issue #258): a held Fragment's group
        // first sequence, and so its completeness, is derived from them the moment it is held.
        PacketRecordReader classify = new(body);
        while (classify.TryReadNext(out PacketRecord record))
        {
            ClassifyRecord(slot, record);
        }
    }

    /// <summary>
    /// Folds however much <paramref name="slot"/>'s <see cref="PeerSlot.AckTracker"/> has deferred,
    /// since this method last read it, into <see cref="PeerSlot.Counters"/>. The tracker exposes
    /// <see cref="OutstandingReliableSequences.HorizonDeferrals"/> as a monotonic total across both
    /// reliable Spaces, while <see cref="Core.PeerCounterSet.RecordHorizonDeferral"/> increments by one
    /// per call, so the two shapes are reconciled here rather than at either end: the difference
    /// between the total and <see cref="PeerSlot.ObservedHorizonDeferrals"/> is folded in one call at a
    /// time, and nothing is folded twice because the observed figure is advanced to match.
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
    /// <see cref="PeerSlot.AckTracker"/>: sweeps each reliable Space for sequences whose retransmit
    /// deadline has passed, raises that Peer's
    /// <see cref="Core.PeerCounterSet.RecordPacketsRetransmitted"/> by however many
    /// <see cref="OutstandingReliableSequences.CollectExpired"/> reports, and — if either Space reported
    /// anything — backs off that Peer's <see cref="PeerSlot.RetransmitTimer"/> exactly once and reports
    /// it. Nothing here resends anything — holding the record bytes is retention's job and packing them
    /// into an outbound datagram is the coalescing flush's; this method stops at the sequences having
    /// been named.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>At most one backoff per Peer per Pump.</b> A single <see cref="RetransmissionTimer"/> is
    /// shared by both of a Peer's reliable Spaces (<see cref="NewAckTracker"/>). Deciding the backoff
    /// inside <see cref="OutstandingReliableSequences.CollectExpired"/>, called once per Space, would
    /// back off the shared timer twice on a Pump where both Spaces expired something, doubling the RTO
    /// twice and raising <see cref="RetransmissionTimer.ConsecutiveBackoffs"/> by two for one loss
    /// event. This method instead sweeps both Spaces first, decides the backoff once here at the Peer
    /// level from their combined count, and only then re-arms each Space's findings —
    /// <see cref="OutstandingReliableSequences.ReArmExpired"/> — at the single resulting
    /// <see cref="RetransmissionTimer.EffectiveTimeout"/>, matching how a horizon slide arms everything
    /// it newly covers from one shared moment and RTO.
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
    /// called for a sent record), so restricting the scan to <see cref="PeerLifecycle.IsEstablished"/>
    /// Peers changes nothing observable; it only skips a scan certain to find nothing.
    /// </para>
    /// </remarks>
    private void ReportExpiredRetransmissions()
    {
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is not PeerSlot slot || !slot.Lifecycle.IsEstablished)
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
    /// Folds one <see cref="OutstandingReliableSequences.ApplyAck"/> call's <paramref name="release"/>
    /// into <paramref name="slot"/>'s <see cref="PeerSlot.StallSignature"/>: called once per
    /// <see cref="OutstandingReliableSequences.ApplyAck"/> call, with that call's own result.
    /// </summary>
    private static void ApplyAckToStallSignature(PeerSlot slot, AckRelease release)
    {
        if (release.ReleasedFragment)
        {
            slot.StallSignature.RecordFragmentAcked();
        }

        if (release.ReleasedUnfragmentedRecord)
        {
            slot.StallSignature.RecordUnfragmentedRecordAcked();
        }
    }

    /// <summary>
    /// Frees the retention slot of every sequence in <paramref name="released"/>, the
    /// <see cref="AckRelease.Sequences"/> one <see cref="OutstandingReliableSequences.ApplyAck"/> call
    /// returned for <paramref name="mode"/>'s reliable Space: called once per
    /// <see cref="OutstandingReliableSequences.ApplyAck"/> call, with that call's own result.
    /// Without this, nothing ever freed a slot <see cref="_retention"/>'s <see cref="RetentionPool.TryAcquire"/>
    /// committed: a quiet Session's idle-triggered Pings and a Peer's reliable Sends would both hold
    /// their slots forever and every further reliable Send would eventually see
    /// <see cref="RetentionSlotOutcome.Exhausted"/>.
    /// </summary>
    /// <remarks>
    /// Allocates nothing: <see cref="AckRelease.Sequences"/> is a reused
    /// scratch span and <c>foreach</c> over a <see cref="ReadOnlySpan{T}"/> does not allocate an
    /// enumerator, matching this whole ack path's no-allocation discipline.
    /// </remarks>
    private void ReleaseRetentionForAck(PeerSlot slot, PacketRecordMode mode, ReadOnlySpan<uint> released)
    {
        int spaceIndex = ReliableSpaceIndex(mode);
        foreach (uint sequence in released)
        {
            _retention.Release(slot.Handle.Index, spaceIndex, sequence);

            if (mode == PacketRecordMode.ReliableOrdered)
            {
                slot.Lifecycle.OrderedSequenceAcked(sequence);
            }
        }
    }

    /// <summary>
    /// Closes every Closing Peer whose Goodbye has been acknowledged
    /// (<see cref="PeerPhase.GoodbyeAcked"/>) with <see cref="DisconnectReason.LocalClose"/>,
    /// once every <c>ReliableOrdered</c> record up to the Goodbye is acknowledged too: a selective ack
    /// of the Goodbye behind an unacknowledged earlier Message would otherwise free the Slot and stop
    /// that Message's retransmission, leaving the remote's Goodbye waiting behind a hole that never
    /// fills. Run right after the read pass, so an ack that arrived this Pump closes the Peer this
    /// Pump, and before <see cref="EnforceLingerExpiry"/>, so an ack arriving in the Pump Linger would
    /// also have run out wins and no second close follows.
    /// </summary>
    private void CloseAckedGoodbyePeers()
    {
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is PeerSlot { Lifecycle.Phase: PeerPhase.GoodbyeAcked } slot
                && !slot.AckTracker.HasOutstandingAtOrBelow(PacketRecordMode.ReliableOrdered, slot.Lifecycle.GoodbyeSequence))
            {
                ClosePeer(slot.Handle, DisconnectReason.LocalClose);
            }
        }
    }

    /// <summary>
    /// Queues every sequence <see cref="ReportExpiredRetransmissions"/> found expired on one reliable
    /// Space back onto <paramref name="slot"/>'s <see cref="PeerSlot.PendingSends"/>, so the same
    /// Pump's <see cref="FlushOutboundDatagrams"/> packs it. No payload copy is made or needed: a
    /// reliable record's bytes, and its record shape — whole or one Fragment of a cut Message, index
    /// and count included — are in <see cref="_retention"/> from the moment <see cref="Send"/>
    /// committed them, and <see cref="TryResolvePendingPayload"/> reads both back at flush time. That
    /// is what makes the resent bytes identical to the original: this queue entry carries nothing of
    /// its own for <see cref="FlushPeer"/> to rebuild the shape from — <see cref="_retention"/> is the
    /// only place it is ever decided.
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
    /// Puts every Established Peer's queued records on the wire, through one <see cref="FlushPeer"/>
    /// call per Peer per <see cref="Pump"/>: a Peer with nothing queued in <see cref="PeerSlot.PendingSends"/> is skipped
    /// entirely, so a Pump following no <see cref="Send"/> puts nothing on the wire for that Peer. A
    /// Peer still mid-handshake has nothing to flush to either — <see cref="Send"/> can queue against
    /// it, but nothing here reaches it until it reaches <see cref="PeerPhase.Open"/>, the
    /// same restriction <see cref="ReportExpiredRetransmissions"/> already applies to its own per-Peer
    /// sweep.
    /// </summary>
    /// <remarks>
    /// A single scan over <see cref="_peers"/> (<see cref="ShareHolderTable{T}.OccupantAt"/>), the same
    /// shape <see cref="ReportExpiredRetransmissions"/> uses: at most <see cref="ShareHolderTable{T}.Capacity"/>
    /// array reads, and a Peer with an empty queue costs nothing more than that one read.
    /// <see cref="FlushPeer"/> itself trims <see cref="PeerSlot.PendingSends"/> down to whatever it
    /// could not pack this Pump, so there is nothing left for this method to clear — a Peer whose queue
    /// drained completely is left with an empty list, which is exactly what makes the next
    /// Pump's <c>Count == 0</c> check above skip it and send no header-only Datagram, unless its final
    /// ack or its delayed ack (<see cref="IsAckDue"/>) is due.
    /// </remarks>
    private void FlushOutboundDatagrams()
    {
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is not PeerSlot slot || !slot.Lifecycle.IsEstablished)
            {
                continue;
            }

            // A Peer whose Goodbye close is due is flushed even with nothing queued: its Slot is freed
            // right after, so this datagram is the last that could carry the ack of the Goodbye.
            // The same holds for a Closing Peer that just acknowledged the remote's Goodbye (AckDue): that
            // datagram is what tells the remote its Goodbye arrived. A Peer whose ack has been owed since
            // an earlier Pump is flushed too, as one header-only Datagram (ADR 0026).
            if (slot.PendingSends.Count == 0 && !IsHeaderOnlyDatagramDue(slot))
            {
                continue;
            }

            FlushPeer(slot);
            slot.Lifecycle.FinalAckFlushed();
        }
    }

    /// <summary>
    /// Whether <paramref name="slot"/> owes an acknowledgement that no Datagram has carried since an
    /// earlier Pump than this one (ADR 0026). The Pump that classified the record does not send the ack
    /// on its own, so a reply queued before the next Pump carries it instead.
    /// </summary>
    private bool IsAckDue(PeerSlot slot) => slot.AckOwedSincePump < _pumpNumber;

    /// <summary>
    /// Whether a flush of <paramref name="slot"/> sends the header and ack blocks even with no record:
    /// its final ack (<see cref="PeerLifecycle.IsFinalAckDue"/>) or its delayed ack (<see cref="IsAckDue"/>) is due.
    /// </summary>
    private bool IsHeaderOnlyDatagramDue(PeerSlot slot) => slot.Lifecycle.IsFinalAckDue || IsAckDue(slot);

    /// <summary>
    /// Whether <paramref name="slot"/> has gone silent past this end's receive-idle deadline: elapsed
    /// time since <see cref="PeerSlot.LastReceiveTimestamp"/> at or past
    /// <see cref="PeerSlot.RemoteSendIdleInterval"/> times <see cref="DatagramSessionOptions.ResolvedMissedHeartbeats"/>.
    /// Mirrors <see cref="StreamSessionReactor.HasExceededReceiveIdleDeadline"/>.
    /// </summary>
    /// <remarks>
    /// This profile has an adaptive RTO already, unlike the stream profile, and the deadline is
    /// deliberately not derived from it (ADR 0012). The RTO answers "when do I resend the probe", a
    /// fact about the current path that must keep adapting. The count answers "how many missed
    /// intervals before I give up", the consumer's own tolerance, which must not adapt with it.
    /// Deriving the deadline from the RTO would stretch that tolerance exactly when the path degrades,
    /// since backing off the RTO is what the estimator does when the path is bad, so the consumer
    /// would wait longest precisely when it most wanted to be told. It would also tie a consumer-facing
    /// promise to an estimator that Karn's algorithm is already discarding samples from. The Ping
    /// still inherits the adaptive RTO for its own retransmission, so the path-adaptivity this profile
    /// has stays there instead of leaking into liveness. Keeping the deadline a plain count also means
    /// a consumer moving between the datagram and stream session profiles does not have to relearn how
    /// liveness works.
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
    /// Moves every <see cref="PeerPhase.Open"/> Peer that has exceeded <see cref="HasExceededReceiveIdleDeadline"/> to
    /// <see cref="PeerPhase.Interrupted"/> and queues its
    /// <see cref="SessionEventKind.PeerInterrupted"/>, run once per <see cref="Pump"/> ahead of
    /// <see cref="EnforceSendIdleTimer"/> — the two timers are independent: this one reads only receive
    /// state, that one only send state, and neither call arms or disarms the other. No
    /// <see cref="DiagnosticGateway"/> record is logged for the transition, since it is already reported
    /// as an Event (ADR 0013). Also stamps <see cref="PeerLifecycle.LingerStartTimestamp"/> on the same Pump
    /// — the moment this Peer's effective Linger begins counting down. Skips a Peer already
    /// <see cref="PeerPhase.Interrupted"/>, so a deadline that stays exceeded after the transition
    /// never raises a second Event, and never re-stamps the timestamp, for it.
    /// </summary>
    private void EnforceReceiveIdleTimer()
    {
        for (int index = 0; index < _peers.Capacity; index++)
        {
            // Only an Open Peer can be interrupted: an Interrupted one is not re-stamped, and a Closing one
            // never goes Interrupted (issue #406), since only its Goodbye's ack or Linger measured from Close
            // (EnforceLingerExpiry) ends it.
            if (_peers.OccupantAt(index) is not PeerSlot { Lifecycle.Phase: PeerPhase.Open } slot
                || !HasExceededReceiveIdleDeadline(slot))
            {
                continue;
            }

            slot.Lifecycle.TryInterrupt(_options.Reactor.TimeProvider.GetTimestamp());
            _pendingEvents.Add(new PendingEvent
            {
                Kind = SessionEventKind.PeerInterrupted,
                Peer = ToPeerId(slot.Handle),
            });
        }
    }

    /// <summary>
    /// Whether an Interrupted or Closing Peer's effective Linger
    /// (<see cref="PeerSlot.EffectiveLinger"/>, negotiated at handshake and already reported via
    /// <c>LingerNegotiated</c>) has run out since <see cref="PeerLifecycle.LingerStartTimestamp"/>, which is
    /// the interruption for an Interrupted Peer and <see cref="Close"/> for a Closing one. Always
    /// <see langword="false"/> for a Peer in any other phase. The Interrupted case is the
    /// datagram twin of <see cref="StreamSessionReactor.HasExceededLinger"/>. Linger is what closes an
    /// Interrupted Peer, not the transport loss that produced it — the loss already happened,
    /// potentially long before this check ever runs (<see cref="EnforceReceiveIdleTimer"/> stamped
    /// <see cref="PeerLifecycle.LingerStartTimestamp"/> on whichever earlier Pump crossed the receive-idle
    /// deadline), so the deadline is measured from the moment this end stopped waiting on that
    /// transport rather than from anything about the transport itself.
    /// </summary>
    private bool HasLingerExpired(PeerSlot slot) =>
        slot.Lifecycle.IsLingering
        && _options.Reactor.TimeProvider.GetElapsedTime(slot.Lifecycle.LingerStartTimestamp) >= slot.EffectiveLinger;

    /// <summary>
    /// Closes every <see cref="PeerPhase.Interrupted"/> Peer whose <see cref="HasLingerExpired"/>
    /// with <see cref="DisconnectReason.LingerExpired"/>, run once per <see cref="Pump"/> right after
    /// <see cref="EnforceReceiveIdleTimer"/> — mirroring
    /// <see cref="StreamSessionReactor.EnforceIdleTimers"/>'s own ordering, where the Linger check for an
    /// already-Interrupted Peer runs in the same pass as, and right after, the receive-idle check that
    /// can newly interrupt one. A Peer <see cref="EnforceReceiveIdleTimer"/> interrupted this very Pump
    /// can in principle be seen by this pass in the same Pump — nothing here waits a Pump before
    /// checking it — but <see cref="HasLingerExpired"/> can never be true for it yet:
    /// <see cref="PeerLifecycle.LingerStartTimestamp"/> was just stamped to this Pump's own clock reading, so
    /// the elapsed time this pass reads is zero, and <see cref="DatagramSessionOptions"/> rejects a zero
    /// <see cref="PeerSlot.EffectiveLinger"/> at construction — <c>0 &gt;= EffectiveLinger</c> can only
    /// hold for an <see cref="PeerSlot.EffectiveLinger"/> that validation already refuses to admit. Runs
    /// ahead of <see cref="EnforceSendIdleTimer"/> so a Peer this pass closes is never offered a Ping
    /// into a Slot it no longer occupies. Also closes a Closing Peer (<see cref="PeerLifecycle.IsClosing"/>)
    /// whose Goodbye is still unacknowledged once its effective Linger has elapsed since
    /// <see cref="PeerLifecycle.LingerStartTimestamp"/>.
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
            // A Closing Peer's Linger runs from Close, an Interrupted one's from the interruption (issue #399).
            if (_peers.OccupantAt(index) is not PeerSlot slot || !HasLingerExpired(slot))
            {
                continue;
            }

            ClosePeer(slot.Handle, DisconnectReason.LingerExpired);
        }
    }

    /// <summary>
    /// Queues one Ping for every Established Peer that has sent no data datagram for
    /// <see cref="DatagramSessionOptions.SendIdleInterval"/>, run once per <see cref="Pump"/> ahead of
    /// <see cref="FlushOutboundDatagrams"/> so a Ping this call queues rides the same Pump's flush out
    /// — a Peer this method queues against is never left waiting a whole further Pump just to reach the
    /// wire. A Ping is a Heartbeat, and a Heartbeat is not a keepalive: it proves the party behind this
    /// Session is still there to receive and eventually acknowledge application data, not merely that
    /// some NAT mapping between here and there is still open — a bare keepalive a middlebox itself
    /// could answer would prove nothing about the party ADR 0011's liveness deadline actually cares
    /// about.
    /// </summary>
    /// <remarks>
    /// Reads <see cref="PeerSlot.LastSendTimestamp"/> against this Reactor's own
    /// <see cref="ReactorOptions.TimeProvider"/>, never the ambient clock, matching every other timer in
    /// this Reactor. A Peer still mid-handshake is skipped exactly as <see cref="FlushOutboundDatagrams"/>
    /// skips one — there is no established Session yet for a Heartbeat to prove anything about. This loop
    /// deliberately does not read <see cref="PeerLifecycle.IsInterrupted"/>: an Interrupted datagram Peer
    /// is still <see cref="PeerLifecycle.IsEstablished"/>, so it keeps
    /// being offered a Ping here exactly like any other Established Peer. A datagram Session has no
    /// re-dial schedule — there is nothing to re-dial, and the interrupted end can only resume by being
    /// heard from again — so it keeps probing the path with Pings, at the send-idle interval or as the
    /// adaptive retransmission of one still unacknowledged, for as long as Linger keeps the Peer alive.
    /// </remarks>
    private void EnforceSendIdleTimer()
    {
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is not PeerSlot slot || !slot.Lifecycle.IsEstablished)
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
    /// address when the Peer is a dialled, <see cref="PeerPhase.Interrupted"/> one and no Resume has
    /// left for it within the last <see cref="DatagramSessionOptions.SendIdleInterval"/>: the first
    /// goes out on the Pump that finds the Peer Interrupted, every later one a send-idle interval after
    /// the previous. Only the dialling end sends Resume, because the accepting end cannot know the
    /// dialler's new address. The datagram is routed by the accepting end's own session id
    /// (<see cref="PeerSlot.RemoteSessionId"/>) and authorised by the secret it issued
    /// (<see cref="PeerSlot.RemoteResumeSecret"/>), ADR 0009.
    /// </summary>
    /// <remarks>
    /// A Resume is deliberately not a data datagram, so it does not stamp
    /// <see cref="PeerSlot.LastSendTimestamp"/>: Pings keep exactly their own send-idle cadence, and the
    /// Resume cadence reads <see cref="PeerLifecycle.LastResumeTimestamp"/> instead. It is still charged to
    /// the Peer's <see cref="PeerCounters.SocketWrites"/> and <see cref="PeerCounters.BytesSent"/>, send
    /// success or not, as a data datagram is (ADR 0013).
    /// </remarks>
    private void SendResumeIfDue(PeerSlot slot)
    {
        if (!slot.IsDialled || !slot.Lifecycle.IsInterrupted)
        {
            return;
        }

        long now = _options.Reactor.TimeProvider.GetTimestamp();
        if (slot.Lifecycle.LastResumeTimestamp is long lastResume
            && _options.Reactor.TimeProvider.GetElapsedTime(lastResume, now) < _options.SendIdleInterval)
        {
            return;
        }

        ControlDatagramCodec.WriteResume(_sendBuffer, slot.RemoteSessionId, slot.RemoteResumeSecret);
        SendControl(slot.Link, slot.Remote);

        // Unlike a handshake control datagram, a Resume has a Peer to charge (ADR 0013).
        slot.Counters.RecordSocketWrite();
        slot.Counters.RecordBytesSent(DatagramSessionProtocol.ControlDatagramSize);
        slot.Lifecycle.ResumeSent(now);
    }

    /// <summary>
    /// Takes an Interrupted Peer back to <see cref="PeerPhase.Open"/> on hearing its session again:
    /// queues <see cref="SessionEventKind.PeerResumed"/> with the same <see cref="PeerId"/> (generation
    /// included), calls <see cref="Core.PeerCounterSet.RecordResume"/> once and logs
    /// <see cref="DiagnosticId.ResumeMatched"/> with <c>Value0</c> 0 (a datagram Session rewinds nothing).
    /// An Interrupted dialled Peer resumes on any well-formed data Datagram for its Session, an
    /// Interrupted accepted Peer only on one from exactly its current <see cref="PeerSlot.Remote"/>
    /// (address and port). Does nothing for a Peer that is not Interrupted.
    /// </summary>
    /// <remarks>
    /// Called once <see cref="ParseDataDatagram"/> has fully parsed a data datagram, the same well-formedness
    /// proof that stamps <see cref="PeerSlot.LastReceiveTimestamp"/>. On the dialling end, an observer who can
    /// see the session id can forge such a datagram, so it can fake this exit. That is accepted: the exit
    /// grants no takeover (moving the accepting end's send address still demands the resume secret,
    /// ADR 0009) and on-path attackers are out of scope. Counters are never reset by a Resume (ADR 0013).
    /// <para>
    /// On the accepting end the datagram is not authenticated either, because the session id is only a
    /// demultiplexing token (ADR 0009). A party able to forge a data Datagram from the held address with the
    /// right session id can already keep <see cref="PeerSlot.LastReceiveTimestamp"/> fresh and stop the Peer
    /// being interrupted at all, so resuming on the same evidence grants it nothing new. This path never moves
    /// the send address, so the comparison is on the whole <see cref="Source"/>: a datagram from the same IP
    /// on another port would otherwise report a resume while sends still go to the old port. A new port or IP
    /// needs a matched Resume (and a path challenge for a new IP, ADR 0021).
    /// </para>
    /// <para>
    /// No "has sent a Resume yet" check is needed on the dialled end (ADR 0015): the Pump that marks a dialled
    /// Peer Interrupted also sends its first Resume (<c>SendResumeIfDue</c> runs after the receive-idle
    /// check), and datagrams are read before either, so every datagram that reaches an Interrupted
    /// dialled Peer arrives after at least one Resume went out.
    /// </para>
    /// </remarks>
    private void ResumeInterruptedPeerOnData(Source source, PeerSlot slot)
    {
        bool resumesOnThisSource = slot.IsDialled || source == slot.Remote;
        if (!resumesOnThisSource || !slot.Lifecycle.TryLeaveInterrupted())
        {
            return;
        }

        PeerId peer = ToPeerId(slot.Handle);
        RecordResumed(slot, peer);
        if (_diagnostics.IsEnabled(DiagnosticLevel.Info))
        {
            _diagnostics.Log(DiagnosticRecord.ForResumeMatched(peer, bytesRewound: 0));
        }
    }

    /// <summary>
    /// Queues one seven-byte control record — the send-idle Ping — on <paramref name="slot"/>'s
    /// <see cref="PeerSlot.PendingSends"/>, exactly as an ordinary single-Fragment reliable
    /// <see cref="Send"/> queues one: the same <c>ReliableUnordered</c> sequence allocation and
    /// retention commit, with <see cref="RetentionPool.TryAcquire"/>'s <c>isControl</c> flag set so
    /// <see cref="TryResolvePendingPayload"/> — on this send and on every later resend
    /// <see cref="ReportExpiredRetransmissions"/> re-queues — packs it through
    /// <see cref="Session.DatagramRecordPacker.TryPackPingRecord"/> rather than as an ordinary
    /// zero-length Message. A Ping is always <c>ReliableUnordered</c> (ADR 0009's control-record
    /// shape): ordering a Heartbeat against application data would only ever add a head-of-line
    /// dependency neither needs.
    /// </summary>
    /// <remarks>
    /// Spends nothing on a refusal, the same contract <see cref="Send"/> keeps: the sequence is only
    /// read, never allocated, until <see cref="RetentionPool.TryAcquire"/> commits, so a Peer whose
    /// <c>ReliableUnordered</c> Space has no free slot this Pump leaves its next sequence exactly where
    /// it was and is tried again — this same check, unconditionally, every Pump — the next time
    /// <see cref="EnforceSendIdleTimer"/> finds it still idle.
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
    /// <c>ReliableOrdered</c> Space — exactly as <see cref="QueuePing"/> queues a Ping: the next
    /// sequence, retained with the control flag so first send and every resend pack it as a Goodbye.
    /// Spends nothing when the Space's retention is full, leaving the Peer in
    /// <see cref="PeerPhase.GoodbyeDeferred"/> for <see cref="QueueDeferredGoodbyes"/> to retry.
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
        slot.Lifecycle.GoodbyeQueued(sequence);
    }

    /// <summary>
    /// Retries the Goodbye of every Closing Peer whose <see cref="Close"/> found its <c>ReliableOrdered</c>
    /// retention full.
    /// </summary>
    private void QueueDeferredGoodbyes()
    {
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is PeerSlot { Lifecycle.Phase: PeerPhase.GoodbyeDeferred } slot)
            {
                QueueGoodbye(slot);
            }
        }
    }

    /// <summary>
    /// Writes <paramref name="slot"/>'s queue as data Datagrams of at most
    /// <see cref="DatagramSessionProtocol.DatagramHeaderSize"/> + <see cref="DatagramSessionProtocol.FragmentRecordSize"/>
    /// + <see cref="PeerSlot.FragmentSize"/> bytes each, in Send order, until the queue is empty or this Pump
    /// has written <see cref="MaxUdpDatagramSize"/> divided by that limit of them for this Peer (issue #485).
    /// </summary>
    /// <remarks>
    /// The limit is the one Datagram size that always holds a full Fragment, so a Datagram never exceeds
    /// <see cref="DatagramSessionOptions.FragmentSize"/> plus this profile's headers and is not split by IP.
    /// The Datagram count keeps one Pump's bytes for one Peer at or below one maximum UDP payload, so a
    /// backlog leaves over several Pumps instead of in one burst a receiver's socket buffer would drop.
    /// What is left waits, in order, for the next Pump. The first Datagram is written even with nothing
    /// queued when the Peer's final ack is due (<see cref="PeerLifecycle.IsFinalAckDue"/>) or its
    /// delayed ack is due (<see cref="IsAckDue"/>).
    /// </remarks>
    private void FlushPeer(PeerSlot slot)
    {
        int datagramLimit = DatagramSessionProtocol.DatagramHeaderSize + DatagramSessionProtocol.FragmentRecordSize + slot.FragmentSize;
        int datagramBudget = MaxUdpDatagramSize / datagramLimit;
        int datagramsWritten = 0;
        do
        {
            int queuedBefore = slot.PendingSends.Count;
            if (FlushOneDatagram(slot, datagramLimit))
            {
                datagramsWritten++;
            }

            Debug.Assert(
                queuedBefore == 0 || slot.PendingSends.Count < queuedBefore,
                "every queued record fits an empty Datagram of datagramLimit bytes, so each pass consumes at least one.");
            if (slot.PendingSends.Count == queuedBefore)
            {
                break;
            }
        }
        while (slot.PendingSends.Count > 0 && datagramsWritten < datagramBudget);

        CompactUnreliableQueue(slot);
    }

    /// <summary>
    /// Moves the unreliable bytes still queued for <paramref name="slot"/> to the front of its region of
    /// <see cref="_unreliableQueue"/>, rewrites their records' offsets, and recomputes
    /// <see cref="PeerSlot.UnreliableQueuedBytes"/> from the records left, so what a flush wrote is free
    /// for the next <see cref="Send"/>.
    /// </summary>
    /// <remarks>
    /// Unreliable records enter the queue only at its end and leave only from its front, so their bytes
    /// are in queue order and the ones a flush consumed are a prefix of the region. The first unreliable
    /// record still queued starts where that prefix ends, which is how far everything moves. The charge
    /// is recomputed rather than decremented, so empty Messages, which move no bytes, release their
    /// record headers too.
    /// </remarks>
    private void CompactUnreliableQueue(PeerSlot slot)
    {
        List<PendingOutboundRecord> queue = slot.PendingSends;
        int shift = slot.UnreliableQueuedPayloadEnd;
        bool foundLive = false;
        int charged = 0;
        for (int i = 0; i < queue.Count; i++)
        {
            PendingOutboundRecord pending = queue[i];
            if (IsReliable(pending.Mode))
            {
                continue;
            }

            if (!foundLive)
            {
                shift = pending.UnreliableOffset;
                foundLive = true;
            }

            charged += pending.UnreliableLength + DatagramSessionProtocol.UnfragmentedRecordSize;
        }

        if (shift > 0)
        {
            Span<byte> region = UnreliableRegion(slot);
            region[shift..slot.UnreliableQueuedPayloadEnd].CopyTo(region);
            for (int i = 0; i < queue.Count; i++)
            {
                PendingOutboundRecord pending = queue[i];
                if (!IsReliable(pending.Mode))
                {
                    queue[i] = pending with { UnreliableOffset = pending.UnreliableOffset - shift };
                }
            }
        }

        slot.UnreliableQueuedPayloadEnd -= shift;
        slot.UnreliableQueuedBytes = charged;
    }

    /// <summary>
    /// Packs as much as fits of <paramref name="slot"/>'s queue into one data Datagram of at most
    /// <paramref name="datagramLimit"/> bytes, in Send order, and sends it: the 20-byte header — this Peer's own <see cref="PeerSlot.RemoteSessionId"/>
    /// stamped as the session, since that is the id the far end asked to be stamped on datagrams sent to
    /// it — followed by both reliable Spaces' ack blocks read fresh from
    /// <see cref="_receiveWindows"/>, so an admission this same Pump's read pass just made is reflected
    /// in the Datagram this same Pump's flush sends. A reliable record's payload is read back from
    /// <see cref="_retention"/> and an unreliable one's from <see cref="_unreliableQueue"/>
    /// (<see cref="TryResolvePendingPayload"/>).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>A record already acked before its flush.</b> An ack read earlier this same Pump can release a
    /// sequence's retention slot before it reaches this flush — queued for its first send, or re-queued
    /// for retransmission by <see cref="ReportExpiredRetransmissions"/> — in which case
    /// <see cref="TryResolvePendingPayload"/> fails and the record below is consumed unsent rather than
    /// retried: resending an already-released sequence would be a duplicate the far end has already
    /// acknowledged.
    /// </para>
    /// <para>
    /// <b>A record that does not fit waits for the next Datagram.</b> Packing stops at the first record
    /// <see cref="DatagramRecordPacker.TryPackRecord"/> refuses, and everything from there to the end of
    /// <see cref="PeerSlot.PendingSends"/> stays queued in order: <see cref="List{T}.RemoveRange"/> trims
    /// only the prefix this call consumed. <see cref="FlushPeer"/> starts the next Datagram there, or the
    /// next Pump does once this Pump's Datagram count for the Peer is spent. A record whose payload failed
    /// to resolve, a reliable sequence already released from retention, is consumed rather than left
    /// queued, since nothing repopulates a released slot.
    /// </para>
    /// <para>
    /// <b>Arming still happens only where the record actually reached a Datagram.</b> A record that did
    /// not fit is not sent, so it must not start its retransmit deadline — the loop below only reaches
    /// the <see cref="OutstandingReliableSequences.RecordSent"/> call for a record
    /// <see cref="DatagramRecordPacker.TryPackRecord"/> just accepted, never for the one that broke the
    /// loop, so a queued-but-unsent record is armed on whichever later Pump actually packs it. Nothing
    /// is sent at all if nothing ended up packed, so a queue whose one record failed to resolve costs a
    /// wasted flush rather than a header-only Datagram with no records in it, unless an ack is due and
    /// the header-only Datagram is what carries it.
    /// </para>
    /// </remarks>
    /// <param name="slot">The Peer whose queue is packed.</param>
    /// <param name="datagramLimit">The most bytes this Datagram may hold, header included.</param>
    /// <returns><see langword="true"/> if a Datagram was written.</returns>
    private bool FlushOneDatagram(PeerSlot slot, int datagramLimit)
    {
        (AckBlock reliableOrdered, AckBlock reliableUnordered) = _receiveWindows[slot.Handle.Index].GenerateAckBlocks();

        DatagramRecordPacker packer = new(
            _outboundDatagramBuffer.AsSpan(0, datagramLimit),
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

        // Nothing packed sends nothing, except for a Peer whose Goodbye close is due, which sends the
        // header and ack blocks alone as the one datagram that can still tell the remote its Goodbye
        // arrived, and a Peer whose ack is due, which sends them alone as its delayed ack (ADR 0026).
        if (packer.RecordsPacked == 0 && !IsHeaderOnlyDatagramDue(slot))
        {
            return false;
        }

        UdpLinkSendResult result = slot.Link.SendTo(slot.Remote, _outboundDatagramBuffer.AsSpan(0, packer.BytesWritten));
        slot.Counters.RecordSocketWrite();
        slot.Counters.RecordBytesSent((ulong)packer.BytesWritten);
        if (result.IsSuccess)
        {
            _counters.RecordDatagramSent();
        }

        // A data datagram just left for this Peer, so the send-idle interval restarts from now (issue
        // #340): a Ping or a header-only ack counts as a sent datagram like any other, which keeps an idle
        // Session at one Datagram per end per interval. Its ack blocks acknowledge everything classified
        // so far, so nothing is owed any more.
        slot.LastSendTimestamp = _options.Reactor.TimeProvider.GetTimestamp();
        slot.AckOwedSincePump = null;
        return true;
    }

    /// <summary>
    /// The bytes, and the record shape, one queued record's flush should pack: <paramref name="pending"/>'s
    /// own copy for an unreliable record, since nothing else retains it, and never a Fragment (ADR 0004
    /// — only reliable modes ever fragment); otherwise both are read back from <see cref="_retention"/>
    /// at the reliable Space <paramref name="pending"/>'s mode addresses, which is where <see cref="Send"/>
    /// recorded them and the only place a retransmit — <paramref name="pending"/> itself carries nothing
    /// of the shape — can recover them from. Fails — nothing to pack — only if a reliable sequence was
    /// already released from retention between being queued and this flush, for example by an ack read
    /// earlier the same Pump. The caller consumes such a record unsent, whatever released it.
    /// </summary>
    /// <param name="slot">The Peer this record was queued against.</param>
    /// <param name="pending">The queued record whose bytes and shape are being resolved.</param>
    /// <param name="payload">The resolved record's payload bytes.</param>
    /// <param name="isFragment">Whether the record is one piece of a Message cut into Fragments.</param>
    /// <param name="fragmentIndex">This piece's position within its Message, meaningful only when <paramref name="isFragment"/> is <see langword="true"/>.</param>
    /// <param name="fragmentCount">The Message's total piece count, meaningful only when <paramref name="isFragment"/> is <see langword="true"/>.</param>
    /// <param name="isControl">
    /// Whether this record is a control record — an idle-triggered Ping — rather than a Message a
    /// consumer Sent, read back from <see cref="_retention"/> exactly as <paramref name="isFragment"/>
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
        if (!IsReliable(pending.Mode))
        {
            payload = UnreliableRegion(slot).Slice(pending.UnreliableOffset, pending.UnreliableLength);
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
    /// refused plus whatever the touched window's slide just retired un-admitted, and delivers what it
    /// admitted.
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
    /// <b>Plain <c>Unreliable</c> is the exception</b> (ADR 0017). Its promise is that a late arrival
    /// is delivered and that duplicate suppression is bounded at 64 sequences, and the receive window
    /// can keep neither: it refuses everything below its base outright, so a repeat delayed past 64
    /// would be discarded rather than delivered, and its capacity is a consumer option rather than the
    /// protocol's 64. So for that Space the receive window still classifies — that is what counts loss
    /// from the gaps it slides past, and what its ack bookkeeping needs — while the delivery decision
    /// comes from <see cref="PeerSlot.UnreliableDuplicates"/> alone. The receive window's own refusals
    /// are not counted for this Space, since the 64-entry window is the authority on what was a
    /// duplicate here and counting both would count one repeat twice.
    /// </para>
    /// <para>
    /// <b>An admitted Fragment is diverted, not delivered</b> (ADR 0004). A Fragment is one piece of a
    /// Message, not a Message, so an admitted one is held in its Space's receive-window slot instead —
    /// <see cref="HoldFragment"/> — rather than reaching <see cref="DeliverAdmittedRecord"/>: no
    /// frame-arena reservation, no pending <see cref="SessionEventKind.MessageReceived"/>, and
    /// <see cref="PeerCounters.MessagesReceived"/> does not rise. <see cref="HoldFragment"/> also
    /// derives the held Fragment's group first sequence and answers whether its group is now complete;
    /// a complete group is handed to <see cref="TryDeliverOrDeferFragmentGroup"/>, which delivers it as
    /// one Message or defers it if delivery cannot happen yet. Only <c>ReliableOrdered</c> and
    /// <c>ReliableUnordered</c> ever carry the fragment shape (ADR 0004); <see cref="PacketRecord.IsFragment"/>
    /// is otherwise ignored exactly as it always was, so a stray fragment bit on an unreliable-mode
    /// record changes nothing this method does. By the time this method runs at all,
    /// <see cref="ParseDataDatagram"/>'s own pass has already refused any Fragment in this datagram
    /// whose declared <c>fragmentCount</c> claims more Fragments than this Reactor's own receive
    /// window, <see cref="DatagramSessionOptions.ResolvedReceiveWindowFragments"/>, could ever hold, so
    /// <see cref="PacketRecord.FragmentCount"/> here is always a legal claim, never one this method itself
    /// needs to bound.
    /// </para>
    /// <para>
    /// <b>An abandoned Fragment set is reported, never acted on</b>. The
    /// <see cref="ReceiveWindow.Classify"/> call above may have
    /// just clobbered the ring slot of an incomplete Fragment set that can never complete. That is only
    /// a set the base has passed entirely, because
    /// <see cref="ReceiveWindow.WouldOverwriteDeliverableFragment"/> refuses to overwrite any other. The
    /// window itself has no <see cref="PeerId"/> to subject a Diagnostic to, so this method reads
    /// <see cref="ReceiveClassification.AbandonedFragmentSet"/> from that call's result and logs
    /// <see cref="DiagnosticRecord.ForFragmentSetAbandoned"/> from it. This is accounting only, so
    /// nothing here closes, disconnects, or otherwise changes Session state — the fragment-size stall
    /// check does that, off a different signal.
    /// </para>
    /// <para>
    /// <b>An admitted control record — a Ping — is acknowledged and never delivered</b>.
    /// <see cref="PacketRecord.IsControl"/> comes from <see cref="PacketRecordCodec.TryRead"/>, which reports it
    /// only for a record that is legally a Ping: <c>ReliableUnordered</c>, unfragmented, zero length.
    /// Admission runs exactly as it does for any other record — the same <see
    /// cref="ReceiveWindow.Classify"/> call above, the same
    /// duplicate and out-of-window counting, the same ack block once <see
    /// cref="ReceiveWindowSpaces.GenerateAckBlocks"/> next runs — so a Ping at sequence N is covered by
    /// the ordinary ack block exactly as a Message at N would be, and a duplicate Ping raises <see
    /// cref="PeerCounters.DuplicatePackets"/> exactly as a duplicate Message does. What differs is what
    /// happens once admitted: neither <see cref="DeliverAdmittedRecord"/> nor <see cref="HoldFragment"/>
    /// is reached for it, so no <see cref="SessionEventKind.MessageReceived"/> is queued and <see
    /// cref="PeerCounters.MessagesReceived"/> does not rise.
    /// </para>
    /// </remarks>
    /// <param name="slot">The Peer the record arrived for.</param>
    /// <param name="record">The record, already proved well-formed by the datagram's first pass.</param>
    private void ClassifyRecord(PeerSlot slot, PacketRecord record)
    {
        // A Goodbye earlier in this same Datagram may have closed the Peer directly (issue #400); its
        // Slot can already belong to no one, and nothing after the Goodbye is admitted.
        if (_peers.TryResolve(slot.Handle) is null)
        {
            return;
        }

        ReceiveWindowSpaces windows = _receiveWindows[slot.Handle.Index];

        // Left unadmitted, so no ack covers it and the far end's retransmission brings it back once the
        // group holding its slot has been delivered and the slot is free (issues #423, #446).
        if (IsReliable(record.Mode)
            && windows.ReliableWindow(record.Mode).WouldOverwriteDeliverableFragment(record.Sequence))
        {
            return;
        }

        if (RefusesForHoldBudget(
                slot, windows, record.Mode, record.Sequence, record.IsFragment, record.Payload.Length))
        {
            slot.Counters.RecordOutOfWindowPacket();
            return;
        }

        if (RefusesForArenaRoom(
                slot, windows, record.Mode, record.Sequence, record.IsFragment, record.IsControl,
                record.Payload.Length))
        {
            return;
        }

        ReceiveClassification classification = windows.Window(record.Mode).Classify(record.Sequence);
        ReceiveWindowOutcome outcome = classification.Outcome;
        int ringSlot = classification.RingSlot;

        // Any reliable record classified here is owed an ack, a duplicate included: a repeat means the
        // ack that covered it was lost (ADR 0026).
        if (IsReliable(record.Mode))
        {
            slot.AckOwedSincePump ??= _pumpNumber;
        }

        // The moment a Fragment set's slot is discarded is the last moment anyone can say how much of
        // it had arrived (issue #297): Classify just clobbered such a slot if this is set. This
        // is accounting only — it says what was lost and not why, which is exactly why it may never
        // close, disconnect, or otherwise change Session state itself.
        (bool abandoned, long held, long expected) = classification.AbandonedFragmentSet;
        if (abandoned && _diagnostics.IsEnabled(DiagnosticLevel.Warn))
        {
            _diagnostics.Log(DiagnosticRecord.ForFragmentSetAbandoned(ToPeerId(slot.Handle), held, expected));
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
        bool acceptsMessages = !slot.Lifecycle.RemoteGoodbyeDelivered;

        if (acceptsMessages && record.Mode == PacketRecordMode.Unreliable)
        {
            DeliverUnreliableRecord(
                slot, record.Sequence, record.Payload, alreadyCountedAsRefused: outcome != ReceiveWindowOutcome.Admitted);
        }
        else if (acceptsMessages && outcome == ReceiveWindowOutcome.Admitted)
        {
            if (record.IsControl)
            {
                // Admitted into its Space's receive window exactly like any other record above, so the
                // ordinary ack block covers it and a duplicate counts like any other duplicate, but it
                // carries no payload for a consumer to receive, so MessagesReceived does not rise.
                // A ReliableUnordered one is a Ping (#339) and nothing more happens. A ReliableOrdered
                // one is a Goodbye (#400): it takes its turn in send order like a Message would, and
                // acts only when delivered. The lowest sequence is kept so a second, misbehaving
                // Goodbye can never displace an earlier one.
                if (record.Mode == PacketRecordMode.ReliableOrdered)
                {
                    if (slot.RemoteGoodbyeSequence is not uint pending || SequenceNumber.IsNewerThan(pending, record.Sequence))
                    {
                        slot.RemoteGoodbyeSequence = record.Sequence;
                    }

                    DeliverAdmittedRecord(slot, record.Mode, record.Sequence, record.Payload);
                }
            }
            else if (record.IsFragment
                && record.Mode is PacketRecordMode.ReliableOrdered or PacketRecordMode.ReliableUnordered)
            {
                bool groupComplete = HoldFragment(
                    slot, record.Mode, record.Sequence, ringSlot, record.FragmentIndex, record.FragmentCount,
                    record.Payload);
                if (groupComplete)
                {
                    uint groupFirstSequence = FragmentGroup.FirstSequenceOf(record.Sequence, record.FragmentIndex);
                    TryDeliverOrDeferFragmentGroup(slot, record.Mode, groupFirstSequence, record.FragmentCount);
                }
            }
            else
            {
                DeliverAdmittedRecord(slot, record.Mode, record.Sequence, record.Payload);
            }
        }

        // No mode test guards this (#176): a window reports a nonzero SlideLoss only if it slid,
        // and only the two unreliable Spaces' windows are built to slide, so this counts both of them
        // and neither reliable Space without restating which Spaces slide. ReceiveWindowSpaces'
        // constructor stays the one place that decides that, so the two cannot drift apart.
        if (classification.SlideLoss > 0)
        {
            slot.Counters.RecordUnreliableLoss(classification.SlideLoss);
        }
    }

    /// <summary>
    /// Decides, before the receive window sees it, whether an ordinary <c>ReliableOrdered</c> record
    /// would go past what <see cref="PeerSlot.OrderedDelivery"/> may hold. A refused record is left
    /// unadmitted, so no ack covers it and the far end's ordinary retransmission brings it back once
    /// the send-order head has moved. A head that never moves stops this Space and no other.
    /// </summary>
    /// <remarks>
    /// A Fragment is never held by the queue, so only the receive window bounds it. A Peer whose Goodbye
    /// has been delivered holds nothing further, so what it sends is still admitted and acknowledged. A
    /// sequence the window would not admit anyway is left for <see cref="ReceiveWindow.Classify"/>
    /// to count as usual.
    /// </remarks>
    /// <returns><see langword="true"/> when the record was refused and must not be classified.</returns>
    private static bool RefusesForHoldBudget(
        PeerSlot slot, ReceiveWindowSpaces windows, PacketRecordMode mode, uint sequence, bool isFragment, int payloadLength) =>
        mode == PacketRecordMode.ReliableOrdered
        && !isFragment
        && !slot.Lifecycle.RemoteGoodbyeDelivered
        && windows.ReliableWindow(mode).WouldAdmit(sequence)
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

        if (!deliversOnAdmission || isFragment || isControl || slot.Lifecycle.RemoteGoodbyeDelivered
            || payloadLength > _options.MaxMessageSize || !windows.ReliableWindow(mode).WouldAdmit(sequence))
        {
            return false;
        }

        return !AccountForReservation(slot, _arena.Check(slot.Handle.Index, payloadLength));
    }

    /// <summary>
    /// The plain <c>Unreliable</c> Space's delivery decision (ADR 0017): deliver unless this Peer's own
    /// 64-entry <see cref="UnreliableDuplicateWindow"/> has already seen <paramref name="sequence"/>. A
    /// repeat inside those 64 sequences is suppressed and counted as a duplicate; a repeat delayed past
    /// them is delivered, because suppression here is bounded rather than absolute and pretending
    /// otherwise would promise something a 64-entry window cannot keep.
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
    /// <paramref name="ringSlot"/> instead (ADR 0004): copies its payload into the fixed-size slot the
    /// receive arena preallocated for that ring position, and records the payload's byte length on the
    /// window so a later Pump can tell a held Fragment apart from an empty slot. Nothing is reserved in
    /// the frame arena, no pending <see cref="SessionEventKind.MessageReceived"/> is queued, and
    /// <see cref="PeerCounters.MessagesReceived"/> does not rise — a Fragment is a piece of a Message,
    /// not a Message, so none of the three steps <see cref="DeliverToConsumer"/> takes apply to it.
    /// Once the length is recorded, this derives the held piece's group first sequence, records that
    /// identity on the window too — so the set can still be named if its slot is later discarded before
    /// it completes — and answers whether every piece of that group is now held.
    /// </summary>
    /// <remarks>
    /// <para>
    /// There is no separate reassembly buffer to copy into instead (ADR 0004): the receive window this
    /// Fragment was already admitted into <em>is</em> the reassembly storage, by design rather than by
    /// omission. A dedicated buffer would be new attack surface — a sender flooding partial Fragment
    /// sets would fill memory taken solely for that purpose — while this design lets such a sender only
    /// ever fill the same receive window an honest sender's traffic already bounds and this Reactor
    /// already defends. <paramref name="ringSlot"/> comes from
    /// <see cref="ReceiveWindow.Classify"/>'s own call in
    /// <see cref="ClassifyRecord"/>, which is why this method takes the slot rather than re-resolving it
    /// from <paramref name="mode"/> and a sequence — the window's <see cref="ReceiveWindow.Base"/> may
    /// already have advanced past that sequence by the time this call is made.
    /// </para>
    /// <para>
    /// <b>Survives an interruption at no cost.</b> A held Fragment's slot is ordinary per-Peer
    /// <see cref="PeerSlot"/> and receive-arena state — the same state a lost and rebuilt transport
    /// leaves untouched, because this profile identifies a Session by its wire token rather than by the
    /// address or link a datagram arrived on (see <see cref="ProcessDataDatagram"/>'s remarks). A
    /// half-arrived group's pieces are therefore still exactly here, still counted against this Peer's
    /// own receive window, no matter how long the gap between one Fragment and the next: nothing reads
    /// this state as part of resuming a Session, and nothing needs to, because nothing about it was
    /// ever tied to the transport that went away. This is the general shape any resume of any kind must
    /// respect — <see cref="StreamSessionReactor"/>'s own Resume makes the same promise for its
    /// profile, stream framing rather than Fragments: resuming fixes <em>where delivery continues
    /// from</em>, at Message granularity, and never discards what a receive window is already holding
    /// piece by piece underneath that.
    /// </para>
    /// </remarks>
    /// <param name="slot">The Peer this Fragment arrived on.</param>
    /// <param name="mode">The Fragment's delivery mode — always <c>ReliableOrdered</c> or <c>ReliableUnordered</c> (ADR 0004).</param>
    /// <param name="sequence">The Fragment's own sequence — see <see cref="FragmentGroup.FirstSequenceOf"/>.</param>
    /// <param name="ringSlot">The ring position the admitting <see cref="ReceiveWindow.Classify"/> call reported.</param>
    /// <param name="fragmentIndex">The Fragment's zero-based position within its group.</param>
    /// <param name="fragmentCount">The group's declared Fragment count.</param>
    /// <param name="payload">The Fragment's payload bytes.</param>
    /// <returns>
    /// Whether the Fragment's group is complete after this piece was held. The payload always fits its
    /// slot: <see cref="ProcessDataDatagram"/> closes the Peer before classification on a Fragment
    /// wider than <see cref="_maxLegalFragmentWidth"/>. The caller delivers the group once this is
    /// <see langword="true"/>.
    /// </returns>
    private bool HoldFragment(
        PeerSlot slot, PacketRecordMode mode, uint sequence, int ringSlot,
        ushort fragmentIndex, ushort fragmentCount, ReadOnlySpan<byte> payload)
    {
        int spaceIndex = ReliableSpaceIndex(mode);
        Span<byte> fragmentSlot = _receiveArena.GetFragmentSlot(slot.Handle.Index, spaceIndex, ringSlot);

        Debug.Assert(
            payload.Length <= fragmentSlot.Length,
            "ProcessDataDatagram closes the Peer on a Fragment wider than the FragmentSize before any record is classified.");

        payload.CopyTo(fragmentSlot);
        ReceiveWindow window = _receiveWindows[slot.Handle.Index].ReliableWindow(mode);
        window.RecordFragmentLength(ringSlot, (ushort)payload.Length);

        uint groupFirstSequence = FragmentGroup.FirstSequenceOf(sequence, fragmentIndex);
        window.RecordFragmentGroup(ringSlot, groupFirstSequence, fragmentCount);
        return window.IsGroupComplete(groupFirstSequence, fragmentCount);
    }

    /// <summary>
    /// Decides how a just-completed Fragment group reaches the consumer. For <c>ReliableUnordered</c>
    /// there is no ordering rule to satisfy, so this attempts delivery immediately and, if the frame
    /// arena had no room, defers it on the Space's <see cref="PendingFragmentGroups"/> for
    /// <see cref="RetryPendingFragmentDeliveries"/> to retry on a later Pump. For
    /// <c>ReliableOrdered</c> the group must additionally wait its turn in this Peer's
    /// <see cref="PeerSlot.OrderedDelivery"/> queue — the same send-order rule an unfragmented record
    /// already obeys — so this records the group as the Space's <see cref="PendingFragmentGroups"/> and
    /// asks <see cref="DrainOrderedDelivery"/> to deliver it the moment that queue's
    /// <see cref="OrderedDeliveryQueue.NextSequence"/> reaches the group's first sequence, whether that
    /// is true already or only after some other sequence still ahead of it in that Space arrives.
    /// </summary>
    /// <summary>
    /// The completed-but-undelivered Fragment groups of <paramref name="slot"/>'s
    /// <paramref name="mode"/> Space. Resolved through <see cref="_receiveWindows"/> by
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
    /// frame arena reservation and queues it as one pending
    /// <see cref="SessionEventKind.MessageReceived"/> — the same three steps
    /// <see cref="DeliverToConsumer"/> takes for an unfragmented record, taken once for the whole group
    /// rather than once per piece, because the consumer is owed one whole Message and not
    /// <paramref name="fragmentCount"/> partial ones. Releases the group's window slots
    /// (<see cref="ReceiveWindow.ReleaseGroup"/>) only on success: a reservation that fails
    /// leaves every piece exactly where <see cref="HoldFragment"/> put it, nothing is counted as
    /// received, and the caller is responsible for retrying — see the Space's
    /// <see cref="PendingFragmentGroups"/>, the Space's <see cref="PendingFragmentGroups"/> and
    /// <see cref="RetryPendingFragmentDeliveries"/>.
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
    /// when its turn comes, rather than when it completes. Its slots are released exactly as a delivery
    /// releases them and its sequences are finished with, so a <c>ReliableOrdered</c> Space moves past
    /// it in send order.
    /// </para>
    /// </remarks>
    /// <returns>
    /// <see langword="true"/> if the group was delivered or discarded, so it is no longer waiting;
    /// <see langword="false"/> if the reservation failed.
    /// </returns>
    private bool TryDeliverFragmentGroup(PeerSlot slot, PacketRecordMode mode, uint groupFirstSequence, ushort fragmentCount)
    {
        ReceiveWindow window = _receiveWindows[slot.Handle.Index].ReliableWindow(mode);
        int spaceIndex = ReliableSpaceIndex(mode);

        int totalLength = 0;
        for (int i = 0; i < fragmentCount; i++)
        {
            uint sequence = unchecked(groupFirstSequence + (uint)i);
            if (!window.TryGetRingSlot(sequence, out int ringSlot))
            {
                // A group HoldFragment/IsGroupComplete just reported complete should always resolve
                // every one of its own pieces; if window state ever moved out from under it before
                // this ran, there is nothing safe to deliver — treat it as not ready rather than throw
                // or deliver a partial Message, and let a later Pump's retry re-derive the answer.
                return false;
            }

            totalLength += window.GetFragmentLength(ringSlot);
        }

        if (RefuseOverLimit(slot, totalLength))
        {
            window.ReleaseGroup(groupFirstSequence, fragmentCount);
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
            window.TryGetRingSlot(sequence, out int ringSlot);
            int length = window.GetFragmentLength(ringSlot);
            ReadOnlySpan<byte> fragmentPayload = _receiveArena.GetFragmentSlot(slot.Handle.Index, spaceIndex, ringSlot)[..length];
            fragmentPayload.CopyTo(destination.Slice(destinationOffset, length));
            destinationOffset += length;
        }

        window.ReleaseGroup(groupFirstSequence, fragmentCount);

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
    /// the Space's <see cref="PendingFragmentGroups"/> if that drain reached its first sequence. The
    /// two are interleaved rather than run once each, because delivering a group advances the queue
    /// past the whole span it occupies (<see cref="OrderedDeliveryQueue.SkipTo"/>), which can itself
    /// unblock a further run of ordinary records that was held behind the group's own gap; the loop
    /// keeps alternating until a full pass delivers nothing further.
    /// </summary>
    private void DrainOrderedDelivery(PeerSlot slot)
    {
        while (true)
        {
            // Nothing behind a delivered Goodbye is ever delivered (issue #400).
            if (slot.Lifecycle.RemoteGoodbyeDelivered)
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
    /// rule an unfragmented <c>ReliableOrdered</c> record already obeys. A pending group whose turn has
    /// not yet arrived, because some earlier sequence in the Space is still missing, is left exactly as
    /// it is: its pieces stay held in the receive window and nothing here disturbs them.
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
    /// Retries every open Peer's Fragment group still waiting to be delivered: one whose frame-arena
    /// reservation failed the moment it completed, in either reliable Space. Also drains each Peer's
    /// <c>ReliableOrdered</c> queue, whose head may be a held record the arena refused on an earlier
    /// Pump. Driven once per Pump, right after the datagram-processing pass that both completes new
    /// groups and can advance a <c>ReliableOrdered</c> group past a gap — this Reactor's own send path
    /// never frees receive-side room, so only a fresh Frame's wiped arena
    /// (<see cref="FrameArena.Reset"/>, at the top of the next <see cref="Pump"/>) can turn a
    /// previously failed reservation into one that succeeds. Allocates nothing: it walks the fixed-size
    /// Slot table by index exactly as the stream profile's own per-Pump sweep does, rather than
    /// tracking a separate list of Peers with something pending.
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
    /// Message identically.
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
    /// <b>The one rule applied here is <c>UnreliableSequenced</c>'s</b>: a record is delivered only if
    /// it is strictly newer than the highest that Space has already delivered. Admission and delivery
    /// part company for this mode alone — a record reordered behind a newer one is a perfectly ordinary
    /// arrival as far as the receive window is concerned, and discarding it is the whole reason the
    /// mode exists. There is deliberately no tolerance window for recovering such a record: the rule is
    /// stated in a sentence with no exceptions — you never receive an older update after a newer one —
    /// and a tolerance would break that sentence to recover exactly what the mode is for discarding.
    /// </para>
    /// <para>
    /// <b>Plain <c>Unreliable</c>'s rule is bounded duplicate suppression</b> (ADR 0017): a record the
    /// Peer's own <see cref="UnreliableDuplicateWindow"/> has already seen is suppressed, and one
    /// delayed past that window's 64 sequences is delivered. That window is deliberately not the
    /// receive window — 64 is a protocol constant, while the receive window's capacity is a consumer
    /// option — so the two are asked separately and a suppression here is counted as a duplicate
    /// exactly as one the receive window caught is.
    /// </para>
    /// <para>
    /// <b><c>ReliableOrdered</c>'s rule is send order</b>: the record goes through this Peer's
    /// <see cref="PeerSlot.OrderedDelivery"/> queue, which holds it whenever an earlier sequence in
    /// that Space has not arrived and releases the whole contiguous run behind the gap the moment it
    /// fills. Held bytes reach the consumer only when they drain, so a Message waiting on a hole is not
    /// counted as received until it does. The queue is per-Peer and per-Space, so a hole here stalls
    /// nothing in the other three Spaces — that is what the four-Space decision buys.
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
    /// The remote's Goodbye has reached its turn in send order, so every Message sent before it has
    /// been delivered. If none of them is still undelivered — see <see cref="HasUndeliveredMessages"/>
    /// — the Peer closes at once with <see cref="DisconnectReason.PeerGoodbye"/> and no
    /// <see cref="SessionEventKind.PeerClosing"/> is raised. Otherwise the Peer enters receiver-side
    /// Closing: <see cref="SessionEventKind.PeerClosing"/> is queued after those Messages, and
    /// <see cref="MarkDrainedClosingPeersCloseDue"/> and <see cref="ClosePeersWithCloseDue"/> raise
    /// <see cref="SessionEventKind.PeerClosed"/> on a later Pump. This mirrors the stream profile's
    /// Goodbye handling.
    /// </summary>
    private void OnGoodbyeDelivered(PeerSlot slot)
    {
        slot.RemoteGoodbyeSequence = null;

        // Both ends closing at once leaves the remote's Goodbye AckDue: acknowledged and nothing more, and
        // the reason stays LocalClose (or LingerExpired), whichever end's Goodbye reached the wire first.
        // With nothing undelivered it is CloseDue: closed after this Pump's flush, not here, because the
        // read pass is still running and the ack of the Goodbye (issue #398) has to reach the wire before
        // the Slot that owns its ack state is freed.
        bool hasUndeliveredMessages = !slot.Lifecycle.IsClosing && HasUndeliveredMessages(slot);
        if (slot.Lifecycle.RemoteGoodbyeArrived(hasUndeliveredMessages) != RemoteCloseState.Closing)
        {
            return;
        }

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
            if (_peers.OccupantAt(index) is PeerSlot { Lifecycle.RemoteClose: RemoteCloseState.Closing } slot
                && PendingGroupsOf(slot, PacketRecordMode.ReliableUnordered).Count == 0)
            {
                slot.Lifecycle.RemoteDrained();
            }
        }
    }

    /// <summary>
    /// Closes every Peer that is <see cref="RemoteCloseState.CloseDue"/> with
    /// <see cref="DisconnectReason.PeerGoodbye"/>, after <see cref="FlushOutboundDatagrams"/>.
    /// <see cref="FlushPeer"/> sent such a Peer one final datagram even with nothing queued, carrying the
    /// ack of the Goodbye, so the sender learns it was received before the Slot that owns that ack state
    /// is freed. <see cref="SessionEventKind.PeerClosed"/> follows in the same Pump with no
    /// <see cref="SessionEventKind.PeerClosing"/> on the direct path.
    /// </summary>
    private void ClosePeersWithCloseDue()
    {
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is PeerSlot { Lifecycle.RemoteClose: RemoteCloseState.CloseDue } slot)
            {
                ClosePeer(slot.Handle, DisconnectReason.PeerGoodbye);
            }
        }
    }

    /// <summary>
    /// Answers whether <paramref name="reservation"/> committed, and charges whichever outcome it had:
    /// a commit raises the Arena high-water, a spent read share is counted on the Peer, and an Arena
    /// with no room is a <see cref="DiagnosticId.ArenaExhausted"/> record.
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
    /// delivered the moment it arrived.
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
    /// Message, reporting one over it with <see cref="DiagnosticId.OversizedMessage"/>. The receive
    /// window bounds what is held, not what is assembled, and a whole record is bounded only by the
    /// Datagram it came in, so this is what makes the announced limit true of what a consumer receives.
    /// </summary>
    /// <remarks>
    /// The Session stays open, because a discarded Message costs nothing behind it. A Send above the limit
    /// is refused at the sending end in every mode, so only a peer ignoring the announcement reaches
    /// this.
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
    /// ever reaches the per-address limit and the capacity check.
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
                out AnnouncedSessionTerms remoteTerms,
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

        // A full Reactor skips this and reports CapacityExhausted, so a limit of Capacity never fires.
        if (_peers.HasFreeSlot && AcceptedPeersAtAddressOf(source) >= _options.ResolvedMaxPeersPerAddress)
        {
            _diagnostics.Log(DiagnosticRecord.ForAddressPeerLimitReached(
                source, _options.ResolvedMaxPeersPerAddress, source.PeerLimitPrefixLength(_options.ResolvedPeerLimitIPv6PrefixLength)));
            RefuseForCapacity(link, source, clientNonce);
            return;
        }

        if (_peers.TryAdd(NewAcceptedPeer(source, link, remoteTerms, remoteResumeSecret, clientNonce))
            is not SlotHandle handle)
        {
            _capacityDiscardsThisFrame++;
            RefuseForCapacity(link, source, clientNonce);
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
    /// How many accepted Peers hold a Slot on a handshake from <paramref name="source"/>'s address, or
    /// for IPv6 from its prefix (ADR 0014).
    /// Counted from <see cref="_acceptedHandshakes"/> rather than kept as a separate tally, so it
    /// cannot drift from the Peers it describes. It costs one walk of at most capacity entries, after a
    /// cookie HMAC, on a path the admission cap already bounds per Pump.
    /// </summary>
    private int AcceptedPeersAtAddressOf(Source source)
    {
        int count = 0;
        foreach ((Source handshakeSource, _) in _acceptedHandshakes.Keys)
        {
            if (handshakeSource.SharesPeerLimitWith(source, _options.ResolvedPeerLimitIPv6PrefixLength))
            {
                count++;
            }
        }

        return count;
    }

    /// <summary>
    /// Counts a Discard and answers a verified handshake with the capacity Refusal, whether the Reactor
    /// is full or the handshake's address is at its limit. The remote end cannot tell which (ADR 0014).
    /// </summary>
    private void RefuseForCapacity(UdpLink link, Source source, ulong clientNonce)
    {
        _counters.RecordAdmissionDiscard();
        ControlDatagramCodec.WriteRefusal(_sendBuffer, clientNonce, RefusalReason.Capacity, 0, 0);
        SendControl(link, source);
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

        if (ResolveDial(source, clientNonce, PeerPhase.AwaitingCookie) is not PeerSlot slot)
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        long now = _options.Reactor.TimeProvider.GetTimestamp();
        slot.Lifecycle.CookieAnswered();
        slot.Cookie = cookie.ToArray();
        slot.CookieEpoch = epoch;
        slot.CookieReceivedTimestamp = now;
        slot.HelloWithCookieSentTimestamp = now;
        slot.HandshakeSentTimestamp = now;
        slot.HandshakeSchedule = null;
        SendHelloWithCookie(link, source, slot);
    }

    /// <summary>Writes and sends <paramref name="slot"/>'s Hello-with-cookie, echoing its stored cookie.</summary>
    private void SendHelloWithCookie(UdpLink link, Source destination, PeerSlot slot)
    {
        // Only called once OnCookie has stored the cookie, which nothing clears while the dial awaits an Accept.
        byte[] cookie = slot.Cookie ?? throw new InvalidOperationException(
            "A Hello-with-cookie was sent for a dial that holds no cookie.");
        ControlDatagramCodec.WriteHelloWithCookie(
            _sendBuffer,
            DatagramSessionProtocol.Version,
            slot.ClientNonce,
            slot.CookieEpoch,
            cookie,
            AnnouncedTermsFor(slot),
            slot.LocalResumeSecret);
        SendControl(link, destination);
    }

    /// <summary>
    /// Resends the current flight of every pending dial whose delay has passed, and ends with
    /// <see cref="DisconnectReason.TransportFailed"/> every dial still pending one
    /// <see cref="DatagramSessionOptions.Linger"/> after <see cref="Connect"/>. Runs after the
    /// datagrams are read, so a dial answered this Pump has already moved to its next flight.
    /// </summary>
    /// <remarks>
    /// The delay is a <see cref="RedialSchedule"/> built from the observed Pump interval and this
    /// Reactor's own Linger, the only Linger a dial knows before the Accept announces the remote's. It is
    /// built on the first Pump of a flight with a positive, observed interval: before a second Pump the
    /// interval reads <see cref="TimeSpan.MaxValue"/>, which would make the first delay the whole Linger.
    /// A flight is resent only once strictly more than its delay has passed, so a clock that has not
    /// moved since the last send never resends.
    /// </remarks>
    private void RetransmitPendingDials()
    {
        if (_dialsByNonce.Count == 0)
        {
            return;
        }

        TimeProvider clock = _options.Reactor.TimeProvider;
        long now = clock.GetTimestamp();
        for (int index = 0; index < _peers.Capacity; index++)
        {
            if (_peers.OccupantAt(index) is not PeerSlot { Lifecycle.IsEstablished: false } slot)
            {
                continue;
            }

            if (clock.GetElapsedTime(slot.DialStartTimestamp, now) >= _options.Linger)
            {
                ClosePeer(slot.Handle, DisconnectReason.TransportFailed);
                continue;
            }

            if (slot.HandshakeSchedule is not RedialSchedule schedule)
            {
                TimeSpan interval = ObservedPumpInterval;
                if (interval <= TimeSpan.Zero || interval == TimeSpan.MaxValue)
                {
                    continue;
                }

                schedule = new RedialSchedule(interval, _options.Linger, _entropy);
                slot.HandshakeSchedule = schedule;
                slot.HandshakeDelay = schedule.NextDelay();
            }

            if (clock.GetElapsedTime(slot.HandshakeSentTimestamp, now) <= slot.HandshakeDelay)
            {
                continue;
            }

            ResendHandshakeFlight(slot, now);
            slot.HandshakeDelay = schedule.NextDelay();
        }
    }

    /// <summary>
    /// Resends a pending dial's current flight. A dial awaiting an Accept echoes its cookie again while
    /// the cookie is younger than one epoch, the least validity ADR 0009 guarantees. An older cookie may
    /// have expired, so the dial returns to <see cref="PeerPhase.AwaitingCookie"/> and sends a fresh
    /// Hello with the same nonce, which the accepter still matches to a Peer it already admitted.
    /// </summary>
    private void ResendHandshakeFlight(PeerSlot slot, long now)
    {
        TimeSpan cookieAge = _options.Reactor.TimeProvider.GetElapsedTime(slot.CookieReceivedTimestamp, now);
        if (slot.Lifecycle.Phase == PeerPhase.AwaitingAccept
            && cookieAge < TimeSpan.FromSeconds(DatagramSessionProtocol.CookieEpochSeconds))
        {
            slot.HelloWithCookieResent = true;
            SendHelloWithCookie(slot.Link, slot.Remote, slot);
        }
        else
        {
            slot.Lifecycle.RestartFromHello();
            slot.Cookie = null;
            ControlDatagramCodec.WriteHello(_sendBuffer, DatagramSessionProtocol.Version, slot.ClientNonce);
            SendControl(slot.Link, slot.Remote);
        }

        slot.HandshakeSentTimestamp = now;
    }

    /// <summary>
    /// Establishes the dialling end's Session. Each end announces the id it wants stamped on datagrams
    /// sent <em>to it</em>, so the two ids differ and neither end has to agree with the other about
    /// which is which. Each end also announces its Fragment size and receive window (ADR 0024): this
    /// end stores the remote values and fixes its send Fragment size at the smaller of the two sizes,
    /// before the Peer is Established.
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
                out AnnouncedSessionTerms remoteTerms,
                remoteResumeSecret))
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        if (ResolveDial(source, clientNonce, PeerPhase.AwaitingAccept) is not PeerSlot slot)
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        // ControlDatagramCodec.TryReadAccept rejects illegal announced limits at parse, so they are
        // already trusted by the time this Peer reaches Established — an internal invariant to assert rather than input to re-validate,
        // mirroring NewAcceptedPeer's assert on the accepting path.
        Debug.Assert(
            remoteTerms.IsLegal(speaksVersion: true),
            "ControlDatagramCodec.TryReadAccept rejects illegal announced limits before this is called.");

        AnnouncedLimits remoteLimits = remoteTerms.Limits;
        TimeSpan remoteLinger = TimeSpan.FromMilliseconds(remoteLimits.LingerMilliseconds);
        TimeSpan announcedLinger = TimeSpan.FromMilliseconds(AnnouncedLingerMilliseconds);

        slot.RemoteSessionId = remoteTerms.SessionIdForMe;
        slot.RemoteMaxMessageSize = remoteLimits.MaxMessageSize;
        slot.RemoteSendIdleInterval = TimeSpan.FromMilliseconds(remoteLimits.SendIdleIntervalMilliseconds);
        slot.RemoteLinger = remoteLinger;
        slot.RemoteFragmentSize = remoteTerms.FragmentSize;
        slot.RemoteReceiveWindowFragments = remoteTerms.ReceiveWindowFragments;
        slot.FragmentSize = Math.Min(slot.FragmentSize, remoteTerms.FragmentSize);
        slot.EffectiveLinger = announcedLinger < remoteLinger ? announcedLinger : remoteLinger;
        slot.RemoteResumeSecret = remoteResumeSecret.ToArray();
        slot.Lifecycle.Establish();

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
        // per Linger and never inside it. A resent Hello-with-cookie makes the leg ambiguous, because the
        // Accept may answer any copy, so it is not sampled (Karn's rule). A leg that measures zero ticks
        // is below the clock's resolution (a frozen clock, never a real network) and says nothing about
        // the round trip, so it leaves the estimator unseeded too.
        TimeSpan handshakeRoundTrip = _options.Reactor.TimeProvider.GetElapsedTime(slot.HelloWithCookieSentTimestamp, now);
        if (handshakeRoundTrip > TimeSpan.Zero && !slot.HelloWithCookieResent)
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

        if (ResolveDial(source, clientNonce, PeerPhase.AwaitingAccept) is not PeerSlot slot)
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
    /// (<see cref="PeerLifecycle.IsClosing"/> or <see cref="PeerLifecycle.RemoteGoodbyeDelivered"/>), is refused
    /// the same way with <c>Value0</c> 1. A matched Resume from the IP address the Peer is already sent
    /// to moves the Peer at once, whatever the port. One from any other IP address moves nothing until
    /// that address answers a <see cref="DatagramControlType.PathChallenge"/> (<see cref="ChallengeNewPath"/>,
    /// ADR 0021), because the secret proves who sent the Resume but not that its source will receive.
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

        // A Goodbye, sent or received, forbids a Resume: the Peer is winding down, so moving its address or
        // resuming it would be a silent success. The secret matched, so this counts as one candidate
        // checked, and nothing about the Peer changes.
        if (!slot.Lifecycle.AcceptsResume)
        {
            LogResumeUnmatched(source, candidatesChecked: 1);
            return;
        }

        if (!source.HasSameAddressAs(slot.Remote))
        {
            ChallengeNewPath(slot, link, source);
            return;
        }

        ResumeInterruptedPeer(slot, link, source);
    }

    /// <summary>
    /// Answers a matched Resume from a new IP address with one <see cref="DatagramControlType.PathChallenge"/>
    /// sent to that address from <paramref name="link"/>, the Link the Resume arrived on, and remembers
    /// the address, the Link and the nonce on <paramref name="slot"/> (ADR 0021). Nothing else about the
    /// Peer changes: it keeps sending to its old address until <see cref="OnPathResponse"/> sees the nonce
    /// come back from this one. A further Resume from the same source repeats the same nonce, so an answer
    /// still in flight when the dialler's next Resume arrives is not voided. One from any other source
    /// replaces the pending challenge with a fresh nonce. One control datagram answers one, so the
    /// exchange cannot amplify (<see cref="DatagramSessionProtocol.ControlDatagramSize"/>). The Resume and
    /// the challenge are charged to the Peer, whose secret the Resume presented (ADR 0013).
    /// </summary>
    private void ChallengeNewPath(PeerSlot slot, UdpLink link, Source source)
    {
        if (slot.PendingPath is not PendingPathChallenge pending || pending.Source != source || pending.Link != link)
        {
            byte[] nonce = new byte[DatagramSessionProtocol.PathNonceLength];
            _entropy.GetBytes(nonce);
            pending = new PendingPathChallenge(source, link, nonce);
            slot.PendingPath = pending;
        }

        AnswerWithPathProbe(slot, DatagramControlType.PathChallenge, pending.Nonce, link, source);
    }

    /// <summary>
    /// Charges the control datagram just received to <paramref name="slot"/>, then sends it a
    /// <paramref name="type"/> probe carrying <paramref name="nonce"/> and routed by its
    /// <see cref="PeerSlot.RemoteSessionId"/>, from <paramref name="link"/> to <paramref name="destination"/>,
    /// and charges that send too (ADR 0013).
    /// </summary>
    private void AnswerWithPathProbe(PeerSlot slot, DatagramControlType type, ReadOnlySpan<byte> nonce, UdpLink link, Source destination)
    {
        slot.Counters.RecordBytesReceived(DatagramSessionProtocol.ControlDatagramSize);
        ControlDatagramCodec.WritePathProbe(_sendBuffer, type, slot.RemoteSessionId, nonce);
        SendControl(link, destination);
        slot.Counters.RecordSocketWrite();
        slot.Counters.RecordBytesSent(DatagramSessionProtocol.ControlDatagramSize);
    }

    /// <summary>
    /// The dialling end's half of ADR 0021: echoes a <see cref="DatagramControlType.PathChallenge"/>'s
    /// nonce in a <see cref="DatagramControlType.PathResponse"/> sent to the address this end already sends
    /// its Peer to, from the Link it already sends from. The response never goes to the challenge's
    /// source, so a forged challenge cannot aim this end's traffic anywhere. A challenge that does not read,
    /// or names no Established dialled Peer this Reactor holds, is discarded as malformed.
    /// </summary>
    private void OnPathChallenge(Source source, ReadOnlySpan<byte> datagram)
    {
        Span<byte> nonce = stackalloc byte[DatagramSessionProtocol.PathNonceLength];
        if (!ControlDatagramCodec.TryReadPathProbe(datagram, DatagramControlType.PathChallenge, out uint targetSessionId, nonce)
            || !_peersBySessionId.TryGetValue(targetSessionId, out SlotHandle handle)
            || _peers.TryResolve(handle) is not PeerSlot slot
            || !slot.IsDialled
            || !slot.Lifecycle.IsEstablished)
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        AnswerWithPathProbe(slot, DatagramControlType.PathResponse, nonce, slot.Link, slot.Remote);
    }

    /// <summary>
    /// Completes a pending path challenge (ADR 0021): a <see cref="DatagramControlType.PathResponse"/> that
    /// names an accepted Peer, arrives from exactly the source the challenge went to and on exactly the
    /// Link it left from, and echoes its nonce (compared in constant time) proves that source receives, so
    /// the Peer resumes there through <see cref="ResumeInterruptedPeer"/>. A Peer that has since entered Closing,
    /// either side, is refused as <see cref="OnResume"/> refuses it. Any other response is discarded as
    /// malformed and changes nothing.
    /// </summary>
    private void OnPathResponse(UdpLink link, Source source, ReadOnlySpan<byte> datagram)
    {
        Span<byte> nonce = stackalloc byte[DatagramSessionProtocol.PathNonceLength];
        if (!ControlDatagramCodec.TryReadPathProbe(datagram, DatagramControlType.PathResponse, out uint targetSessionId, nonce)
            || !_peersBySessionId.TryGetValue(targetSessionId, out SlotHandle handle)
            || _peers.TryResolve(handle) is not PeerSlot slot
            || slot.IsDialled
            || slot.PendingPath is not PendingPathChallenge pending
            || pending.Source != source
            || pending.Link != link
            || !CryptographicOperations.FixedTimeEquals(nonce, pending.Nonce))
        {
            DiscardMalformed(source, datagram.Length);
            return;
        }

        if (!slot.Lifecycle.AcceptsResume)
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

        // A move settles where the Peer is, so any challenge still pending is moot: a late answer to it
        // must not move the Peer a second time.
        slot.PendingPath = null;
    }

    /// <summary>
    /// The matched-Resume branch. For an Established Peer of any liveness it points sends at the
    /// Resume's source (<see cref="MoveSendAddress"/>), charges the Resume to
    /// <see cref="PeerCounters.BytesReceived"/> (ADR 0013), stamps <see cref="PeerSlot.LastReceiveTimestamp"/>
    /// so the receive-idle deadline restarts from the Resume, and logs
    /// <see cref="DiagnosticId.ResumeMatched"/> with <c>Value0</c> 0, since nothing is rewound on this
    /// profile: retransmission already re-sends anything unacknowledged. Sequence state, ack state,
    /// receive windows and held Fragments are not touched. If the Peer is
    /// <see cref="PeerPhase.Interrupted"/> it also leaves Interrupted, queues
    /// <see cref="SessionEventKind.PeerResumed"/> for the same <see cref="PeerId"/> it had before,
    /// generation included, and counts it once in <see cref="PeerCounters.Resumes"/>. Linger stops counting because the Peer is no longer Interrupted, and a later
    /// interruption stamps <see cref="PeerLifecycle.LingerStartTimestamp"/> afresh. A Peer this end does not
    /// consider Interrupted (the dialling end can notice an interruption first) raises no event and
    /// leaves <see cref="PeerCounters.Resumes"/> unchanged.
    /// </summary>
    private void ResumeInterruptedPeer(PeerSlot slot, UdpLink link, Source source)
    {
        MoveSendAddress(slot, link, source);
        slot.Counters.RecordBytesReceived(DatagramSessionProtocol.ControlDatagramSize);

        // The Resume is hearing from the Peer. Pump reads datagrams before it runs the timers, so a
        // stale timestamp here would have EnforceReceiveIdleTimer interrupt the Peer again on this very Pump.
        slot.LastReceiveTimestamp = _options.Reactor.TimeProvider.GetTimestamp();

        PeerId peer = ToPeerId(slot.Handle);
        if (slot.Lifecycle.TryLeaveInterrupted())
        {
            RecordResumed(slot, peer);
        }

        if (_diagnostics.IsEnabled(DiagnosticLevel.Info))
        {
            _diagnostics.Log(DiagnosticRecord.ForResumeMatched(peer, bytesRewound: 0));
        }
    }

    /// <summary>
    /// Records a Peer that has just left <see cref="PeerPhase.Interrupted"/>, the steps both ends
    /// share: counts one <see cref="PeerCounters.Resumes"/> and queues
    /// <see cref="SessionEventKind.PeerResumed"/> for <paramref name="peer"/>, the same
    /// <see cref="PeerId"/> it had before, generation included.
    /// </summary>
    private void RecordResumed(PeerSlot slot, PeerId peer)
    {
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
    private PeerSlot? ResolveDial(Source source, ulong clientNonce, PeerPhase expectedPhase)
    {
        if (!_dialsByNonce.TryGetValue(clientNonce, out SlotHandle handle)
            || _peers.TryResolve(handle) is not PeerSlot slot
            || slot.Lifecycle.Phase != expectedPhase
            || slot.Remote != source)
        {
            return null;
        }

        return slot;
    }

    private PeerSlot NewAcceptedPeer(
        Source source,
        UdpLink link,
        AnnouncedSessionTerms remoteTerms,
        ReadOnlySpan<byte> remoteResumeSecret,
        ulong clientNonce)
    {
        // ControlDatagramCodec.TryReadHelloWithCookie rejects illegal announced limits at parse, and
        // the version Refusal handles any other version before this is called, so by the time a Peer is built here, straight into
        // Established, the limits are already trusted — an internal invariant to assert rather than
        // input to re-validate.
        Debug.Assert(
            remoteTerms.IsLegal(speaksVersion: true),
            "ControlDatagramCodec.TryReadHelloWithCookie rejects illegal announced limits before this is called.");

        AnnouncedLimits remoteLimits = remoteTerms.Limits;
        TimeSpan remoteLinger = TimeSpan.FromMilliseconds(remoteLimits.LingerMilliseconds);
        TimeSpan announcedLinger = TimeSpan.FromMilliseconds(AnnouncedLingerMilliseconds);
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
            RemoteSessionId = remoteTerms.SessionIdForMe,
            RemoteMaxMessageSize = remoteLimits.MaxMessageSize,
            RemoteSendIdleInterval = TimeSpan.FromMilliseconds(remoteLimits.SendIdleIntervalMilliseconds),
            RemoteLinger = remoteLinger,
            RemoteFragmentSize = remoteTerms.FragmentSize,
            RemoteReceiveWindowFragments = remoteTerms.ReceiveWindowFragments,
            EffectiveLinger = effectiveLinger,
            RemoteResumeSecret = remoteResumeSecret.ToArray(),
            Lifecycle = PeerLifecycle.Accepted(),
            AckTracker = NewAckTracker(retransmitTimer),
            RetransmitTimer = retransmitTimer,
            StallSignature = new(_options.Reactor.TimeProvider),
            FragmentSize = Math.Min(_options.ResolvedFragmentSize, remoteTerms.FragmentSize),
            OrderedDelivery = new(_options.OrderedHoldSlots, _options.ResolvedFragmentSize),
            LastSendTimestamp = now,
            LastReceiveTimestamp = now,
        };
    }

    /// <summary>
    /// Builds a fresh outstanding-sequence tracker for a Peer newly admitted into a Slot, timed from
    /// this Reactor's own <see cref="ReactorOptions.TimeProvider"/> and backed by
    /// <paramref name="retransmitTimer"/> — the same instance stored on the Slot's own
    /// <see cref="PeerSlot.RetransmitTimer"/>, so the tracker's estimator samples and the Slot's own
    /// effective-RTO reads are two views of one timer, not two that happen to agree.
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
            AnnouncedTermsFor(slot),
            slot.LocalResumeSecret);
        SendControl(link, destination);
    }

    /// <summary>
    /// The terms this end announces for <paramref name="slot"/> in its Hello-with-cookie or Accept:
    /// the slot's own local session id, this Reactor's announced limits and intervals, and its own
    /// Fragment size and receive window.
    /// </summary>
    private AnnouncedSessionTerms AnnouncedTermsFor(PeerSlot slot) => new(
        slot.LocalSessionId,
        new AnnouncedLimits((uint)_options.MaxMessageSize, AnnouncedSendIdleIntervalMilliseconds, AnnouncedLingerMilliseconds),
        (ushort)_options.ResolvedFragmentSize,
        (uint)_options.ResolvedReceiveWindowFragments);

    /// <summary>
    /// Spends one unit of the per-Pump admission budget, or Discards. The cap bounds work rather than
    /// state — one HMAC minted or verified is one unit, and both halves of the handshake cost one —
    /// while the cookie is what bounds state. Beyond the cap a datagram already read is gone, because
    /// nothing holds it for a later Pump: this profile Discards where the others defer, and the count is
    /// exact because the Reactor had the work in hand when it let it go.
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
    /// instead.
    /// </summary>
    private void DiscardMalformed(Source source, int bytesReceived)
    {
        _counters.RecordUnknownSourceDatagram();
        _diagnostics.Log(DiagnosticRecord.ForDatagramMalformed(source, bytesReceived));
    }

    /// <summary>
    /// Discards a data datagram that named a live Session and then failed to parse, counting it on
    /// <see cref="ReactorCounters.MalformedSessionDatagrams"/> rather than
    /// <see cref="ReactorCounters.UnknownSourceDatagrams"/>: the id resolved, which is the only reason
    /// parsing was reached at all, so this is not traffic that named nobody. The Diagnostic is the same
    /// <see cref="DiagnosticId.DatagramMalformed"/> and stays Source-subjected — a body that did not
    /// parse is no proof the sender is the Peer whose id it carried, so neither the record nor the
    /// figure is charged to that Peer.
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
    /// Remembers a Peer that just closed on a Goodbye in <see cref="_closedSessions"/> for one effective
    /// Linger, so a resend of the remote's Goodbye can still be acknowledged (ADR 0017, second amendment).
    /// Applies when the remote may still be waiting for the ack of its Goodbye: a close with
    /// <see cref="DisconnectReason.PeerGoodbye"/>, or a <see cref="DisconnectReason.LocalClose"/> of a
    /// Closing Peer, whose remote Goodbye may not have been delivered yet. A Closing Peer whose remote
    /// Goodbye was delivered but not yet acknowledged on the wire (<see cref="RemoteCloseState.AckDue"/>)
    /// has that ack sent here, since the Pump's flush no longer reaches its Slot.
    /// </summary>
    private void RememberClosedSession(PeerSlot slot, DisconnectReason reason)
    {
        bool goodbyeClose = reason == DisconnectReason.PeerGoodbye
            || (reason == DisconnectReason.LocalClose && slot.Lifecycle.IsClosing);
        if (!goodbyeClose)
        {
            return;
        }

        (AckBlock reliableOrdered, AckBlock reliableUnordered) = _receiveWindows[slot.Handle.Index].GenerateAckBlocks();

        ClosedSessionAck finalAck = new(slot.Remote, slot.RemoteSessionId, reliableOrdered, reliableUnordered);
        _closedSessions.Remember(slot.LocalSessionId, finalAck, slot.EffectiveLinger);

        if (slot.Lifecycle.RemoteClose == RemoteCloseState.AckDue)
        {
            SendClosedSessionAck(slot.Link, finalAck);
        }
    }

    /// <summary>
    /// Answers a data datagram naming a recently closed Session with one header-only datagram carrying the
    /// stored ack blocks, and reports whether it did. <see cref="ClosedSessionMemory.TryReAck"/> decides
    /// whether to answer; when it declines, the caller discards as for any unknown session. The reply is
    /// the 20-byte header alone, never larger than the datagram that triggered it, and raises no event,
    /// takes no Slot and touches no per-Peer counter.
    /// </summary>
    private bool TryReAckClosedSession(UdpLink link, Source source, uint session, ReadOnlySpan<byte> datagram)
    {
        if (!_closedSessions.TryReAck(session, source, datagram, out ClosedSessionAck ack))
        {
            return false;
        }

        SendClosedSessionAck(link, ack);
        return true;
    }

    /// <summary>Writes and sends the header-only ack datagram <paramref name="ack"/> describes to its remote endpoint.</summary>
    private void SendClosedSessionAck(UdpLink link, in ClosedSessionAck ack)
    {
        DatagramHeaderCodec.WriteHeader(
            _outboundDatagramBuffer,
            ack.RemoteSessionId,
            ack.ReliableOrdered.Base,
            ack.ReliableOrdered.Bits,
            ack.ReliableUnordered.Base,
            ack.ReliableUnordered.Bits);

        UdpLinkSendResult result = link.SendTo(ack.Remote, _outboundDatagramBuffer.AsSpan(0, DatagramSessionProtocol.DatagramHeaderSize));
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
                && !_closedSessions.Holds(candidate))
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
    /// (<see cref="FlushOutboundDatagrams"/>): the wire mode and sequence <see cref="Send"/> already
    /// allocated, plus, for the two unreliable modes only, where the Message's bytes sit in the Peer's region of <see cref="_unreliableQueue"/>. A reliable
    /// record carries no copy of its own and no record shape of its own either: its bytes, and whether
    /// it is a whole Message or one Fragment of a cut one (with which index and count), already live in
    /// <see cref="_retention"/> from the moment <see cref="Send"/> committed them, and
    /// <see cref="TryResolvePendingPayload"/> reads both back from there at flush time instead of
    /// duplicating them here, so a queue entry can never disagree with what was retained. Nothing
    /// retains an unreliable Send's bytes anywhere else — plain <see cref="DeliveryMode.Unreliable"/>
    /// and <see cref="DeliveryMode.UnreliableSequenced"/> take no retention slot at all and are never a
    /// Fragment (ADR 0004) — so this is the simplest place that can hold them until the Pump that
    /// finally needs them: copied once into preallocated storage and released when
    /// <see cref="FlushPeer"/> has written them.
    /// </summary>
    private readonly struct PendingOutboundRecord
    {
        /// <summary>The wire mode this record was allocated under.</summary>
        public required PacketRecordMode Mode { get; init; }

        /// <summary>The sequence number <see cref="Send"/> allocated for this record.</summary>
        public required uint Sequence { get; init; }

        /// <summary>
        /// Whether this queue entry is a resend rather than a first transmission. What it decides at
        /// flush time is one thing: a first transmission arms its retransmit deadline through
        /// <see cref="OutstandingReliableSequences.RecordSent"/>, while a resend has already been
        /// re-armed by <see cref="OutstandingReliableSequences.ReArmExpired"/> at the single RTO its
        /// Peer's one backoff for that Pump produced. Recording a resend as newly sent would overwrite
        /// that deadline with a second one and count the sequence as outstanding twice.
        /// </summary>
        public bool IsRetransmission { get; init; }

        /// <summary>
        /// Where an unreliable record's bytes start in its Peer's region of <see cref="_unreliableQueue"/>.
        /// Unused for a reliable record, whose bytes live in <see cref="_retention"/>.
        /// </summary>
        public int UnreliableOffset { get; init; }

        /// <summary>An unreliable record's length in bytes. Unused for a reliable record.</summary>
        public int UnreliableLength { get; init; }
    }

    /// <summary>
    /// Where one Peer is in its life on this end: dialling, Established and live, Interrupted, or Closing
    /// through its own <see cref="Close"/>. One value rather than independent flags, so a Peer cannot be
    /// Interrupted and Closing at once, nor either before it is Established. Declaration order is part of
    /// the contract: every phase from <see cref="Open"/> on is Established and every phase from
    /// <see cref="GoodbyeDeferred"/> on is Closing, which <see cref="PeerLifecycle.IsEstablished"/> and
    /// <see cref="PeerLifecycle.IsClosing"/> compare against. Moved only by <see cref="PeerLifecycle"/>.
    /// </summary>
    private enum PeerPhase
    {
        /// <summary>A dial has sent its Hello and is waiting for the Cookie that answers it.</summary>
        AwaitingCookie,

        /// <summary>A dial has echoed the cookie and is waiting for an Accept or a Refusal.</summary>
        AwaitingAccept,

        /// <summary>The Session exists, both ends know each other's session id, and the remote is being heard.</summary>
        Open,

        /// <summary>
        /// The receive-idle deadline (<see cref="HasExceededReceiveIdleDeadline"/>) fired. The Peer stays open
        /// until it is resumed back to <see cref="Open"/> or its effective Linger runs out.
        /// </summary>
        /// <remarks>
        /// <see cref="_peers"/> still holds the Slot, <see cref="CountersOf"/> keeps answering, and nothing
        /// resets any queue or retention. <see cref="Send"/> keeps succeeding until the Space's retention
        /// fills, then returns <see cref="SendOutcome.RetentionExhausted"/> while the Peer stays Interrupted,
        /// the same rule as the stream profile. The Interrupted end keeps sending Pings, since
        /// <see cref="EnforceSendIdleTimer"/> does not read this phase: a datagram Session has no re-dial
        /// schedule, so probing the path is the only way it can be heard from again. The dialling end also
        /// sends a Resume each send-idle interval (<see cref="SendResumeIfDue"/>). The Peer's
        /// <see cref="PeerSlot.EffectiveLinger"/>, elapsed since <see cref="PeerLifecycle.LingerStartTimestamp"/>,
        /// ends it with <see cref="DisconnectReason.LingerExpired"/>, mirroring
        /// <see cref="StreamSessionReactor.HasExceededLinger"/>. Resuming is the other way out. The accepting
        /// end resumes on a Resume matched by session id and secret (<see cref="ResumeInterruptedPeer"/>) or on
        /// a well-formed data Datagram from the exact address and port it sends to. The dialling end resumes
        /// on hearing its session again after it has sent a Resume. Both data exits are
        /// <see cref="ResumeInterruptedPeerOnData"/>. Each exit raises
        /// <see cref="SessionEventKind.PeerResumed"/>.
        /// </remarks>
        Interrupted,

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

    /// <summary>
    /// One Peer's lifecycle state and the only code that moves it: its <see cref="PeerPhase"/>, how far the
    /// remote's Goodbye has got (<see cref="RemoteCloseState"/>), and the timestamps and sequence those
    /// transitions stamp. Each transition checks its own starting state and does nothing from any other,
    /// so a caller asks for the move rather than guarding for it.
    /// </summary>
    private sealed class PeerLifecycle
    {
        private PeerLifecycle(PeerPhase phase)
        {
            Phase = phase;
        }

        /// <summary>Where this Peer is in its life on this end.</summary>
        public PeerPhase Phase { get; private set; }

        /// <summary>
        /// How far the remote's Goodbye has got. Orthogonal to <see cref="Phase"/> once Established: the
        /// remote can say Goodbye to an Open, Interrupted or Closing Peer.
        /// </summary>
        public RemoteCloseState RemoteClose { get; private set; }

        /// <summary>
        /// The <see cref="TimeProvider.GetTimestamp"/> value at which this Peer entered
        /// <see cref="PeerPhase.Interrupted"/> or Closing, whichever it is in: the instant its effective
        /// Linger began counting down. Meaningless while <see cref="IsLingering"/> is <see langword="false"/>.
        /// Interrupted and Closing exclude each other, so one timestamp serves both.
        /// </summary>
        public long LingerStartTimestamp { get; private set; }

        /// <summary>
        /// The <c>ReliableOrdered</c> sequence this end's Goodbye was queued at, valid from
        /// <see cref="PeerPhase.GoodbyeUnacked"/> on.
        /// </summary>
        public uint GoodbyeSequence { get; private set; }

        /// <summary>
        /// The <see cref="TimeProvider.GetTimestamp"/> value of this Peer's latest Resume datagram, or
        /// <see langword="null"/> when none has left since it last entered <see cref="PeerPhase.Interrupted"/>,
        /// which clears it. <c>SendResumeIfDue</c> paces the next Resume from it.
        /// </summary>
        public long? LastResumeTimestamp { get; private set; }

        /// <summary>Whether the handshake has finished: every phase from <see cref="PeerPhase.Open"/> on.</summary>
        public bool IsEstablished => Phase >= PeerPhase.Open;

        /// <summary>Whether the receive-idle deadline has put this Peer in <see cref="PeerPhase.Interrupted"/>.</summary>
        public bool IsInterrupted => Phase == PeerPhase.Interrupted;

        /// <summary>Whether this end's own <see cref="Close"/> has put this Peer in Closing.</summary>
        public bool IsClosing => Phase >= PeerPhase.GoodbyeDeferred;

        /// <summary>Whether this Peer's effective Linger is counting down from <see cref="LingerStartTimestamp"/>.</summary>
        public bool IsLingering => IsInterrupted || IsClosing;

        /// <summary>
        /// Whether the remote's Goodbye has been delivered in order: the Peer accepts no further Messages
        /// and <see cref="DatagramSessionReactor.Send"/> answers <see cref="SendOutcome.Closed"/>.
        /// </summary>
        public bool RemoteGoodbyeDelivered => RemoteClose != RemoteCloseState.None;

        /// <summary>
        /// Whether this Pump's flush must reach the remote even with nothing queued, because it carries the
        /// ack of the remote's Goodbye (<see cref="RemoteCloseState.CloseDue"/> or <see cref="RemoteCloseState.AckDue"/>).
        /// </summary>
        public bool IsFinalAckDue => RemoteClose is RemoteCloseState.CloseDue or RemoteCloseState.AckDue;

        /// <summary>
        /// Whether a Resume or a path response may move this Peer: it is Established and neither end has
        /// said Goodbye. A Goodbye, sent or received, means the Peer is winding down.
        /// </summary>
        public bool AcceptsResume => Phase is (PeerPhase.Open or PeerPhase.Interrupted) && !RemoteGoodbyeDelivered;

        /// <summary>
        /// Why <see cref="DatagramSessionReactor.Send"/> must refuse this Peer, or <see langword="null"/>
        /// when it may send: <see cref="SendOutcome.Closed"/> once either end has said Goodbye,
        /// <see cref="SendOutcome.NotEstablished"/> while the handshake is unfinished.
        /// </summary>
        public SendOutcome? SendRefusal => this switch
        {
            { RemoteGoodbyeDelivered: true } => SendOutcome.Closed,
            { IsEstablished: false } => SendOutcome.NotEstablished,
            { IsClosing: true } => SendOutcome.Closed,
            _ => null,
        };

        /// <summary>A dialled Peer that has sent its Hello and awaits the Cookie.</summary>
        public static PeerLifecycle Dialling() => new(PeerPhase.AwaitingCookie);

        /// <summary>An accepted Peer, Established from the moment it is admitted.</summary>
        public static PeerLifecycle Accepted() => new(PeerPhase.Open);

        /// <summary>Moves a dial from <see cref="PeerPhase.AwaitingCookie"/> to <see cref="PeerPhase.AwaitingAccept"/>.</summary>
        public void CookieAnswered()
        {
            if (Phase == PeerPhase.AwaitingCookie)
            {
                Phase = PeerPhase.AwaitingAccept;
            }
        }

        /// <summary>
        /// Moves a dial from <see cref="PeerPhase.AwaitingAccept"/> back to <see cref="PeerPhase.AwaitingCookie"/>,
        /// when its cookie may have expired and the dial restarts from a fresh Hello.
        /// </summary>
        public void RestartFromHello()
        {
            if (Phase == PeerPhase.AwaitingAccept)
            {
                Phase = PeerPhase.AwaitingCookie;
            }
        }

        /// <summary>Moves a dial from <see cref="PeerPhase.AwaitingAccept"/> to <see cref="PeerPhase.Open"/>.</summary>
        public void Establish()
        {
            if (Phase == PeerPhase.AwaitingAccept)
            {
                Phase = PeerPhase.Open;
            }
        }

        /// <summary>
        /// Moves an <see cref="PeerPhase.Open"/> Peer to <see cref="PeerPhase.Interrupted"/>, starts its
        /// Linger at <paramref name="now"/> and clears <see cref="LastResumeTimestamp"/>. A Peer in any other
        /// phase is untouched: an Interrupted one is not re-stamped, and a Closing one never goes Interrupted
        /// (issue #406), since only its Goodbye's ack or Linger measured from Close ends it.
        /// </summary>
        /// <returns><see langword="true"/> when the Peer was moved.</returns>
        public bool TryInterrupt(long now)
        {
            if (Phase != PeerPhase.Open)
            {
                return false;
            }

            Phase = PeerPhase.Interrupted;
            LingerStartTimestamp = now;
            LastResumeTimestamp = null;
            return true;
        }

        /// <summary>Moves an <see cref="PeerPhase.Interrupted"/> Peer back to <see cref="PeerPhase.Open"/>.</summary>
        /// <returns><see langword="true"/> when the Peer was Interrupted and has left it.</returns>
        public bool TryLeaveInterrupted()
        {
            if (Phase != PeerPhase.Interrupted)
            {
                return false;
            }

            Phase = PeerPhase.Open;
            return true;
        }

        /// <summary>Records that a Resume datagram left at <paramref name="now"/>.</summary>
        public void ResumeSent(long now) => LastResumeTimestamp = now;

        /// <summary>
        /// Moves an <see cref="PeerPhase.Open"/> Peer to <see cref="PeerPhase.GoodbyeDeferred"/> and starts
        /// its Linger at <paramref name="now"/>. A Peer in any other phase is untouched.
        /// </summary>
        /// <returns><see langword="true"/> when the Peer was moved and its Goodbye must now be queued.</returns>
        public bool TryBeginClose(long now)
        {
            if (Phase != PeerPhase.Open)
            {
                return false;
            }

            Phase = PeerPhase.GoodbyeDeferred;
            LingerStartTimestamp = now;
            return true;
        }

        /// <summary>
        /// Records that this end's Goodbye was queued at <paramref name="sequence"/>, moving
        /// <see cref="PeerPhase.GoodbyeDeferred"/> to <see cref="PeerPhase.GoodbyeUnacked"/>.
        /// </summary>
        public void GoodbyeQueued(uint sequence)
        {
            if (Phase == PeerPhase.GoodbyeDeferred)
            {
                GoodbyeSequence = sequence;
                Phase = PeerPhase.GoodbyeUnacked;
            }
        }

        /// <summary>
        /// Moves <see cref="PeerPhase.GoodbyeUnacked"/> to <see cref="PeerPhase.GoodbyeAcked"/> when an ack
        /// released <paramref name="sequence"/> from the <c>ReliableOrdered</c> Space and it is the Goodbye's.
        /// </summary>
        public void OrderedSequenceAcked(uint sequence)
        {
            if (Phase == PeerPhase.GoodbyeUnacked && sequence == GoodbyeSequence)
            {
                Phase = PeerPhase.GoodbyeAcked;
            }
        }

        /// <summary>
        /// Records the remote's Goodbye reaching its turn in send order. A Closing Peer moves to
        /// <see cref="RemoteCloseState.AckDue"/> whatever <paramref name="hasUndeliveredMessages"/> says.
        /// Otherwise the Peer moves to <see cref="RemoteCloseState.Closing"/> while Messages are still
        /// undelivered, or straight to <see cref="RemoteCloseState.CloseDue"/>.
        /// </summary>
        /// <returns>
        /// The state the remote's Goodbye is now in. A Peer not yet Established, or one whose remote Goodbye
        /// already arrived, is untouched and its current state is returned.
        /// </returns>
        public RemoteCloseState RemoteGoodbyeArrived(bool hasUndeliveredMessages)
        {
            if (!IsEstablished || RemoteGoodbyeDelivered)
            {
                return RemoteClose;
            }

            RemoteClose = IsClosing ? RemoteCloseState.AckDue
                : hasUndeliveredMessages ? RemoteCloseState.Closing
                : RemoteCloseState.CloseDue;
            return RemoteClose;
        }

        /// <summary>Moves <see cref="RemoteCloseState.Closing"/> to <see cref="RemoteCloseState.CloseDue"/> once the Peer has drained.</summary>
        public void RemoteDrained()
        {
            if (RemoteClose == RemoteCloseState.Closing)
            {
                RemoteClose = RemoteCloseState.CloseDue;
            }
        }

        /// <summary>Moves <see cref="RemoteCloseState.AckDue"/> to <see cref="RemoteCloseState.Acked"/> once the flush carrying that ack has run.</summary>
        public void FinalAckFlushed()
        {
            if (RemoteClose == RemoteCloseState.AckDue)
            {
                RemoteClose = RemoteCloseState.Acked;
            }
        }
    }

    /// <summary>Where the remote's Goodbye has got a Peer.</summary>
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
    /// A path challenge in flight (ADR 0021): the new source a matched Resume came from, the Link it
    /// arrived on and the challenge was sent from, and the nonce that must come back from that source on
    /// that Link before the Peer's sends move there.
    /// </summary>
    private sealed record PendingPathChallenge(Source Source, UdpLink Link, byte[] Nonce);

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

        /// <summary>
        /// The path challenge an accepted Peer is waiting to have answered, if a matched Resume arrived
        /// from a new IP address (ADR 0021). <see langword="null"/> otherwise, and cleared whenever the
        /// Peer's send address moves. Never set on a dialled Peer.
        /// </summary>
        public PendingPathChallenge? PendingPath { get; set; }

        /// <summary>The nonce that ties every datagram of this handshake to this Peer and no other.</summary>
        public required ulong ClientNonce { get; init; }

        /// <summary>
        /// <see cref="TimeProvider.GetTimestamp"/> at the moment a dialled Peer first sent its
        /// Hello-with-cookie. <see cref="OnAccept"/> measures the Accept's arrival against it to seed
        /// <see cref="RetransmitTimer"/>'s estimator, unless <see cref="HelloWithCookieResent"/> is set.
        /// Unused on an accepted Peer.
        /// </summary>
        public long HelloWithCookieSentTimestamp { get; set; }

        /// <summary>
        /// Whether this dial has sent its Hello-with-cookie more than once. An Accept may then answer any
        /// of the copies, so <see cref="OnAccept"/> does not seed the round-trip estimate (Karn's rule).
        /// Never cleared, because a restart from a fresh Hello keeps the same nonce and the accepter
        /// answers every copy with the same Accept.
        /// </summary>
        public bool HelloWithCookieResent { get; set; }

        /// <summary>
        /// <see cref="TimeProvider.GetTimestamp"/> at <see cref="Connect"/>. A dial still pending this
        /// Reactor's <see cref="DatagramSessionOptions.Linger"/> later ends with
        /// <see cref="DisconnectReason.TransportFailed"/> (<see cref="RetransmitPendingDials"/>). Unused on
        /// an accepted Peer.
        /// </summary>
        public long DialStartTimestamp { get; init; }

        /// <summary>
        /// <see cref="TimeProvider.GetTimestamp"/> at the last send of this dial's current handshake flight,
        /// the Hello or the Hello-with-cookie. <see cref="HandshakeDelay"/> is measured from it.
        /// </summary>
        public long HandshakeSentTimestamp { get; set; }

        /// <summary>
        /// The backoff this dial's handshake flight is resent on, built on the first Pump of the flight
        /// that has a usable observed Pump interval. <see langword="null"/> until then, and reset when
        /// the dial moves to a new flight.
        /// </summary>
        public RedialSchedule? HandshakeSchedule { get; set; }

        /// <summary>How long after <see cref="HandshakeSentTimestamp"/> the current flight is resent.</summary>
        public TimeSpan HandshakeDelay { get; set; }

        /// <summary>
        /// The cookie a dial in <see cref="PeerPhase.AwaitingAccept"/> echoes in every Hello-with-cookie it
        /// sends. <see langword="null"/> before a Cookie arrives and after a restart from a fresh Hello.
        /// </summary>
        public byte[]? Cookie { get; set; }

        /// <summary>The accepter's epoch that <see cref="Cookie"/> was minted in, echoed back with it.</summary>
        public long CookieEpoch { get; set; }

        /// <summary>
        /// <see cref="TimeProvider.GetTimestamp"/> when <see cref="Cookie"/> arrived. A cookie stays valid
        /// for at least one epoch after it is minted (ADR 0009), so a resend later than that restarts
        /// from a fresh Hello instead of echoing it.
        /// </summary>
        public long CookieReceivedTimestamp { get; set; }

        /// <summary>The id this Reactor assigned, which the remote stamps on datagrams sent here.</summary>
        public required uint LocalSessionId { get; init; }

        /// <summary>The secret this Reactor will require to be presented before it resumes this Session.</summary>
        public required byte[] LocalResumeSecret { get; init; }

        /// <summary>The id the remote assigned, which this Reactor stamps on datagrams sent there.</summary>
        public uint RemoteSessionId { get; set; }

        /// <summary>
        /// The largest Message the remote said it will accept, announced during the handshake.
        /// <see cref="DatagramSessionReactor.Send"/> refuses a Message wider than this with
        /// <see cref="SendOutcome.RemoteMessageTooLarge"/> rather than sending bytes the far end has
        /// already said it cannot hold — which is why an unset value here would not refuse nothing, it
        /// would refuse everything: every non-empty Send would exceed a limit of zero. Both Established
        /// paths, <see cref="OnHelloWithCookie"/> (via <see cref="NewAcceptedPeer"/>) and
        /// <see cref="OnAccept"/>, therefore reject a zero announcement off the wire before a Peer ever
        /// reaches Established, and each asserts or re-asserts the resulting non-zero invariant rather
        /// than defaulting a missing value to something permissive or something that refuses. The
        /// Datagram profile's wire-level Resume carries no parameters: what looks like "surviving an
        /// interruption" here is a Session identified by its wire token rather than by address, so an
        /// outage, and the Resume that ends it, never tear down the <see cref="PeerSlot"/>. This field
        /// is never re-read or re-established — it simply was never lost, and a Resume only moves the
        /// send address.
        /// </summary>
        public uint RemoteMaxMessageSize { get; set; }

        /// <summary>
        /// The Fragment size in bytes the peer announced during the handshake, never this end's own
        /// <see cref="FragmentSize"/>. Set on both Established paths and always within the legal
        /// announced range, because <see cref="ControlDatagramCodec"/> rejects any other value at parse.
        /// </summary>
        public ushort RemoteFragmentSize { get; set; }

        /// <summary>
        /// The receive window in Fragments the peer announced during the handshake, never this end's own.
        /// Set on both Established paths and never zero, because <see cref="ControlDatagramCodec"/>
        /// rejects zero at parse.
        /// </summary>
        public uint RemoteReceiveWindowFragments { get; set; }

        /// <summary>
        /// The send-idle interval the peer itself announced during the handshake — never this end's own
        /// <see cref="DatagramSessionOptions.SendIdleInterval"/>. Set on both Established paths,
        /// <see cref="DatagramSessionReactor.NewAcceptedPeer"/> (from the Hello-with-cookie field) and
        /// <see cref="DatagramSessionReactor.OnAccept"/> (from the Accept field), from the value
        /// <see cref="ControlDatagramCodec.TryReadHelloWithCookie"/> and
        /// <see cref="ControlDatagramCodec.TryReadAccept"/> already reject as zero at parse, so it is
        /// always positive once a Peer reaches Established. The stream profile's
        /// <see cref="StreamSessionReactor.PeerSlot.RemoteSendIdleInterval"/> is the model this mirrors.
        /// <see cref="DatagramSessionReactor.HasExceededReceiveIdleDeadline"/> multiplies it by the
        /// missed-heartbeat count to get this Peer's receive-idle deadline.
        /// </summary>
        public TimeSpan RemoteSendIdleInterval { get; set; }

        /// <summary>
        /// The Linger the peer itself announced during the handshake — never this end's own
        /// <see cref="DatagramSessionOptions.Linger"/>. Set on both Established paths,
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
        /// negotiated parameter is. This is the RTO's ceiling: every
        /// <see cref="RetransmissionTimer.EffectiveTimeout"/> call in <see cref="DatagramSessionReactor"/>
        /// clamps against this value, never against this end's own
        /// <see cref="DatagramSessionOptions.Linger"/>, so the ceiling and Linger expiry read the same
        /// effective value.
        /// </summary>
        public TimeSpan EffectiveLinger { get; set; }

        /// <summary>
        /// This Peer's send Fragment size: the smaller of <see cref="DatagramSessionOptions.ResolvedFragmentSize"/>
        /// and <see cref="RemoteFragmentSize"/> (ADR 0024). The accepting end sets it at admission. The
        /// dialling end starts at its own size and lowers it once, in <see cref="DatagramSessionReactor.OnAccept"/>,
        /// before the Peer is Established; <see cref="DatagramSessionReactor.Send"/> refuses until then.
        /// Nothing writes it after Established, a Resume included, so every Fragment a Message is cut into
        /// and every retransmit of one uses this same value for the Peer's whole lifetime. Never larger
        /// than this end's own Fragment size, so a retention slot always holds a Fragment cut at it.
        /// </summary>
        public required int FragmentSize { get; set; }

        /// <summary>The secret the remote issued for resuming this Session. The dialling end sends it in each Resume (<see cref="DatagramSessionReactor.SendResumeIfDue"/>). The accepting end stores it but never reads it: it authorises a Resume against <see cref="PeerSlot.LocalResumeSecret"/>, the secret it issued.</summary>
        public byte[] RemoteResumeSecret { get; set; } = [];

        /// <summary>The Slot this Peer occupies, filled in once the table has admitted it.</summary>
        public SlotHandle Handle { get; set; }

        /// <summary>
        /// Where this Peer is in its life — handshake, Interrupted, Closing, the remote's Goodbye — and the
        /// only code that moves it.
        /// </summary>
        public required PeerLifecycle Lifecycle { get; init; }

        /// <summary>
        /// The <see cref="TimeProvider.GetTimestamp"/> reading of this Peer's most recent data
        /// datagram sent — set the moment this Peer reaches <see cref="PeerPhase.Open"/>,
        /// on both the dialling path (<see cref="DatagramSessionReactor.OnAccept"/>) and the accepting
        /// one (<see cref="DatagramSessionReactor.NewAcceptedPeer"/>), and restarted by
        /// <see cref="DatagramSessionReactor.FlushPeer"/> every time a data Datagram actually reaches
        /// the wire for this Peer. The handshake's own control datagrams never touch this: they are not
        /// data datagrams, and a Session that took several retransmitted Hellos to connect should not
        /// start its idle clock earlier than the moment it actually became a Session.
        /// </summary>
        public long LastSendTimestamp { get; set; }

        /// <summary>
        /// The <see cref="DatagramSessionReactor._pumpNumber"/> of the Pump that first classified a
        /// reliable record from this Peer since this end last sent it a data Datagram, or
        /// <see langword="null"/> when every such record has been acknowledged on the wire. Set by
        /// <see cref="DatagramSessionReactor.ClassifyRecord"/> and cleared by
        /// <see cref="DatagramSessionReactor.FlushOneDatagram"/> whenever a Datagram leaves, since every
        /// data Datagram carries both ack blocks (ADR 0026).
        /// </summary>
        public long? AckOwedSincePump { get; set; }

        /// <summary>
        /// The <see cref="TimeProvider.GetTimestamp"/> reading of the most recent moment this Peer's
        /// wire proved it was still there — set the instant this Peer reaches
        /// <see cref="PeerPhase.Open"/>, on both the dialling path
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
        /// Whether this end dialled this Peer (<see cref="Connect"/>) rather than accepted it. Only a
        /// dialled Peer sends Resume: the accepting end cannot know the dialler's new address. Derived
        /// from <see cref="HandshakeSource"/>, which only an accepted Peer has.
        /// </summary>
        public bool IsDialled => HandshakeSource is null;

        /// <summary>
        /// This Peer's own mutable counters (ADR 0013), incremented on the hot path and read only
        /// through <see cref="CountersOf"/>. Mirrors <see cref="StreamSessionReactor"/>'s equivalent
        /// field. The parse loop's per-Space classification increments
        /// <see cref="Core.PeerCounterSet.OutOfWindowPackets"/>,
        /// <see cref="Core.PeerCounterSet.DuplicatePackets"/> and
        /// <see cref="Core.PeerCounterSet.UnreliableLossPackets"/>; a freshly connected Peer reads zero
        /// for all three.
        /// </summary>
        public PeerCounterSet Counters { get; } = new();

        /// <summary>
        /// This Peer's four outbound sequence allocators, one per delivery mode.
        /// <see cref="DatagramSessionReactor.Send"/> allocates from it — one sequence per Send,
        /// whatever the mode — and the coalescing flush puts what was built from that sequence on the
        /// wire.
        /// </summary>
        public OutboundSequenceSpaces OutboundSequences { get; } = new();

        /// <summary>
        /// Records this Peer has Sent since its last flush, in Send order: appended to by
        /// <see cref="DatagramSessionReactor.Send"/> once a record is accepted, packed into data
        /// Datagrams by <see cref="FlushPeer"/> on the next <see cref="Pump"/>, which trims what it sent
        /// and leaves the rest, in order, for the Pump after. A fresh
        /// Slot always starts with an empty queue — <see cref="Connect"/> and the accepted-Peer path in
        /// <see cref="OnHelloWithCookie"/> both construct a brand new <see cref="PeerSlot"/> rather than
        /// reusing one, so a Slot a later Peer occupies never inherits a previous occupant's queue.
        /// An unreliable record's bytes sit in the Reactor's preallocated unreliable queue, not in the list.
        /// </summary>
        public List<PendingOutboundRecord> PendingSends { get; } = [];

        /// <summary>
        /// The bytes this Peer's queued unreliable records are charged against
        /// <see cref="DatagramSessionOptions.UnreliableQueueBytes"/>: each one's length plus the 7-byte record header.
        /// </summary>
        public int UnreliableQueuedBytes { get; set; }

        /// <summary>
        /// The end of the payload bytes queued in this Peer's region of the Reactor's unreliable queue, where the
        /// next unreliable Send's bytes go.
        /// </summary>
        public int UnreliableQueuedPayloadEnd { get; set; }

        /// <summary>
        /// This Peer's own tracker of outstanding reliable sequences and their ack horizon: built fresh
        /// alongside <see cref="OutboundSequences"/> at the same two admission sites and released with
        /// the Slot, so a Slot a later Peer reuses inherits none of the previous occupant's outstanding
        /// sequences or horizon. Time comes from this Reactor's own
        /// <see cref="ReactorOptions.TimeProvider"/>, never the ambient clock.
        /// </summary>
        public required OutstandingReliableSequences AckTracker { get; init; }

        /// <summary>
        /// This Peer's own adaptive retransmission timer: the same instance passed into
        /// <see cref="AckTracker"/> at construction, so a release <see cref="AckTracker"/> processes
        /// and an effective-RTO read from this property are two views of one timer rather than two that
        /// happen to agree. Built fresh alongside <see cref="OutboundSequences"/> and
        /// <see cref="AckTracker"/> at the same two admission sites and released with the Slot. The
        /// per-Pump expiry sweep reads
        /// <see cref="RetransmissionTimer.BackOff"/>/<see cref="RetransmissionTimer.RecordDelivery"/>
        /// through it.
        /// </summary>
        public required RetransmissionTimer RetransmitTimer { get; init; }

        /// <summary>
        /// This Peer's own fragment-size stall signature state: built fresh from this Reactor's own
        /// <see cref="ReactorOptions.TimeProvider"/> at the same two admission sites as
        /// <see cref="AckTracker"/> and <see cref="RetransmitTimer"/>, and released with the Slot, so a
        /// Slot a later Peer reuses inherits none of the previous occupant's stall state. Fed from the
        /// same flush, ack and retransmission-sweep paths that already run for <see cref="AckTracker"/>
        /// and <see cref="RetransmitTimer"/>; nothing new reads the clock or sweeps for it.
        /// </summary>
        public required FragmentStallSignature StallSignature { get; init; }

        /// <summary>
        /// The value of <see cref="OutstandingReliableSequences.HorizonDeferrals"/> last folded into
        /// <see cref="Counters"/>. The tracker exposes a monotonic total rather than a per-event
        /// signal, while <see cref="Core.PeerCounterSet.RecordHorizonDeferral"/> increments by one per
        /// call, so reconciling the two means comparing the tracker's total against this and folding in
        /// only the difference, once, each time the tracker's state is consulted. Zero for a freshly
        /// built Slot, matching a tracker that has deferred nothing yet.
        /// </summary>
        public ulong ObservedHorizonDeferrals { get; set; }

        /// <summary>
        /// The highest sequence this Peer has delivered in its <c>UnreliableSequenced</c> Space, or
        /// <see langword="null"/> if it has delivered none. What the mode's whole rule is stated
        /// against: a record is delivered only if it is strictly newer than this, so a Message
        /// superseded before it arrived is discarded rather than delivered late.
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
        /// This Peer's bounded duplicate suppression for the plain <c>Unreliable</c> Space (ADR 0017):
        /// 64 sequences wide, a protocol constant rather than anything derived from this Reactor's
        /// options. Built with the Slot and released with it, so a Slot a later Peer occupies never
        /// inherits the previous occupant's suppression state.
        /// </summary>
        public UnreliableDuplicateWindow UnreliableDuplicates { get; } = new();

        /// <summary>
        /// This Peer's send-order release queue for its <c>ReliableOrdered</c> Space. One queue, for
        /// that Space alone: the other three Spaces have none, which is what confines head-of-line
        /// blocking to this Space by construction rather than by convention. Built with the Slot and
        /// released with it, so a Slot a later Peer occupies never inherits the previous occupant's
        /// held run or its next expected sequence.
        /// </summary>
        public required OrderedDeliveryQueue OrderedDelivery { get; init; }

        /// <summary>
        /// The lowest <c>ReliableOrdered</c> sequence a received Goodbye record has arrived at and not
        /// yet been delivered, or <see langword="null"/> when none is waiting. The record sits in
        /// <see cref="OrderedDelivery"/> like any other, so it acts only once every record before it
        /// has been delivered; the ordered release path compares each released sequence against this to
        /// tell a Goodbye from a zero-length Message, which the queue itself cannot.
        /// </summary>
        public uint? RemoteGoodbyeSequence { get; set; }

    }
}
