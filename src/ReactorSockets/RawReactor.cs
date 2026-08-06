using System.Net;
using System.Net.Sockets;
using ReactorSockets.Core;
using ReactorSockets.Transports;

namespace ReactorSockets;

/// <summary>
/// The escape hatch (ADR 0007): a consumer registers UDP sockets at runtime, gets a <see cref="LinkId"/>
/// back for each, and advances everything with a single synchronous <see cref="Pump"/> call from their
/// own loop. Nothing is delivered except in response to a call the consumer made, on the thread that
/// constructed this Reactor. TCP joins the datagram surface here: a listening Link and the connections
/// it accepts are registered and admitted the same way, and a stream Link's reads drain as
/// <see cref="RawEventKind.SegmentReceived"/> and <see cref="RawEventKind.EndOfStream"/> exactly like a
/// datagram Link's. A stream Link's <see cref="Send"/> takes what the socket will take and reports the
/// count, arming exactly one <see cref="RawEventKind.Writable"/> when it took fewer bytes than
/// offered; <see cref="Shutdown"/> sends this Link's own FIN while it stays open for reading.
/// </summary>
/// <remarks>
/// <para>
/// <b>How a bad <see cref="LinkId"/> is reported.</b> A member that already returns a
/// <see cref="Result"/> reports it as a failed Result: the channel exists, and a Link closing under a
/// caller is an ordinary race rather than a programmer error, so <see cref="SendTo"/>,
/// <see cref="Send"/> and <see cref="Shutdown"/> never throw for one. A member that returns data has
/// no such channel — <see cref="LocalOf"/>, <see cref="RemoteOf"/> and <see cref="CountersOf"/> would
/// have to invent a sentinel or widen every call site into an unwrap — so they throw
/// <see cref="ArgumentException"/> instead, whether the <see cref="LinkId"/> does not address an open
/// Link at all, or it does but the Link is the wrong kind to answer the question (<see cref="RemoteOf"/>
/// on a datagram or listening Link — the same wrong-kind case <see cref="Send"/>, <see cref="SendTo"/>
/// and <see cref="Shutdown"/> report as a failed Result instead, because they can). <see cref="Close"/>
/// is neither: it is idempotent by design, so a stale <see cref="LinkId"/> is simply a no-op.
/// </para>
/// <para>
/// <b>Which members check the calling thread.</b> Every member that mutates this Reactor or its
/// sockets enforces the constructing-thread rule (ADR 0002): <see cref="Pump"/>, <see cref="Bind"/>,
/// <see cref="SendTo"/>, <see cref="Send"/>, <see cref="Shutdown"/>, <see cref="Close"/> and
/// <see cref="Dispose"/>. The pure reads — <see cref="LocalOf"/>, <see cref="RemoteOf"/>,
/// <see cref="CountersOf"/> and <see cref="Counters"/> — deliberately do not, because each only
/// copies out a value that the Reactor never mutates in place.
/// </para>
/// <para>
/// That is not a claim that they are safe to call concurrently with a Pump. The counters behind
/// <see cref="Counters"/> and <see cref="CountersOf"/> are plain fields incremented without a barrier,
/// so a reader on another thread has no guarantee of freshness and may observe a snapshot whose
/// fields come from either side of a Pump. What it will not observe is a half-written value or a
/// disposed socket. Treat a cross-thread counter read as a sample, not as a consistent cut; a
/// consumer that needs one should read from its own Pump thread between Pumps.
/// </para>
/// </remarks>
public sealed class RawReactor : IDisposable
{
    /// <summary>
    /// The largest a UDP datagram can be, and this Reactor's default scratch buffer size and arena
    /// floor: a Segment is as large as one read, so the arena must hold at least one (ADR 0012).
    /// </summary>
    public const int MaxUdpDatagramSize = 65_507;

    private readonly ReadinessDispatcher _dispatcher;
    private readonly FrameArena _arena;
    private readonly DiagnosticGateway _diagnostics;
    private readonly ReactorCounterSet _counters;
    private readonly ShareHolderTable<LinkSlot> _links;
    private readonly byte[] _scratchBuffer;
    private readonly List<PendingEvent> _pendingEvents = [];
    private readonly List<LinkId> _connectingLinks = [];
    private readonly int _constructingThreadId;
    private readonly int _admissionCap;
    private int _admissionsThisFrame;
    private int _capDeferralsThisFrame;
    private int _capacityDeferralsThisFrame;
    private bool _inPump;
    private bool _disposed;

    /// <summary>
    /// Constructs a Reactor bound to the calling thread (ADR 0002) — every future mutating call must
    /// come from this same thread or it throws.
    /// </summary>
    /// <param name="options">
    /// The shared options value. Every knob on it is validated here, and
    /// <see cref="ReactorOptions.ArenaSize"/> is resolved against this profile's floor; a
    /// combination that cannot work throws at construction and nowhere else.
    /// </param>
    /// <param name="scratchBufferSize">
    /// The single scratch buffer every Link's receive lands in, reused across Links since only one is
    /// read at any instant inside a single-threaded Pump (ADR 0012). Defaults to
    /// <see cref="MaxUdpDatagramSize"/>, at which truncation is unreachable; this is the profile-specific
    /// knob a consumer lowers to opt into truncation, kept off <see cref="ReactorOptions"/> because it
    /// has no meaning outside Raw's datagram surface.
    /// </param>
    public RawReactor(ReactorOptions options, int? scratchBufferSize = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        int resolvedScratchBufferSize = scratchBufferSize ?? MaxUdpDatagramSize;
        if (resolvedScratchBufferSize < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(scratchBufferSize),
                resolvedScratchBufferSize,
                $"scratch buffer of {resolvedScratchBufferSize} bytes cannot hold anything — scratch buffer size must be at least 1.");
        }

        int arenaSize = options.ResolveArenaSize(
            resolvedScratchBufferSize,
            $"one maximum-sized read of {resolvedScratchBufferSize} bytes");

        _constructingThreadId = Environment.CurrentManagedThreadId;
        _dispatcher = new ReadinessDispatcher();
        _arena = new FrameArena(arenaSize, options.Capacity);
        _counters = new ReactorCounterSet();
        _diagnostics = new DiagnosticGateway(options.Sink, options.MinimumLevel, _counters);
        _links = new ShareHolderTable<LinkSlot>(options.Capacity);
        _scratchBuffer = new byte[resolvedScratchBufferSize];
        _admissionCap = options.ResolvedAdmissionCap;
    }

    internal int PendingEventCount => _pendingEvents.Count;

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
    /// One synchronous advance: wipes the previous Frame's events and arena, then runs the readiness
    /// dispatch that reads whatever registered Link is ready right now. Throws
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
            _capDeferralsThisFrame = 0;
            _capacityDeferralsThisFrame = 0;
            _dispatcher.Pump();
            ProcessConnectingLinks();
            RecordAdmissionDeferrals();
        }
        finally
        {
            _inPump = false;
        }
    }

    /// <summary>
    /// Drains every event produced this Frame — since the last <see cref="Pump"/> — in the order it was
    /// produced. Every drained <see cref="RawEvent"/>'s <see cref="RawEvent.Payload"/> is valid only
    /// until the next <see cref="Pump"/>.
    /// </summary>
    public RawEventEnumerator Drain()
    {
        EnsureUsable();
        return new(this);
    }

    /// <summary>
    /// Closes every Link still open and releases its socket. A Reactor owns every socket it bound, so
    /// a consumer that drops one without disposing it leaks those sockets until the finalizer each
    /// <see cref="Socket"/> carries runs. No <see cref="RawEventKind.Closed"/> events are queued:
    /// nothing can drain a disposed Reactor. Idempotent; every other member throws
    /// <see cref="ObjectDisposedException"/> afterwards.
    /// </summary>
    /// <remarks>
    /// There is deliberately no finalizer here. This type holds nothing unmanaged of its own — the
    /// sockets do, and each already finalizes itself — so a finalizer would add a GC cost to every
    /// Reactor to duplicate a guarantee the runtime already makes.
    /// </remarks>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        EnsureCallingThreadIsConstructingThread();
        EnsureNotInsideAPump();

        foreach (LinkSlot slot in _links.DrainAll())
        {
            _dispatcher.Unregister(slot.Socket);
            slot.Dispose();
        }

        _pendingEvents.Clear();
        _disposed = true;
    }

    /// <summary>
    /// Binds a new UDP socket to <paramref name="endpoint"/> and returns the <see cref="LinkId"/> that
    /// addresses it. Raw registers its transports at runtime, unlike a Session Reactor which declares
    /// its endpoints at construction (ADR 0014).
    /// </summary>
    public LinkId Bind(IPEndPoint endpoint)
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();
        ArgumentNullException.ThrowIfNull(endpoint);

        EnsureCapacityForAnotherLink();
        UdpLink link = UdpLink.Bind(endpoint);
        SlotHandle handle = AddOrThrow(new DatagramLinkSlot(link));
        _dispatcher.Register(link.Socket, () => OnLinkReadable(handle));
        return ToLinkId(handle);
    }

    /// <summary>
    /// Binds and listens on a new TCP socket at <paramref name="endpoint"/>, returning the
    /// <see cref="LinkId"/> that addresses it. A listening socket is a Link like any other — one
    /// identity type, one lifecycle, one <see cref="RawEventKind.Closed"/> event — because "Listener"
    /// is a role a Link plays for a consumer, not a kind of thing this library names (ADR 0008). It
    /// consumes a capacity slot exactly like any other Link, but holds no read share: accepting reads
    /// nothing into the frame arena (ADR 0014).
    /// </summary>
    /// <remarks>
    /// Inbound connections are admitted by whichever <see cref="Pump"/> finds this Link ready, bounded
    /// by the shared admission cap (<see cref="ReactorOptions.ResolvedAdmissionCap"/>) every accept and
    /// handshake in this library draws from. Beyond the cap, a pending connection is left exactly where
    /// it was — in the OS backlog — for a later Pump to admit; Raw invents nothing on the wire, so it
    /// says nothing to a remote waiting past the cap (ADR 0007). Binding rules are unchanged from
    /// <see cref="Bind"/>: exclusive, no dual-mode, no hostname, no configure callback.
    /// </remarks>
    public LinkId Listen(IPEndPoint endpoint)
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();
        ArgumentNullException.ThrowIfNull(endpoint);

        EnsureCapacityForAnotherLink();
        TcpListenLink link = TcpListenLink.Listen(endpoint);
        SlotHandle handle = AddOrThrow(new ListeningLinkSlot(link));
        _dispatcher.Register(link.Socket, () => OnListeningLinkReadable(handle));
        return ToLinkId(handle);
    }

    /// <summary>
    /// Begins dialling a new outbound TCP connection to <paramref name="endpoint"/> and returns the
    /// <see cref="LinkId"/> that addresses it immediately — connecting is advanced by the consumer's
    /// own loop like everything else, so this call never blocks and the handshake's outcome arrives
    /// later, from whichever <see cref="Pump"/> first finds it resolved. A dial that succeeds produces
    /// exactly one <see cref="RawEventKind.Connected"/>; a dial that fails produces a
    /// <see cref="RawEventKind.Closed"/> naming <see cref="CloseReason.Refused"/> or
    /// <see cref="CloseReason.Unreachable"/> (or <see cref="CloseReason.Failed"/> for anything else),
    /// alongside a <see cref="DiagnosticId.LinkFailed"/> carrying the platform
    /// <see cref="SocketError"/>. Nothing about a dial in progress is delivered except in response to a
    /// Pump.
    /// </summary>
    /// <remarks>
    /// There is no hostname overload and no connect-timeout knob (parent slice spec): nothing ticks
    /// unless the consumer Pumps, and a dial going nowhere is closed by the consumer or by the OS, not
    /// by a timer this library invented. The dialled Link consumes a capacity slot exactly like a Bind
    /// or a Listen; <see cref="LocalOf"/> answers the address TCP bound implicitly the moment the
    /// connect began, whether or not the handshake has finished.
    /// </remarks>
    public LinkId Connect(IPEndPoint endpoint)
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();
        ArgumentNullException.ThrowIfNull(endpoint);

        EnsureCapacityForAnotherLink();
        TcpLink link = TcpLink.Dial(endpoint);
        LinkId linkId = ToLinkId(AddOrThrow(new StreamLinkSlot(link)));
        _connectingLinks.Add(linkId);
        return linkId;
    }

    /// <summary>
    /// Sends <paramref name="payload"/> to <paramref name="destination"/> in one datagram, all-or-nothing
    /// (ADR 0008). Fails without throwing when <paramref name="link"/> does not address an open Link or
    /// the send itself fails.
    /// </summary>
    public Result SendTo(LinkId link, Source destination, ReadOnlySpan<byte> payload)
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();

        LinkSlot? slot = ResolveSlot(link);
        if (slot is null)
        {
            return Result.Failure($"Link {link} is not open.");
        }

        if (slot is ListeningLinkSlot)
        {
            return Result.Failure("SendTo is illegal on a listening Link.");
        }

        if (slot is not DatagramLinkSlot datagram)
        {
            return Result.Failure("SendTo is illegal on a stream Link — use Send.");
        }

        UdpLinkSendResult result = datagram.Link.SendTo(destination, payload);
        if (!result.IsSuccess)
        {
            return result.Error == SocketError.Success
                ? Result.Failure($"SendTo sent {result.BytesSent} of {payload.Length} bytes — a datagram send is all-or-nothing.")
                : Result.Failure($"SendTo failed: {result.Error}.");
        }

        datagram.Counters.RecordBytesSent(payload.Length);
        _counters.RecordDatagramSent();
        return Result.Success();
    }

    /// <summary>
    /// <c>Send</c> is stream-only and illegal on a datagram or listening Link. Takes what the socket
    /// will accept of <paramref name="payload"/> right now and reports the count — never blocks, never
    /// copies and never queues (ADR 0008): whatever the socket did not take stays with the caller, who
    /// decides whether and when to offer it again. Queuing was rejected on principle, not merely left
    /// out: an unbounded queue is a memory leak with a network cause, and a bounded one needs a limit
    /// this library would have to justify plus a way to learn when to retry — which reintroduces this
    /// same event under another name. Either way the Reactor would be inventing a per-Link FIFO policy,
    /// which is machinery a Session has and Raw was said not to lend.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A send that takes fewer bytes than <paramref name="payload"/>.Length arms exactly one
    /// <see cref="RawEventKind.Writable"/> for <paramref name="link"/>: the next <see cref="Pump"/>
    /// that finds the socket writable queues it and clears the mark, so a consumer using this Link
    /// only ever sees one <c>Writable</c> per short send, never one for every Pump a socket happens to
    /// be writable (nearly always). A Link never over-sent on produces no <c>Writable</c> at all — the
    /// event is a reply to something the consumer did, not a status this Reactor volunteers.
    /// </para>
    /// <para>
    /// Fails without throwing (ADR 0008), naming whichever applies: <paramref name="link"/> does not
    /// address an open Link, it does but cannot take a stream send, it was already shut down for
    /// writing by <see cref="Shutdown"/>, or the socket itself failed. The open check comes first —
    /// answering an illegality to a stale or garbage <see cref="LinkId"/> would be a lie about a Link
    /// that does not exist.
    /// </para>
    /// </remarks>
    public SendResult Send(LinkId link, ReadOnlySpan<byte> payload)
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();

        LinkSlot? slot = ResolveSlot(link);
        if (slot is null)
        {
            return SendResult.Failure($"Link {link} is not open.");
        }

        if (slot is DatagramLinkSlot)
        {
            return SendResult.Failure("Send is illegal on a datagram Link — use SendTo.");
        }

        if (slot is not StreamLinkSlot stream)
        {
            return SendResult.Failure("Send is illegal on a listening Link.");
        }

        if (stream.WriteShutdown)
        {
            return SendResult.Failure($"Link {link} was already shut down for writing — Shutdown is one-way.");
        }

        TcpLinkSendResult result = stream.Link.Send(payload);
        if (!result.IsSuccess)
        {
            return SendResult.Failure($"Send failed: {result.Error}.");
        }

        stream.Counters.RecordBytesSent(result.BytesSent);
        if (result.BytesSent < payload.Length)
        {
            _dispatcher.ArmWritable(stream.Link.Socket);
        }

        return SendResult.Success(result.BytesSent);
    }

    /// <summary>
    /// <c>Shutdown</c> is stream-only and illegal on a datagram or listening Link. Sends this Link's
    /// own FIN, closing the send half while the Link stays open for reading — this is what lets a
    /// consumer build a request-response protocol that signals end-of-request by closing the write
    /// half, and it is a one-way door: a further <see cref="Send"/> on this Link fails afterwards.
    /// Fails without throwing (ADR 0008), naming whichever applies: <paramref name="link"/> does not
    /// address an open Link, or it does and that Link cannot be shut down. The open check comes first,
    /// for the reason on <see cref="Send"/>.
    /// </summary>
    public Result Shutdown(LinkId link)
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();

        LinkSlot? slot = ResolveSlot(link);
        if (slot is null)
        {
            return Result.Failure($"Link {link} is not open.");
        }

        if (slot is DatagramLinkSlot)
        {
            return Result.Failure("Shutdown is illegal on a datagram Link.");
        }

        if (slot is not StreamLinkSlot stream)
        {
            return Result.Failure("Shutdown is illegal on a listening Link.");
        }

        if (stream.WriteShutdown)
        {
            return Result.Failure($"Link {link} was already shut down for writing.");
        }

        TcpLinkShutdownResult result = stream.Link.Shutdown();
        if (!result.IsSuccess)
        {
            return Result.Failure($"Shutdown failed: {result.Error}.");
        }

        stream.WriteShutdown = true;
        return Result.Success();
    }

    /// <summary>
    /// Returns the bound address of <paramref name="link"/>. Link state, constant for the Link's
    /// lifetime, and therefore not event data (ADR 0014). Throws <see cref="ArgumentException"/> if
    /// <paramref name="link"/> does not address an open Link.
    /// </summary>
    public IPEndPoint LocalOf(LinkId link)
    {
        EnsureUsable();

        LinkSlot? slot = ResolveSlot(link) ?? throw new ArgumentException($"Link {link} is not open.", nameof(link));

        return slot.LocalEndPoint;
    }

    /// <summary>
    /// Returns the remote address of the stream Link <paramref name="link"/> — Link state, constant
    /// for the Link's lifetime and therefore not event data, which is exactly why
    /// <see cref="RawEventKind.Accepted"/> carries no address (ADR 0014). Illegal on a datagram or
    /// listening Link, since neither has a single remote for its whole lifetime the way a stream
    /// connection does — a bound UDP socket answers many senders, and a listening socket answers
    /// none. Throws <see cref="ArgumentException"/> if <paramref name="link"/> does not address an
    /// open Link, or does but is the wrong kind, for the reason given on this type's own remarks: a
    /// member that returns data has no <see cref="Result"/> channel to report a wrong-kind call
    /// through instead.
    /// </summary>
    public IPEndPoint RemoteOf(LinkId link)
    {
        EnsureUsable();

        LinkSlot? slot = ResolveSlot(link) ?? throw new ArgumentException($"Link {link} is not open.", nameof(link));

        if (slot is not StreamLinkSlot stream)
        {
            throw new ArgumentException(
                $"Link {link} is not a stream Link — RemoteOf is illegal on a datagram or listening Link.", nameof(link));
        }

        return stream.Link.RemoteEndPoint;
    }

    /// <summary>
    /// Closes <paramref name="link"/> and queues exactly one <see cref="RawEventKind.Closed"/> event
    /// carrying <see cref="CloseReason.LocalClose"/>. A no-op if <paramref name="link"/> does not address
    /// an open Link. The slot's generation advances, so a stale copy of <paramref name="link"/> can never
    /// address whatever binds into this slot next (ADR 0014).
    /// </summary>
    public void Close(LinkId link)
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();

        CloseSlot(ToHandle(link), CloseReason.LocalClose);
    }

    /// <summary>A point-in-time snapshot of one Link's counters (ADR 0013). Throws if the Link is not open.</summary>
    public LinkCounters CountersOf(LinkId link)
    {
        EnsureUsable();

        LinkSlot? slot = ResolveSlot(link) ?? throw new ArgumentException($"Link {link} is not open.", nameof(link));

        return slot.Counters.Snapshot();
    }

    internal RawEvent GetPendingEvent(int index)
    {
        PendingEvent pending = _pendingEvents[index];
        return pending.Kind switch
        {
            RawEventKind.DatagramReceived => RawEvent.ForDatagramReceived(
                pending.Link, pending.Source, _arena.GetCommitted(pending.PayloadOffset, pending.PayloadLength)),
            RawEventKind.Closed => RawEvent.ForClosed(pending.Link, pending.Reason),
            RawEventKind.Accepted => RawEvent.ForAccepted(pending.ListeningLink, pending.Link),
            RawEventKind.Connected => RawEvent.ForConnected(pending.Link),
            RawEventKind.SegmentReceived => RawEvent.ForSegmentReceived(
                pending.Link, _arena.GetCommitted(pending.PayloadOffset, pending.PayloadLength)),
            RawEventKind.EndOfStream => RawEvent.ForEndOfStream(pending.Link),
            RawEventKind.Writable => RawEvent.ForWritable(pending.Link),
            _ => throw new InvalidOperationException($"{pending.Kind} is not a recognised RawEventKind."),
        };
    }

    /// <summary>
    /// Reads a ready Link exactly once, then returns to the scan. This single read — not the frame
    /// arena's fair share — is what bounds how much of one Pump a single Link can consume, and it is
    /// the deliberate choice for the Raw profile: it is the strongest possible fairness guarantee
    /// (every ready Link is served before any Link is served twice) and it costs one syscall per Link
    /// per Pump, which a consumer already controls by choosing how often to Pump. A stream Link
    /// delivering a run longer than one read is simply read again on a later Pump, once the dispatcher
    /// finds it ready a second time — the same bound, applied to a transport with no message
    /// boundaries of its own.
    /// </summary>
    /// <remarks>
    /// The consequence is that a Link cannot reach a second <c>Reserve</c> in one Frame, so
    /// <see cref="ArenaReservationOutcome.ShareExhausted"/> is unreachable through this Reactor's
    /// public surface. The arena's share is therefore a second, looser ceiling here: it only bites
    /// through the one-whole-unit escape, when a single datagram or Segment exceeds a share. Both
    /// mechanisms are kept because a later profile that reads a Link in a loop needs the share to be
    /// the bound — changing this method to loop until <c>ShareExhausted</c> is what makes that switch,
    /// and the arena is already sized for it.
    /// </remarks>
    private void OnLinkReadable(SlotHandle handle)
    {
        switch (_links.TryResolve(handle))
        {
            case DatagramLinkSlot datagramSlot:
                OnDatagramLinkReadable(handle, datagramSlot);
                break;
            case StreamLinkSlot streamSlot:
                OnStreamLinkReadable(handle, streamSlot);
                break;
        }
    }

    /// <summary>
    /// Queues exactly one <see cref="RawEventKind.Writable"/> for the stream Link at <paramref name="index"/>.
    /// Called only by the dispatcher, and only for a socket it just found writable while armed
    /// (<see cref="Core.ReadinessDispatcher.ArmWritable"/>) — the dispatcher itself disarms before
    /// invoking this, so nothing here needs to re-check that the mark was still set.
    /// </summary>
    private void OnLinkWritable(SlotHandle handle)
    {
        if (_links.TryResolve(handle) is StreamLinkSlot)
        {
            _pendingEvents.Add(new PendingEvent
            {
                Kind = RawEventKind.Writable,
                Link = ToLinkId(handle),
            });
        }
    }

    private void OnDatagramLinkReadable(SlotHandle handle, DatagramLinkSlot slot)
    {
        UdpLinkReceiveResult result = slot.Link.Receive(_scratchBuffer);
        switch (result.Outcome)
        {
            case UdpLinkReceiveOutcome.Delivered:
                DeliverDatagram(handle, slot, result.Source, result.Payload);
                break;
            case UdpLinkReceiveOutcome.Truncated:
                RecordDatagramTruncated(result.Source);
                break;
            case UdpLinkReceiveOutcome.Unreachable:
                RecordDestinationUnreachable(ToLinkId(handle), result.Error);
                break;
            case UdpLinkReceiveOutcome.Failed:
                FailLink(handle, slot, result.Error);
                break;
        }
    }

    /// <summary>
    /// Reads a ready stream Link exactly once, the same fairness rule <see cref="OnLinkReadable"/>
    /// applies to a datagram Link. A Link that has already delivered <c>EndOfStream</c> is still read
    /// once more: the FIN that produced it leaves the socket permanently readable, so the alternative
    /// is to stop reading and never learn of a subsequent reset — but a second <c>EndOfStream</c> is
    /// never queued, since the remote already said everything it is going to say.
    /// </summary>
    private void OnStreamLinkReadable(SlotHandle handle, StreamLinkSlot slot)
    {
        TcpLinkReceiveResult result = slot.Link.Receive(_scratchBuffer);
        switch (result.Outcome)
        {
            case TcpLinkReceiveOutcome.Delivered:
                DeliverSegment(handle, slot, result.Payload);
                break;
            case TcpLinkReceiveOutcome.EndOfStream:
                if (!slot.EndOfStreamDelivered)
                {
                    slot.EndOfStreamDelivered = true;
                    _pendingEvents.Add(new PendingEvent
                    {
                        Kind = RawEventKind.EndOfStream,
                        Link = ToLinkId(handle),
                    });
                }

                break;
            case TcpLinkReceiveOutcome.Failed:
                FailLink(handle, slot, result.Error);
                break;
        }
    }

    private void DeliverDatagram(SlotHandle handle, LinkSlot slot, Source source, ReadOnlySpan<byte> data)
    {
        int offsetBeforeReserve = _arena.BytesUsed;
        ArenaReservation reservation = _arena.Reserve(handle.Index, data);
        switch (reservation.Outcome)
        {
            case ArenaReservationOutcome.Committed:
                _pendingEvents.Add(new PendingEvent
                {
                    Kind = RawEventKind.DatagramReceived,
                    Link = ToLinkId(handle),
                    Source = source,
                    PayloadOffset = offsetBeforeReserve,
                    PayloadLength = data.Length,
                });
                slot.Counters.RecordBytesReceived(data.Length);
                _counters.RecordDatagramReceived();
                _counters.ObserveArenaUsage(_arena.BytesUsed);
                break;
            case ArenaReservationOutcome.ShareExhausted:
                slot.Counters.RecordReadShareExhaustion();
                break;
            case ArenaReservationOutcome.ArenaExhausted:
                if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
                {
                    _diagnostics.Log(DiagnosticRecord.ForArenaExhausted(
                        ToLinkId(handle), reservation.BytesRequested, reservation.BytesFree));
                }

                break;
        }
    }

    /// <summary>
    /// Commits a Segment's bytes into the frame arena and queues the <see cref="RawEventKind.SegmentReceived"/>
    /// event, or records the refusal — <see cref="ArenaReservationOutcome.ShareExhausted"/> as a per-Link
    /// counter (ADR 0013: the condition can recur every Pump under sustained load, so it fails the
    /// record test) and <see cref="ArenaReservationOutcome.ArenaExhausted"/> as a <c>Warn</c> Diagnostic.
    /// The reservation logic is identical to <see cref="DeliverDatagram"/>'s; only the event shape and
    /// the absence of a claimed Source differ, because a stream Link has one remote for its whole
    /// lifetime and nothing per-Segment to claim.
    /// </summary>
    private void DeliverSegment(SlotHandle handle, LinkSlot slot, ReadOnlySpan<byte> data)
    {
        int offsetBeforeReserve = _arena.BytesUsed;
        ArenaReservation reservation = _arena.Reserve(handle.Index, data);
        switch (reservation.Outcome)
        {
            case ArenaReservationOutcome.Committed:
                _pendingEvents.Add(new PendingEvent
                {
                    Kind = RawEventKind.SegmentReceived,
                    Link = ToLinkId(handle),
                    PayloadOffset = offsetBeforeReserve,
                    PayloadLength = data.Length,
                });
                slot.Counters.RecordBytesReceived(data.Length);
                _counters.ObserveArenaUsage(_arena.BytesUsed);
                break;
            case ArenaReservationOutcome.ShareExhausted:
                slot.Counters.RecordReadShareExhaustion();
                break;
            case ArenaReservationOutcome.ArenaExhausted:
                if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
                {
                    _diagnostics.Log(DiagnosticRecord.ForArenaExhausted(
                        ToLinkId(handle), reservation.BytesRequested, reservation.BytesFree));
                }

                break;
        }
    }

    private void RecordDatagramTruncated(Source source)
    {
        if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
        {
            _diagnostics.Log(DiagnosticRecord.ForDatagramTruncated(source, _scratchBuffer.Length));
        }
    }

    private void RecordDestinationUnreachable(LinkId link, SocketError error)
    {
        if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
        {
            _diagnostics.Log(DiagnosticRecord.ForDestinationUnreachable(link, error));
        }
    }

    /// <summary>
    /// Closes a Link whose read failed outright, logging the platform <see cref="SocketError"/> first.
    /// A reset is reported as <see cref="CloseReason.ResetByRemote"/> rather than the generic
    /// <see cref="CloseReason.Failed"/> — a stranger resetting the connection is a condition a
    /// consumer's retry logic wants to distinguish from every other read failure, and the one this
    /// library already names for it (user story: a reset is a stranger's prerogative, reported
    /// <c>Warn</c> rather than <c>Error</c>).
    /// </summary>
    private void FailLink(SlotHandle handle, LinkSlot slot, SocketError error)
    {
        if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
        {
            _diagnostics.Log(DiagnosticRecord.ForLinkFailed(ToLinkId(handle), error));
        }

        CloseReason reason = error == SocketError.ConnectionReset ? CloseReason.ResetByRemote : CloseReason.Failed;
        CloseSlot(handle, reason);
    }

    /// <summary>
    /// Polls every Link still dialling for a resolved handshake, in the order each was begun. A
    /// resolved dial is removed from the tracking list whether it succeeded or failed; one still in
    /// flight is left for the next Pump to check again. Each entry's generation is re-checked against
    /// the slot before polling, so a Link the consumer closed mid-dial — freeing its slot for reuse
    /// this same Frame — is dropped rather than polling whatever now occupies that slot.
    /// </summary>
    private void ProcessConnectingLinks()
    {
        // Forward, with a write index compacting the survivors down behind it, rather than the usual
        // reverse loop that lets RemoveAt be safe: several dials resolving in the same Pump must queue
        // their events in the order they were begun, and iterating backwards would reverse them.
        int surviving = 0;
        for (int i = 0; i < _connectingLinks.Count; i++)
        {
            LinkId link = _connectingLinks[i];
            SlotHandle handle = ToHandle(link);
            if (_links.TryResolve(handle) is not StreamLinkSlot slot)
            {
                continue;
            }

            TcpConnectPollResult result = slot.Link.PollConnect();
            switch (result.Outcome)
            {
                case TcpConnectPollOutcome.Pending:
                    _connectingLinks[surviving++] = link;
                    break;
                case TcpConnectPollOutcome.Connected:
                    _dispatcher.Register(slot.Link.Socket, () => OnLinkReadable(handle), () => OnLinkWritable(handle));
                    _pendingEvents.Add(new PendingEvent { Kind = RawEventKind.Connected, Link = link });
                    break;
                case TcpConnectPollOutcome.Failed:
                    FailDial(handle, slot, result.Error);
                    break;
            }
        }

        _connectingLinks.RemoveRange(surviving, _connectingLinks.Count - surviving);
    }

    /// <summary>
    /// Closes a Link whose dial failed, translating the platform <see cref="SocketError"/> into the
    /// small, stable <see cref="CloseReason"/> roster a consumer's retry logic switches on — the exact
    /// error still travels in the <see cref="DiagnosticId.LinkFailed"/> record this logs alongside.
    /// </summary>
    private void FailDial(SlotHandle handle, LinkSlot slot, SocketError error)
    {
        if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
        {
            _diagnostics.Log(DiagnosticRecord.ForLinkFailed(ToLinkId(handle), error));
        }

        CloseReason reason = error switch
        {
            SocketError.ConnectionRefused => CloseReason.Refused,
            SocketError.HostUnreachable or SocketError.NetworkUnreachable => CloseReason.Unreachable,
            _ => CloseReason.Failed,
        };

        CloseSlot(handle, reason);
    }

    /// <summary>
    /// Accepts as many pending connections as the shared admission cap still allows this Frame,
    /// stopping the moment the cap is spent or the backlog runs dry — whichever comes first. Unlike
    /// <see cref="OnLinkReadable"/>'s one-read-per-Pump fairness rule, a listening Link may produce
    /// several <see cref="RawEventKind.Accepted"/> events in a single Pump: the admission cap, not a
    /// per-Link read share, is what bounds a burst of inbound connections here, because accepting
    /// holds no read share to bound it with in the first place (ADR 0012, ADR 0014).
    /// </summary>
    private void OnListeningLinkReadable(SlotHandle handle)
    {
        if (_links.TryResolve(handle) is not ListeningLinkSlot listening)
        {
            return;
        }

        while (true)
        {
            if (_admissionsThisFrame >= _admissionCap)
            {
                if (listening.Link.HasPendingConnection)
                {
                    _capDeferralsThisFrame++;
                }

                return;
            }

            // Every slot is occupied — a different exhaustion than the admission cap, reachable
            // because the cap can be configured above ReactorOptions.Capacity. Checked before the
            // accept rather than after, so a connection this Reactor has no room for stays in the OS
            // backlog untouched: accepting it first would complete the handshake and then drop it,
            // showing the remote an established-then-closed connection where the slice spec requires
            // that a Reactor at capacity say nothing on the wire at all. No slot frees up later in
            // this same Pump, so there is nothing to gain by looping further either way. It carries
            // its own diagnostic rather than borrowing AdmissionCapReached, because the two are
            // fixed by different knobs: one wants Capacity raised, the other the cap lowered.
            if (!_links.HasFreeSlot)
            {
                if (listening.Link.HasPendingConnection)
                {
                    _capacityDeferralsThisFrame++;
                }

                return;
            }

            TcpAcceptResult result = listening.Link.TryAccept();
            switch (result.Outcome)
            {
                case TcpAcceptOutcome.NoPendingConnection:
                    return;
                case TcpAcceptOutcome.Failed:
                    RecordAcceptFailed(handle, result.Error);
                    return;
                case TcpAcceptOutcome.Accepted:
                    AdmitAccepted(handle, result.Socket!);
                    break;
            }
        }
    }

    /// <summary>
    /// Registers a socket a listening Link just accepted as a new stream Link, and queues the
    /// <see cref="RawEventKind.Accepted"/> event naming both it and the listening Link. The caller
    /// checks <see cref="ShareHolderTable{T}.HasFreeSlot"/> before accepting rather than letting this
    /// method look a Slot up, so that a Reactor with nowhere to put a connection never takes it off
    /// the backlog in the first place — see the remarks in <see cref="OnListeningLinkReadable"/>.
    /// </summary>
    /// <remarks>
    /// The full-table branch below is therefore unreachable: a Reactor is single-threaded (ADR 0002),
    /// so nothing can claim the Slot between that check and this add. It closes the socket rather than
    /// throwing anyway, because throwing here would escape a dispatcher callback mid-Pump, which is
    /// the thing the check upstream exists to avoid.
    /// </remarks>
    private void AdmitAccepted(SlotHandle listeningHandle, Socket acceptedSocket)
    {
        TcpLink link = TcpLink.FromAccepted(acceptedSocket);
        if (_links.TryAdd(new StreamLinkSlot(link)) is not SlotHandle handle)
        {
            link.Dispose();
            return;
        }

        _dispatcher.Register(link.Socket, () => OnLinkReadable(handle), () => OnLinkWritable(handle));
        _admissionsThisFrame++;
        _pendingEvents.Add(new PendingEvent
        {
            Kind = RawEventKind.Accepted,
            Link = ToLinkId(handle),
            ListeningLink = ToLinkId(listeningHandle),
        });
    }

    /// <summary>
    /// Logs this Frame's deferrals as at most one record per cause — a
    /// <see cref="DiagnosticId.AdmissionCapReached"/> for the Links the admission cap turned away and
    /// a <see cref="DiagnosticId.CapacityExhausted"/> for the Links that found no free slot. Both
    /// feed the one <c>AdmissionDeferrals</c> counter, which does not distinguish them.
    /// </summary>
    private void RecordAdmissionDeferrals()
    {
        RecordAdmissionDeferrals(_capDeferralsThisFrame);
        RecordAdmissionDeferrals(_capacityDeferralsThisFrame);

        if (!_diagnostics.IsEnabled(DiagnosticLevel.Warn))
        {
            return;
        }

        if (_capDeferralsThisFrame > 0)
        {
            _diagnostics.Log(DiagnosticRecord.ForAdmissionCapReached(_admissionCap, _capDeferralsThisFrame));
        }

        if (_capacityDeferralsThisFrame > 0)
        {
            _diagnostics.Log(DiagnosticRecord.ForCapacityExhausted(_links.Capacity, _capacityDeferralsThisFrame));
        }
    }

    /// <summary>Adds <paramref name="deferrals"/> to the Reactor-scope deferral counter.</summary>
    private void RecordAdmissionDeferrals(int deferrals)
    {
        for (int i = 0; i < deferrals; i++)
        {
            _counters.RecordAdmissionDeferral();
        }
    }

    /// <summary>
    /// Logs a failed accept attempt without closing the listening Link — see the remarks on
    /// <see cref="TcpListenLink.TryAccept"/> for why one bad connection never stops a listening Link
    /// from admitting the next one.
    /// </summary>
    private void RecordAcceptFailed(SlotHandle handle, SocketError error)
    {
        if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
        {
            _diagnostics.Log(DiagnosticRecord.ForLinkFailed(ToLinkId(handle), error));
        }
    }

    /// <summary>
    /// Removes the Link <paramref name="handle"/> addresses, releases what it held, and queues exactly
    /// one <see cref="RawEventKind.Closed"/> naming it; a no-op if the handle addresses nothing, which
    /// is what makes <see cref="Close"/> idempotent. The Event carries <paramref name="handle"/>'s own
    /// generation — the one that just died — so a consumer can match it to the <see cref="LinkId"/>
    /// they were holding, even though the Slot has already moved on.
    /// </summary>
    private void CloseSlot(SlotHandle handle, CloseReason reason)
    {
        if (_links.Remove(handle) is not LinkSlot slot)
        {
            return;
        }

        _dispatcher.Unregister(slot.Socket);
        slot.Dispose();
        _pendingEvents.Add(new PendingEvent
        {
            Kind = RawEventKind.Closed,
            Link = ToLinkId(handle),
            Reason = reason,
        });
    }

    /// <summary>
    /// Refuses another Link before any socket is created, so a Reactor at capacity never binds a port
    /// it is about to have to release. <see cref="AddOrThrow"/> repeats the check the type system
    /// needs; this one is what makes that repetition unreachable.
    /// </summary>
    private void EnsureCapacityForAnotherLink()
    {
        if (!_links.HasFreeSlot)
        {
            throw new InvalidOperationException(
                $"Reactor is at capacity ({_links.Capacity}) — close a Link before binding another.");
        }
    }

    /// <summary>
    /// Admits <paramref name="slot"/> to the Share-holder table. The table answers with mechanism —
    /// <see langword="null"/> when full — and this is where Raw turns that into its own policy for the
    /// registration surface, which is to throw. The accept path applies the other policy and defers.
    /// </summary>
    private SlotHandle AddOrThrow(LinkSlot slot) =>
        _links.TryAdd(slot) ?? throw new InvalidOperationException(
            $"Reactor is at capacity ({_links.Capacity}) — close a Link before binding another.");

    private LinkSlot? ResolveSlot(LinkId link) => _links.TryResolve(ToHandle(link));

    /// <summary>Converts L0's internal Slot identity into the public one Raw hands consumers (ADR 0003).</summary>
    private static LinkId ToLinkId(SlotHandle handle) => new(handle.Index, handle.Generation);

    /// <summary>The inverse of <see cref="ToLinkId"/>, applied to every <see cref="LinkId"/> arriving from a consumer.</summary>
    private static SlotHandle ToHandle(LinkId link) => new(link.Index, link.Generation);

    /// <summary>
    /// The two checks every public member starts with: this Reactor still owns its sockets, and the
    /// call is not arriving from inside its own Pump.
    /// </summary>
    private void EnsureUsable()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        EnsureNotInsideAPump();
    }

    /// <summary>
    /// Rejects re-entry (ADR 0005). The only caller the library hands control to mid-Pump is a
    /// Diagnostic Sink, so this is what stops a Sink acting on the Reactor it is reporting on —
    /// which would clear the pending-event list halfway through the Frame that is producing it.
    /// A Sink that lets this exception escape has thrown, so the gateway then faults and disables
    /// it exactly as it would for any other Sink bug, and the Frame still completes.
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

    /// <summary>
    /// The state one occupied slot holds, common to every kind of Link this Reactor registers: the
    /// underlying socket (so the dispatcher and <see cref="Dispose"/> never need to know which kind
    /// they are unregistering or disposing), the bound local address, and this Link's own counters. A
    /// listening Link, a stream Link and a datagram Link each subclass this only to add what makes
    /// them able to send or receive differently — "Listener" and "stream" are never surfaced as
    /// public types, only as which subclass a slot happens to be (ADR 0008).
    /// </summary>
    private abstract class LinkSlot(Socket socket, IPEndPoint localEndPoint)
    {
        public Socket Socket { get; } = socket;

        public IPEndPoint LocalEndPoint { get; } = localEndPoint;

        public LinkCounterSet Counters { get; } = new();

        public abstract void Dispose();
    }

    /// <summary>A slot occupied by a bound UDP Link.</summary>
    private sealed class DatagramLinkSlot(UdpLink link) : LinkSlot(link.Socket, link.LocalEndPoint)
    {
        public UdpLink Link { get; } = link;

        public override void Dispose() => Link.Dispose();
    }

    /// <summary>
    /// A slot occupied by a listening TCP Link — a Link like any other, consuming a capacity slot but
    /// holding no read share, since accepting reads nothing into the frame arena (ADR 0014).
    /// </summary>
    private sealed class ListeningLinkSlot(TcpListenLink link) : LinkSlot(link.Socket, link.LocalEndPoint)
    {
        public TcpListenLink Link { get; } = link;

        public override void Dispose() => Link.Dispose();
    }

    /// <summary>
    /// A slot occupied by a stream TCP Link, whether produced by an accept or by dialling out — the two
    /// are indistinguishable once registered, and a dialled one is still occupying this same kind of
    /// slot while its handshake is in flight, before <see cref="RawEventKind.Connected"/> or a failed
    /// <see cref="RawEventKind.Closed"/> resolves it.
    /// </summary>
    private sealed class StreamLinkSlot(TcpLink link) : LinkSlot(link.Socket, link.LocalEndPoint)
    {
        public TcpLink Link { get; } = link;

        /// <summary>
        /// Whether <see cref="RawEventKind.EndOfStream"/> has already been queued for this Link. The
        /// FIN that produces it leaves the socket permanently readable, so this is what stops every
        /// later Pump from queuing the same event again — the remote already said everything it is
        /// going to say (ADR 0008).
        /// </summary>
        public bool EndOfStreamDelivered { get; set; }

        /// <summary>
        /// Whether <see cref="RawReactor.Shutdown"/> has already sent this Link's own FIN. Checked by
        /// <see cref="RawReactor.Send"/> so a further send after Shutdown fails deterministically —
        /// rather than relying on whatever <see cref="System.Net.Sockets.SocketError"/> a platform
        /// happens to raise for writing to an already-shutdown socket, which is exactly the kind of
        /// platform-specific detail <see cref="CloseReason"/> and this Result-based surface exist to
        /// paper over. <c>Shutdown</c> is one-way: nothing ever clears this back.
        /// </summary>
        public bool WriteShutdown { get; set; }

        public override void Dispose() => Link.Dispose();
    }

    private readonly record struct PendingEvent
    {
        public RawEventKind Kind { get; init; }

        public LinkId Link { get; init; }

        public LinkId ListeningLink { get; init; }

        public Source Source { get; init; }

        public int PayloadOffset { get; init; }

        public int PayloadLength { get; init; }

        public CloseReason Reason { get; init; }
    }
}
