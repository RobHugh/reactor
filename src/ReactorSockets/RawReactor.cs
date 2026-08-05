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
/// no such channel — <see cref="LocalOf"/> and <see cref="CountersOf"/> would have to invent a
/// sentinel or widen every call site into an unwrap — so they throw
/// <see cref="ArgumentException"/> instead. <see cref="Close"/> is neither: it is idempotent by
/// design, so a stale <see cref="LinkId"/> is simply a no-op.
/// </para>
/// <para>
/// <b>Which members check the calling thread.</b> Every member that mutates this Reactor or its
/// sockets enforces the constructing-thread rule (ADR 0002): <see cref="Pump"/>, <see cref="Bind"/>,
/// <see cref="SendTo"/>, <see cref="Send"/>, <see cref="Shutdown"/>, <see cref="Close"/> and
/// <see cref="Dispose"/>. The pure reads — <see cref="LocalOf"/>, <see cref="CountersOf"/> and
/// <see cref="Counters"/> — deliberately do not, because each only copies out a value that the
/// Reactor never mutates in place.
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
    private readonly LinkSlot?[] _slots;
    private readonly int[] _generations;
    private readonly byte[] _scratchBuffer;
    private readonly List<PendingEvent> _pendingEvents = [];
    private readonly List<LinkId> _connectingLinks = [];
    private readonly int _constructingThreadId;
    private readonly int _admissionCap;
    private int _admissionsThisFrame;
    private int _admissionsDeferredThisFrame;
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
        _slots = new LinkSlot?[options.Capacity];
        _generations = new int[options.Capacity];
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
            _admissionsDeferredThisFrame = 0;
            _dispatcher.Pump();
            ProcessConnectingLinks();
            if (_admissionsDeferredThisFrame > 0)
            {
                RecordAdmissionCapReached();
            }
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

        for (int index = 0; index < _slots.Length; index++)
        {
            if (_slots[index] is LinkSlot slot)
            {
                _dispatcher.Unregister(slot.Socket);
                slot.Dispose();
                _slots[index] = null;
            }
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

        int index = FindFreeSlot();
        UdpLink link = UdpLink.Bind(endpoint);
        _dispatcher.Register(link.Socket, () => OnLinkReadable(index));
        _slots[index] = new DatagramLinkSlot(link);
        return new LinkId(index, _generations[index]);
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
    /// says nothing to a peer waiting past the cap (ADR 0007). Binding rules are unchanged from
    /// <see cref="Bind"/>: exclusive, no dual-mode, no hostname, no configure callback.
    /// </remarks>
    public LinkId Listen(IPEndPoint endpoint)
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();
        ArgumentNullException.ThrowIfNull(endpoint);

        int index = FindFreeSlot();
        TcpListenLink link = TcpListenLink.Listen(endpoint);
        _dispatcher.Register(link.Socket, () => OnListenerReadable(index));
        _slots[index] = new ListeningLinkSlot(link);
        return new LinkId(index, _generations[index]);
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

        int index = FindFreeSlot();
        TcpLink link = TcpLink.Dial(endpoint);
        _slots[index] = new StreamLinkSlot(link);
        LinkId linkId = new(index, _generations[index]);
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
    /// Closes <paramref name="link"/> and queues exactly one <see cref="RawEventKind.Closed"/> event
    /// carrying <see cref="CloseReason.LocalClose"/>. A no-op if <paramref name="link"/> does not address
    /// an open Link. The slot's generation advances, so a stale copy of <paramref name="link"/> can never
    /// address whatever binds into this slot next (ADR 0014).
    /// </summary>
    public void Close(LinkId link)
    {
        EnsureUsable();
        EnsureCallingThreadIsConstructingThread();

        LinkSlot? slot = ResolveSlot(link);
        if (slot is null)
        {
            return;
        }

        CloseSlot(link.Index, slot, CloseReason.LocalClose);
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
            RawEventKind.Accepted => RawEvent.ForAccepted(pending.Listener, pending.Link),
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
    private void OnLinkReadable(int index)
    {
        switch (_slots[index])
        {
            case DatagramLinkSlot datagramSlot:
                OnDatagramLinkReadable(index, datagramSlot);
                break;
            case StreamLinkSlot streamSlot:
                OnStreamLinkReadable(index, streamSlot);
                break;
        }
    }

    /// <summary>
    /// Queues exactly one <see cref="RawEventKind.Writable"/> for the stream Link at <paramref name="index"/>.
    /// Called only by the dispatcher, and only for a socket it just found writable while armed
    /// (<see cref="Core.ReadinessDispatcher.ArmWritable"/>) — the dispatcher itself disarms before
    /// invoking this, so nothing here needs to re-check that the mark was still set.
    /// </summary>
    private void OnLinkWritable(int index)
    {
        if (_slots[index] is StreamLinkSlot)
        {
            _pendingEvents.Add(new PendingEvent
            {
                Kind = RawEventKind.Writable,
                Link = new LinkId(index, _generations[index]),
            });
        }
    }

    private void OnDatagramLinkReadable(int index, DatagramLinkSlot slot)
    {
        UdpLinkReceiveResult result = slot.Link.Receive(_scratchBuffer);
        switch (result.Outcome)
        {
            case UdpLinkReceiveOutcome.Delivered:
                DeliverDatagram(index, slot, result.Source, result.Payload);
                break;
            case UdpLinkReceiveOutcome.Truncated:
                RecordDatagramTruncated(result.Source);
                break;
            case UdpLinkReceiveOutcome.Unreachable:
                RecordDestinationUnreachable(new LinkId(index, _generations[index]), result.Error);
                break;
            case UdpLinkReceiveOutcome.Failed:
                FailLink(index, slot, result.Error);
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
    private void OnStreamLinkReadable(int index, StreamLinkSlot slot)
    {
        TcpLinkReceiveResult result = slot.Link.Receive(_scratchBuffer);
        switch (result.Outcome)
        {
            case TcpLinkReceiveOutcome.Delivered:
                DeliverSegment(index, slot, result.Payload);
                break;
            case TcpLinkReceiveOutcome.EndOfStream:
                if (!slot.EndOfStreamDelivered)
                {
                    slot.EndOfStreamDelivered = true;
                    _pendingEvents.Add(new PendingEvent
                    {
                        Kind = RawEventKind.EndOfStream,
                        Link = new LinkId(index, _generations[index]),
                    });
                }

                break;
            case TcpLinkReceiveOutcome.Failed:
                FailLink(index, slot, result.Error);
                break;
        }
    }

    private void DeliverDatagram(int index, LinkSlot slot, Source source, ReadOnlySpan<byte> data)
    {
        int offsetBeforeReserve = _arena.BytesUsed;
        ArenaReservation reservation = _arena.Reserve(index, data);
        switch (reservation.Outcome)
        {
            case ArenaReservationOutcome.Committed:
                _pendingEvents.Add(new PendingEvent
                {
                    Kind = RawEventKind.DatagramReceived,
                    Link = new LinkId(index, _generations[index]),
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
                        new LinkId(index, _generations[index]), reservation.BytesRequested, reservation.BytesFree));
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
    /// the absence of a claimed Source differ, because a stream Link has one peer for its whole
    /// lifetime and nothing per-Segment to claim.
    /// </summary>
    private void DeliverSegment(int index, LinkSlot slot, ReadOnlySpan<byte> data)
    {
        int offsetBeforeReserve = _arena.BytesUsed;
        ArenaReservation reservation = _arena.Reserve(index, data);
        switch (reservation.Outcome)
        {
            case ArenaReservationOutcome.Committed:
                _pendingEvents.Add(new PendingEvent
                {
                    Kind = RawEventKind.SegmentReceived,
                    Link = new LinkId(index, _generations[index]),
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
                        new LinkId(index, _generations[index]), reservation.BytesRequested, reservation.BytesFree));
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
    /// A reset is reported as <see cref="CloseReason.ResetByPeer"/> rather than the generic
    /// <see cref="CloseReason.Failed"/> — a stranger resetting the connection is a condition a
    /// consumer's retry logic wants to distinguish from every other read failure, and the one this
    /// library already names for it (user story: a reset is a stranger's prerogative, reported
    /// <c>Warn</c> rather than <c>Error</c>).
    /// </summary>
    private void FailLink(int index, LinkSlot slot, SocketError error)
    {
        if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
        {
            _diagnostics.Log(DiagnosticRecord.ForLinkFailed(new LinkId(index, _generations[index]), error));
        }

        CloseReason reason = error == SocketError.ConnectionReset ? CloseReason.ResetByPeer : CloseReason.Failed;
        CloseSlot(index, slot, reason);
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
        for (int i = _connectingLinks.Count - 1; i >= 0; i--)
        {
            LinkId link = _connectingLinks[i];
            if (_generations[link.Index] != link.Generation || _slots[link.Index] is not StreamLinkSlot slot)
            {
                _connectingLinks.RemoveAt(i);
                continue;
            }

            TcpConnectPollResult result = slot.Link.PollConnect();
            switch (result.Outcome)
            {
                case TcpConnectPollOutcome.Pending:
                    continue;
                case TcpConnectPollOutcome.Connected:
                    _connectingLinks.RemoveAt(i);
                    _dispatcher.Register(slot.Link.Socket, () => OnLinkReadable(link.Index), () => OnLinkWritable(link.Index));
                    _pendingEvents.Add(new PendingEvent { Kind = RawEventKind.Connected, Link = link });
                    break;
                case TcpConnectPollOutcome.Failed:
                    _connectingLinks.RemoveAt(i);
                    FailDial(link.Index, slot, result.Error);
                    break;
            }
        }
    }

    /// <summary>
    /// Closes a Link whose dial failed, translating the platform <see cref="SocketError"/> into the
    /// small, stable <see cref="CloseReason"/> roster a consumer's retry logic switches on — the exact
    /// error still travels in the <see cref="DiagnosticId.LinkFailed"/> record this logs alongside.
    /// </summary>
    private void FailDial(int index, LinkSlot slot, SocketError error)
    {
        if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
        {
            _diagnostics.Log(DiagnosticRecord.ForLinkFailed(new LinkId(index, _generations[index]), error));
        }

        CloseReason reason = error switch
        {
            SocketError.ConnectionRefused => CloseReason.Refused,
            SocketError.HostUnreachable or SocketError.NetworkUnreachable => CloseReason.Unreachable,
            _ => CloseReason.Failed,
        };

        CloseSlot(index, slot, reason);
    }

    /// <summary>
    /// Accepts as many pending connections as the shared admission cap still allows this Frame,
    /// stopping the moment the cap is spent or the backlog runs dry — whichever comes first. Unlike
    /// <see cref="OnLinkReadable"/>'s one-read-per-Pump fairness rule, a listening Link may produce
    /// several <see cref="RawEventKind.Accepted"/> events in a single Pump: the admission cap, not a
    /// per-Link read share, is what bounds a burst of inbound connections here, because accepting
    /// holds no read share to bound it with in the first place (ADR 0012, ADR 0014).
    /// </summary>
    private void OnListenerReadable(int index)
    {
        if (_slots[index] is not ListeningLinkSlot listening)
        {
            return;
        }

        while (true)
        {
            if (_admissionsThisFrame >= _admissionCap)
            {
                if (listening.Link.HasPendingConnection)
                {
                    _admissionsDeferredThisFrame++;
                }

                return;
            }

            TcpAcceptResult result = listening.Link.TryAccept();
            switch (result.Outcome)
            {
                case TcpAcceptOutcome.NoPendingConnection:
                    return;
                case TcpAcceptOutcome.Failed:
                    RecordListenerAcceptFailed(index, result.Error);
                    return;
                case TcpAcceptOutcome.Accepted:
                    if (!AdmitAccepted(index, result.Socket!))
                    {
                        // Every slot is occupied — a different exhaustion than the admission cap,
                        // reachable because the cap can be configured above ReactorOptions.Capacity.
                        // No slot will free up later in this same Pump, so looping further would only
                        // accept and immediately drop every remaining backlogged connection instead of
                        // leaving them for a Pump that runs after a slot closes.
                        return;
                    }

                    break;
            }
        }
    }

    /// <summary>
    /// Registers a socket a listening Link just accepted as a new stream Link, and queues the
    /// <see cref="RawEventKind.Accepted"/> event naming both it and the listener. Returns
    /// <see langword="false"/> if every slot is already occupied, in which case the accepted socket is
    /// disposed immediately: it is already off the OS backlog and cannot be put back, so holding it
    /// unregistered would leak the handle instead of merely deferring it.
    /// </summary>
    private bool AdmitAccepted(int listenerIndex, Socket acceptedSocket)
    {
        int? newIndex = TryFindFreeSlot();
        if (newIndex is null)
        {
            acceptedSocket.Dispose();
            return false;
        }

        TcpLink link = TcpLink.FromAccepted(acceptedSocket);
        _slots[newIndex.Value] = new StreamLinkSlot(link);
        int acceptedIndex = newIndex.Value;
        _dispatcher.Register(link.Socket, () => OnLinkReadable(acceptedIndex), () => OnLinkWritable(acceptedIndex));
        _admissionsThisFrame++;
        _pendingEvents.Add(new PendingEvent
        {
            Kind = RawEventKind.Accepted,
            Link = new LinkId(newIndex.Value, _generations[newIndex.Value]),
            Listener = new LinkId(listenerIndex, _generations[listenerIndex]),
        });
        return true;
    }

    /// <summary>Logs the deferred admissions this Frame as a single <see cref="DiagnosticId.AdmissionCapReached"/> record.</summary>
    private void RecordAdmissionCapReached()
    {
        for (int i = 0; i < _admissionsDeferredThisFrame; i++)
        {
            _counters.RecordAdmissionDeferred();
        }

        if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
        {
            _diagnostics.Log(DiagnosticRecord.ForAdmissionCapReached(_admissionCap, _admissionsDeferredThisFrame));
        }
    }

    /// <summary>
    /// Logs a failed accept attempt without closing the listening Link — see the remarks on
    /// <see cref="TcpListenLink.TryAccept"/> for why one bad connection never stops a listener from
    /// admitting the next one.
    /// </summary>
    private void RecordListenerAcceptFailed(int index, SocketError error)
    {
        if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
        {
            _diagnostics.Log(DiagnosticRecord.ForLinkFailed(new LinkId(index, _generations[index]), error));
        }
    }

    private void CloseSlot(int index, LinkSlot slot, CloseReason reason)
    {
        _dispatcher.Unregister(slot.Socket);
        slot.Dispose();
        _slots[index] = null;
        int generation = _generations[index];
        _generations[index]++;
        _pendingEvents.Add(new PendingEvent
        {
            Kind = RawEventKind.Closed,
            Link = new LinkId(index, generation),
            Reason = reason,
        });
    }

    private int FindFreeSlot() =>
        TryFindFreeSlot() ?? throw new InvalidOperationException(
            $"Reactor is at capacity ({_slots.Length}) — close a Link before binding another.");

    /// <summary>
    /// The non-throwing counterpart to <see cref="FindFreeSlot"/>, for callers inside a Pump — an
    /// accept that finds the Reactor full must leave the connection to be dealt with rather than throw
    /// out of a dispatcher callback (ADR 0007's "say nothing" applies here too).
    /// </summary>
    private int? TryFindFreeSlot()
    {
        for (int i = 0; i < _slots.Length; i++)
        {
            if (_slots[i] is null)
            {
                return i;
            }
        }

        return null;
    }

    private LinkSlot? ResolveSlot(LinkId link)
    {
        if (link.Index < 0 || link.Index >= _slots.Length)
        {
            return null;
        }

        return _generations[link.Index] == link.Generation ? _slots[link.Index] : null;
    }

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

        public LinkId Listener { get; init; }

        public Source Source { get; init; }

        public int PayloadOffset { get; init; }

        public int PayloadLength { get; init; }

        public CloseReason Reason { get; init; }
    }
}
