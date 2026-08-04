using System.Net;
using System.Net.Sockets;
using ReactorSockets.Core;
using ReactorSockets.Transports;

namespace ReactorSockets;

/// <summary>
/// The escape hatch (ADR 0007): a consumer registers UDP sockets at runtime, gets a <see cref="LinkId"/>
/// back for each, and advances everything with a single synchronous <see cref="Pump"/> call from their
/// own loop. Nothing is delivered except in response to a call the consumer made, on the thread that
/// constructed this Reactor. This slice implements the datagram half only — TCP joins in slice 2.
/// </summary>
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
    private readonly int _constructingThreadId;
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
        _dispatcher = new ReadinessDispatcher(options.TimeProvider);
        _arena = new FrameArena(arenaSize, options.Capacity);
        _counters = new ReactorCounterSet();
        _diagnostics = new DiagnosticGateway(options.Sink, options.MinimumLevel, _counters);
        _slots = new LinkSlot?[options.Capacity];
        _generations = new int[options.Capacity];
        _scratchBuffer = new byte[resolvedScratchBufferSize];
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
            _dispatcher.Pump();
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
                _dispatcher.Unregister(slot.Link.Socket);
                slot.Link.Dispose();
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
        _slots[index] = new LinkSlot(link);
        return new LinkId(index, _generations[index]);
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

        UdpLinkSendResult result = slot.Link.SendTo(destination, payload);
        if (!result.IsSuccess)
        {
            return result.Error == SocketError.Success
                ? Result.Failure($"SendTo sent {result.BytesSent} of {payload.Length} bytes — a datagram send is all-or-nothing.")
                : Result.Failure($"SendTo failed: {result.Error}.");
        }

        slot.Counters.RecordBytesSent(payload.Length);
        _counters.RecordDatagramSent();
        return Result.Success();
    }

    /// <summary>
    /// <c>Send</c> is stream-only and illegal on a datagram Link. Always returns a failed
    /// <see cref="Result"/> naming the illegality rather than throwing (ADR 0008) — Raw has no stream
    /// Links until slice 2, so every Link this Reactor can hand out is datagram-only.
    /// </summary>
    public Result Send(LinkId link, ReadOnlySpan<byte> payload)
    {
        EnsureUsable();
        return Result.Failure("Send is illegal on a datagram Link — use SendTo.");
    }

    /// <summary>
    /// <c>Shutdown</c> is stream-only and illegal on a datagram Link. Always returns a failed
    /// <see cref="Result"/> naming the illegality rather than throwing (ADR 0008).
    /// </summary>
    public Result Shutdown(LinkId link)
    {
        EnsureUsable();
        return Result.Failure("Shutdown is illegal on a datagram Link.");
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

        return slot.Link.LocalEndPoint;
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
            _ => throw new InvalidOperationException($"{pending.Kind} is not a recognised RawEventKind."),
        };
    }

    private void OnLinkReadable(int index)
    {
        LinkSlot? slot = _slots[index];
        if (slot is null)
        {
            return;
        }

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

    private void FailLink(int index, LinkSlot slot, SocketError error)
    {
        if (_diagnostics.IsEnabled(DiagnosticLevel.Warn))
        {
            _diagnostics.Log(DiagnosticRecord.ForLinkFailed(new LinkId(index, _generations[index]), error));
        }

        CloseSlot(index, slot, CloseReason.Failed);
    }

    private void CloseSlot(int index, LinkSlot slot, CloseReason reason)
    {
        _dispatcher.Unregister(slot.Link.Socket);
        slot.Link.Dispose();
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

    private int FindFreeSlot()
    {
        for (int i = 0; i < _slots.Length; i++)
        {
            if (_slots[i] is null)
            {
                return i;
            }
        }

        throw new InvalidOperationException(
            $"Reactor is at capacity ({_slots.Length}) — close a Link before binding another.");
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

    private sealed class LinkSlot(UdpLink link)
    {
        public UdpLink Link { get; } = link;

        public LinkCounterSet Counters { get; } = new();
    }

    private readonly record struct PendingEvent
    {
        public RawEventKind Kind { get; init; }

        public LinkId Link { get; init; }

        public Source Source { get; init; }

        public int PayloadOffset { get; init; }

        public int PayloadLength { get; init; }

        public CloseReason Reason { get; init; }
    }
}
