# <a id="ReactorSockets_RawReactor"></a> Class RawReactor

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

The escape hatch (ADR 0007): a consumer registers UDP sockets at runtime, gets a [`LinkId`](ReactorSockets.LinkId.md)
back for each, and advances everything with a single synchronous [`RawReactor.Pump()`](ReactorSockets.RawReactor.Pump.md#ReactorSockets_RawReactor_Pump) call from their
own loop. Nothing is delivered except in response to a call the consumer made, on the thread that
constructed this Reactor. TCP joins the datagram surface here: a listening Link and the connections
it accepts are registered and admitted the same way, and a stream Link's reads drain as
[`RawEventKind.SegmentReceived`](ReactorSockets.RawEventKind.md) and [`RawEventKind.EndOfStream`](ReactorSockets.RawEventKind.md) exactly like a
datagram Link's. A stream Link's [`RawReactor.Send(LinkId, ReadOnlySpan<byte>)`](ReactorSockets.RawReactor.Send.md#ReactorSockets_RawReactor_Send_ReactorSockets_LinkId_System_ReadOnlySpan_System_Byte__) takes what the socket will take and reports the
count, arming exactly one [`RawEventKind.Writable`](ReactorSockets.RawEventKind.md) when it took fewer bytes than
offered; [`RawReactor.Shutdown(LinkId)`](ReactorSockets.RawReactor.Shutdown.md#ReactorSockets_RawReactor_Shutdown_ReactorSockets_LinkId_) sends this Link's own FIN while it stays open for reading.

```csharp
public sealed class RawReactor : IDisposable
```

#### Inheritance

[object](https://learn.microsoft.com/dotnet/api/system.object) ← 
[RawReactor](ReactorSockets.RawReactor.md)

#### Implements

[IDisposable](https://learn.microsoft.com/dotnet/api/system.idisposable)

#### Inherited Members

[object.Equals\(object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\)), 
[object.Equals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\-system\-object\)), 
[object.GetHashCode\(\)](https://learn.microsoft.com/dotnet/api/system.object.gethashcode), 
[object.GetType\(\)](https://learn.microsoft.com/dotnet/api/system.object.gettype), 
[object.ReferenceEquals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.referenceequals), 
[object.ToString\(\)](https://learn.microsoft.com/dotnet/api/system.object.tostring)

## Remarks

<p>
<b>How a bad [`LinkId`](ReactorSockets.LinkId.md) is reported.</b> A member that already returns a
[`Result`](ReactorSockets.Result.md) reports it as a failed Result: the channel exists, and a Link closing under a
caller is an ordinary race rather than a programmer error, so [`RawReactor.SendTo(LinkId, Source, ReadOnlySpan<byte>)`](ReactorSockets.RawReactor.SendTo.md#ReactorSockets_RawReactor_SendTo_ReactorSockets_LinkId_ReactorSockets_Source_System_ReadOnlySpan_System_Byte__),
[`RawReactor.Send(LinkId, ReadOnlySpan<byte>)`](ReactorSockets.RawReactor.Send.md#ReactorSockets_RawReactor_Send_ReactorSockets_LinkId_System_ReadOnlySpan_System_Byte__) and [`RawReactor.Shutdown(LinkId)`](ReactorSockets.RawReactor.Shutdown.md#ReactorSockets_RawReactor_Shutdown_ReactorSockets_LinkId_) never throw for one. A member that returns data has
no such channel — [`RawReactor.LocalOf(LinkId)`](ReactorSockets.RawReactor.LocalOf.md#ReactorSockets_RawReactor_LocalOf_ReactorSockets_LinkId_), [`RawReactor.RemoteOf(LinkId)`](ReactorSockets.RawReactor.RemoteOf.md#ReactorSockets_RawReactor_RemoteOf_ReactorSockets_LinkId_) and [`RawReactor.CountersOf(LinkId)`](ReactorSockets.RawReactor.CountersOf.md#ReactorSockets_RawReactor_CountersOf_ReactorSockets_LinkId_) would
have to invent a sentinel or widen every call site into an unwrap — so they throw
[`ArgumentException`](https://learn.microsoft.com/dotnet/api/system.argumentexception) instead, whether the [`LinkId`](ReactorSockets.LinkId.md) does not address an open
Link at all, or it does but the Link is the wrong kind to answer the question ([`RawReactor.RemoteOf(LinkId)`](ReactorSockets.RawReactor.RemoteOf.md#ReactorSockets_RawReactor_RemoteOf_ReactorSockets_LinkId_)
on a datagram or listening Link — the same wrong-kind case [`RawReactor.Send(LinkId, ReadOnlySpan<byte>)`](ReactorSockets.RawReactor.Send.md#ReactorSockets_RawReactor_Send_ReactorSockets_LinkId_System_ReadOnlySpan_System_Byte__), [`RawReactor.SendTo(LinkId, Source, ReadOnlySpan<byte>)`](ReactorSockets.RawReactor.SendTo.md#ReactorSockets_RawReactor_SendTo_ReactorSockets_LinkId_ReactorSockets_Source_System_ReadOnlySpan_System_Byte__)
and [`RawReactor.Shutdown(LinkId)`](ReactorSockets.RawReactor.Shutdown.md#ReactorSockets_RawReactor_Shutdown_ReactorSockets_LinkId_) report as a failed Result instead, because they can). [`RawReactor.Close(LinkId)`](ReactorSockets.RawReactor.Close.md#ReactorSockets_RawReactor_Close_ReactorSockets_LinkId_)
is neither: it is idempotent by design, so a stale [`LinkId`](ReactorSockets.LinkId.md) is simply a no-op.
</p>
<p>
<b>Which members check the calling thread.</b> Every member that mutates this Reactor or its
sockets enforces the constructing-thread rule (ADR 0002): [`RawReactor.Pump()`](ReactorSockets.RawReactor.Pump.md#ReactorSockets_RawReactor_Pump), [`RawReactor.Bind(IPEndPoint)`](ReactorSockets.RawReactor.Bind.md#ReactorSockets_RawReactor_Bind_System_Net_IPEndPoint_),
[`RawReactor.SendTo(LinkId, Source, ReadOnlySpan<byte>)`](ReactorSockets.RawReactor.SendTo.md#ReactorSockets_RawReactor_SendTo_ReactorSockets_LinkId_ReactorSockets_Source_System_ReadOnlySpan_System_Byte__), [`RawReactor.Send(LinkId, ReadOnlySpan<byte>)`](ReactorSockets.RawReactor.Send.md#ReactorSockets_RawReactor_Send_ReactorSockets_LinkId_System_ReadOnlySpan_System_Byte__), [`RawReactor.Shutdown(LinkId)`](ReactorSockets.RawReactor.Shutdown.md#ReactorSockets_RawReactor_Shutdown_ReactorSockets_LinkId_), [`RawReactor.Close(LinkId)`](ReactorSockets.RawReactor.Close.md#ReactorSockets_RawReactor_Close_ReactorSockets_LinkId_) and
[`RawReactor.Dispose()`](ReactorSockets.RawReactor.Dispose.md#ReactorSockets_RawReactor_Dispose). The pure reads — [`RawReactor.LocalOf(LinkId)`](ReactorSockets.RawReactor.LocalOf.md#ReactorSockets_RawReactor_LocalOf_ReactorSockets_LinkId_), [`RawReactor.RemoteOf(LinkId)`](ReactorSockets.RawReactor.RemoteOf.md#ReactorSockets_RawReactor_RemoteOf_ReactorSockets_LinkId_),
[`RawReactor.CountersOf(LinkId)`](ReactorSockets.RawReactor.CountersOf.md#ReactorSockets_RawReactor_CountersOf_ReactorSockets_LinkId_) and [`RawReactor.Counters`](ReactorSockets.RawReactor.Counters.md#ReactorSockets_RawReactor_Counters) — deliberately do not, because each only
copies out a value that the Reactor never mutates in place.
</p>
<p>
That is not a claim that they are safe to call concurrently with a Pump. The counters behind
[`RawReactor.Counters`](ReactorSockets.RawReactor.Counters.md#ReactorSockets_RawReactor_Counters) and [`RawReactor.CountersOf(LinkId)`](ReactorSockets.RawReactor.CountersOf.md#ReactorSockets_RawReactor_CountersOf_ReactorSockets_LinkId_) are plain fields incremented without a barrier,
so a reader on another thread has no guarantee of freshness and may observe a snapshot whose
fields come from either side of a Pump. What it will not observe is a half-written value or a
disposed socket. Treat a cross-thread counter read as a sample, not as a consistent cut; a
consumer that needs one should read from its own Pump thread between Pumps.
</p>

## Constructors

 [RawReactor\(ReactorOptions, int?\)](ReactorSockets.RawReactor.\-ctor.md\#ReactorSockets\_RawReactor\_\_ctor\_ReactorSockets\_ReactorOptions\_System\_Nullable\_System\_Int32\_\_)

Constructs a Reactor bound to the calling thread (ADR 0002) — every future mutating call must
come from this same thread or it throws.

## Fields

 [MaxUdpDatagramSize](ReactorSockets.RawReactor.MaxUdpDatagramSize.md\#ReactorSockets\_RawReactor\_MaxUdpDatagramSize)

The largest a UDP datagram can be, and this Reactor's default scratch buffer size and arena
floor: a Segment is as large as one read, so the arena must hold at least one (ADR 0012).

## Properties

 [Counters](ReactorSockets.RawReactor.Counters.md\#ReactorSockets\_RawReactor\_Counters)

A point-in-time snapshot of this Reactor's own counters (ADR 0013). No reset.

## Methods

 [Bind\(IPEndPoint\)](ReactorSockets.RawReactor.Bind.md\#ReactorSockets\_RawReactor\_Bind\_System\_Net\_IPEndPoint\_)

Binds a new UDP socket to <code class="paramref">endpoint</code> and returns the [`LinkId`](ReactorSockets.LinkId.md) that
addresses it. Raw registers its transports at runtime, unlike a Session Reactor which declares
its endpoints at construction (ADR 0014).

 [Close\(LinkId\)](ReactorSockets.RawReactor.Close.md\#ReactorSockets\_RawReactor\_Close\_ReactorSockets\_LinkId\_)

Closes <code class="paramref">link</code> and queues exactly one [`RawEventKind.Closed`](ReactorSockets.RawEventKind.md) event
carrying [`CloseReason.LocalClose`](ReactorSockets.CloseReason.md). A no-op if <code class="paramref">link</code> does not address
an open Link. The slot's generation advances, so a stale copy of <code class="paramref">link</code> can never
address whatever binds into this slot next (ADR 0014).

 [Connect\(IPEndPoint\)](ReactorSockets.RawReactor.Connect.md\#ReactorSockets\_RawReactor\_Connect\_System\_Net\_IPEndPoint\_)

Begins dialling a new outbound TCP connection to <code class="paramref">endpoint</code> and returns the
[`LinkId`](ReactorSockets.LinkId.md) that addresses it immediately — connecting is advanced by the consumer's
own loop like everything else, so this call never blocks and the handshake's outcome arrives
later, from whichever [`RawReactor.Pump()`](ReactorSockets.RawReactor.Pump.md#ReactorSockets_RawReactor_Pump) first finds it resolved. A dial that succeeds produces
exactly one [`RawEventKind.Connected`](ReactorSockets.RawEventKind.md); a dial that fails produces a
[`RawEventKind.Closed`](ReactorSockets.RawEventKind.md) naming [`CloseReason.Refused`](ReactorSockets.CloseReason.md) or
[`CloseReason.Unreachable`](ReactorSockets.CloseReason.md) (or [`CloseReason.Failed`](ReactorSockets.CloseReason.md) for anything else),
alongside a [`DiagnosticId.LinkFailed`](ReactorSockets.DiagnosticId.md) carrying the platform
[`SocketError`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror). Nothing about a dial in progress is delivered except in response to a
Pump.

 [CountersOf\(LinkId\)](ReactorSockets.RawReactor.CountersOf.md\#ReactorSockets\_RawReactor\_CountersOf\_ReactorSockets\_LinkId\_)

A point-in-time snapshot of one Link's counters (ADR 0013). Throws if the Link is not open.

 [Dispose\(\)](ReactorSockets.RawReactor.Dispose.md\#ReactorSockets\_RawReactor\_Dispose)

Closes every Link still open and releases its socket. A Reactor owns every socket it bound, so
a consumer that drops one without disposing it leaks those sockets until the finalizer each
[`Socket`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socket) carries runs. No [`RawEventKind.Closed`](ReactorSockets.RawEventKind.md) events are queued:
nothing can drain a disposed Reactor. Idempotent; every other member throws
[`ObjectDisposedException`](https://learn.microsoft.com/dotnet/api/system.objectdisposedexception) afterwards.

 [Drain\(\)](ReactorSockets.RawReactor.Drain.md\#ReactorSockets\_RawReactor\_Drain)

Drains every event produced this Frame — since the last [`RawReactor.Pump()`](ReactorSockets.RawReactor.Pump.md#ReactorSockets_RawReactor_Pump) — in the order it was
produced. Every drained [`RawEvent`](ReactorSockets.RawEvent.md)'s [`RawEvent.Payload`](ReactorSockets.RawEvent.Payload.md#ReactorSockets_RawEvent_Payload) is valid only
until the next [`RawReactor.Pump()`](ReactorSockets.RawReactor.Pump.md#ReactorSockets_RawReactor_Pump).

 [Listen\(IPEndPoint\)](ReactorSockets.RawReactor.Listen.md\#ReactorSockets\_RawReactor\_Listen\_System\_Net\_IPEndPoint\_)

Binds and listens on a new TCP socket at <code class="paramref">endpoint</code>, returning the
[`LinkId`](ReactorSockets.LinkId.md) that addresses it. A listening socket is a Link like any other — one
identity type, one lifecycle, one [`RawEventKind.Closed`](ReactorSockets.RawEventKind.md) event — because "Listener"
is a role a Link plays for a consumer, not a kind of thing this library names (ADR 0008). It
consumes a capacity slot exactly like any other Link, but holds no read share: accepting reads
nothing into the frame arena (ADR 0014).

 [LocalOf\(LinkId\)](ReactorSockets.RawReactor.LocalOf.md\#ReactorSockets\_RawReactor\_LocalOf\_ReactorSockets\_LinkId\_)

Returns the bound address of <code class="paramref">link</code>. Link state, constant for the Link's
lifetime, and therefore not event data (ADR 0014). Throws [`ArgumentException`](https://learn.microsoft.com/dotnet/api/system.argumentexception) if
<code class="paramref">link</code> does not address an open Link.

 [Pump\(\)](ReactorSockets.RawReactor.Pump.md\#ReactorSockets\_RawReactor\_Pump)

One synchronous advance: wipes the previous Frame's events and arena, then runs the readiness
dispatch that reads whatever registered Link is ready right now. Throws
[`InvalidOperationException`](https://learn.microsoft.com/dotnet/api/system.invalidoperationexception) if called from any thread other than the one that
constructed this Reactor, or from inside a Pump already in progress.

 [RemoteOf\(LinkId\)](ReactorSockets.RawReactor.RemoteOf.md\#ReactorSockets\_RawReactor\_RemoteOf\_ReactorSockets\_LinkId\_)

Returns the remote address of the stream Link <code class="paramref">link</code> — Link state, constant
for the Link's lifetime and therefore not event data, which is exactly why
[`RawEventKind.Accepted`](ReactorSockets.RawEventKind.md) carries no address (ADR 0014). Illegal on a datagram or
listening Link, since neither has a single remote for its whole lifetime the way a stream
connection does — a bound UDP socket answers many senders, and a listening socket answers
none. Throws [`ArgumentException`](https://learn.microsoft.com/dotnet/api/system.argumentexception) if <code class="paramref">link</code> does not address an
open Link, or does but is the wrong kind, for the reason given on this type's own remarks: a
member that returns data has no [`Result`](ReactorSockets.Result.md) channel to report a wrong-kind call
through instead.

 [Send\(LinkId, ReadOnlySpan<byte\>\)](ReactorSockets.RawReactor.Send.md\#ReactorSockets\_RawReactor\_Send\_ReactorSockets\_LinkId\_System\_ReadOnlySpan\_System\_Byte\_\_)

<code>Send</code> is stream-only and illegal on a datagram or listening Link. Takes what the socket
will accept of <code class="paramref">payload</code> right now and reports the count — never blocks, never
copies and never queues (ADR 0008): whatever the socket did not take stays with the caller, who
decides whether and when to offer it again. Queuing was rejected on principle, not merely left
out: an unbounded queue is a memory leak with a network cause, and a bounded one needs a limit
this library would have to justify plus a way to learn when to retry — which reintroduces this
same event under another name. Either way the Reactor would be inventing a per-Link FIFO policy,
which is machinery a Session has and Raw was said not to lend.

 [SendTo\(LinkId, Source, ReadOnlySpan<byte\>\)](ReactorSockets.RawReactor.SendTo.md\#ReactorSockets\_RawReactor\_SendTo\_ReactorSockets\_LinkId\_ReactorSockets\_Source\_System\_ReadOnlySpan\_System\_Byte\_\_)

Sends <code class="paramref">payload</code> to <code class="paramref">destination</code> in one datagram, all-or-nothing
(ADR 0008). Fails without throwing when <code class="paramref">link</code> does not address an open Link or
the send itself fails.

 [Shutdown\(LinkId\)](ReactorSockets.RawReactor.Shutdown.md\#ReactorSockets\_RawReactor\_Shutdown\_ReactorSockets\_LinkId\_)

<code>Shutdown</code> is stream-only and illegal on a datagram or listening Link. Sends this Link's
own FIN, closing the send half while the Link stays open for reading — this is what lets a
consumer build a request-response protocol that signals end-of-request by closing the write
half, and it is a one-way door: a further [`RawReactor.Send(LinkId, ReadOnlySpan<byte>)`](ReactorSockets.RawReactor.Send.md#ReactorSockets_RawReactor_Send_ReactorSockets_LinkId_System_ReadOnlySpan_System_Byte__) on this Link fails afterwards.
Fails without throwing (ADR 0008), naming whichever applies: <code class="paramref">link</code> does not
address an open Link, or it does and that Link cannot be shut down. The open check comes first,
for the reason on [`RawReactor.Send(LinkId, ReadOnlySpan<byte>)`](ReactorSockets.RawReactor.Send.md#ReactorSockets_RawReactor_Send_ReactorSockets_LinkId_System_ReadOnlySpan_System_Byte__).

