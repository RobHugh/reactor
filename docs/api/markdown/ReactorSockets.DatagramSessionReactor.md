# <a id="ReactorSockets_DatagramSessionReactor"></a> Class DatagramSessionReactor

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

The datagram Session profile (ADR 0009): Sessions established over UDP, addressed as Peers, driven
by a single synchronous [`DatagramSessionReactor.Pump()`](ReactorSockets.DatagramSessionReactor.Pump.md#ReactorSockets_DatagramSessionReactor_Pump) from the consumer's own loop. It carries the handshake
and everything that guards it — the stateless cookie, the amplification bound, version and
capacity refusals, session identity and demultiplexing — and the data plane behind it: packet
records in four sequence Spaces, one per [`DeliveryMode`](ReactorSockets.DeliveryMode.md), fragmentation of reliable
Messages, acknowledgement and retransmission, heartbeats, Goodbye, interruption and Resume.

```csharp
public sealed class DatagramSessionReactor : IDisposable
```

#### Inheritance

[object](https://learn.microsoft.com/dotnet/api/system.object) ← 
[DatagramSessionReactor](ReactorSockets.DatagramSessionReactor.md)

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
<b>Endpoints are declared, not registered.</b> Unlike [`RawReactor`](ReactorSockets.RawReactor.md), which hands back a
[`LinkId`](ReactorSockets.LinkId.md) for every socket a consumer registers, this Reactor takes its endpoints on
[`DatagramSessionOptions.Endpoints`](ReactorSockets.DatagramSessionOptions.Endpoints.md#ReactorSockets_DatagramSessionOptions_Endpoints) at construction and never rebinds (ADR 0014).
Binding returns no identity because there is nothing to address: a consumer addresses Peers, and a
Peer is not a socket. A Reactor that declares no endpoints binds nothing until its first
[`DatagramSessionReactor.Connect(IPEndPoint)`](ReactorSockets.DatagramSessionReactor.Connect.md#ReactorSockets_DatagramSessionReactor_Connect_System_Net_IPEndPoint_), which binds one wildcard, port-zero socket in the destination's address
family and appends it to [`DatagramSessionReactor.BoundEndpoints`](ReactorSockets.DatagramSessionReactor.BoundEndpoints.md#ReactorSockets_DatagramSessionReactor_BoundEndpoints).
</p>
<p>
<b>Capacity counts Peers.</b> The bound sockets are not share-holders and hold no read share
(ADR 0014): a Peer's Messages arrive through whichever socket happens to carry them, so charging
the arena per socket would give one Peer on a single-socket Reactor the whole arena and sixty-four
Peers a sixty-fourth each — exactly inverted. The read share is the arena divided by
[`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity), and it belongs to the Peer.
</p>
<p>
<b>What a stranger can make this Reactor do.</b> Very little, and that is the design. Anything an
unauthenticated remote can generate at will goes to the Diagnostic Sink and never to the event
stream, so a stranger cannot write into the consumer's protocol. Every reply to an unverified
sender is exactly one control datagram of
`ControlDatagramSize` bytes, answering exactly one control
datagram of the same size, so bytes sent never exceed bytes received and this Reactor cannot be
used to amplify a flood at a spoofed third party. A bare Hello is answered with a Cookie whatever
it claimed — never a Refusal — so nothing about this Reactor's version or occupancy is learnable
without first proving the ability to receive at the address claimed.
</p>
<p>
<b>The wire session id is not an authenticator.</b> It is a receiver-assigned random 32-bit
demultiplexing token: unguessable enough that an off-path attacker cannot address a Session it was
never introduced to, and nothing more. This library does not encrypt, does not authenticate the
data plane, and an on-path attacker who can read a datagram can forge one. Consumers needing
confidentiality or integrity must layer it themselves.
</p>

## Constructors

 [DatagramSessionReactor\(DatagramSessionOptions\)](ReactorSockets.DatagramSessionReactor.\-ctor.md\#ReactorSockets\_DatagramSessionReactor\_\_ctor\_ReactorSockets\_DatagramSessionOptions\_)

Constructs a Reactor bound to the calling thread (ADR 0002) — every future mutating call must
come from this same thread or it throws — and binds every declared endpoint in the order
supplied. A bind that fails throws here, naming the endpoint: binding is part of what this
Reactor is, so there is no half-bound Reactor to hand back and no
[`DiagnosticId`](ReactorSockets.DiagnosticId.md) member for the failure.

## Properties

 [BoundEndpoints](ReactorSockets.DatagramSessionReactor.BoundEndpoints.md\#ReactorSockets\_DatagramSessionReactor\_BoundEndpoints)

The endpoints this Reactor is bound to right now, in the order they were declared, with any
socket bound by a dial appended. A snapshot: it answers what is bound, and there is no member
that binds another.

 [Counters](ReactorSockets.DatagramSessionReactor.Counters.md\#ReactorSockets\_DatagramSessionReactor\_Counters)

A point-in-time snapshot of this Reactor's own counters (ADR 0013). No reset.

## Methods

 [Close\(PeerId\)](ReactorSockets.DatagramSessionReactor.Close.md\#ReactorSockets\_DatagramSessionReactor\_Close\_ReactorSockets\_PeerId\_)

Ends the Session with <code class="paramref">peer</code>. On an Established, non-Interrupted Peer this queues a
Goodbye in the Peer's <code>ReliableOrdered</code> Space and puts the Peer in Closing: later
[`DatagramSessionReactor.Send(PeerId, DeliveryMode, ReadOnlySpan<byte>)`](ReactorSockets.DatagramSessionReactor.Send.md#ReactorSockets_DatagramSessionReactor_Send_ReactorSockets_PeerId_ReactorSockets_DeliveryMode_System_ReadOnlySpan_System_Byte__) calls return [`SendOutcome.Closed`](ReactorSockets.SendOutcome.md) and no
[`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md) is raised yet. On any other Peer (dialling or Interrupted)
it queues a [`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md) carrying [`DisconnectReason.LocalClose`](ReactorSockets.DisconnectReason.md)
at once. Idempotent: a stale [`PeerId`](ReactorSockets.PeerId.md) is a no-op, because a Peer closing under a caller
is an ordinary race rather than a programmer error, and a second call on a Closing Peer queues no
second Goodbye.

 [Connect\(IPEndPoint\)](ReactorSockets.DatagramSessionReactor.Connect.md\#ReactorSockets\_DatagramSessionReactor\_Connect\_System\_Net\_IPEndPoint\_)

Begins a Session with <code class="paramref">destination</code>, returning the [`PeerId`](ReactorSockets.PeerId.md) that
will address it. The Peer occupies a capacity Slot from this moment, exactly as a dialling Link
does in [`RawReactor`](ReactorSockets.RawReactor.md): the handshake is work this Reactor is holding, and pretending
otherwise would let a consumer dial past capacity and discover it only on the far side of a
round trip. [`SessionEventKind.PeerConnected`](ReactorSockets.SessionEventKind.md) or
[`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md) resolves it on a later Pump.

 [CountersOf\(PeerId\)](ReactorSockets.DatagramSessionReactor.CountersOf.md\#ReactorSockets\_DatagramSessionReactor\_CountersOf\_ReactorSockets\_PeerId\_)

A point-in-time snapshot of one Peer's counters (ADR 0013). Throws if the Peer is not open,
matching [`StreamSessionReactor.CountersOf(PeerId)`](ReactorSockets.StreamSessionReactor.CountersOf.md#ReactorSockets_StreamSessionReactor_CountersOf_ReactorSockets_PeerId_)'s shape for the stream profile —
the datagram profile had no per-Peer counter reader until this member.
[`PeerCounters.OutOfWindowPackets`](ReactorSockets.PeerCounters.OutOfWindowPackets.md#ReactorSockets_PeerCounters_OutOfWindowPackets) and [`PeerCounters.DuplicatePackets`](ReactorSockets.PeerCounters.DuplicatePackets.md#ReactorSockets_PeerCounters_DuplicatePackets)
now reflect what the four per-Space receive windows have refused;
[`PeerCounters.UnreliableLossPackets`](ReactorSockets.PeerCounters.UnreliableLossPackets.md#ReactorSockets_PeerCounters_UnreliableLossPackets) reflects gaps either unreliable Space's window
has slid past. [`PeerCounters.MessagesReceived`](ReactorSockets.PeerCounters.MessagesReceived.md#ReactorSockets_PeerCounters_MessagesReceived) counts whole Messages handed to the
frame arena for delivery, and not a Message the arena refused.
[`PeerCounters.Resumes`](ReactorSockets.PeerCounters.Resumes.md#ReactorSockets_PeerCounters_Resumes) counts the Peer's resumes: once per
[`SessionEventKind.PeerResumed`](ReactorSockets.SessionEventKind.md) raised for it, on the accepting end when a matching
Resume, or a well-formed data Datagram from the address it sends to, moves it out of Interrupted and
on the dialling end when it hears its session again after sending a Resume, and never reset by one
(ADR 0013).
[`PeerCounters.RetentionHighWaterBytes`](ReactorSockets.PeerCounters.RetentionHighWaterBytes.md#ReactorSockets_PeerCounters_RetentionHighWaterBytes) reads the true per-Peer figure — summed
across both reliable Spaces — from `_retention`.

 [Dispose\(\)](ReactorSockets.DatagramSessionReactor.Dispose.md\#ReactorSockets\_DatagramSessionReactor\_Dispose)

Releases every socket this Reactor bound. A Reactor owns every socket it bound, so a consumer
that drops one without disposing it leaks those sockets until each [`Socket`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socket)'s
finalizer runs. No [`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md) events are queued: nothing can
drain a disposed Reactor. Idempotent; every other member throws
[`ObjectDisposedException`](https://learn.microsoft.com/dotnet/api/system.objectdisposedexception) afterwards.

 [Drain\(\)](ReactorSockets.DatagramSessionReactor.Drain.md\#ReactorSockets\_DatagramSessionReactor\_Drain)

Drains every event produced this Frame — since the last [`DatagramSessionReactor.Pump()`](ReactorSockets.DatagramSessionReactor.Pump.md#ReactorSockets_DatagramSessionReactor_Pump) — in the order it was
produced.

 [Pump\(\)](ReactorSockets.DatagramSessionReactor.Pump.md\#ReactorSockets\_DatagramSessionReactor\_Pump)

One synchronous advance: records this call's arrival against the last one
(`ObservedPumpInterval`) before anything else, then wipes the previous Frame's
events and arena, then reads whatever bound socket is ready right now, running the handshake for
everything that arrived. Throws [`InvalidOperationException`](https://learn.microsoft.com/dotnet/api/system.invalidoperationexception) if called from any
thread other than the one that constructed this Reactor, or from inside a Pump already in
progress.

 [Send\(PeerId, DeliveryMode, ReadOnlySpan<byte\>\)](ReactorSockets.DatagramSessionReactor.Send.md\#ReactorSockets\_DatagramSessionReactor\_Send\_ReactorSockets\_PeerId\_ReactorSockets\_DeliveryMode\_System\_ReadOnlySpan\_System\_Byte\_\_)

Sends <code class="paramref">message</code> to <code class="paramref">peer</code> under <code class="paramref">mode</code> (ADR
0009). A Message too wide for its mode to carry is refused before any work is done on it: no
sequence is allocated and no retention is taken, so a rejected Send leaves no trace on the
Session. How wide is too wide depends on the mode and is decided by
`Resolve`, the one place the rule is stated: an unreliable mode is capped
at a single Datagram and refused above it as [`SendOutcome.MessageTooLarge`](ReactorSockets.SendOutcome.md), a reliable
mode fragments, and every mode is refused above a second cap as
[`SendOutcome.RemoteMessageTooLarge`](ReactorSockets.SendOutcome.md) (ADR 0004). That second cap is
<code class="paramref">peer</code>'s own announced receive limit, captured on its Slot at handshake, not this
Reactor's own [`DatagramSessionOptions.MaxMessageSize`](ReactorSockets.DatagramSessionOptions.MaxMessageSize.md#ReactorSockets_DatagramSessionOptions_MaxMessageSize): <code>Send</code> is therefore not
purely local to this Reactor's configuration, because the same payload can be accepted for one
Peer and refused for another whose handshake announced a smaller limit. That gate sits behind
the <code class="paramref">peer</code> lookup rather than in front of it, because both caps it measures
against, fragment size and remote max message size alike, are the Peer's own captured values; a
Peer this Reactor is not holding open returns [`SendOutcome.Closed`](ReactorSockets.SendOutcome.md) whatever the
Message's width. Past that gate, a reliable mode allocates the next sequence in its own Space
and retains the Message whole there, returning [`SendOutcome.RetentionExhausted`](ReactorSockets.SendOutcome.md) once
that Space has no slot left — and that refusal spends nothing either: the sequence is claimed
only once retention has committed, so a refused reliable Send leaves the Space's next sequence
exactly where it was, rather than opening a gap the receiver's window would wait on for a record
that is never sent; [`DeliveryMode.Unreliable`](ReactorSockets.DeliveryMode.md) and
[`DeliveryMode.UnreliableSequenced`](ReactorSockets.DeliveryMode.md) still allocate a sequence — every mode gets one,
the same as every other (`OutboundSequenceSpaces`) — but retain nothing, because
nothing unreliable is ever retransmitted. A Message this call accepts is queued on
<code class="paramref">peer</code>'s Slot (`PendingSends`) and leaves as one packet
record inside the next [`DatagramSessionReactor.Pump()`](ReactorSockets.DatagramSessionReactor.Pump.md#ReactorSockets_DatagramSessionReactor_Pump)'s single coalesced Datagram to that Peer, in Send
order (`FlushOutboundDatagrams`) — nothing reaches the wire before that Pump runs,
and a <code>Send</code> with no following Pump puts nothing on the wire at all. That next Pump is only
the first one this record is <em>eligible</em> to leave in: if this Peer already has more queued
than one Datagram holds, a record queued behind the boundary waits for however many further
Pumps it takes to reach the front, still in the order it was Sent.

