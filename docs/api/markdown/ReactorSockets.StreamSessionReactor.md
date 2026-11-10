# <a id="ReactorSockets_StreamSessionReactor"></a> Class StreamSessionReactor

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

The stream Session profile (ADR 0010): Sessions established over TCP, addressed as Peers,
delivering <code>u32</code> length-prefixed Messages rather than the byte runs the transport gives.
Construction validates [`StreamSessionOptions`](ReactorSockets.StreamSessionOptions.md) and its allocation bill and binds the
declared endpoint set; each Peer is carried through the handshake to an established Session. Each
established Peer reassembles Messages in its own persistent receive buffer and delivers each whole
one as a [`SessionEventKind.MessageReceived`](ReactorSockets.SessionEventKind.md) event copied into the shared frame arena.

```csharp
public sealed class StreamSessionReactor : IDisposable
```

#### Inheritance

[object](https://learn.microsoft.com/dotnet/api/system.object) ← 
[StreamSessionReactor](ReactorSockets.StreamSessionReactor.md)

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
There is no cookie, no ack, no sequence space, no fragment and no RTO in this profile: TCP already
provides what those were built for. The datagram Session protocol is deliberately not carried over
TCP, because that would put retransmission on top of retransmission.
</p>
<p>
<b>Send timing (ADR 0011).</b> Nagle (<code>TCP_NODELAY</code>) is off, unconditionally, on every stream
Session socket — it is not a knob on [`StreamSessionOptions`](ReactorSockets.StreamSessionOptions.md) and cannot be turned back
on. In its place, every [`StreamSessionReactor.Send(PeerId, ReadOnlySpan<byte>)`](ReactorSockets.StreamSessionReactor.Send.md#ReactorSockets_StreamSessionReactor_Send_ReactorSockets_PeerId_System_ReadOnlySpan_System_Byte__) queued since the last [`StreamSessionReactor.Pump()`](ReactorSockets.StreamSessionReactor.Pump.md#ReactorSockets_StreamSessionReactor_Pump) is written to
the wire together, once, by that [`StreamSessionReactor.Pump()`](ReactorSockets.StreamSessionReactor.Pump.md#ReactorSockets_StreamSessionReactor_Pump) call: batching we do ourselves, on our own
schedule, rather than the connection's delayed acknowledgements deciding when a small Message may
leave. That is a better version of what Nagle exists for, without Nagle's cost — a small Message
waiting up to a round trip behind outstanding data — so a consumer measuring latency needs to know
which of the two is doing the batching: theirs, by calling [`StreamSessionReactor.Pump()`](ReactorSockets.StreamSessionReactor.Pump.md#ReactorSockets_StreamSessionReactor_Pump) less often, or ours,
always.
</p>

## Constructors

 [StreamSessionReactor\(StreamSessionOptions\)](ReactorSockets.StreamSessionReactor.\-ctor.md\#ReactorSockets\_StreamSessionReactor\_\_ctor\_ReactorSockets\_StreamSessionOptions\_)

Validates <code class="paramref">options</code> — every knob and cross-check, then the frame arena's
floor — before anything is allocated or bound, then binds every declared endpoint in the order
supplied. A bind that fails throws here, naming the endpoint: binding is part of what this
Reactor is, so there is no half-built Reactor to hand back.

## Properties

 [BoundEndpoints](ReactorSockets.StreamSessionReactor.BoundEndpoints.md\#ReactorSockets\_StreamSessionReactor\_BoundEndpoints)

The endpoints this Reactor is bound to right now, in the order they were declared. A snapshot:
it answers what is bound, and there is no member that binds another — there is no rebinding
(ADR 0014), and unlike the datagram profile a stream dial never binds a wildcard socket of its
own, since TCP's own connect already binds an ephemeral local port implicitly.

 [Counters](ReactorSockets.StreamSessionReactor.Counters.md\#ReactorSockets\_StreamSessionReactor\_Counters)

A point-in-time snapshot of this Reactor's own counters (ADR 0013). No reset.

## Methods

 [Close\(PeerId\)](ReactorSockets.StreamSessionReactor.Close.md\#ReactorSockets\_StreamSessionReactor\_Close\_ReactorSockets\_PeerId\_)

Announces this end is ending the Session with <code class="paramref">peer</code> on purpose: attempts to
append a Goodbye control record to whatever this Peer's retention region already holds, so it
is the last thing flushed and everything queued before it is delivered first, then forbids any
further [`StreamSessionReactor.Send(PeerId, ReadOnlySpan<byte>)`](ReactorSockets.StreamSessionReactor.Send.md#ReactorSockets_StreamSessionReactor_Send_ReactorSockets_PeerId_System_ReadOnlySpan_System_Byte__) to it. If retention has no room for it yet, the attempt is retried
on every later Pump (`RetryGoodbyeAppends`) until it commits — [`StreamSessionReactor.Close(PeerId)`](ReactorSockets.StreamSessionReactor.Close.md#ReactorSockets_StreamSessionReactor_Close_ReactorSockets_PeerId_)
must never leave the far end open indefinitely because the Goodbye it promised was silently
dropped, and this end never closes itself locally until its own Goodbye has actually been
appended and flushed. The [`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md) carrying
[`DisconnectReason.LocalClose`](ReactorSockets.DisconnectReason.md) is queued once that flush fully drains — immediately,
on a later Pump, if nothing was queued when [`StreamSessionReactor.Close(PeerId)`](ReactorSockets.StreamSessionReactor.Close.md#ReactorSockets_StreamSessionReactor_Close_ReactorSockets_PeerId_) was called, or after however
many Pumps a short write (or a retention append retry) takes to carry everything already
retained plus the Goodbye itself. Idempotent: a stale [`PeerId`](ReactorSockets.PeerId.md), or a Peer already
closing, is a no-op — a Peer closing under a caller, or a caller that closes the same Peer
twice, is an ordinary race rather than a programmer error.

 [Connect\(IPEndPoint\)](ReactorSockets.StreamSessionReactor.Connect.md\#ReactorSockets\_StreamSessionReactor\_Connect\_System\_Net\_IPEndPoint\_)

Begins a Session with <code class="paramref">destination</code>, returning the [`PeerId`](ReactorSockets.PeerId.md) that
will address it. The Peer occupies a capacity Slot from this moment, exactly as a dialling Link
does in [`RawReactor`](ReactorSockets.RawReactor.md): the handshake is work this Reactor is holding, and pretending
otherwise would let a consumer dial past capacity and discover it only on the far side of a
round trip. [`SessionEventKind.PeerConnected`](ReactorSockets.SessionEventKind.md) or [`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md)
resolves it on a later Pump.

 [CountersOf\(PeerId\)](ReactorSockets.StreamSessionReactor.CountersOf.md\#ReactorSockets\_StreamSessionReactor\_CountersOf\_ReactorSockets\_PeerId\_)

A point-in-time snapshot of one Peer's counters (ADR 0013). Throws if the Peer is not open.
Mirrors [`RawReactor.CountersOf(LinkId)`](ReactorSockets.RawReactor.CountersOf.md#ReactorSockets_RawReactor_CountersOf_ReactorSockets_LinkId_)'s shape for the Raw profile.

 [Dispose\(\)](ReactorSockets.StreamSessionReactor.Dispose.md\#ReactorSockets\_StreamSessionReactor\_Dispose)

Releases every socket this Reactor bound or accepted. A Reactor owns every socket it bound, so
a consumer that drops one without disposing it leaks those sockets until each
[`Socket`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socket)'s finalizer runs. No [`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md) events are
queued: nothing can drain a disposed Reactor. Idempotent; every other member throws
[`ObjectDisposedException`](https://learn.microsoft.com/dotnet/api/system.objectdisposedexception) afterwards.

 [Drain\(\)](ReactorSockets.StreamSessionReactor.Drain.md\#ReactorSockets\_StreamSessionReactor\_Drain)

Drains every event produced this Frame — since the last [`StreamSessionReactor.Pump()`](ReactorSockets.StreamSessionReactor.Pump.md#ReactorSockets_StreamSessionReactor_Pump) — in the order it was
produced.

 [Pump\(\)](ReactorSockets.StreamSessionReactor.Pump.md\#ReactorSockets\_StreamSessionReactor\_Pump)

One synchronous advance: records this call's arrival against the last one
(`ObservedPumpInterval`) before anything else, then wipes the previous Frame's
events — which is also the moment a Peer still [`SessionEventKind.PeerClosing`](ReactorSockets.SessionEventKind.md) only
because the last Frame's Messages had not been drained past yet becomes eligible to close
(`RetryPeersPendingClose`) — then admits ready connections, advances every in-flight
dial and reads whatever bound socket is ready right now, running the handshake for everything
that arrived. Every Peer whose received count advanced somewhere in that work — this Frame's own
reads, or a held-back Message finally delivered by `RetryPeersWithBufferedMessages`
— then has its delivery counter emitted exactly once (`EmitDeliveryCounters`),
before every Peer's queued retention bytes are flushed exactly once
(`FlushPendingRetentionWrites`) regardless of how many [`StreamSessionReactor.Send(PeerId, ReadOnlySpan<byte>)`](ReactorSockets.StreamSessionReactor.Send.md#ReactorSockets_StreamSessionReactor_Send_ReactorSockets_PeerId_System_ReadOnlySpan_System_Byte__) calls
queued them. Finally, every established Peer's two independent idle timers are enforced in one
scan (`EnforceIdleTimers`): a Peer from which nothing has been received for
`RemoteSendIdleInterval` ×
[`StreamSessionOptions.ResolvedMissedHeartbeats`](ReactorSockets.StreamSessionOptions.ResolvedMissedHeartbeats.md#ReactorSockets_StreamSessionOptions_ResolvedMissedHeartbeats) moves to
[`SessionEventKind.PeerInterrupted`](ReactorSockets.SessionEventKind.md), an already-Interrupted Peer instead closes with
[`DisconnectReason.LingerExpired`](ReactorSockets.DisconnectReason.md) once its effective Linger has elapsed since the
interruption, and every surviving Peer that has written nothing to the wire — including whatever
this Frame's own flush just carried — for at least
[`StreamSessionOptions.SendIdleInterval`](ReactorSockets.StreamSessionOptions.SendIdleInterval.md#ReactorSockets_StreamSessionOptions_SendIdleInterval) sends the same delivery-counter record as a
heartbeat, unconditionally and regardless of whether anything was ever received from it. Once
idle timers may have closed an Interrupted Peer on Linger expiry, every Interrupted Peer this
Reactor itself dialled is given its re-dial attempt for this Frame
(`ProcessRedials`) — a schedule derived from the observed Pump interval just
recorded above and the effective Linger, never a separate attempt-count knob, so a Peer whose
Linger closed it this same Frame is never also given one. Throws
[`InvalidOperationException`](https://learn.microsoft.com/dotnet/api/system.invalidoperationexception) if called from any thread other than the one that
constructed this Reactor, or from inside a Pump already in progress.

 [Send\(PeerId, ReadOnlySpan<byte\>\)](ReactorSockets.StreamSessionReactor.Send.md\#ReactorSockets\_StreamSessionReactor\_Send\_ReactorSockets\_PeerId\_System\_ReadOnlySpan\_System\_Byte\_\_)

Frames <code class="paramref">message</code> as a <code>u32</code> big-endian length prefix followed by its
bytes (`MessageFramingCodec`, ADR 0010) and appends the framed bytes to
<code class="paramref">peer</code>'s retention region. Nothing is written to the socket here, ever (write
batching) — a successful append only sets this Peer's pending-write marker
(`HasPendingRetentionWrite`), which the next [`StreamSessionReactor.Pump()`](ReactorSockets.StreamSessionReactor.Pump.md#ReactorSockets_StreamSessionReactor_Pump)'s explicit
flush step (`FlushPendingRetentionWrites`) drains in a single write carrying
everything queued, including whatever was already sitting there. A Send with no following Pump
call puts nothing on the wire. Refuses once a Goodbye has passed in either direction — this
end's own (`LocalGoodbyeSent`) or the far end's arrived and this Peer is
already [`SessionEventKind.PeerClosing`](ReactorSockets.SessionEventKind.md) or on its way to
[`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md) (`PendingCloseReasonAfterRetentionDrains`)
— since a Message queued after a Goodbye is not one the far end has any business receiving. Also
refuses a Peer whose handshake has not reached `Established` with
[`SendOutcome.NotEstablished`](ReactorSockets.SendOutcome.md): flushing that Message would write to a socket whose
connect has not resolved, closing the Peer with [`DisconnectReason.TransportFailed`](ReactorSockets.DisconnectReason.md),
or send it before the far end's handshake record has been read.

