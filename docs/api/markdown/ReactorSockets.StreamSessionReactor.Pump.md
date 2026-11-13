# <a id="ReactorSockets_StreamSessionReactor_Pump"></a> Method Pump

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionReactor_Pump"></a> Pump\(\)

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
heartbeat, unconditionally and regardless of whether anything was ever received from it. Every
accepted connection still waiting for its first handshake record once its deadline has passed is
then closed (`ExpirePendingAdmissions`). The same scan closes with
[`DisconnectReason.TransportFailed`](ReactorSockets.DisconnectReason.md) every Peer this Reactor dialled that has not
established one [`StreamSessionOptions.Linger`](ReactorSockets.StreamSessionOptions.Linger.md#ReactorSockets_StreamSessionOptions_Linger) after [`StreamSessionReactor.Connect(IPEndPoint)`](ReactorSockets.StreamSessionReactor.Connect.md#ReactorSockets_StreamSessionReactor_Connect_System_Net_IPEndPoint_). Once
idle timers may have closed an Interrupted Peer on Linger expiry, every Interrupted Peer this
Reactor itself dialled is given its re-dial attempt for this Frame
(`ProcessRedials`) — a schedule derived from the observed Pump interval just
recorded above and the effective Linger, never a separate attempt-count knob, so a Peer whose
Linger closed it this same Frame is never also given one. Throws
[`InvalidOperationException`](https://learn.microsoft.com/dotnet/api/system.invalidoperationexception) if called from any thread other than the one that
constructed this Reactor, or from inside a Pump already in progress.

```csharp
public void Pump()
```

