# <a id="ReactorSockets_StreamSessionReactor_Close_ReactorSockets_PeerId_"></a> Method Close

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionReactor_Close_ReactorSockets_PeerId_"></a> Close\(PeerId\)

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

```csharp
public void Close(PeerId peer)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

