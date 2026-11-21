# <a id="ReactorSockets_StreamSessionReactor_Send_ReactorSockets_PeerId_System_ReadOnlySpan_System_Byte__"></a> Method Send

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionReactor_Send_ReactorSockets_PeerId_System_ReadOnlySpan_System_Byte__"></a> Send\(PeerId, ReadOnlySpan<byte\>\)

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
or send it before the far end's handshake record has been read. Once the Peer is established,
refuses a Message longer than the <code>MaxMessageSize</code> it announced in its Hello
(`RemoteMaxMessageSize`) with [`SendOutcome.RemoteMessageTooLarge`](ReactorSockets.SendOutcome.md),
copying nothing: the far end would close the Session with
[`DisconnectReason.ProtocolViolation`](ReactorSockets.DisconnectReason.md) on its length prefix.

```csharp
public SendOutcome Send(PeerId peer, ReadOnlySpan<byte> message)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

The Peer to send to. Throws if not open, matching [`StreamSessionReactor.CountersOf(PeerId)`](ReactorSockets.StreamSessionReactor.CountersOf.md#ReactorSockets_StreamSessionReactor_CountersOf_ReactorSockets_PeerId_).

`message` [ReadOnlySpan](https://learn.microsoft.com/dotnet/api/system.readonlyspan\-1)<[byte](https://learn.microsoft.com/dotnet/api/system.byte)\>

The Message payload, unframed. Throws if empty or longer than this Reactor's own
[`StreamSessionOptions.MaxMessageSize`](ReactorSockets.StreamSessionOptions.MaxMessageSize.md#ReactorSockets_StreamSessionOptions_MaxMessageSize). A Message within that limit but longer than the
Peer's announced limit returns [`SendOutcome.RemoteMessageTooLarge`](ReactorSockets.SendOutcome.md), so this never frames
a Message the far end would reject.

### Returns

 [SendOutcome](ReactorSockets.SendOutcome.md)

### Remarks

`IsInterrupted` is deliberately not one of the refusal conditions above
(ADR 0010's backpressure rule): a Send against an Interrupted Peer is exactly as welcome as one
against a live Peer, because holding retention across the interruption for a possible Resume is
the entire point of not closing the Peer in the first place. The append behaves identically
either way — the difference is only downstream, in `FlushRetention`'s own
<code>IsInterrupted</code> guard, which leaves these bytes queued rather than attempting a dead
socket. Once the ring genuinely has no room, `Exhausted` reports
the same way it would for a live Peer — the ring does not know or care why nothing has drained
it — so an interruption that outlasts the send rate reaches
[`SendOutcome.RetentionExhausted`](ReactorSockets.SendOutcome.md) exactly like a single oversized Message or a
sustained rate would, the third of the three paths ADR 0010 names to it.

