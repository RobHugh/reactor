# <a id="ReactorSockets_DatagramSessionReactor_Close_ReactorSockets_PeerId_"></a> Method Close

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionReactor_Close_ReactorSockets_PeerId_"></a> Close\(PeerId\)

Ends the Session with <code class="paramref">peer</code>. On an Established, non-Interrupted Peer this queues a
Goodbye in the Peer's <code>ReliableOrdered</code> Space and puts the Peer in Closing: later
[`DatagramSessionReactor.Send(PeerId, DeliveryMode, ReadOnlySpan<byte>)`](ReactorSockets.DatagramSessionReactor.Send.md#ReactorSockets_DatagramSessionReactor_Send_ReactorSockets_PeerId_ReactorSockets_DeliveryMode_System_ReadOnlySpan_System_Byte__) calls return [`SendOutcome.Closed`](ReactorSockets.SendOutcome.md) and no
[`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md) is raised yet. On any other Peer (dialling or Interrupted)
it queues a [`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md) carrying [`DisconnectReason.LocalClose`](ReactorSockets.DisconnectReason.md)
at once. Idempotent: a stale [`PeerId`](ReactorSockets.PeerId.md) is a no-op, because a Peer closing under a caller
is an ordinary race rather than a programmer error, and a second call on a Closing Peer queues no
second Goodbye.

```csharp
public void Close(PeerId peer)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

### Remarks

A Closing Peer ends with [`DisconnectReason.LocalClose`](ReactorSockets.DisconnectReason.md) once the Goodbye and every
<code>ReliableOrdered</code> record before it are acknowledged, or [`DisconnectReason.LingerExpired`](ReactorSockets.DisconnectReason.md) once its effective Linger has elapsed since this
call with the Goodbye still unacknowledged. It is never Interrupted and never sends a Resume.
When both ends Close at once, each ends with [`DisconnectReason.LocalClose`](ReactorSockets.DisconnectReason.md): a Closing Peer
that receives the remote's Goodbye acknowledges it and raises neither
[`SessionEventKind.PeerClosing`](ReactorSockets.SessionEventKind.md) nor [`DisconnectReason.PeerGoodbye`](ReactorSockets.DisconnectReason.md).
Accepted gap: the Goodbye is ordered only within <code>ReliableOrdered</code>, so a
<code>ReliableUnordered</code> Message sent before this call can still arrive after the Goodbye, and
the remote, already closed or Closing, does not deliver it.

