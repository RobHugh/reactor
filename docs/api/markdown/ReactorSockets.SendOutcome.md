# <a id="ReactorSockets_SendOutcome"></a> Enum SendOutcome

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

The outcome of one [`StreamSessionReactor.Send(PeerId, ReadOnlySpan<byte>)`](ReactorSockets.StreamSessionReactor.Send.md#ReactorSockets_StreamSessionReactor_Send_ReactorSockets_PeerId_System_ReadOnlySpan_System_Byte__) call. A bare
enum rather than a result struct (unlike [`SendResult`](ReactorSockets.SendResult.md)): a Session-level send has no
partial-completion quantity worth reporting back — the framed Message either fit in the Peer's
retention region whole, or it did not, so the outcome is the whole story.

```csharp
public enum SendOutcome
```

## Fields

`Sent = 0` 

The framed Message was copied whole into the Peer's retention region.



`RetentionExhausted = 1` 

The Peer's retention region had no room for the framed Message. Nothing was copied and nothing
about the Session changed — the caller may retry once bytes have been released, or otherwise.



`Closed = 2` 

A Goodbye for this Peer has already passed in one direction or the other — this end sent its
own (<code>Close</code>), or the far end's arrived and the Peer is <code>Closing</code>. Either way the
Session is ending and nothing was copied: a Message queued after a Goodbye would have to either
jump it in retention or follow it, and neither is a Message the far end has any business
receiving.



`MessageTooLarge = 3` 

The Message is wider than one Datagram can carry, and the Space it was sent on cannot
fragment it. Nothing was copied. Deliberately its own value rather than folded into
[`SendOutcome.RetentionExhausted`](ReactorSockets.SendOutcome.md): retention exhaustion is backpressure the caller can retry
once bytes are released, while an oversized Message will never fit however long the caller
waits — a consumer that cannot tell the two apart cannot tell "retry later" from "split it or
use a reliable mode".



`RemoteMessageTooLarge = 4` 

A Message, in any delivery mode, exceeds the <code>MaxMessageSize</code> the remote end announced during
the handshake, or a reliable Message needs more Fragments than the receive window the remote end
announced (ADR 0024). Nothing was copied, and the caller may retry immediately with a smaller payload.
An unreliable Message over both limits reports [`SendOutcome.MessageTooLarge`](ReactorSockets.SendOutcome.md) instead. Distinct
from [`SendOutcome.MessageTooLarge`](ReactorSockets.SendOutcome.md) purely by whose limit was exceeded: that value means a local
configuration fact the caller's own settings could have predicted (the single-Datagram cap an
unreliable mode cannot exceed); this one means the far end's announced receive limit, which the
caller has no way to know in advance.



`NotEstablished = 5` 

The Peer's handshake has not completed: a dial is still connecting, or waiting for the far end's
handshake record. Nothing was copied and nothing about the Session changed. Wait for
[`SessionEventKind.PeerConnected`](ReactorSockets.SessionEventKind.md) for this Peer before Sending. Refusing here rather
than queueing keeps a Message from reaching a socket whose connect has not resolved, or going
out before this end knows the far end's version and announced <code>MaxMessageSize</code>.



## Remarks

<p>
Deliberately its own type rather than folded into [`DisconnectReason`](ReactorSockets.DisconnectReason.md): exhausting
retention does not end the Session, so it belongs on the return of the call that can produce it
rather than mixed into the roster of reasons a Peer is gone.
</p>
<p>
Explicit values throughout, the same house style as [`DisconnectReason`](ReactorSockets.DisconnectReason.md): a member is
added at the end with the next free number, never inserted, so a value a consumer has already
persisted keeps its meaning across a library upgrade.
</p>

