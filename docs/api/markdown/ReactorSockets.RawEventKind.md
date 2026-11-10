# <a id="ReactorSockets_RawEventKind"></a> Enum RawEventKind

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

Which kind of [`RawEvent`](ReactorSockets.RawEvent.md) a drained event is (ADR 0008). [`RawEventKind.Accepted`](ReactorSockets.RawEventKind.md) comes
from a listening Link, [`RawEventKind.Connected`](ReactorSockets.RawEventKind.md) from dialling, [`RawEventKind.SegmentReceived`](ReactorSockets.RawEventKind.md) and
[`RawEventKind.EndOfStream`](ReactorSockets.RawEventKind.md) from stream reads, and [`RawEventKind.Writable`](ReactorSockets.RawEventKind.md) from partial sends.

```csharp
public enum RawEventKind
```

## Fields

`DatagramReceived = 0` 

A datagram arrived on a bound UDP Link. [`RawEvent.Link`](ReactorSockets.RawEvent.Link.md#ReactorSockets_RawEvent_Link), [`RawEvent.Source`](ReactorSockets.RawEvent.Source.md#ReactorSockets_RawEvent_Source)
and [`RawEvent.Payload`](ReactorSockets.RawEvent.Payload.md#ReactorSockets_RawEvent_Payload) are populated.



`Closed = 1` 

A Link closed. [`RawEvent.Link`](ReactorSockets.RawEvent.Link.md#ReactorSockets_RawEvent_Link) and [`RawEvent.Reason`](ReactorSockets.RawEvent.Reason.md#ReactorSockets_RawEvent_Reason) are populated.



`Accepted = 2` 

A connection was accepted on a listening Link, inside the Pump that found it ready.
[`RawEvent.ListeningLink`](ReactorSockets.RawEvent.ListeningLink.md#ReactorSockets_RawEvent_ListeningLink) names the listening Link that fired; [`RawEvent.Link`](ReactorSockets.RawEvent.Link.md#ReactorSockets_RawEvent_Link)
names the new connection, already registered and consuming a capacity slot of its own.



`Connected = 3` 

An outbound dial completed successfully, inside the Pump that found the connecting socket
writable. [`RawEvent.Link`](ReactorSockets.RawEvent.Link.md#ReactorSockets_RawEvent_Link) is populated; a dial that fails instead produces
[`RawEventKind.Closed`](ReactorSockets.RawEventKind.md) with [`CloseReason.Refused`](ReactorSockets.CloseReason.md) or [`CloseReason.Unreachable`](ReactorSockets.CloseReason.md).



`SegmentReceived = 4` 

A run of bytes arrived on a stream Link. [`RawEvent.Link`](ReactorSockets.RawEvent.Link.md#ReactorSockets_RawEvent_Link) and
[`RawEvent.Payload`](ReactorSockets.RawEvent.Payload.md#ReactorSockets_RawEvent_Payload) are populated. Carries no claim of being a whole anything — a
counterparty's single write may be drained as one or several of these, and framing is the
consumer's decision, not this library's.



`EndOfStream = 5` 

The remote end of a stream Link will send nothing further while this Link remains open for
sending — TCP's own half-close, kept separate from [`RawEventKind.Closed`](ReactorSockets.RawEventKind.md) because collapsing them
would decide on the consumer's behalf that their protocol cannot use half-close.
[`RawEvent.Link`](ReactorSockets.RawEvent.Link.md#ReactorSockets_RawEvent_Link) is populated. A consumer that never calls <code>Close</code> after this
holds the Link and its slot until the Reactor is disposed — there is no Linger in Raw to
reclaim it.



`Writable = 6` 

A stream Link that took fewer bytes than offered on a previous <code>Send</code> has become writable
again. [`RawEvent.Link`](ReactorSockets.RawEvent.Link.md#ReactorSockets_RawEvent_Link) is populated. Armed, not level-triggered: exactly one of
these follows a short send, and a Link never over-sent on produces none at all — a reply to
something the consumer did, not a status the Reactor volunteers every Pump a socket happens to
be writable (which is nearly always).



