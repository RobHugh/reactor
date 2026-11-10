# <a id="ReactorSockets_DatagramSessionReactor_Send_ReactorSockets_PeerId_ReactorSockets_DeliveryMode_System_ReadOnlySpan_System_Byte__"></a> Method Send

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionReactor_Send_ReactorSockets_PeerId_ReactorSockets_DeliveryMode_System_ReadOnlySpan_System_Byte__"></a> Send\(PeerId, DeliveryMode, ReadOnlySpan<byte\>\)

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

```csharp
public SendOutcome Send(PeerId peer, DeliveryMode mode, ReadOnlySpan<byte> message)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

The Peer to send to.

`mode` [DeliveryMode](ReactorSockets.DeliveryMode.md)

Which of the four delivery modes carries this Message; see <code>docs/delivery-modes.md</code>.

`message` [ReadOnlySpan](https://learn.microsoft.com/dotnet/api/system.readonlyspan\-1)<[byte](https://learn.microsoft.com/dotnet/api/system.byte)\>

The Message payload, unframed. The bytes are copied before this call returns — into
`_retention` for a reliable mode, into the queued record for an unreliable one — so
the caller owns its buffer again the instant <code>Send</code> returns and may overwrite or reuse it
without changing what is eventually put on the wire. A Message wider than
[`DatagramSessionOptions.ResolvedFragmentSize`](ReactorSockets.DatagramSessionOptions.ResolvedFragmentSize.md#ReactorSockets_DatagramSessionOptions_ResolvedFragmentSize) cannot fit in one Datagram, and for
[`DeliveryMode.Unreliable`](ReactorSockets.DeliveryMode.md) and [`DeliveryMode.UnreliableSequenced`](ReactorSockets.DeliveryMode.md) that cap
is reported back as [`SendOutcome.MessageTooLarge`](ReactorSockets.SendOutcome.md) rather than thrown, and it is
permanent — no option lifts it: you may only fragment what you can retransmit (ADR 0004), because
fragmenting what cannot be repaired multiplies the failure rate by the number of pieces — a
ten-Fragment unreliable Message on a path losing 1% of Datagrams arrives about 90% of the time,
having spent ten Datagrams of bandwidth on a coin flip (<code>docs/delivery-modes.md</code>). A Message
within that cap but wider than <code class="paramref">peer</code>'s own announced <code>MaxMessageSize</code> is
refused with [`SendOutcome.RemoteMessageTooLarge`](ReactorSockets.SendOutcome.md), because the far end would discard it
on arrival; a Message over both caps reports [`SendOutcome.MessageTooLarge`](ReactorSockets.SendOutcome.md). For
[`DeliveryMode.ReliableOrdered`](ReactorSockets.DeliveryMode.md) and [`DeliveryMode.ReliableUnordered`](ReactorSockets.DeliveryMode.md) the
single-Datagram cap does not apply: a Message up to <code class="paramref">peer</code>'s own announced
<code>MaxMessageSize</code> (see the summary above) is cut into <code>ceil(length / fragmentSize)</code>
Fragments, each retained and sequenced as its own ordinary reliable packet, and only a Message
wider than that remote limit is refused with [`SendOutcome.RemoteMessageTooLarge`](ReactorSockets.SendOutcome.md).
That <code>fragmentSize</code> is this Peer's own `FragmentSize`, captured from
[`DatagramSessionOptions.ResolvedFragmentSize`](ReactorSockets.DatagramSessionOptions.ResolvedFragmentSize.md#ReactorSockets_DatagramSessionOptions_ResolvedFragmentSize) once when the Peer was admitted — not
re-read from options on this or any later call. Fragment size is unilateral, per-direction, and
decided once, at <code>Send</code> (ADR 0004): a retransmit re-sends bytes already cut rather than
re-deciding anything, so two Messages sent to the same Peer at different times, and every
retransmit of either, are always cut with the same size. It is never announced in the handshake,
because a receiver reassembles a Fragment set by <code>fragmentIndex</code> and <code>fragmentCount</code>
alone and never needs to know the sender's choice.

### Returns

 [SendOutcome](ReactorSockets.SendOutcome.md)

### Remarks

<b>[`SendOutcome.RetentionExhausted`](ReactorSockets.SendOutcome.md) is reachable from one <code>Send</code> call</b> (ADR
0004's "one Message can fill the send window alone"): the retention budget is sized in
Fragments, not Messages, so a single Message cut into more Fragments than its Space has free
slots for exhausts that Space by itself, with nothing else needing to have sent anything first.
The refusal is whole, not partial — this checks the Space's free-slot count against the Fragment
count the Message needs <em>before</em> retaining or sequencing a single Fragment, so a refused
Send leaves retention holding no Fragment of that Message and leaves the Space's next sequence
exactly where [`DatagramSessionReactor.Send(PeerId, DeliveryMode, ReadOnlySpan<byte>)`](ReactorSockets.DatagramSessionReactor.Send.md#ReactorSockets_DatagramSessionReactor_Send_ReactorSockets_PeerId_ReactorSockets_DeliveryMode_System_ReadOnlySpan_System_Byte__) found it, the same "spend nothing on a refusal" contract a
whole small Message keeps. A half-retained Message would be unrepairable: the receiver's
reassembly waits forever for a set that was never cut in full. A smaller Message that fits the
Space's free slots still succeeds afterward — the refusal costs the Space nothing.

<p>
Sending to a <code class="paramref">peer</code> this Reactor is not currently holding open — unknown, or
already closed — returns [`SendOutcome.Closed`](ReactorSockets.SendOutcome.md) rather than throwing, unlike
[`DatagramSessionReactor.CountersOf(PeerId)`](ReactorSockets.DatagramSessionReactor.CountersOf.md#ReactorSockets_DatagramSessionReactor_CountersOf_ReactorSockets_PeerId_): a Peer closing under a caller is an ordinary race, the same reason
[`DatagramSessionReactor.Close(PeerId)`](ReactorSockets.DatagramSessionReactor.Close.md#ReactorSockets_DatagramSessionReactor_Close_ReactorSockets_PeerId_) tolerates a stale [`PeerId`](ReactorSockets.PeerId.md).
</p>
<p>
Sending to a <code class="paramref">peer</code> whose handshake has not reached
`Open` returns [`SendOutcome.NotEstablished`](ReactorSockets.SendOutcome.md) under
every mode, checked before any width gate. Before the handshake the Peer has no remote-announced
<code>MaxMessageSize</code>, so a Send would otherwise be misreported as
[`SendOutcome.RemoteMessageTooLarge`](ReactorSockets.SendOutcome.md).
</p>

