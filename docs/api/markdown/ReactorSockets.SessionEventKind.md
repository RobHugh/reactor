# <a id="ReactorSockets_SessionEventKind"></a> Enum SessionEventKind

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

Which kind of [`SessionEvent`](ReactorSockets.SessionEvent.md) was drained, and therefore which of its fields carry
meaning. Shared by both Session profiles (ADR 0010).

```csharp
public enum SessionEventKind
```

## Fields

`PeerConnected = 1` 

A Session is established and the Peer is addressable. Raised on both ends: on the dialler when
the Accept arrives, on the accepting end when a Hello is admitted.



`PeerClosed = 2` 

A Session ended, or a dial never became one. [`SessionEvent.Reason`](ReactorSockets.SessionEvent.Reason.md#ReactorSockets_SessionEvent_Reason) says why, and
the [`PeerId`](ReactorSockets.PeerId.md) is dead from this event onwards.



`MessageReceived = 3` 

A whole Message was reassembled from the Peer's stream and copied into the frame arena.
[`SessionEvent.Payload`](ReactorSockets.SessionEvent.Payload.md#ReactorSockets_SessionEvent_Payload) carries it, valid only until the next Pump (ADR 0008).



`PeerClosing = 4` 

The Peer's Goodbye arrived while something was still outstanding to deliver: either Messages
this end had already reassembled from it but the consumer had not yet drained (even though it
reached [`SessionEventKind.MessageReceived`](ReactorSockets.SessionEventKind.md) first), or bytes still buffered in its
receive share awaiting arena room, or this end's own outbound retention for it still being
flushed to the socket. The Peer accepts no further <code>Send</code>s worth making from here and a
[`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md) carrying [`DisconnectReason.PeerGoodbye`](ReactorSockets.DisconnectReason.md)
follows once all of that is exhausted. Never raised when nothing was outstanding on either side
— a Goodbye that finds the Peer already caught up closes it directly, with no zero-Frame Closing
observable in between. In the datagram profile the Goodbye acts at its turn in
<code>ReliableOrdered</code> send order, and "outstanding" means Messages received but not yet
delivered.



`PeerInterrupted = 5` 

The stream profile's transport for this Peer is gone, but the Peer itself is not: its
[`PeerId`](ReactorSockets.PeerId.md) — including generation — stays live, [`StreamSessionReactor.CountersOf(PeerId)`](ReactorSockets.StreamSessionReactor.CountersOf.md#ReactorSockets_StreamSessionReactor_CountersOf_ReactorSockets_PeerId_)
keeps answering for it, and its undelivered inbound Messages and outbound retention ring are
held rather than discarded (ADR 0010). Raised whenever an established Peer's transport ends
without a Goodbye first: an inbound FIN carrying nothing further, a reset, a local write
failure, or this end's own receive-idle deadline expiring (which closed the Peer outright before
this kind existed). <b>Distinct from [`SessionEventKind.PeerClosing`](ReactorSockets.SessionEventKind.md)</b>: Closing means a Goodbye
arrived and the Session is ending on purpose, with [`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md) to follow once both
sides drain — nothing here is ever resumable. Interrupted means the opposite — no Goodbye was
ever seen, the Session is not ending, and the Peer sits ready for a Resume for as long as Linger
allows. A Peer that sent Goodbye and then a bare FIN reaches [`SessionEventKind.PeerClosing`](ReactorSockets.SessionEventKind.md) or
[`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md), never this. Carries only [`SessionEvent.Peer`](ReactorSockets.SessionEvent.Peer.md#ReactorSockets_SessionEvent_Peer) — there is no
[`DisconnectReason`](ReactorSockets.DisconnectReason.md) to report, because nothing has ended.



`PeerResumed = 6` 

A Session held [`SessionEventKind.PeerInterrupted`](ReactorSockets.SessionEventKind.md) reclaimed its transport and is delivering again.
Raised on whichever side completes the match, and in both cases only once that side knows where
to resend from: on the listening side from <code>ResumeHeldPeer</code>, once the peer's Resume record
checks out and its carried delivery counter has driven the rewind; on the dialling side not from
<code>ResumeDialledPeer</code>, which only sends the Resume record, but later, when the listening
side's reciprocal delivery-counter record arrives and is rewound off. Only that success path
reaches here: a resume refused because the peer named a Message this end no longer retains
closes the Peer instead ([`DisconnectReason.RetentionGap`](ReactorSockets.DisconnectReason.md)) and never raises this.
[`SessionEvent.Peer`](ReactorSockets.SessionEvent.Peer.md#ReactorSockets_SessionEvent_Peer)'s [`PeerId`](ReactorSockets.PeerId.md), generation included, is exactly the one
[`SessionEventKind.PeerInterrupted`](ReactorSockets.SessionEventKind.md) carried — a Resume is the same Peer's transport coming back, never
a new one — and no [`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md) is ever observed between the two. The consumer is
asked for no reconciliation: Messages sent before the interruption arrive without being re-sent,
since the rewind that makes this event possible already put this end's outbound retention back
to the peer's last confirmed point before this event is raised. Carries only
[`SessionEvent.Peer`](ReactorSockets.SessionEvent.Peer.md#ReactorSockets_SessionEvent_Peer), the same as [`SessionEventKind.PeerInterrupted`](ReactorSockets.SessionEventKind.md) — nothing has ended,
so there is no [`DisconnectReason`](ReactorSockets.DisconnectReason.md) to report.



## Remarks

Members are appended, never inserted.

