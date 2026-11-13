# The loop

You own the loop. The library owns no threads, starts nothing, and waits for nothing.

## Pump and Drain

`Pump()` is one synchronous advance: read the Datagrams that are ready, up to `MaxDatagramsPerPump`
per bound socket, run the handshake, apply acknowledgements, deliver Messages, retransmit what is
overdue, enforce timers, and write one
Datagram per Peer with something to send. It builds the events for this Frame.

`Drain()` reads those events. It never pumps.

```csharp
reactor.Pump();
foreach (SessionEvent evt in reactor.Drain())
{
    // handle
}
```

`Pump` never blocks. It reports only what is ready at the instant it is called, so an idle Reactor
returns immediately. Your loop decides the cadence — sleep, wait on your own signal, or run flat out.

## The Frame

A Frame is the span from one `Pump` to the next. It is the lifetime of everything the Reactor hands
you.

- `SessionEvent.Payload` aliases a shared arena that the next `Pump` wipes. **Copy it out if you
  keep it past the Frame.**
- `PeerId` is a value and outlives the Frame safely.
- `Drain()` is re-entrant within a Frame: call it twice and you get the same events twice. The next
  `Pump` clears them.

## Cadence has consequences

Pumping less often is not free:

- Messages you `Send` are queued until the next `Pump` writes them. **A `Send` with no following
  `Pump` puts nothing on the wire.**
- Each `Pump` writes up to `65 507 / (FragmentSize + 31)` Datagrams per Peer, 53 at the default,
  each at most `FragmentSize + 31` bytes. Whatever does not fit waits, in order, for the next
  `Pump`. Your Pump rate times about 64 KB is a ceiling on each Peer's send rate.
- Datagrams the kernel receives while you are not pumping wait in its receive buffer, and a full
  buffer drops them.
- Retransmission, heartbeats, Resume, Linger expiry and every other timer are evaluated inside
  `Pump`, so their timing is quantised to your cadence.
- The retransmission timeout is never shorter than the Pump interval the Reactor observes from your
  loop, because a timeout shorter than that could not be acted on.

## Thread affinity

Every member must be called from the thread that constructed the Reactor. `Pump`, `Connect`, `Send`,
`Close` and `Dispose` enforce it and throw `InvalidOperationException`.

`Pump` also refuses re-entry. Calling back into the Reactor from a Diagnostic Sink is a programmer
error and throws.

Several independent Reactors on several threads is fine. One Reactor across threads is not.

## Reading is bounded per Pump

One Pump makes at most `MaxDatagramsPerPump` reads on each bound socket, so a flood cannot keep a Pump
from returning. A queued socket error spends a read too. What is left waits in the socket's receive
buffer for a later Pump. If that buffer fills, the operating system drops what arrives next and no
Counter sees it. A Pump that reaches the limit logs `DatagramReadLimitReached`.

Reads are first come, first served, so a live Peer's Datagrams wait behind a flood on the same socket.
A long enough wait looks like silence to the receive-idle deadline, and the Peer is interrupted.

A dial suffers most. A Hello read after `AdmissionCap` is spent is discarded. The dial resends it, so a
dial behind a backlog of Hellos completes once the backlog has cleared. It ends with `TransportFailed` if
that takes longer than its `Linger`.

The default allows a full Reactor's worth of traffic, which grows with `Capacity` and the receive window
and reaches hundreds of thousands of reads with a large `MaxMessageSize`. Set it lower if a Pump must
return sooner, and accept that wait growing in exchange.

## Delivery is bounded per Frame

Each Peer may deliver `ArenaSize / Capacity` bytes of Messages per Frame, with one escape: a Peer's
first Message in a Frame is always delivered, even when it exceeds that share.

What happens to a Message that does not fit depends on its delivery mode:

| What was refused | What happens to it |
|---|---|
| An `Unreliable` or `UnreliableSequenced` Message | Lost. |
| A reassembled Fragment set | Held and retried on later Pumps. |
| A whole `ReliableUnordered` Message, or a whole `ReliableOrdered` Message that is next in order | Not acknowledged. The sender retransmits it, and it is delivered on a later Pump. |
| A whole `ReliableOrdered` Message that was waiting behind a missing earlier one | Held and retried on later Pumps. |

No reliable Message is lost to the arena. A retransmitted Message waits at least one retransmission
timeout, so a refusal still delays it.

Each refusal for the share raises that Peer's `ReadShareExhaustions` counter. A refusal because the
whole arena is full logs `ArenaExhausted`. Either one means you should raise `ArenaSize`. Size the
arena so neither happens in normal operation.

## The event roster

`SessionEvent.Kind` decides which fields carry meaning. `Peer` is always populated.

| Kind | Meaning | Fields |
|---|---|---|
| `PeerConnected` | The Session is established and the Peer is addressable. | `Peer` |
| `MessageReceived` | A whole Message arrived. | `Peer`, `Payload` |
| `PeerClosing` | The peer said Goodbye, but Messages it sent are still undelivered. | `Peer` |
| `PeerClosed` | The Session ended. The `PeerId` is dead from here. | `Peer`, `Reason` |
| `PeerInterrupted` | The peer went silent past its deadline; the Peer is held. | `Peer` |
| `PeerResumed` | An interrupted Peer was heard from again. | `Peer` |

`PeerConnected` is raised on the accepting end when it accepts a verified Hello, and on the dialling
end when the Accept arrives.

Nothing a stranger sends ever reaches this roster. Malformed Datagrams, forged cookies and unmatched
Resumes go to the Diagnostic Sink and the counters, never to your event stream.

A minimum consumer handles three: `PeerConnected`, `MessageReceived`, `PeerClosed`. The other three
let you react to a Session that is ending or in trouble, but ignoring them is safe — the Reactor
handles all of it without you.
