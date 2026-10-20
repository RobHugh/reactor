# The loop

You own the loop. The library owns no threads, starts nothing, and waits for nothing.

## Pump and Drain

`Pump()` is one synchronous advance: accept what is ready, advance in-flight dials, read what is
readable, flush what is queued, enforce timers. It builds the events for this Frame.

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

- Messages you `Send` sit in retention until the next `Pump` writes them. **A `Send` with no
  following `Pump` puts nothing on the wire.**
- Received Messages wait in kernel buffers, then your receive buffer.
- Idle timers, Linger expiry and re-dial attempts are all evaluated inside `Pump`, so their timing
  is quantised to your cadence.
- The re-dial schedule's first delay is the Pump interval the Reactor observes from your loop.

Every `Send` queued since the last `Pump` is written together, once, by that `Pump`. Nagle is off
unconditionally on every socket, so the batching is ours and yours, never the connection's.

## Thread affinity

Every member must be called from the thread that constructed the Reactor. `Pump`, `Connect`, `Send`,
`Close` and `Dispose` enforce it and throw `InvalidOperationException`.

`Pump` also refuses re-entry. Calling back into the Reactor from a Diagnostic Sink is a programmer
error and throws.

Several independent Reactors on several threads is fine. One Reactor across threads is not.

## Delivery is bounded per Frame

Each Peer may deliver `ArenaSize / Capacity` bytes of Messages per Frame, with one escape: a Peer
may always deliver one whole Message even when it exceeds that share.

A Message that does not fit is held, in order, and delivered on a later Pump. Nothing is dropped and
nothing is reordered. Each time the share holds a Message back, that Peer's `ReadShareExhaustions`
counter rises. A steady rise means you should raise `ArenaSize`. Share exhaustion logs no
diagnostic. Only the arena filling outright logs `ArenaExhausted`.

## The event roster

`SessionEvent.Kind` decides which fields carry meaning. `Peer` is always populated.

| Kind | Meaning | Fields |
|---|---|---|
| `PeerConnected` | The Session is established and the Peer is addressable. | `Peer` |
| `MessageReceived` | A whole Message arrived. | `Peer`, `Payload` |
| `PeerClosing` | The peer said Goodbye, but something is still outstanding. | `Peer` |
| `PeerClosed` | The Session ended. The `PeerId` is dead from here. | `Peer`, `Reason` |
| `PeerInterrupted` | The transport is gone; the Peer is not. | `Peer` |
| `PeerResumed` | An interrupted Peer reclaimed its transport. | `Peer` |

`PeerConnected` is raised on both ends, once each has sent its own handshake record and read the
other's.

A minimum consumer handles three: `PeerConnected`, `MessageReceived`, `PeerClosed`. The other three
let you react to a Session that is ending or in trouble, but ignoring them is safe — the Reactor
handles all of it without you.
