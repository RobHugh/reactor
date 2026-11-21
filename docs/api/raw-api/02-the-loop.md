# The loop

You own the loop. The library owns no threads, starts nothing, and waits for nothing.

## Pump and Drain

`Pump()` is one synchronous advance: clear the last Frame, read each ready Link once, accept what
the admission cap allows, resolve dials in flight, and raise `Writable` for armed Links.

`Drain()` reads the events that produced. It never pumps.

```csharp
reactor.Pump();
foreach (RawEvent evt in reactor.Drain())
{
    // handle
}
```

`Pump` never blocks. It polls with a zero timeout, so an idle Reactor returns immediately. Your loop
decides the cadence.

## The Frame

A Frame is the span from one `Pump` to the next. It is the lifetime of everything the Reactor hands
you.

- `RawEvent.Payload` aliases a shared arena that the next `Pump` wipes. **Copy it out if you keep it
  past the Frame.**
- `RawEvent` is a `ref struct`. It cannot be stored in a field or a collection.
- `LinkId` and `Source` are values and outlive the Frame safely.
- `Drain()` is re-entrant within a Frame: call it twice and you get the same events twice.

Calls you make between Pumps can add to the current Frame. `Close` appends its `Closed` event at
once, so a `Close` inside a `foreach` over `Drain()` is reached later in the same loop. A `Close` after
the loop is only seen by another `Drain()` before the next `Pump`.

## Cadence has consequences

Raw writes immediately. `SendTo` and `Send` are socket calls, not queue appends, so a send does not
wait for a Pump. Nagle is off on every TCP socket.

Reading is different. **Each ready Link is read exactly once per Pump.** A datagram Link delivers at
most one datagram per Pump, and a stream Link at most one Segment. Everything else waits in the
kernel buffer for later Pumps.

So your Pump rate is your receive rate per Link. A UDP Link pumped every millisecond can deliver at
most a thousand datagrams a second. If the kernel buffer fills before you drain it, the kernel drops
datagrams and nothing in this library sees them.

Dial results and `Writable` are also evaluated inside `Pump`, so they arrive no sooner than your next
Pump.

## Fairness

Every ready Link is served before any Link is served twice, and the scan order rotates each Pump. A
listening Link is the exception: it accepts several connections in one Pump, bounded by
`AdmissionCap` and free Slots.

The arena adds a second, looser bound. A stream Link is read only as far as the arena has room. When
a Frame's reads fill `ArenaSize`, the remaining stream Links are not read that Pump. Their bytes stay
in the kernel's receive buffer and are read on a later Pump, so nothing is lost. A datagram that does
not fit is not delivered, and `ArenaExhausted` is logged. With the default `ArenaSize` of one scratch
buffer, two Links receiving large reads in the same Pump take turns across Pumps. Raise `ArenaSize`
to serve more of them per Pump.

## Thread affinity

Every mutating member must be called from the thread that constructed the Reactor: `Pump`, `Bind`,
`Listen`, `Connect`, `SendTo`, `Send`, `Shutdown`, `Close` and `Dispose`. They throw
`InvalidOperationException` otherwise.

The reads, `LocalOf`, `RemoteOf`, `CountersOf` and `Counters`, do not check the thread. A counter
read from another thread is a sample, not a consistent snapshot. Read from the Pump thread between
Pumps if you need one.

`Pump` refuses re-entry. Calling back into the Reactor from a Diagnostic Sink throws.

Several independent Reactors on several threads is fine. One Reactor across threads is not.

## The event roster

`RawEvent.Kind` decides which fields carry meaning. `Link` is always populated.

| Kind | Meaning | Fields |
|---|---|---|
| `DatagramReceived` | A datagram arrived on a bound UDP Link. | `Link`, `Source`, `Payload` |
| `Accepted` | A listening Link accepted a connection, now its own Link. | `Link` (new), `ListeningLink` |
| `Connected` | A dial completed. | `Link` |
| `SegmentReceived` | A run of bytes arrived on a stream Link. | `Link`, `Payload` |
| `Writable` | A stream Link that took a short send has room again. | `Link` |
| `EndOfStream` | The remote sent its FIN. The Link stays open for sending. | `Link` |
| `Closed` | The Link is gone. The `LinkId` is dead from here. | `Link`, `Reason` |

A datagram-only consumer handles two: `DatagramReceived` and `Closed`. A stream consumer needs
`SegmentReceived`, `EndOfStream` and `Closed`, plus `Writable` if it ever sends more than the socket
takes at once.
