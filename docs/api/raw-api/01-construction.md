# Construction

A Raw Reactor takes the shared `ReactorOptions` and one optional buffer size. Everything is
validated and allocated in the constructor, so a Reactor you hold is a Reactor that works.

```csharp
var reactor = new RawReactor(new ReactorOptions { Capacity = 16 });
```

## Required knobs

| Knob | What it is |
|---|---|
| `Capacity` | How many Links this Reactor holds at once. Listening Links count. Minimum 1. |

## Defaulted knobs

| Knob | Default | Where the default comes from |
|---|---|---|
| `AdmissionCap` | `Capacity` | One Pump never accepts more than a full Reactor's worth. |
| `ArenaSize` | `scratchBufferSize` | An arena too small for one read could never deliver one. |
| `scratchBufferSize` | `65 507` (`MaxUdpDatagramSize`) | The largest UDP payload, at which truncation cannot happen. |
| `TimeProvider` | `TimeProvider.System` | Raw measures no durations and never reads it. |
| `Sink` / `MinimumLevel` | absent | Diagnostics off. |

## The scratch buffer

Every receive on every Link lands in one shared scratch buffer, then is copied into the Frame arena.
Its size is the largest single read:

- On a datagram Link, a datagram larger than the buffer is discarded whole and logged as
  `DatagramTruncated`. See [Datagrams](04-datagrams.md).
- On a stream Link, it caps the size of one `SegmentReceived`. Longer runs arrive over later Pumps.

Lower it only if you know your datagrams are smaller and want the memory back.

## Choosing capacity

Every Link takes one Slot: each bound UDP socket, each listening socket, each accepted connection,
and each dial from the moment `Connect` returns. Size capacity for listeners plus peak connections.

A stream Link that has reached `EndOfStream` still holds its Slot until you `Close` it. Raw has no
Linger to reclaim it for you.

## The memory bill

```
scratchBufferSize + ArenaSize
```

One scratch buffer and one arena, neither multiplied by capacity. Raw holds nothing for a Link across
a Pump. With both defaults that is about 128 KiB.

Raising `ArenaSize` above the floor lets more Links deliver in the same Frame. See
[The loop](02-the-loop.md).

## Failure

Construction throws. Every check is deterministic and entirely yours.

- `ArgumentOutOfRangeException` for a single bad value: `Capacity`, `AdmissionCap` or
  `scratchBufferSize` below 1, `ArenaSize` above `Array.MaxLength` (2 147 483 591), or
  `scratchBufferSize` above `Array.MaxLength - 1` (2 147 483 590). The arena is one array, and the
  scratch buffer is one array one byte longer than `scratchBufferSize`.
- `ArgumentException` for a combination that cannot work: `ArenaSize` below `scratchBufferSize`, or
  `Sink` without `MinimumLevel`.

Messages state the arithmetic:

```
arena of 1024 bytes cannot hold one maximum-sized read of 65507 bytes — raise ArenaSize to at least 65507.
```

## Diagnostics are one argument

`Sink` and `MinimumLevel` are supplied together or not at all. Both absent means diagnostics are off.

## Disposal

`Dispose` closes every Link and releases its socket. It is idempotent, and every other member throws
`ObjectDisposedException` afterwards. No `Closed` events are queued, because nothing can drain a
disposed Reactor.

Each socket is closed by the OS as a plain socket close: a FIN, or a reset if unread data was still
in the receive buffer. Data you had not finished exchanging is lost. Finish your own protocol's
ending first if the far end needs one.
