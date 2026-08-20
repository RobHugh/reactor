# Construction

A Reactor is fully described by the `StreamSessionOptions` you hand its constructor. Everything is
validated and allocated there, so a Reactor you hold is a Reactor that works.

## Required knobs

None of these has a default, because none can be derived from anything the library owns. Guessing
one would be guessing at your application.

| Knob | What it is |
|---|---|
| `Reactor.Capacity` | How many Peers this Reactor admits. Multiplies every preallocation. |
| `MaxMessageSize` | The largest Message you will send or accept, in bytes. |
| `RetentionBytes` | Bytes held per Peer for the send path. Your backpressure budget. |
| `SendIdleInterval` | How long a Session may stay silent before it heartbeats. |
| `Linger` | How long a closing or interrupted Session stays alive. |

## Defaulted knobs

| Knob | Default | Where the default comes from |
|---|---|---|
| `Reactor.AdmissionCap` | `Capacity` | One Pump never does more admission work than a full Reactor's worth. |
| `Reactor.ArenaSize` | `MaxMessageSize` | An arena too small for one Message could never deliver one. |
| `Reactor.TimeProvider` | `TimeProvider.System` | Only monotonic reads are ever taken. |
| `ReceiveBufferSize` | `MaxMessageSize + 4` | One incomplete maximum Message plus its length prefix. |
| `MissedHeartbeats` | `3` | RFC 4271's hold time. Minimum 2. |
| `Entropy` | `RandomNumberGenerator.Create()` | The platform CSPRNG. Never disposed by the Reactor. |
| `SendBufferSize` | `null` | The OS default and its own autotuning. |
| `Endpoints` | empty | An empty set is dial-only, a real configuration rather than a hole. |

## Binding

`Endpoints` are bound in the order supplied, during the constructor. A bind that fails throws and
names the endpoint — there is no half-built Reactor.

Bind port 0 and read the assigned port back:

```csharp
IPEndPoint bound = reactor.BoundEndpoints[0];
```

There is no rebinding. A Reactor that must listen elsewhere is a new Reactor.

A dial-only Reactor supplies no endpoints. TCP's own connect binds an ephemeral local port, so
`Connect` still works with nothing bound.

## Choosing capacity

Capacity is the number of Peers, but interrupted and closing Peers still hold their Slots for up to
`Linger`. Size capacity for the peak Peer count *plus* the reconnect churn you expect within a
Linger window, or fresh dials get refused while a departed Peer's Slot is still reserved.

## Choosing retention

Retention is the send path, not a side buffer. `Send` frames the Message and appends it here; the
socket write is a copy out of it.

Bytes are released only when the peer confirms it received the Messages they carried — not when the
local kernel accepts them. So retention must cover a round trip's worth of traffic at your send
rate, plus whatever you want to survive an interruption.

Retention must be at least `MaxMessageSize + 4`. Below that a single maximum Message could never be
retained.

## The memory bill

A Reactor preallocates, per Peer:

```
RetentionBytes + ReceiveBufferSize + Message-boundary ring (≈ RetentionBytes / 4 × 4 bytes)
```

Read the three lines directly before you construct anything:

```csharp
long total = options.SendBudgetBytes
           + options.ReceiveBudgetBytes
           + options.BoundaryRingBudgetBytes;
```

Retention is buffered twice over — once here, once again in the kernel's own send buffer — so a
Peer's real footprint is this bound plus whatever the OS holds. Set `SendBufferSize` if you need the
OS half bounded at a known point.

## Failure

Construction throws. It never returns a bad Reactor and never reports through a result type, because
every check is deterministic and entirely yours.

- `ArgumentOutOfRangeException` for a single bad value.
- `ArgumentException` for a combination that cannot work.

Messages state the arithmetic, not the rule:

```
retention of 65536 bytes cannot hold one Message of 1048576 bytes plus its 4-byte length
prefix — raise RetentionBytes to at least 1048580, or lower MaxMessageSize.
```

## Diagnostics are one argument

`Sink` and `MinimumLevel` are supplied together or not at all. Neither alone is valid: with no Sink
there is nowhere for a record to go, so no level can matter. Both absent means diagnostics are off.

## Disposal

`Dispose` releases every socket the Reactor bound or accepted. It is idempotent, and every other
member throws `ObjectDisposedException` afterwards. No `PeerClosed` events are queued — nothing can
drain a disposed Reactor.

Peers are not told. If you want the far ends to see a clean ending, `Close` them and pump until each
`PeerClosed` arrives before disposing.
