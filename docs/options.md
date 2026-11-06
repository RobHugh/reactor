# Options and what they cost

Every number you can set on a Reactor is listed here, with where its default comes from or why it has
none. Every number you cannot set is listed too, with the reason. Then the page states what the options
cost in memory and the properties they guarantee, in a form you can check against your own
configuration.

## The rule

A **knob** trades your own resources: memory, latency, tolerance. A knob has a default only when that
default is **derived**, from machinery the library owns, from another knob, or from a citable standard.
A knob with no derivation is **required**, because any default would be a guess about your application.

A **constant** is fixed by the protocol. Both ends agree on it, or neither end can see it, and it changes
only with a version. A constant is not configurable at all. A number both ends must agree on is a
constant by construction, because a knob neither end can change on its own is not a knob.
[ADR 0012](adr/0012-the-datagram-and-raw-construction-surfaces.md) draws this line.

The rule is enforced by `OptionsSurfaceTests`. It walks everything a Reactor's constructor reaches and
fails if a member is neither required nor documented with a `Provenance:` sentence, or if a value
appears that is not in the audited list below.

## Every knob

### Shared by all three profiles (`ReactorOptions`)

| Knob | Default | Provenance |
|---|---|---|
| `Capacity` | required | None exists. It counts share-holders and multiplies every allocation. |
| `AdmissionCap` | `Capacity` | Another knob. One Pump never admits more than a full Reactor's worth. |
| `ArenaSize` | the profile's floor | The profile. One maximum deliverable unit, below which nothing can be delivered. |
| `TimeProvider` | `TimeProvider.System` | The framework. The only requirement is monotonic reads. |
| `Sink` | absent (diagnostics off) | With no Sink there is nowhere for a record to go. |
| `MinimumLevel` | absent, only with `Sink` | Supplied with `Sink` or not at all. A default level would be a guess. |

### Raw (`RawReactor` constructor)

| Knob | Default | Provenance |
|---|---|---|
| `scratchBufferSize` | 65 507 | The largest UDP payload, at which truncation cannot happen. |

### Datagram Session (`DatagramSessionOptions`)

| Knob | Default | Provenance |
|---|---|---|
| `Reactor` | required | |
| `MaxMessageSize` | required | A statement about your protocol. |
| `RetentionBytes` | required | Your memory against your tolerance for stalling on loss. |
| `MaxPeersPerAddress` | required | How many Peers one address may legitimately hold against how many Slots one reachable address may take. `Reactor.Capacity` or above sets no limit. At least 1. |
| `SendIdleInterval` | required | Bandwidth on an idle link against time to notice a dead Peer. At most 2^32 - 1 ms (about 49.7 days), the most a handshake may announce. |
| `Linger` | required | A prompt teardown against a clean one. At most 2^32 - 1 ms (about 49.7 days), the most a handshake may announce. |
| `Endpoints` | empty | No declaration. The first dial binds a wildcard, port-zero socket. |
| `FragmentSize` | 1200 | QUIC's floor under the 1280-byte IPv6 minimum MTU. Range 576 (RFC 791) to 65 507 (UDP). |
| `ReceiveWindowFragments` | fragments in one maximum Message | `MaxMessageSize` and `FragmentSize`. A narrower window deadlocks (ADR 0004). |
| `MissedHeartbeats` | 3 | RFC 4271's hold time. At least 2. |
| `Entropy` | the platform CSPRNG | The platform. |
| `MaxDatagramsPerPump` | `Capacity × ReceiveWindowFragments × 4 + AdmissionCap` | A full Reactor's worth of reads per bound socket: one window for every Space of every Peer, plus every handshake one Pump may admit. Lower it for a shorter Pump. At least 1. |

### Stream Session (`StreamSessionOptions`)

| Knob | Default | Provenance |
|---|---|---|
| `Reactor` | required | |
| `MaxMessageSize` | required | A statement about your protocol. |
| `RetentionBytes` | required | Your memory against your tolerance for a stalled Peer. |
| `SendIdleInterval` | required | As for the datagram profile. |
| `Linger` | required | As for the datagram profile. |
| `Endpoints` | empty | No declaration. Dial-only. |
| `ReceiveBufferSize` | `MaxMessageSize + 4` | `MaxMessageSize`. One incomplete maximum Message and its length prefix. |
| `SendBufferSize` | absent (OS default) | The library sets nothing it cannot derive. |
| `MissedHeartbeats` | 3 | RFC 4271's hold time. At least 2. |
| `Entropy` | the platform CSPRNG | The platform. |

## What you cannot set

**Protocol constants.** The 32-bit acknowledgement width, the 64-entry duplicate window, the four
sequence Spaces, zero as the reserved session value, the 20-byte header and the 30-second cookie epoch.
Changing one changes what a datagram means.

**Derived values.** Each has inputs the Reactor already knows, so a knob would only be a second number to
keep consistent with them.

| Value | Derived from |
|---|---|
| Receive-idle deadline | The remote's announced `SendIdleInterval` × your `MissedHeartbeats` |
| Re-dial schedule | Starts at the observed Pump interval, doubles, stops at the effective Linger, with full jitter |
| Per-share-holder read cap | `ArenaSize ÷ Capacity` bytes per Pump, with one whole unit always allowed |
| RTO floor and ceiling | The observed Pump interval and the effective Linger |
| Dial-only socket's family and port | The destination's family and an OS-assigned port |
| Ordered hold limit | 2 × `ReceiveWindowFragments` sequences, each no wider than `FragmentSize`. A Fragment group holding the head spans one window, and the receive window spans one more past it |

## Memory

Every formula below has no traffic term. Each is fixed by your options before the first byte arrives,
and `MemoryBillTests` checks each line construction allocates against the bytes it actually allocates.

### Stream

```
Capacity × (RetentionBytes + receive buffer)
```

The Message-boundary ring adds about a quarter of retention again: one `uint` for every four retained
bytes. `ReceiveBudgetBytes`, `SendBudgetBytes` and `BoundaryRingBudgetBytes` report the three lines.

Sent data is **buffered twice**: once in this library's retention until the peer confirms it, and once
in the kernel's send buffer. The kernel's share is not fully yours to control. `SendBufferSize` requests
a size, and the OS may double it or hold it to its own floor.

### Datagram

```
receive:  Capacity × 2 × ReceiveWindowFragments × FragmentSize
send:     Capacity × 2 × RetentionBytes
```

`FragmentSize` appears in one line and not the other, because the window counts packets and retention
counts bytes. The 2 is the two reliable Spaces, the only ones that reassemble and retransmit. Each line
also carries a few bytes of bookkeeping per fragment slot, under 2% at the default fragment size.
`ReceiveBudgetBytes` and `SendBudgetBytes` report both lines exactly, bookkeeping included.

One further cost is a ceiling rather than a preallocation, so nothing allocates it at construction:

```
held:     Capacity × 2 × ReceiveWindowFragments × FragmentSize
```

These are ordinary `ReliableOrdered` records waiting behind a gap, on the heap and only under traffic.
The 2 is two windows of sequences in the one Space, not two Spaces. A record past that reach, or wider
than `FragmentSize`, is refused unacknowledged and counted on `OutOfWindowPackets`, and retransmission
brings it back. `HeldRecordBudgetBytes` reports this line, bookkeeping included, and
`DatagramSessionOrderedHoldBudgetTests` checks it against the bytes a flood allocates.

### Raw

```
scratchBufferSize + ArenaSize
```

One scratch buffer and one arena, neither multiplied by capacity. Raw holds nothing for a Link across a
Pump. That is what inventing nothing on the wire looks like from the constructor.

The frame arena (`ArenaSize`) is also allocated by both Session profiles, once per Reactor, on top of
the lines above.

## `MaxMessageSize`

`MaxMessageSize` appears in three places at once: it is announced to the remote as a receive limit, it
floors a buffer, and it floors retention. It is the one knob whose careless enlargement multiplies
memory in two directions.

On the stream profile the buffer is the receive buffer and both floors apply directly. On the datagram
profile the buffer is the receive window, which is per Peer and per reliable Space, and retention is
floored by one fragment instead of one Message, because a reliable Space retransmits fragment by
fragment. On both, `MaxMessageSize` also floors the shared arena.

## Every per-Pump cost a remote party can inflate is bounded at construction

One property, three mechanisms.

1. **Reading** is bounded by each share-holder's share of the arena.
2. **Admission**, a TCP accept, is bounded by `AdmissionCap`.
3. **Handshakes**, each costing an HMAC, are bounded by the same `AdmissionCap`.

Control traffic never reaches the arena, so the read share cannot bound handshakes. That is why
admission has its own cap. Anything added later that a remote party can make more expensive inside one
Pump needs a bound fixed at construction too. The failure these prevent is the same each time, and it is
invisible until someone points a load generator at the Reactor.

## Capacity

`Capacity` counts share-holders: **Links** in Raw, **Peers** in a Session. It multiplies every
per-share-holder allocation above.

The library is designed for capacity in the tens and is unsuitable above the low hundreds. 64 survives as
that statement of intent, not as a default. Capacity is **not capped**. A cap would be permanent, would
defend a number with no derivation, and would refuse a consumer who has measured their own case. A
capacity too large to allocate fails at construction, naming the arithmetic.

## Stream profile: Nagle is off

Nagle (`TCP_NODELAY`) is off on every stream Session socket and cannot be turned on. This library
batches instead: every `Send` since the last `Pump` is written once, by that `Pump`. So a latency
measurement measures your Pump cadence, not TCP's acknowledgement clock. To batch more, Pump less often.

## Datagram profile: security

- The session field is **not an authenticator**. It makes blind off-path injection a 32-bit guess, and
  it does nothing against anyone who has seen one datagram of the Session.
- This library **does not encrypt**.
- **On-path attackers are out of scope.** If you need confidentiality or integrity against one, provide
  it inside your Messages.

Resume is authorised by a separate secret exchanged during the handshake, never by the session field
([ADR 0009](adr/0009-the-datagram-session-wire-format.md)). The secret does not prove the Resume's
source address receives, so a Resume from a new IP address moves sends there only after that address
echoes a path challenge. A Resume from the same IP address on a new port moves at once
([ADR 0021](adr/0021-a-resume-from-a-new-ip-address-is-proven-by-a-path-challenge.md)).
