# Construction

A Reactor is fully described by the `DatagramSessionOptions` you hand its constructor. Everything is
validated and allocated there, so a Reactor you hold is a Reactor that works.

## Required knobs

None of these has a default, because none can be derived from anything the library owns. Guessing
one would be guessing at your application.

| Knob | What it is |
|---|---|
| `Reactor.Capacity` | How many Peers this Reactor admits. Multiplies every preallocation. |
| `MaxMessageSize` | The largest Message you will send or accept, in bytes. |
| `RetentionBytes` | Bytes each reliable Space retains per Peer until acknowledged. Your backpressure budget. |
| `SendIdleInterval` | How long a Session may go without sending before it sends a Ping. |
| `Linger` | How long a closing or interrupted Session stays alive. |

## Defaulted knobs

| Knob | Default | Where the default comes from |
|---|---|---|
| `Reactor.AdmissionCap` | `Capacity` | One Pump never does more handshake work than a full Reactor's worth. |
| `MaxPeersPerAddress` | `Capacity - 1`, at least 1 | The largest limit that still keeps one address from holding every Slot. |
| `PeerLimitIPv6PrefixLength` | `64` | RFC 4291 §2.5.1: a unicast interface identifier is 64 bits, so a /64 is one link. From 1 to 128. |
| `Reactor.ArenaSize` | `MaxMessageSize` | An arena too small for one Message could never deliver one. |
| `Reactor.TimeProvider` | `TimeProvider.System` | Only monotonic reads are ever taken. |
| `FragmentSize` | `1200` | QUIC's floor beneath the 1280-byte IPv6 minimum MTU (RFC 9000). |
| `ReceiveWindowFragments` | Fragments in one maximum Message | A narrower window can never deliver one Message, so it deadlocks. |
| `MissedHeartbeats` | `3` | RFC 4271's hold time. Minimum 2. |
| `MaxDatagramsPerPump` | `Capacity × ReceiveWindowFragments × 4 + AdmissionCap` | A full Reactor's worth of reads per bound socket: one window for every Space of every Peer, plus every handshake one Pump may admit. Minimum 1. |
| `Entropy` | `RandomNumberGenerator.Create()` | The platform CSPRNG. Never disposed by the Reactor. |
| `Endpoints` | empty | An empty set is dial-only, a real configuration rather than a hole. |

The resolved values are readable on the options object: `ResolvedFragmentSize`,
`ResolvedReceiveWindowFragments`, `ReceiveWindowFloor`, `ResolvedMissedHeartbeats`,
`ResolvedMaxDatagramsPerPump` and `RetentionSlots`.

Values fixed by the wire format are not options: the four sequence Spaces, the 32-bit ack width, the
64-entry duplicate window, the 20-byte data header and the thirty-second cookie epoch. Changing one
would change what a Datagram means.

## Binding

`Endpoints` are bound in the order supplied, during the constructor. A bind that fails throws and
names the endpoint — there is no half-built Reactor. A null or duplicated endpoint is refused before
anything binds.

Bind port 0 and read the assigned port back:

```csharp
IPEndPoint bound = reactor.BoundEndpoints[0];
```

There is no rebinding. A Reactor that must listen elsewhere is a new Reactor.

A dial-only Reactor supplies no endpoints. Its first `Connect` binds one wildcard, port-0 socket in
the destination's address family and appends it to `BoundEndpoints`.

## Choosing fragment size

`FragmentSize` is the payload bytes one Datagram carries, and the unit every buffer is measured in.
It must lie between 576 (RFC 791's guaranteed reassembly minimum) and 65 507 (the largest UDP
payload).

- Leave it at 1200 unless you control every path between the two ends.
- It caps the unreliable modes: an `Unreliable` or `UnreliableSequenced` Message can be no larger
  than one fragment.
- It is per direction and never negotiated. Two ends with different values interoperate, each
  cutting its own Messages by its own size.
- Each Peer captures the value when it is admitted. Nothing changes it mid-Session.

A size larger than the path will carry ends the Session with `FragmentSizeUnviable`. See
[Closing](04-closing.md).

## Choosing capacity

Capacity is the number of Peers, but dialling, interrupted and closing Peers all hold their Slots.
Size capacity for the peak Peer count *plus* the Peers you expect to be interrupted or closing at
once, or fresh handshakes are refused while those Slots are still held.

## Choosing the per-address limit

`MaxPeersPerAddress` stops one reachable address holding every Slot. The default, one fewer than
`Capacity`, guarantees only that one Slot stays free for other addresses. Set it above the number of
Peers you expect behind one NAT or proxy, and well below `Capacity` if strangers can reach this Reactor.
If every Peer shares one address, for example behind one proxy, set it to `Capacity`, or the default
leaves one Slot unused.

The limit counts addresses, not ports, because one host can use many ports. Peers this Reactor
dialled do not count.

IPv6 addresses are counted by prefix. `PeerLimitIPv6PrefixLength`, 64 by default, sets how many leading
bits form the key, so every address in one /64 shares one limit. Every on-link host on an IPv6 LAN is
in one /64, so set it to 128 when Peers legitimately arrive from one link. A shorter length groups more
of a customer's delegated prefix, at the cost of grouping unrelated customers. IPv4 addresses are
always counted whole.

A Peer counts against the address of its handshake until its Slot is released, so leave room for two
cases. Interrupted and Closing Peers still count, so a client that restarts with a fresh handshake can
be refused for up to Linger. A Peer that a Resume moved to another address also still counts, so other
hosts behind its old address have that much less room for as long as the Session lasts.

## Choosing retention

Retention is per reliable Space, per Peer: one budget for `ReliableOrdered` and another for
`ReliableUnordered`. A reliable `Send` copies the Message into its Space's retention, and the
retransmit path reads it back from there.

Bytes are released only when the peer acknowledges them. Retention must cover a round trip of
traffic in each reliable mode at your send rate, plus whatever you want to keep queuing through an
interruption.

Retention is counted in whole fragment slots: `RetentionSlots = RetentionBytes / FragmentSize`,
remainder discarded. It must hold at least one fragment. It does not need to hold a whole maximum
Message, because retransmission is per fragment, but a Message needing more slots than are free is
refused whole. To send a maximum Message in one `Send`, give retention at least
`ReceiveWindowFloor` fragments.

The Ping a quiet Session sends uses one `ReliableUnordered` slot, and your own Goodbye uses one
`ReliableOrdered` slot.

## The memory bill

A Reactor preallocates two lines, both multiplied by capacity, neither with a traffic term:

```
receive: Capacity × 2 reliable Spaces × ReceiveWindowFragments × FragmentSize   (+ bookkeeping)
send:    Capacity × 2 reliable Spaces × RetentionSlots × FragmentSize          (+ bookkeeping)
```

Read the exact figures, bookkeeping included, before you construct anything:

```csharp
long total = options.ReceiveBudgetBytes + options.SendBudgetBytes;
```

The frame arena (`Reactor.ArenaSize`) is extra, and is shared across the whole Reactor rather than
multiplied by capacity.

Ordinary `ReliableOrdered` records waiting behind a gap are also extra. They are not preallocated, but
`options.HeldRecordBudgetBytes` is their ceiling, about the size of the receive line. Records past it
are refused and retransmitted rather than held.

`MaxMessageSize` raises memory in two directions at once. It floors the receive window, which is per
Peer and per Space, and it floors the arena. Raise it with care.

## Failure

Construction throws. It never returns a bad Reactor and never reports through a result type, because
every check is deterministic and entirely yours.

- `ArgumentOutOfRangeException` for a single bad value.
- `ArgumentException` for a combination that cannot work.

Messages state the arithmetic, not the rule:

```
a receive window of 10 fragments cannot hold one 65536-byte Message, which occupies 55 fragments of
1200 bytes. ... Raise ReceiveWindowFragments to at least 55, or lower MaxMessageSize.
```

A combination whose two budget lines together exceed what one allocation can hold is refused with
both lines and their total spelled out.

## Diagnostics are one argument

`Sink` and `MinimumLevel` on `ReactorOptions` are supplied together or not at all. Neither alone is
valid: with no Sink there is nowhere for a record to go, so no level can matter. Both absent means
diagnostics are off.

## Security

The session id on every Datagram is a demultiplexing token, not an authenticator. It makes blind
off-path injection a 32-bit guess and does nothing against anyone who has seen one Datagram of the
Session. This library does not encrypt. If you need confidentiality or integrity against an on-path
attacker, provide it inside the Message.

## Disposal

`Dispose` releases every socket the Reactor bound. It is idempotent, and every other member throws
`ObjectDisposedException` afterwards. No `PeerClosed` events are queued — nothing can drain a
disposed Reactor. `Entropy` is not disposed, because it belongs to you.

Peers are not told. If you want the far ends to see a clean ending, `Close` them and pump until each
`PeerClosed` arrives before disposing.
