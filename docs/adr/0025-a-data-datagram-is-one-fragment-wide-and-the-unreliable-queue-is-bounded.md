# A data Datagram is one Fragment wide, and the unreliable queue is bounded

Two parts of the datagram profile's send path had no bound (issue #485). A Peer's queued records
were packed into one Datagram of up to 65 507 bytes, whatever `FragmentSize` said, so a backlog of
small Messages left as one Datagram the network would split at the IP layer or drop. Unreliable
Messages queued without limit, one heap copy each, so a sender above its Pump rate grew the heap
until the process failed. ADR 0012 requires every cost a caller or a remote party can grow to be
bounded at construction.

## Decision

**A data Datagram is at most `FragmentSize + 31` bytes**: the 20-byte Datagram header, an 11-byte
Fragment record header and one Fragment. That is the smallest limit that always holds a full
Fragment, so no record ever waits for a larger Datagram. At the default fragment size it is 1231
bytes, 1279 with IPv6 and UDP headers, under the 1280-byte IPv6 minimum MTU. It applies to every
mode.

**One Pump writes up to `65 507 / (FragmentSize + 31)` data Datagrams per Peer**: 53 at the
default, 1 from a fragment size of 32 723. Records go in Send order, and what does not fit waits for
the next Pump. A Pump's bytes per Peer stay at or below one maximum UDP payload, the ceiling ADR 0009
already had. Draining the whole queue in one Pump was tried and rejected: 57 Datagrams of 32 KB in
one Pump overran a loopback receive buffer and were retransmitted. It is a derived constant, not an
option.

In both bounds `FragmentSize` is the Peer's send Fragment size, the smaller of the two ends' values
(ADR 0024). Against a peer configured at 576 a Datagram is at most 607 bytes, and one Pump writes up
to 107 of them.

**Unreliable Sends are charged against a per-Peer byte budget**, `UnreliableQueueBytes`, at payload
plus the 7-byte record header each, so empty Messages are bounded too. Both unreliable modes share
it. A Send past it returns the new `SendOutcome.SendQueueFull` and spends nothing, not even a
sequence. The default is `RetentionSlots × (FragmentSize + 7)`: as many full unreliable Messages as
one reliable Space retains Fragments. It must be at least one full unreliable Message. It is
optional, because a new required option would break every 1.0 caller.

**The queue is preallocated**: one array of `Capacity × UnreliableQueueBytes` bytes, one region per
Peer, so an unreliable Send allocates nothing. After each flush the bytes still queued move to the
front of the region in one copy.

## Rejected

- **Reusing `RetentionExhausted`.** Its name and documentation define it by reliable retention,
  which frees only when the far end acknowledges. This queue frees as this end's Pumps write it.
- **A default of 65 507 bytes.** One maximum UDP payload per Peer costs `Capacity × 64 KB` before
  anything is sent.
- **An option for the Datagram count per Pump.** The derived count keeps ADR 0009's per-Pump byte
  ceiling, and no evidence asks for another.
- **Cutting unreliable Messages into Fragments.** ADR 0004 keeps the unreliable modes one Datagram
  each.

## Consequences

- **The memory bill has a third line**, `Capacity × UnreliableQueueBytes`, about one reliable
  Space's retention per Peer at the default.
- **A consumer switching over `SendOutcome` handles `SendQueueFull`.** It is the unreliable modes'
  backpressure: retry after the next Pump, or drop the Message.
- **A burst is still bounded by the per-Pump count, not paced inside the Pump.** Up to 53 Datagrams
  leave back to back. Pacing would need a timer inside the Pump.
- **A test that relays one Datagram per Pump through a Raw Link sees only the first of several.**
  Tests that read the wire relay until the expected count has arrived.
