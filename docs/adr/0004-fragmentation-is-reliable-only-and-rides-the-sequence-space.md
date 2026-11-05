# Fragmentation is reliable-only and rides the reliable sequence space

A UDP Datagram is capped by the path, not by `MaxMessageSize`, so a Session promising Messages of
arbitrary size must cut them into Fragments. We do this for `ReliableOrdered` and
`ReliableUnordered` only: `Unreliable` and `UnreliableSequenced` are capped at a single Datagram and
`Send` returns `MessageTooLarge` above it. You may only fragment what you can retransmit — a
ten-Fragment unreliable Message on a path with 1% loss arrives about 90% of the time, having spent
ten Datagrams of bandwidth on a coin flip and held receive-window space while doing it. Raw UDP
never fragments at all; a Fragment header would be a protocol, and Raw's entire value is addressing
endpoints that never heard of us.

Each Fragment is an ordinary reliable packet carrying its own sequence number. The ack and
retransmit machinery operates purely on sequence numbers and does not know Fragments exist, so a
lost Fragment is repaired by exactly the path that repairs a lost small Message, and costs one
Datagram rather than the whole Message. Reassembly is a thin layer above reliability that watches
for a complete set.

Because Fragments of one Message occupy contiguous sequence numbers, `firstSeq = seq -
fragmentIndex` identifies the group, and **no message ID exists on the wire**. One flag bit in the
common header marks a Fragment; `fragmentIndex` and `fragmentCount` follow as two 16-bit fields on
Fragments only, so unfragmented Messages pay one bit rather than four bytes. Contiguity is therefore
a correctness invariant, not an implementation detail, and is asserted in code.

Arriving Fragments occupy the reliable receive window they are already in. There is no separate
reassembly buffer: a complete set is copied into the frame arena as one contiguous Message and the
slots are released. Nothing new is preallocated, and a sender flooding partial sets is only filling
their own receive window, which is already bounded and already defended.

Fragment size defaults to 1200-byte Datagrams — the floor QUIC assumes without probing, under the
1280-byte IPv6 minimum MTU — and is configurable at construction within `[576, 65507]`. It is
unilateral and per-direction: a receiver reassembles by index and count and never needs to know the
sender's choice. We do not probe (RFC 8899 DPLPMTUD) and do not consult the OS path MTU: probing
demands per-Peer path state and dynamically sized buffers, which contradicts preallocating
everything at construction, and ICMP-based discovery produces confident wrong answers on exactly the
filtered paths where being wrong hurts.

`MaxMessageSize` is announced in the handshake and is honestly a *receive* limit. A sender validates
against the peer's announced value, so an oversized payload fails synchronously at `Send` rather
than as an unexplained remote disconnect part-way through a transfer.

## Consequences

- **The receive window must hold at least `MaxMessageSize / fragmentSize` packets.** Below that, a
  large Message does not become slow, it deadlocks permanently: the receiver cannot advance past a
  set it cannot complete and the sender cannot advance past unacked data. Construction rejects any
  options combination that violates this.
- **`Send` is no longer purely local.** The same payload may be accepted for one Peer and refused
  for another, because the limit belongs to the remote. This must be documented, not hidden.
- **A misconfigured fragment size cannot fail silently**, because only reliable modes fragment — it
  stalls instead. Recently acked unfragmented packets combined with zero acked Fragments after
  several retransmission rounds is a signature nothing else produces, and closes the Session with a
  reason naming the cause rather than a generic timeout. We diagnose; we do not recover.
- **Head-of-line blocking is amplified.** A 57-Fragment `ReliableOrdered` Message blocks every later
  Message in that ordering domain until it completes. This is inherent to ordered delivery rather
  than to fragmentation, but fragmentation makes the stall proportionally longer. Bulk transfer and
  latency-sensitive traffic should not share an ordering domain; `ReliableUnordered` is the answer.
- **A single large Message can fill the send window alone.** The retention budget is sized in
  Fragments, not Messages, and `RetentionExhausted` is reachable from one `Send`.
- Fragmentation is decided once, at `Send`, when the consumer's span is copied into retention.
  Retransmits are byte-identical and fragment size cannot change mid-Message, including across a
  Resume.
- Partial Fragment sets survive `Interrupted` at no cost, because the receive window *is* the
  Session state that Resume preserves. "Resume at Message granularity" governs the negotiated resume
  point, not the discarding of buffered work.
- Two header shapes exist on the wire, so parsing branches and both must be covered by tests.
- Consumers on jumbo-frame LANs must configure the larger size themselves; nothing discovers it for
  them.

**Amended (issue #425).** The receiver enforces `MaxMessageSize` as well as announcing it. The receive
window is not the bound on a Message: it can be widened past the floor for a peer that cuts smaller
Fragments, and a whole record is bounded only by its Datagram. A completed Message above the limit is
discarded and reported with `OversizedMessage` (207). The Session stays open, and the Message stays
acknowledged and keeps its turn in send order, so nothing behind it stalls. The check runs when a
Fragment group is due for delivery, so a group whose turn never comes is never reported. The main cost
is that the sending end's check still covers reliable modes only: an unreliable Message above the
peer's limit returns `Sent` and is certain to be discarded on arrival.

**Amended (issue #449).** The sending end checks the peer's limit in every mode, which removes the cost
the #425 amendment names. An unreliable Message above the limit now returns `RemoteMessageTooLarge` and
spends nothing, the outcome a reliable Message already gets for the same limit. The one-Datagram cap is
checked first, so a Message over both caps still returns `MessageTooLarge`. The main cost is that the
refusal no longer follows from the mode alone: a consumer that read `RemoteMessageTooLarge` as
reliable-only has to handle it for unreliable Sends too.
