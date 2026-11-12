# Fragment size and receive window are announced in the handshake

ADR 0004 made fragment size unilateral and per-direction: a receiver reassembles by index and count
and never needs to know the sender's choice. That is false. A receiver holds a Fragment in a
receive slot that is its own `FragmentSize` wide, so a Fragment wider than that cannot be held. The
datagram is admitted, the record is acknowledged, and the Fragment is dropped. On `ReliableOrdered`
the Space then stops delivering for the rest of the Session. On `ReliableUnordered` that Message is
lost. The sender's `Send` returns `Sent`, and neither end raises `PeerClosed` (issue #479).

Two correctly configured Reactors with different `FragmentSize` values therefore cannot exchange
large reliable Messages in one direction, and nothing at the handshake refuses the pairing. The
receive window has a second gap. A sender that cuts smaller Fragments than the receiver expects can
produce more Fragments per Message than the receiver's window floor allows.

## Decision

1. **Both handshake records that announce terms carry two more fields.** Hello-with-cookie and
   Accept each carry the announcing end's `FragmentSize` (u16, big-endian) and
   `ReceiveWindowFragments` (u32, big-endian), in that order, after `AnnouncedLimits` in
   `AnnouncedSessionTerms`. They are not added to `AnnouncedLimits`, because the stream profile
   shares that block and has no Fragments.
2. **Illegal values are refused on the interval path.** An announced `FragmentSize` outside 576 to
   65 507, or a `ReceiveWindowFragments` of 0, is illegal. It is handled on the same path as an
   illegal announced interval.
3. **`DatagramSessionProtocol.Version` goes from 1 to 2.** ADR 0009 matches versions exactly, so a
   mixed pair cannot connect instead of losing data silently. A version 1 dialler gets a
   `VersionMismatch` Refusal from a version 2 Reactor. A version 2 dialler gets no answer from a
   version 1 Reactor: the version 1 shape check requires zero padding after its own, shorter
   Hello-with-cookie body, so it discards the longer datagram as malformed before reading the
   version, and the dial ends as any unanswered dial does.
4. **A Peer sends at the smaller of the two sizes.** Each Peer's send Fragment size is
   `min(local FragmentSize, remote FragmentSize)`. It is fixed once, when the Peer reaches
   Established, and does not change across a Resume. It is also the single-Datagram cap of the
   unreliable modes, so an unreliable Message wider than it returns `MessageTooLarge`.
5. **`Send` refuses a Message the remote window cannot hold.** `Send` on a reliable mode returns
   `RemoteMessageTooLarge` when the Message's Fragment count at the send size exceeds the remote
   `ReceiveWindowFragments`. This covers a sender whose own `FragmentSize` is smaller than the
   receiver's, which cuts more Fragments than the receiver's window floor allows.
6. **A Fragment wider than the receiver's own `FragmentSize` is a protocol violation.** The
   receiver closes the Peer with `ProtocolViolation`. The check runs in the same pre-classification
   pass as the `fragmentCount` check, so nothing in that datagram is acknowledged. After point 4
   only a non-conforming peer can trigger it.
7. **Receive memory does not change.** Retention slots stay the sender's own `FragmentSize` wide.

## Alternative rejected

**Hold a Fragment set by byte offset in a per-set buffer limited by `MaxMessageSize`.** This would
let a receiver accept any Fragment width. It fails for three reasons:

- The receiver cannot place a Fragment without knowing the sender's size, and only a non-last
  Fragment reveals it.
- One `ReliableUnordered` Space can hold several incomplete sets, so a buffer per set breaks ADR
  0012's memory bound unless a new allocator shares one region.
- It does not fix point 5's case, because the receive window still bounds the Fragment count.

## Consequences

- **A peer with a smaller `FragmentSize` makes each Message need more retention slots.**
  `RetentionExhausted` arrives sooner, and it is returned straight from `Send`.
- **A mixed-version pair can no longer connect.** Only a version 1 dialler is told why, with
  `VersionMismatch`. A version 2 dialler to a version 1 Reactor sees an unanswered dial.
- **`Send` depends on two remote values.** The same Message may be accepted for one Peer and
  refused for another because of the remote `ReceiveWindowFragments`, as it already may be for the
  remote `MaxMessageSize`.
- **The stall signature in ADR 0004 no longer has a cause.** A conforming pair cannot produce a
  Fragment the receiver cannot hold.
- **The stream profile is unchanged.** `AnnouncedLimits` keeps its shape.
