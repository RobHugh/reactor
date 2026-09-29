# Duplicate suppression is its own window, and an ack rides a Datagram

Two decisions taken while implementing the four delivery modes (issue #44), both about the gap
between what `docs/delivery-modes.md` promises a consumer and what the machinery underneath it
happened to do.

## Plain `Unreliable` suppresses duplicates in its own 64-entry window

`docs/delivery-modes.md` promises two things about plain `Unreliable`: duplicates are suppressed
within 64 sequences, and a late arrival is delivered. The per-Space receive window could keep
neither.

Its capacity is `ResolvedReceiveWindowFragments`, derived from the consumer's `MaxMessageSize` and
fragment size. Suppression sized from it would be 64 wide only for consumers who happened to choose
options that made it so, and the promise in the table is not conditional on anyone's options — the
64 is a protocol constant both ends agree to without negotiating it. Worse, that window refuses
everything below its base outright, so a repeat delayed past 64 sequences would be discarded rather
than delivered, which is the opposite of the row that says late arrivals arrive.

So plain `Unreliable` gets `UnreliableDuplicateWindow`: 64 entries, one `ulong`, the newest sequence
plus a bit per sequence below it. It decides delivery for that Space alone. The receive window still
runs there — it is what counts loss from the gaps it slides past — but it no longer decides whether
a Message reaches the consumer.

**The alternative was forcing that Space's receive window to exactly 64.** It was declined because
the two windows answer different questions. The receive window exists to measure what the network
did, and its width is a memory decision the consumer makes; suppression exists to keep a promise, and
its width is a protocol constant. Tying them together would mean a consumer who lowered
`ReceiveWindowFragments` to save memory silently weakened a guarantee, with nothing in the API
suggesting the two were connected. It would also have changed the receive arena's sizing for one
Space out of four.

**64 because that is one `ulong`.** The whole window is one shift and one test, it allocates nothing,
and the width cannot quietly drift from the constant the protocol names, because the type that holds
it has exactly that many bits.

### What this does to the duplicate counter

A repeat is refused once and counted once, but not always on the same figure. A repeat the receive
window refuses — the common case, since a repeat usually arrives below a base that has already passed
it — lands on `OutOfWindowPackets`. A repeat that window admitted and only the suppression window
recognised lands on `DuplicatePackets`. Both descriptions are true of such a record; what matters is
that it is counted once, and neither figure double-counts. A repeat delayed past 64 sequences raises
neither, because it is delivered: these figures count what was refused, not what was repeated.

## An ack rides a data Datagram, and #224 stands unchanged

Both ack blocks are always present in a datagram header, but a datagram is only sent for a Peer that
has records queued (ADR 0009's one-datagram-per-Peer-per-Pump flush). A receiver that sends nothing
therefore never acknowledges anything, and the sender retransmits every reliable sequence on every
deadline until some reverse traffic appears to carry an ack home.

**This is accepted rather than fixed here.** The fix is a datagram sent for a Peer with nothing to
say, which is a Ping — slice 8's, where liveness is designed as a whole rather than bolted on as a
side effect of acknowledgement needing a ride. Inventing a header-only keepalive now would put a
second timer in the protocol, one this slice has no argument for the period of, and would pre-empt
the slice that owns the question.

The consequence is bounded and visible: retransmission is capped by Linger as the RTO ceiling, and
the traffic it produces shows up on `PacketsRetransmitted`. A genuinely one-way reliable flow is also
not the shape this profile is built around — a Session that receives has acks to send on its own
records.

**Issue #224's acceptance criteria stay exactly as written.** They describe the `ReliableOrdered`
promise — every Message arrives exactly once, in send order, under scripted drops and reordering —
and that promise is true today for any exchange with traffic in both directions. Weakening the
ticket to match the one-way case would record this limitation as though it were the guarantee, which
it is not. The test gives the ack reverse traffic to travel on, which is what a real Session has;
what remains is a note for slice 8 rather than a change to what the mode promises.

## Amendment 2026-09-29: a Peer closing on a received Goodbye sends one final ack-only datagram

The decision above stands: an ack rides a data Datagram, and a Peer with nothing queued sends none.
Slice 8.5 (issue #57) adds one exception. A Peer that closes on a received Goodbye sends one final
datagram even with nothing else to say, carrying the header and both ack blocks and no records. Its Slot
is freed right after that flush, so no later datagram could carry the ack of the Goodbye, and the sender
would otherwise retransmit the Goodbye until its Linger ran out.

The datagram is the Peer's ordinary flush, not a second one. `FlushPeer` sends it in the same Pump
the Goodbye is delivered, alongside any record queued for that Peer, so ADR 0009's once-per-Peer-per-Pump
rule holds. The Peer is closed after that flush, in the same Pump. The exception covers only a Goodbye
close. A Peer receiving ordinary traffic still sends nothing until it has records of its own.

## Amendment 2026-09-29 (second): a closed Session re-acks a resent Goodbye

The final ack-only datagram above can itself be lost. The sender then resends its Goodbye to a Session id the receiver has freed, which is discarded as unknown, and the sender ends `LingerExpired` although the remote closed cleanly. To cover this, a Peer that closes with `PeerGoodbye` leaves a closed-Session record for one effective Linger. It holds the local session id, the remote endpoint, the remote's session id, both reliable ack blocks and a close timestamp. It holds no capacity Slot.

A well-formed data datagram naming that id, from that same remote endpoint, inside the Linger, and carrying at least one reliable record, is answered with one header-only datagram (20 bytes, never larger than the datagram that triggered it) carrying the stored ack blocks. Each reliable record in the trigger is first admitted into the stored blocks, so a Goodbye the closed end never received is acknowledged as well. No event is raised, no Slot is taken, no Resume is honoured (a Resume still gets `ResumeUnmatched`) and no per-Peer counter moves. The reliable-record condition stops two closed ends answering each other's replies.

Records live in a ring of Capacity entries and are purged lazily on lookup. When the ring is full the oldest record is overwritten, so more than Capacity Goodbye closes inside one Linger can lose the oldest re-ack. That sender falls back to `LingerExpired`, as before this amendment. Nothing is allocated per datagram.

The same record covers a Peer that closes with `LocalClose` after its own Close, because the remote's Goodbye may not have reached it yet or its ack may have been lost (both ends closing at once). Such a Peer that has delivered the remote's Goodbye during its own Close sends the ack of it as a header-only final flush, like the `CloseDue` case, and sends it at once when it closes in the same Pump. The Peer does not raise `PeerClosing` or `PeerGoodbye`: an end whose own Close is in progress reports `LocalClose` or `LingerExpired`.
