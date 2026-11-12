# The datagram Session wire format

A datagram is a **container**, not a Message. One datagram carries a header and then as many packet
records as fit, flushed once per Peer per Pump — which is a boundary the consumer's loop already
defines, so the protocol borrows it rather than inventing a timer. Every record is an ordinary packet
with its own sequence number, so ADR 0004's promise holds unchanged: the ack and retransmit machinery
operates purely on sequence numbers and does not know Fragments exist.

```
datagram header
  u32  session
  u32  base, u32 bits          ReliableOrdered   ack block
  u32  base, u32 bits          ReliableUnordered ack block
                                                          = 20 bytes
packet record
  u8   flags                   2 bits mode, 1 bit fragment, 5 spare
  u32  seq
  u16  length                                             =  7 bytes
  u16  fragIndex, u16 fragCount    fragments only          = 11 bytes
```

The alternative was QUIC's two-level form, where the datagram carries a packet number for
acknowledgement and Messages are frames inside it. It buys unambiguous RTT samples, loss measurement
across unreliable traffic, and a free AEAD nonce. We decline all three: there is no congestion
controller here to spend the precision on, we do not encrypt, and the ambiguity has a known remedy in
Karn's algorithm. The second number would cost bytes on every datagram and a per-Peer map from packet
numbers to the sequences they carried.

**There are four sequence spaces per Peer, one per delivery mode.** The reason is that a receiver
missing sequence 12 cannot know what mode 12 was — the flags of a packet that never arrived are not
available — so whatever shares a space shares head-of-line blocking whether or not that was intended.
With one shared space, ADR 0004's advice that "bulk transfer and latency-sensitive traffic should not
share an ordering domain; `ReliableUnordered` is the answer" would simply be false, because a hole made
by the bulk transfer would stall the unordered stream too. The space is identified by the mode bits
already in the flags byte, so independence costs nothing on the wire.

Plain `Unreliable` has a space and a sequence number like everything else. Four bytes per packet buys a
single record shape — no field present-or-absent by mode — duplicate suppression for a mode that would
otherwise deliver UDP's occasional duplicates silently, and a loss Counter derived from gaps, which is
the one thing genuinely given up by declining packet numbers. The honest difference between the two
unreliable modes is what happens to a stale packet, not what is on the wire.

## Acknowledgement and the horizon

Each ack block is a cumulative base plus a bitfield describing the 32 sequences above it. Both blocks
are always present and fixed-size. Presence flags would save six bytes on a 1200-byte datagram and turn
the datagram header into four parse shapes, on top of the two record shapes ADR 0004 already accepted.
Only the two reliable spaces are acked; the unreliable spaces are observed, never acknowledged, which
is why there are four spaces and two blocks.

**A sequence outside `[base, base+32]` is not yet judged.** Its retransmit timer does not start until
the horizon slides over it. Without this rule a Message larger than 33 Fragments would have its later
Fragments arrive, be undescribable while an early one is missing, and be retransmitted although nothing
was lost — reintroducing at scale the spurious-retransmit failure that cumulative-only acking was
rejected for. With it, the window stays unbounded, `MaxMessageSize` keeps ADR 0004's promise of
arbitrary size, and the ack stays eight bytes.

The rule is unconditional machinery and is implemented and tested as the normal path. It is tempting to
justify a 32-bit field by observing that a 32K Message is 28 Fragments and therefore fits inside one
block — but that is a prediction about someone else's program, and the width must not depend on one.
The workload-free argument is that the horizon makes every width correct: width trades bytes against
detection latency for far-ahead sequences, degrading by roughly one round trip rather than failing. 32
is chosen over 64 on who pays — the extra eight bytes would be paid by every Session in both
directions, including those that never lose a packet, while the benefit accrues only to Sessions with
many sequences in flight, which are the same Sessions best able to amortise header bytes across a
Pump's worth of coalesced Messages.

Sequences are `u32`. Comparisons wrap in any width; `u32` does not remove serial arithmetic, it removes
the hazard, because with a window bounded at construction the wrap can never fall inside it. A `u16`
sequence would make safety depend on the consumer's send rate and the network's willingness to hold a
stale datagram, and neither is ours to bound. Truncating a `u32` to 16 bits on the wire — QUIC's
approach — would save two bytes per Message by putting a reconstruction step in the receive path whose
failure mode is decoding a packet as the wrong sequence.

## Identity, and what it is allowed to prove

The session field is a receiver-assigned random `u32`: each end tells the other, during the handshake,
the value to stamp on datagrams sent to it. It is required rather than convenient, because a Session
that resumes over a replacement transport arrives from a different address, so source address cannot
demultiplex. Slot index plus generation was rejected: with 64 slots it is guessable in a handful of
tries, and the field addresses a live Session.

**The session field is a demultiplexing token, not an authenticator.** It raises the cost of blind
off-path injection — an attacker must guess 32 bits and land inside a live window — and it does nothing
against anyone who has observed a single datagram. We do not encrypt, so on-path attackers are out of
scope, and that is stated here rather than left for a random-looking field to imply. The direct
consequence is that **Resume cannot be authorised by the session field**; it carries a separate secret
established during the handshake, which is never used as a routing field.

Demultiplexing is therefore a lookup in a 64-entry map rather than an array index — one probe per
datagram, not per Message, since a Pump's traffic to a Peer arrives as one datagram.

## Control, and the handshake

Zero is reserved as a session value, which costs one value in four billion and buys a single branch at
the top of the receive path:

```
session == 0   → control datagram: u8 type, then body
session != 0   → look up; 20-byte header, then records
```

Pre-Session traffic needs its own shape whatever we decide, because before a Session exists there is no
receiver-assigned session id and no ack state, so the 20-byte header could only be filled with lies.
The split is therefore drawn at the Session boundary rather than at "control versus data": the
pre-Session types are Hello, Cookie, Hello-with-cookie, Accept, Refusal and Resume, while in-Session
control is an ordinary record with a control bit.

That earns Goodbye its machinery for free. As a record in a reliable space it is retransmitted,
ordered and acknowledged by the existing path, and it arrives after the Messages it follows rather than
racing them — which matters because the glossary makes its presence the thing that forbids a Resume, so
losing that race would be a correctness bug rather than a delay.

**The cookie round trip is always paid.** Challenging only under pressure would save one round trip on
an operation that happens once per Session, and would mean the defended path is exercised only in the
situation where being wrong is worst. The exchange is Hello, Cookie, Hello-with-cookie, Accept. Each
end announces in it the session id it wants stamped on datagrams addressed to it, and its own
`MaxMessageSize` as the receive limit ADR 0004 describes; fragment size is not announced, because that
ADR made it unilateral and per-direction.

The cookie is an HMAC over the source address, port, client nonce and an epoch counter, keyed by a
secret rotated on a thirty-second epoch, accepting the current and previous epoch — so effective
validity is one to two epochs and no clock is shared with anyone. **Sequence spaces start at zero**
rather than at a random ISN: the random session id already prices out blind injection, and a random
start would buy a second, weaker copy of the same defence at the cost of an untestable initial state.
**Refusal answers only a cookie-validated Hello**, never a bare one, or it becomes the amplification
vector the cookie exists to close.

## Time

The retransmit timer is adaptive — smoothed RTT and variance in the manner of RFC 6298, with Karn's
algorithm discarding samples from any sequence that was retransmitted, and exponential backoff above
it. A fixed RTO would need a constant that is right for a LAN and wrong for a satellite link, which is
a claim about the consumer's network that this design declines to make.

RFC 6298 brings its own constant — a one-second minimum — and both bounds are instead derived from
machinery we own. **The floor is the observed Pump interval**, because an RTO shorter than the gap
between Pumps cannot be acted on; the Reactor measures this itself and adapts to a loop running at 10Hz
or 1000Hz without being told which. **The ceiling is Linger**, because backing off past the point where
the Session would be abandoned achieves nothing.

Liveness is an idle-triggered reliable Ping: a Session carrying traffic proves itself continuously
through acks and needs no probe, and a quiet one sends a seven-byte control record that inherits the
adaptive timer, the backoff and the acknowledgement path. The liveness deadline and Linger are consumer
knobs with policy defaults, stated in the consumer's terms — how long before you are told the transport
is lost, how long before you lose the option to resume — because the consumer is the only party who
knows the answer. Construction rejects a deadline that does not exceed the RTO floor by enough for one
probe and one retransmission.

All timing reads come from `TimeProvider` timestamps and are monotonic, including the Pump-interval
measurement, which is otherwise exactly where a wall clock would slip in and let one adjustment fire
every timer at once.

## Windows, staleness, and version

The receive window is its own construction knob, defaulted to ADR 0004's floor of
`MaxMessageSize / fragmentSize` and validated against it. Deriving it outright would conflate two
unrelated jobs: the floor answers whether a maximum-size Message can ever complete, the window answers
how much may be outstanding at once, and the second depends on send rate and round-trip time, neither
of which is ours to guess. The window is **per space**, since sharing one buffer across spaces would
reintroduce through memory the head-of-line blocking that separate sequence spaces removed.

`UnreliableSequenced` delivers a packet only if it is **strictly newer** than the highest already
delivered in its space. This is the only rule that can be stated in a sentence with no exceptions —
*you never receive an older update after a newer one* — and a tolerance window would break that
sentence to recover reordered packets, which is the thing the mode exists to discard. Plain
`Unreliable` deduplicates within a bounded sliding window of 64 sequences, so suppression is bounded
rather than absolute; a duplicate delayed by more than that is delivered.

The protocol version must **match exactly**, and a mismatch produces a Refusal that names both versions
and carries its own `DisconnectReason`, distinguishable from a capacity refusal — otherwise the first
thing anyone meets on an upgrade looks like a load problem. Range negotiation was rejected because its
value is proportional to how independently the two ends are deployed, and every supported pair would be
permanent under ADR 0003. **Unknown flag bits are a protocol violation, not something to ignore**: with
exact version matching, leniency buys nothing, and it would cost the ability to ever trust a bit's
absence. The five spare bits stay genuinely spare, and a version bump unlocks them.

## Consequences

- **ADR 0006's "1.0 waits for the wire format to freeze" now has a date it can be met on.** Version 1 of
  the datagram Session protocol is what this ADR describes.
- **Retransmission is byte-identical at packet granularity, not at datagram granularity.** A retransmit
  resends the record, repacked into whatever container goes out next. ADR 0004's wording survives; the
  reading that a datagram is reproducible does not.
- **`Datagram` in the glossary stops meaning one Message inside a Session.** The wire unit and the
  protocol unit have separated, and the term now describes the container in a Session and the whole unit
  in Raw.
- **`RetentionExhausted` is reachable from message rate**, not only from ADR 0004's single large
  Message, because sequences in flight are bounded by the bandwidth-delay product regardless of the ack
  horizon.
- **The receive window is the library's main memory dial**, multiplying as `peers × 2 spaces × window ×
  fragmentSize` — a few megabytes at the defaults across 64 Peers, tens if set generously, preallocated
  at construction and never seen to grow. The arithmetic belongs in the options documentation.
- **Demultiplexing is a map probe, not an array index**, and the map is sized at construction like
  everything else.
- **The horizon rule, the adaptive RTO and the cookie epoch are each testable in isolation and invisible
  in normal operation**, which is the profile of code that rots. All three need tests that assert the
  mechanism, not just the outcome.
- The `DiagnosticId` members this ADR implies — a horizon deferral, an RTO backoff, a cookie rejection, a
  version refusal — join the open diagnostic-roster gap.

## Amendment 2026-11-06: a Resume from a new IP address is challenged

ADR 0021 adds `PathChallenge` (7) and `PathResponse` (8) to the pre-Session roster. A matched Resume from
the IP address the Peer is already sent to still moves the send address at once. One from any other IP
address moves it only after that address echoes a random nonce, because the secret proves who sent the
Resume but not that its source receives.

## Amendment 2026-11-10: a pending dial resends its handshake and times out

Issue #456. A dial sent its Hello once and resent nothing, so a lost Hello, Cookie or Accept left the dial
pending, holding its Slot, until the consumer closed it. A Hello discarded behind a flood of Hellos (ADR
0012, amendment for issue #428) had the same result after the flood stopped.

**What is resent.** A dial awaiting a Cookie resends its Hello. A dial awaiting an Accept resends its
Hello-with-cookie, echoing the stored cookie. Every copy keeps the dial's client nonce. A cookie is valid for
at least one epoch after it is minted, so a resend made one epoch or more after the Cookie arrived goes back
to a fresh Hello instead. A cookie resent near the end of that epoch can still arrive expired. The accepter
then answers with silence, and the next resend starts from a fresh Hello.

**Schedule.** The stream profile's re-dial schedule (`RedialSchedule`): the first delay is the observed Pump
interval, each later one doubles, the ceiling is this end's `Linger`, and each delay is drawn with full
jitter so diallers do not retry in step against a flooded accepter. The schedule is built on the first Pump
of a flight that has observed a positive Pump interval. Before a second Pump the interval reads `MaxValue`,
which would make the first delay the whole `Linger`. A flight that is answered moves on, and the next flight
starts its own schedule. A restart from a fresh Hello after an old cookie is a resend, not an answer, so it
keeps the current schedule. A flight is resent only when strictly more than its delay has passed, so a frozen
clock never resends and the tests that count exact datagrams on a frozen clock are unchanged.

**Timeout.** A dial still pending one `Linger` after `Connect` closes. No knob is added: the `Linger` is
already this end's declared bound on how long it waits for a silent remote, and it is the only `Linger` a dial
knows before the Accept announces the remote's.

**Close reason.** `TransportFailed`, not a new member. That member already covers a dial that could not
connect, which is what a stream dial reports when the operating system's own connect retries time out. A new
public member would be permanent for a condition an existing one already names.

**Accepting end.** No change. A repeated Hello-with-cookie whose (source, nonce) names an admitted Peer is
answered with that Peer's Accept, whether the Peer is Open, Interrupted or Closing, and a fresh Hello with the
same nonce yields a cookie that reaches the same entry. Once the accepted Peer has closed, its entry is gone,
and a later copy carrying a valid cookie is admitted as a new Peer. The dialler never reached the first Peer,
so it establishes against the new one. If the Accepts are lost for longer than the accepter's receive-idle
deadline, the accepted Peer goes Interrupted. It still answers, and the dialler's ordinary Resume brings it
back.

**Traffic.** The accepter's amplification bound holds per datagram: each Hello still gets one Cookie of the
same size, and a resent Hello-with-cookie gets at most one Accept or Refusal. Each resend spends the
accepter's admission cap like any other Hello. The computed delay doubles on every resend, so a flight makes about
log2(`Linger` / Pump interval) resends before the delay reaches `Linger`, and an expected two per `Linger`
after that.

**Round-trip seed.** Karn's rule now applies to ADR 0018's seed. A dial that sent its Hello-with-cookie more
than once is not seeded, because the Accept may answer any copy.

**Wire compatibility.** No datagram changes shape. An accepter on the current version already answers
repeated Hellos and repeated Hellos-with-cookie, so a resending dialler needs nothing new from it.

**Amended (issue #479).** Fragment size is now announced, which supersedes "fragment size is not announced, because that
ADR made it unilateral and per-direction". Hello-with-cookie and Accept each carry the announcing end's
`FragmentSize` and `ReceiveWindowFragments` after `AnnouncedLimits`, and `DatagramSessionProtocol.Version` goes
from 1 to 2. A version 2 Reactor refuses a version 1 Hello-with-cookie with `VersionMismatch`. A version 1 Reactor
discards a version 2 Hello-with-cookie as malformed, because its padding check runs before the version is read, so
that dial goes unanswered.
The fields, their legal ranges and the send rule are in ADR 0024. The main cost is that a mixed-version pair
can no longer connect.

## Amendment 2026-11-11: an accepted Peer resumes on data from the address it sends to

Issue #480. Only the dialling end sends a Resume, and only while it is itself Interrupted. An accepting end
that was interrupted alone, because the dialler's loop stalled or packets were lost in one direction only,
never received one. It kept delivering the dialler's Messages and closed with `LingerExpired` although the
path worked.

An Interrupted accepted Peer now also resumes when it receives a well-formed data Datagram for its Session
whose `Source` equals its current `Remote`, address and port both. It raises `PeerResumed`, counts one
`Resumes` and logs `ResumeMatched` with `Value0` 0.

A data Datagram from the same IP on another port does not resume the Peer. The data path never moves the send
address, so resuming on a new port would report a resume while sends still go to the old port. That port, and
any other address, still needs a matched Resume, plus the path challenge for a new IP (ADR 0021).

This is safe because the session id is a demultiplexing token, not an authenticator (see Identity). A forger
able to send from the held address with the session id can already keep the receive-idle deadline fresh, so
resuming on the same evidence grants nothing new.

Resume authorisation by secret, ADR 0021's path challenge and "the accepting end moves its send address only
on a matched Resume" do not change. The sentence "the dialler's ordinary Resume brings it back" in the
2026-11-10 amendment is no longer the only way back: an accepted Peer interrupted by lost Accepts also resumes
on the dialler's first data Datagram from its held address.
