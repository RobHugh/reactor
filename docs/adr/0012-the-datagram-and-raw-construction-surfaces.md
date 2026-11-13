# The datagram and Raw construction surfaces

ADR 0011 settled the stream profile's surface and stated a rule it declared retroactive: a knob may
default only when the default is derived, and otherwise it is required. This ADR applies that rule to
the other two profiles, and in doing so has to give it a boundary, because applied literally it sweeps
in every number in the design.

Listing the two surfaces also found knobs that belong to **all three** profiles and were missing from
ADR 0011's inventory: the **frame arena's size** and the **per-Handle read cap**, both decided at L0 in
the first session and never sized, and `MinimumLevel` from ADR 0005. They are settled here for
everyone rather than for one profile.

**Raw has no knobs of its own beyond one buffer.** That is not an omission; it is what "invents nothing
on the wire" looks like from the constructor.

## Knobs and constants are different things

The rule from ADR 0011 governs **knobs**, and a knob is a value that trades the *consumer's own*
resources — memory, latency, tolerance. A **constant** is fixed by the protocol design, agreed by both
ends or invisible to both, and changed only by a version bump. Constants are argued once in the ADR that
introduces them and are not configurable at all.

The design has been making this distinction since ADR 0009 without naming it. The 32-bit ack width, the
64-entry duplicate window, four sequence Spaces, zero as the reserved session value, the twenty-byte
header — none is a knob, none was ever going to be, and nobody has felt the absence.

The cookie epoch is what forced the distinction into the open. ADR 0009 fixed it at thirty seconds with
no citation, so the rule read literally would make it a required argument — asking a consumer to choose
between a replay window and handshake failures on slow paths, using information they do not have, about
a mechanism they cannot observe. It belongs with the constants: it is one end's private key-rotation
schedule, and the reason it needs no agreement is that we accept the current and previous epoch, which
is what makes effective validity one-to-two epochs rather than a number both ends must share. It stands
on ADR 0009's own terms, its real requirement being to comfortably exceed a handshake round trip, which
it does by a wide margin on any path where a Session is viable at all.

Without this boundary the rule reads as "every number needs provenance", which would have made ADR 0009
unwritable. With it: **constants are argued once and frozen by the version; knobs are the consumer's
resources, and their defaults must be derived.**

## The shared value

The universal knobs are **composed** into each options type as one member, not inherited from a base.
ADR 0011 rejected a shared base and its argument was specific: a base is how a profile inherits a knob
it does not use. Composition cannot do that — a member is either present and meaningful or absent — and
there is no inheritance, no virtual surface and nothing protected. What it describes is real and
singular: the budget L0 spends inside a Pump, and L0 is one implementation behind all three profiles.
Repeating eight properties three times would be duplicating the description of one object.

It carries capacity, the admission cap, the arena size, the read cap, `TimeProvider`, and the Sink with
its level.

**Nothing protocol-specific may ever enter it.** `MaxMessageSize` is the standing test case: it looks
universal, it appears in both session profiles, and it must stay out, because Raw has no Messages. The
moment the shared type admits one knob that two of three profiles use, it has become the base class
ADR 0011 refused.

## The arena, and the read cap that falls out of it

**The arena is a knob defaulted to its floor.** The floor is one maximum deliverable unit —
`MaxMessageSize` in either session profile, the receive buffer in Raw, since a Segment is as large as
one read. Below it that unit can never be delivered at all, which is the permanent-deadlock shape of
ADR 0004's window floor rather than a slowdown.

This is the third knob sized by this exact rule, after the datagram receive window and the stream
receive buffer, and three knobs sized one way is a rule a consumer can learn once. Defaulting to the
floor also keeps the discipline: one maximum Message per Pump is the smallest correct answer, and every
increase above it is the consumer telling us something about their traffic we were not entitled to
guess. **The floor is profile-specific, so the validation lives in each Reactor's constructor** — the
shared type carries the number, the profile knows what it must exceed.

**The per-Handle read cap stops being a knob and becomes a derived byte fair-share.** The original
decision was "at most N Messages per Handle per Pump", and the count unit has the flaw that sank
retention-by-count: Messages differ by orders of magnitude, so a fair share by count is not a fair share
of the arena. The rule is instead that no Handle may consume more than `arena ÷ capacity` bytes in one
Pump, with the escape that a Handle may always deliver **one whole Message** even when that exceeds its
share, since otherwise a large Message is unreachable whenever capacity is high.

It composes with the arena default in a way that was not designed and is worth recording: at the default
arena the share is smaller than any Message, so the escape fires every time and the behaviour is exactly
**one Message per Handle per Pump** — the fairest possible schedule, and the original design's intent
arrived at without a constant. A consumer who buys a larger arena gets a proportionally larger share.
The read cap is a function of how much arena they paid for, which is the only thing it could honestly be
a function of.

The escape does not weaken the bound. Reading still stops when the arena fills, so a Handle is merely
not blocked *by its share*; it can still be blocked by the arena. The worst case remains one arena,
never `capacity × MaxMessageSize`.

Removing the cap entirely was rejected: the rotating start offset only spreads unfairness *across*
Pumps, so within any single Pump the first Handle could take the whole arena, and a loud Peer would
starve a quiet one every Frame in a way the consumer sees as latency they cannot attribute.

## One admission cap, and the rule behind it

Raw and the stream profile bound admission by capping accepts. UDP has no accept: a Hello is a datagram
on the one bound socket, and processing it costs an HMAC to issue a cookie or an HMAC to verify one, so
a flood of Hellos is unbounded work inside one Pump on the consumer's thread.

**The read cap does not cover this**, and the reason is not obvious: that cap is a share of the arena,
and **control traffic never reaches the arena**. Hellos, cookies, acks and Pings are consumed internally
and never become Events, so a bound derived from arena consumption bounds none of them. The cookie
bounds *state* — nothing is allocated until a validated Hello-with-cookie arrives — but not *work*, and
work on the consumer's thread is the resource this library sells.

So there is **one admission cap in the shared value, defaulted to capacity**, covering TCP accepts and
UDP handshakes alike. They are the same knob wearing different clothes on different transports, and two
names would drift. The default has provenance: one Pump never spends more admission work than a full
Reactor's worth, and the excess waits in the OS backlog or the socket buffer — or is dropped, which for
UDP is normal behaviour costing us nothing.

Behind all three caps is one rule worth more than any of them: **every per-Pump cost a remote party can
inflate is bounded at construction.** Reading is bounded by the arena share, admission by the admission
cap, handshakes by the same. Three mechanisms, one reason, and a checklist for anything added later —
because the failure they prevent is the same failure each time and it is invisible until someone points
a load generator at it.

**Amended (issue #428).** The datagram receive loop broke this rule: it read until the socket was empty,
and the arena share does not bound that, because control traffic and refused datagrams never reach the
arena. `DatagramSessionOptions.MaxDatagramsPerPump` is the fourth mechanism. It limits the reads one Pump
makes on each bound socket, so a Pump's read work is at most the limit times the bound sockets. It sits on
the datagram options because it is protocol-specific: Raw and an established stream Peer already make one
read per Pump. The default has the same provenance as the admission cap, a full Reactor's worth: one
receive window for every Space of every Peer, plus the admission cap. The excess waits in the socket buffer.
That wait is the main cost, and it means an overflow no longer costs nothing. A flood can displace a Hello
that would have been read and counted, with no Counter seeing it. A live Peer's datagrams also wait behind
the flood, for up to the queue depth divided by the limit in Pumps, and a long enough wait looks like
silence to the receive-idle deadline. A dial suffers most. A Hello read after the admission cap is spent is
discarded. A pending dial resends its handshake on a jittered backoff (ADR 0009, amendment of 2026-11-10), so a
dial behind a backlog of Hellos completes once the backlog has cleared, provided that happens within the
dialler's Linger. The default is sized for traffic rather than latency and reaches
hundreds of thousands of reads with a large `MaxMessageSize`, so a consumer that needs a shorter Pump sets
it lower and accepts a longer wait.

**Amended (issue #482).** The stream profile held every accepted connection until its first handshake
record arrived, with no limit on how many and no deadline. A remote that connected and sent nothing added
a socket to every Pump's readiness check for as long as it kept the connection open. The admission cap
bounds how many connections one Pump accepts, not how many stay held. Two derived bounds close this:

- **A held connection has a deadline of this end's `SendIdleInterval` × `MissedHeartbeats`, fixed at
  accept.** It is the silence an established Peer is allowed before it is lost, and a dialler sends its
  Hello as soon as TCP connects, so a conforming dialler needs far less. Bytes that arrive do not move the
  deadline, or a sender of one byte per deadline would hold its socket for 41 deadlines. A connection
  still held at the deadline is closed with nothing sent and reported as `PendingAdmissionExpired`. The
  check runs after the readiness pass, so a record already in the socket buffer is read before its
  connection can expire.
- **At most `Capacity` connections are held at once.** Each held connection is a claim on a Slot, a fresh
  dial asking for one or a Resume reclaiming one, and a Reactor has `Capacity` Slots. At the limit the
  Reactor stops accepting for that Pump, and the rest wait in the OS backlog, as the excess of the
  admission cap does. Each declining counts as an `AdmissionDeferral`, and the Pump logs one
  `PendingAdmissionLimitReached`. The admission cap was rejected as the source of the limit because it
  can be set to 1, and one silent connection would then stop every accept until its deadline.

The cost is that remotes holding `Capacity` silent connections delay every new connection by up to the
deadline. ADR 0014's per-address limit keeps one address from doing that alone. A Reactor whose
`SendIdleInterval` × `MissedHeartbeats` is shorter than a dialler's Pump interval closes that dialler
before its Hello arrives.

## Datagram retention: slots, not a ring

**Retention is a byte bound, per Peer, per reliable Space.**

The stream profile's ring does not port. There, a head pointer works because TCP's guarantee makes one
cumulative number a complete description of what arrived. Here, acknowledgement is a base **plus a
bitfield**, and `ReliableUnordered` releases out of order by design, so a head pointer would either
stall behind a hole or release data still unacknowledged.

What is available instead is a property the stream profile lacks: **everything retained is at most one
fragment**, because Messages are cut into packets before being retained. So the structure is a pool of
uniform `fragmentSize` slots, out-of-order release is free, and the byte bound and the slot count are
the same number divided by a constant we already own. The bound is stated in bytes rather than slots so
that both profiles' backpressure is denominated in the same unit, and so it does not move behind the
consumer's back when `fragmentSize` changes.

**Per Space** is the same argument ADR 0009 used to make the receive window per Space, and it holds just
as exactly on the send side: a shared pool would let a large `ReliableOrdered` transfer consume all of
it and start failing `ReliableUnordered` Sends, reintroducing through memory the interference separate
sequence Spaces exist to prevent. Protecting receive and leaving send breachable would be a guarantee
half-built. Only the two reliable Spaces have retention at all, since nothing unreliable is ever
retransmitted.

The memory documentation therefore has two lines rather than one — `capacity × 2 × window ×
fragmentSize` for receive and `capacity × 2 × retention` for send — with `fragmentSize` in one and not
the other, because the window counts packets and retention counts bytes.

## Datagram liveness adopts the stream profile's shape

ADR 0009 called the liveness deadline and Linger *"consumer knobs with policy defaults"*. The first half
survives; **"policy defaults" is what ADR 0011's rule forbids, and that sentence is superseded** on the
same grounds ADR 0001's "default 64" was — it was written before the rule existed, and the rule is
retroactive.

The shape is ADR 0011's, unchanged: send-idle interval required, missed-heartbeat count defaulted to 3
citing RFC 4271, deadline derived as `peerSendIdle × count`, with the interval and Linger announced in
the handshake and the shorter Linger governing.

The datagram profile could have derived the deadline from its adaptive RTO, which the stream profile
cannot do. It was rejected because it conflates two different things: **the RTO answers "when do I
resend the probe", the count answers "how many probes before I give up"** — one is a fact about the
network and must adapt, the other is a statement of the consumer's tolerance and must not. Deriving the
deadline from the RTO makes the tolerance stretch exactly when the path degrades, since that is when the
RTO is backing off by design, so the consumer waits longest precisely when they most wanted to be told.
It would also tie a consumer-facing promise to an estimator Karn's algorithm is already discarding
samples from. The Ping still inherits the adaptive timer for its *retransmission*, so path-adaptivity
stays where it belongs.

Uniformity is the second argument and not a weak one: a consumer moving between the session profiles
should not relearn liveness. It also makes ADR 0009's other validation structural rather than
arithmetic — *"construction rejects a deadline that does not exceed the RTO floor by enough for one
probe and one retransmission"* is a consequence of the count being at least 2, because the RTO floor is
the Pump interval and the send-idle interval necessarily exceeds one Pump.

The rest of the datagram surface passes the rule unchanged. Fragment size's 1200 is QUIC's assumed floor
under the 1280-byte IPv6 minimum MTU, and its range `[576, 65507]` is RFC 791's minimum reassembly and
UDP's maximum payload — citable, all of it. `MaxMessageSize` is required, as on the stream profile. The
receive window keeps its derived floor.

## Raw: one buffer, and it is not per Link

**Raw does not reassemble.** L1 keeps a persistent per-connection buffer because a partial Message tail
cannot live in an arena wiped every Pump — but Raw delivers Segments, and a Segment is by definition
whatever arrived. Nothing is held across a Pump, so the buffer is scratch space between the socket and
the arena rather than state.

It is therefore **one buffer per Reactor, reused for every Link**, not one per Link: inside a
single-threaded Pump only one Link is being read at any instant, so multiplying it by capacity buys
nothing. Reading directly into the arena's free space was rejected because it makes what counts as an
oversized datagram depend on how full the arena currently is — the same 9 KB datagram delivered or
truncated according to what arrived before it in the same Pump. A protocol limit that moves with
unrelated traffic is not a limit. The per-read copy is bought deliberately in exchange for a fixed
truncation boundary.

**It defaults to 65 507 bytes**, the largest a UDP datagram can be, which is derived rather than chosen
and has a property worth having: **truncation is impossible unless the consumer opts into it.** The
oversized-datagram diagnostic ADR 0008 introduced is unreachable at the default and reachable only for a
consumer who deliberately lowered the bound, which is the right way round for something Raw reports
about a peer it makes no claims about. Because it is one buffer and not `capacity` of them, 64 KB is a
rounding error against the arena and the per-Peer allocations — which is what makes a safe default
affordable here and nowhere else.

**Amended (issue #477).** 65 507 bytes is the largest UDP payload over IPv4 only: 65 535 less the 20-byte
IPv4 header and the 8-byte UDP header. An IPv6 payload length excludes the 40-byte fixed header, so an
IPv6 UDP datagram can carry up to 65 527 bytes. At the default, truncation is unreachable over IPv4, but
an IPv6 datagram of 65 508 to 65 527 bytes is reported as `DatagramTruncated` and never delivered. Both
profiles share the 65 507 bound, so the same holds for the Datagram Session's scratch buffer. The read
buffer is one byte longer than the bound, and a read that fills it is the truncation signal.

## Diagnostics are installed at a level

**The Sink and `MinimumLevel` are one argument, not two.** Both present or both absent. Absent means
diagnostics are off entirely, which is derived rather than chosen: with no Sink there is nowhere for a
record to go, so no level can matter. Present means the consumer stated both, and neither was guessed.

Two independent properties would have needed a default level, and a default level is a claim about how
much diagnostic volume the consumer wants, which nothing derives. Dropping the level and leaving it to
the Sink's `IsEnabled` would discard the cheap pre-gate ADR 0005 introduced deliberately, putting a
virtual call on the path of every record that was going to be suppressed anyway.

This is the third time in two sessions that a default with no provenance turned out to be a sign that
two things had been separated which should not have been — after the receive-idle deadline and the
retention message count.

`Counter` needs nothing: ADR 0005 made counters pull-only and always on, costing one add.

`TimeProvider` defaults to `TimeProvider.System`. The framework supplies it, and the standing constraint
is only that reads be monotonic, which `GetTimestamp` satisfies regardless of who provides it.

## Consequences

- **ADR 0009's "policy defaults" for the liveness deadline and Linger is superseded**, and its cookie
  epoch is reclassified from an unjustified default to a protocol constant, which is what it always was.
- **ADR 0008's `MaxDatagramSize` disappears as a separate concept.** It was deferred with the
  wire-format gap; on Raw it is simply the scratch buffer's size, so there is nothing left to settle.
- **ADR 0011's "no shared base" survives but is narrowed to inheritance**, and its inventory was
  incomplete — the arena, the read cap and `MinimumLevel` were missing from it.
- **The knob-versus-constant distinction is the boundary ADR 0011's rule was missing** and should be
  cited whenever a new number appears. A number that both ends must agree on is a constant by
  construction, because a knob neither end can change unilaterally is not a knob.
- **The per-Handle read cap stops being a knob**, so the L0 decision it came from now reads as a
  behaviour rather than a setting, and its tests assert the fair-share arithmetic rather than a
  configured N.
- **Three per-Pump cost bounds now exist for one reason**, and the rule behind them belongs in the
  documentation as a property of the library rather than as three separate notes.
- **Every default in the library now has a stated provenance or is a constant.** That is a claim worth
  auditing at 1.0 rather than assuming, and it is cheap to check because the list is short.
- Binding and dual-stack, and the diagnostic roster, remain the open gaps. The roster gains the
  admission cap being reached on both session profiles and the read-share being hit.
