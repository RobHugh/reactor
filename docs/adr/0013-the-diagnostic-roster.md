# The diagnostic roster

ADR 0005 fixed the *shape* of a diagnostic — a closed `DiagnosticId`, a level, a subject, two numeric
slots, a static formatter — and warned that every member is a permanent public API addition that can
never be withdrawn. Four ADRs since have deferred candidates into this gap: ADR 0008 an unreachable, an
oversized datagram and a link failure carrying a `SocketError`; ADR 0009 a horizon deferral, an RTO
backoff, a cookie rejection and a version refusal; ADR 0011 the re-dial, the resume match and its
failure, the accept cap and the receive-idle expiry; ADR 0012 the admission cap and the read share.

Deciding those one at a time would have settled nothing about the next one. So this ADR is four rules
and the roster they produce, in that order.

## What earns a member

**Events say what happened to a Peer or a Link. Counters say how often. Diagnostics say why this one.**

A record therefore earns a `DiagnosticId` only if it carries something a count cannot — an identity, a
reason, or a number specific to that occurrence. **If the only interesting thing about an occurrence is
how often it happens, it is a Counter and gets no member.**

The constraint being respected here is not scarcity for its own sake. `DiagnosticId` only grows, so the
expensive mistake is not a missing member — that is one additive change later — but a member shipped for
something that turned out to be pure volume, which we are then formatting, documenting and adapting
forever. A generous roster filtered at runtime by `MinimumLevel` was rejected on exactly that ground:
the level filters volume, and cannot un-ship an enum member, so it manages the wrong cost.

**A rejected candidate does not automatically become a Counter.** ADR 0005's snapshot is read by field,
so every field in it is as permanent as an enum member and every increment is an add on the hot path.
Counters have their own test: **a counter earns a field only if it is not derivable from the other
counters, and its rate or ratio names a condition the consumer could act on.** Non-derivability is
objective in a way an argument about usefulness never is — "datagrams discarded" and "datagrams
received" already give the discard ratio, so a discard-ratio field is not a decision to weigh but
provably redundant. A candidate failing the record test and the counter test simply does not exist.

## A member names a condition, not a category

Members are **flat: one per condition.** A malformed datagram, a duplicate, an out-of-window packet, a
rejected cookie and a refused version are separate members, not one `DatagramDiscarded` with a reason in
`Value0`.

The deciding argument is the gate rather than the roster size. ADR 0005's pre-gate is a `MinimumLevel`
comparison, so it **filters by level, not by id** — and grouping forces every reason in a category to
share one level. These plainly do not: a malformed datagram from an unknown Source is evidence someone
is probing you, while a duplicate inside the window is routine. Grouped, you either raise the duplicate
until the level means nothing, or lower the malformed until it can only be seen inside the volume you
enabled to find it. Grouping does not even save the formatter work — it switches on `Value0` instead of
on the id — so the complexity moves and the level distinction is lost for nothing.

The line that keeps this from unbounded growth: **a reason we invented gets its own member; a code we
are passing through from outside gets a value slot.** Raw's link failure is one member with the
`SocketError` in `Value0`, because the OS error space is not ours to enumerate and mirroring it would be
an unbounded, partly platform-specific, permanent commitment.

## What a level means

A level is evidence about a category of cause, not a severity ranking and not a volume tier:

| Level | Means |
| --- | --- |
| `Error` | The local process is in a state the consumer must fix. |
| `Warn` | The consumer can act on this, and nothing is locally broken. |
| `Info` | A lifecycle transition that never became an Event. |
| `Trace` | Protocol machinery working correctly. |

**Anything a remote party can cause at will can never be `Error`.** This is ADR 0008's rule — anything a
remote can generate at will goes to the Sink, never the event stream — applied one level further down.
Having refused to let a stranger consume the consumer's Drain, it would be strange to let one fill their
error log instead. A malformed datagram is *normal* on a public port; making it an `Error` wires every
consumer's alerting to a condition anyone can trigger, and the first thing they do is stop trusting the
level. This is why `LinkFailed` is `Warn` despite genuinely being a failure: a reset causes it, and a
reset is a stranger's prerogative.

`Warn` is deliberately wider than "a remote broke the protocol". Drafting the roster produced a whole
class the narrow reading had no slot for — **a configured bound was hit**: the admission cap, the arena,
the read share, a lowered Raw buffer truncating. Nothing is locally broken and no remote misbehaved,
because connecting and sending are permitted. But a bound is a number the consumer chose, which makes it
the *most* actionable thing the library can report. The two causes differ in origin and not at all in
what the consumer does about them, and a level's only job is to answer "do I want to see this". A fifth
level was rejected: it costs a public change to ADR 0005's four for a distinction the formatter's
wording already carries, and five levels is where consumers stop remembering the order.

A prediction of the rule, which the roster confirms: **`Error` is empty.** A library that owns no
threads, allocates at construction and treats bugs as throws has almost nothing that is both non-fatal
and locally broken. `SinkFaulted` would have been the archetype and is a counter, because there is by
definition nowhere to send a record about the Sink.

## One enum, in reserved blocks

Separate enums per profile were foreclosed by ADR 0008, which committed `IReactorDiagnostics`,
`DiagnosticFormatter` and `ReactorSockets.Logging` to staying single and made the subject
profile-neutral. Since the record carries its id as a typed field, per-profile ids mean either a generic
record or three record types, and either way three facades.

The single enum has **explicit numeric values in reserved blocks**: common `1–99`, Raw `100–199`,
datagram session `200–299`, stream session `300–399`. Explicit values are not optional for a permanent
public enum — implicit ordinals mean inserting a member alphabetically silently renumbers everything
after it, breaking any consumer who persisted an id. Once the numbers are written down, blocks are free:
a Raw-only Sink range-checks instead of listing members, and a member added in two years lands beside its
relatives, so reading the enum stays a way of learning what a profile does rather than a record of our
development order.

The block that justifies the scheme is **common** — the Sink, the arena and the admission cap exist in
all three profiles, so a strict per-profile split would have been wrong even had we wanted one. That is
ADR 0012's composed shared options value showing up in a second place.

**There is no `None` or `Unknown` member and zero is not used.** Every record has a cause by
construction; a zero value would exist only to be a bug that formats as text.

## The subject includes a Source

`DatagramMalformed`, `CookieRejected` and `VersionRefused` fire on datagrams from a party that is *by
construction* not a Peer — that is the whole reason ADR 0009 has a cookie. With the subject limited to a
Peer or a Link, all three reduce to "it happened", which a counter already said, so all three failed the
admission rule above.

**The subject becomes a small tagged value: none, Peer, Link, or Source**, the Source holding a full
endpoint. Widening the subject rather than adding a parallel field keeps the invariant that a record has
exactly one subject, so the formatter and every adapter stay a single switch — and it is structurally
honest, because a Source is not a degraded Peer but a different kind of thing, and the tag says so at the
type level.

Packing the address into the value slots was rejected: an IPv4 endpoint fits in one `ulong`, but IPv6
consumes both slots for the address alone and still has no room for the port. That would make this the
only place in the library where IPv6 is second class, immediately before the binding and dual-stack gap.

**What makes the growth affordable is a consequence of the admission rule that was not visible when ADR
0005 sized this struct.** That struct was designed for a hot path this ADR has emptied: everything
per-packet is now a counter, and every surviving record is rare, built only after the `MinimumLevel`
comparison passes, living on the stack and passed by `in`. Roughly 24 bytes to roughly 56, on an event
that by construction does not recur, is a different trade from the one ADR 0005 refused.

**The formatter renders a Source as a claim, not a fact.** `CONTEXT.md` commits to a Source being
unauthenticated by construction — a hint about origin, never an identity — so the wording must not invite
a consumer to firewall on it, because an attacker chooses what it says.

## The roster

Value slot meanings belong in XML docs on each member, per ADR 0005; they are given here to fix intent.

### Common

| # | Member | Level | Subject | `Value0` / `Value1` |
| --- | --- | --- | --- | --- |
| 1 | `AdmissionCapReached` | `Warn` | none | cap / Admissions declined |
| 2 | `ArenaExhausted` | `Warn` | Peer or Link | bytes requested / bytes free |
| 3 | `CapacityExhausted` | `Warn` | none | capacity / Admissions declined |

A **deferral** is one declining of an Admission that leaves the work where it was. Where the work waits
out of reach it is counted once per Frame per Link or bound endpoint, not once per connection waiting:
backlog depth is unknowable without accepting, and accepting is precisely what a Reactor at its cap or
at capacity must not do — it would complete the handshake and then drop it, showing the peer an
established-then-closed connection where the wire must stay silent. A count nobody can measure is worse
than an honest smaller one, so the slot carries what is actually known. Where the Reactor is already
holding the work — a stream Session handshake accepted but not completed — one declining is one piece of
work and the figure is exact.

Work the Reactor holds and cannot leave waiting is a **Discard**, not a deferral, and is counted
separately on `AdmissionDiscards`. The distinction is by fate rather than by profile: deferred work
arrives on a later Pump and Discarded work never does, which is the one difference a consumer acts on.
A datagram Session over its cap Discards the Hellos it cannot admit — there is no backlog for them to
wait in — so the same `Value1` slot on these two ids carries an exact count there and a coarse one on
the stream transports. Both ids stay honest under that split because each says only "this many
Admissions declined", leaving what became of the work to the counter that names it.

`AdmissionCapReached` and `CapacityExhausted` are separate ids rather than one id with a cause slot,
because they are fixed by different knobs — the first wants the cap lowered or the Frame budget raised,
the second wants `Capacity` raised — and an operator who has to read `Value1` to know which lever to pull
is being asked to do the roster's job. They share one counter — `AdmissionDeferrals` or
`AdmissionDiscards`, whichever matches the fate of the work; a consumer that needs the two *causes*
apart reads the diagnostics, which is what diagnostics are for.

### Raw

| # | Member | Level | Subject | `Value0` / `Value1` |
| --- | --- | --- | --- | --- |
| 100 | `LinkFailed` | `Warn` | Link | `SocketError` / — |
| 101 | `DestinationUnreachable` | `Warn` | Link | `SocketError` / — |
| 102 | `DatagramTruncated` | `Warn` | Source | buffer size / — |

### Datagram session

| # | Member | Level | Subject | `Value0` / `Value1` |
| --- | --- | --- | --- | --- |
| 200 | `DatagramMalformed` | `Warn` | Source | bytes received / — |
| 201 | `CookieRejected` | `Warn` | Source | epoch offered / epoch current |
| 202 | `VersionRefused` | `Warn` | Source | version offered / version required |
| 203 | `FragmentSetAbandoned` | `Warn` | Peer | fragments held / fragments expected |
| 204 | `RetransmissionTimerBackedOff` | `Trace` | Peer | new RTO in ms / consecutive backoffs |
| 205 | `OversizedFragmentSet` | `Warn` | Peer | declared count / largest legal count |
| 206 | `UnreliableFragment` | `Warn` | Source | claimed mode bits / — |
| 207 | `OversizedMessage` | `Warn` | Peer | Message length / `MaxMessageSize` |
| 208 | `AddressPeerLimitReached` | `Warn` | Source | `MaxPeersPerAddress` / — |
| 209 | `DatagramReadLimitReached` | `Warn` | none | `MaxDatagramsPerPump` / index in `BoundEndpoints` |
| 210 | `OversizedFragment` | `Warn` | Peer | Fragment payload length / `ResolvedFragmentSize` |

### Stream session

| # | Member | Level | Subject | `Value0` / `Value1` |
| --- | --- | --- | --- | --- |
| 300 | `PeerRefused` | `Info` | Source | capacity / — (capacity refusal); version offered / version required (version refusal) |
| 301 | `LingerNegotiated` | `Info` | Peer | ours in ms / effective in ms |
| 302 | `MalformedFraming` | `Warn` | Peer | offending length prefix, 0 for a truncation / `MaxMessageSize` |
| 303 | `DataAfterGoodbye` | `Warn` | Peer | — / — |
| 304 | `ResumeMatched` | `Info` | Peer | bytes rewound / — |
| 305 | `ResumeUnmatched` | `Warn` | Source | reservations checked / claim record type |
| 306 | `RedialAttempted` | `Info` | Peer | attempt number / delay in ms |
| 307 | `RedialFailed` | `Info` | Peer | `SocketError` / attempt number |
| 308 | `DeliveryCounterRefused` | `Warn` | Peer | claimed count / appended count |
| 309 | `PendingAdmissionExpired` | `Warn` | Source | bytes of the first record received / deadline in ms |
| 310 | `PendingAdmissionLimitReached` | `Warn` | none | the limit, `Capacity` / Admissions declined |

**Amended (slice 3, slice 4.1).** `MalformedFraming` and `DataAfterGoodbye` came out of the framing work
and took 302–303 — the values this table had drafted for `ResumeMatched` and `ResumeUnmatched`. The
Resume and re-dial four therefore shift to **304–307**, and slice 4.1's `DeliveryCounterRefused` takes
**308** rather than the next free value, so those four keep contiguous numbers reserved ahead of the
tickets that implement them. Explicit values are not optional here precisely because this happens: a
value published in a closed permanent enum cannot be moved later, so this table follows the code once
the code has shipped a value, never the other way round.

**Amended (issue #425).** `OversizedMessage` takes 207. Unlike 205 and 302 it does not close the
Session, so it can recur, once per over-limit Message. It still passes the admission rule: loss and
reordering never produce it, because it fires only when a sender ignores the announced limit, and no
counter carries the Peer or the length against the limit. It stays at `Warn` for the reason
`UnreliableFragment` does: it names a misconfiguration a consumer can act on.

**Amended (issue #427).** `AddressPeerLimitReached` takes 208. `AdmissionDiscards` already counts these
refusals, but no counter names the address, which is what a consumer needs in order to raise the limit
or block the address. It emits one record per refusal rather than one per Frame like
`CapacityExhausted`, because each record has its own subject, and the admission cap bounds the volume.

**Amended (issue #452).** The stream profile emits `AddressPeerLimitReached` too, under ADR 0020's rule
that one condition is one member. No stream admission counter moves for it, so the record is the only
place the refusal shows.

**Amended (issue #428).** `DatagramReadLimitReached` takes 209, under the configured-bound class. What a
count cannot carry is the identity of the socket that spent `MaxDatagramsPerPump`, as an index in
`BoundEndpoints`, which tells a consumer which address is flooded. It emits at most one record per bound
socket per Pump. It means the limit was spent, not that datagrams remain, because the socket's queue depth
cannot be read past queued errors. The Discard rule above still holds for a Hello the Reactor has read. A
Hello this limit leaves unread waits in the socket buffer, and no counter sees it until it is read or the
operating system drops it.

**Amended (issue #479).** `OversizedFragment` takes 210. A reliable Fragment wider than the receiver's
own `FragmentSize` closes the Peer with `ProtocolViolation` (ADR 0024). It passes the admission rule the
way 205 does: loss and reordering never produce it, because the handshake announces the size and only a
non-conforming peer sends a wider Fragment, and no counter carries the Peer or the width against the size.
It closes the Session, so it fires once per Peer.

**Amended (slice 4.4.4).** `ResumeUnmatched` shipped with both value slots used, where this table had
drafted neither. It carries the number of reserved Slots the claim was checked against and the wire
record type the claim arrived as, because the three ways a Resume is refused are otherwise one record
to an operator: a wrong secret and a once-valid secret arriving after its Linger expired differ only
in whether any reservation was there to check (the count is 0 for the latter), and a Hello announcing
a secret a reservation is holding — refused as a protocol violation — differs only in the record type.
Per the rule above, the table follows the shipped code.

**Amended (slice 6.2.6).** `UnreliableLossPackets` joins the per-subject scope, carrying how many
sequences an unreliable Space's receive window retired un-admitted as it slid forward. It passes
the derivability test — no other counter exposes it, since a sequence that never arrives is by
definition never classified — and it passes the record test the same way `duplicate packets` does: a
lossy path produces a gap every Pump, so a per-gap Diagnostic would fill the Sink with exactly the
volume this ADR's admission rule exists to keep out. It is the figure bought back by giving plain
`Unreliable` a sequence number, and it is the one thing genuinely given up by declining QUIC's packet
numbers. Per the rule above, the table follows the shipped code.

**Amended (issue #176).** The figure covers **both** unreliable Spaces, `Unreliable` and
`UnreliableSequenced`, summed — not `Unreliable` alone, as 6.2.6 first shipped it. Both slide on the
same rule and lose sequences the same way, so leaving one of them counted nowhere meant a consumer
running `UnreliableSequenced` over a lossy path saw nothing at all: its loss is not out-of-window
either, because a far-ahead arrival in a sliding Space is admitted after the slide rather than refused.
It shares the existing field rather than earning a second one because `OutOfWindowPackets` and
`DuplicatePackets` already aggregate across all four Spaces — a per-Space loss field would be the
anomaly, and every added field here is a permanent commitment. The cost is accepted deliberately: the
two Spaces' loss is not separable from this figure, and a consumer needing that separation is asking
for per-Space counters generally, which is a different decision from this one. The two reliable Spaces
still contribute nothing, because their windows do not slide.

**Amended (issue #325).** `SocketWrites` joins the per-subject scope, counting socket send calls made
on a Peer's transport: one per call, whether it wrote everything, part, or failed. It passes the
derivability test outright — no byte figure says how many calls carried the bytes. It was proposed to
make a cost claim testable: #132's claim that a delivery counter rides an existing write, and #90's
one write per Pump. ADR 0015 leaves Counters as the only seam that could observe either, and two
back-to-back writes inside one Pump reach the far end as one read, so nothing else can. That motive
alone would not earn a field, and the counter test was applied as written: a Peer's writes rising well
above the Pump rate names a write path that stopped batching, which a consumer can act on by reporting
it or by sizing their Pump cadence against it. The cost accepted is the one every field carries, a
permanent member and one add per send call. On the stream profile the figure includes handshake and
Resume records, because those are writes on the Peer's connection. On the datagram profile it excludes
handshake control datagrams, for the same reason bytes sent does: no Peer exists yet to charge.

**Amended (issue #56, slice 8.4).** A datagram-profile Resume control datagram does have a Peer to
charge, so it is counted, consistent with the stream profile counting Resume records. The dialling end
charges each Resume it sends to that Peer's socket writes and bytes sent, at the fixed 256-byte control
size and regardless of send success, as it does a data Datagram. The accepting end charges a matched
Resume to the Peer's bytes received. A Resume that names no Peer, names a dialled Peer, or presents the
wrong secret charges no Peer. Handshake control datagrams are still excluded.

**Amended (issue #59).** The datagram session profile emits `LingerNegotiated`, `ResumeMatched` and
`ResumeUnmatched` from the stream block. ADR 0020 decides this is deliberate: a block names the profile
that defined a condition, not the profile that emitted a record. The supported Sink style is
range-checking the blocks.

**Amended (issue #60).** Four rows had fallen behind the member docs and now follow the code.
`AdmissionCapReached` and `CapacityExhausted` carry Admissions declined in `Value1`, deferrals and
Discards together, as the paragraphs under the Common table already say. `PeerRefused` shipped a version
refusal as well as a capacity refusal, each with its own slots. `MalformedFraming` carries this Reactor's
`MaxMessageSize` in `Value1`. `DiagnosticFormatter` renders every member from the member docs, which stay
the authority.

**Amended (issue #482).** Two stream members join. `PendingAdmissionExpired` takes 309. It names the
Source of a connection closed for sending no whole first record within its deadline, which no counter
carries, and its first value tells a silent connection from a partial one. It emits one record per
connection, at most `Capacity` per Pump, because no more connections than that are held.
`PendingAdmissionLimitReached` takes 310. `AdmissionDeferrals` counts its declinings together with the
admission cap's, so, like `AdmissionCapReached`, the record exists to say which bound declined them. It
emits one record per Pump. `AddressPeerLimitReached` also covers a held stream connection closed under
the per-address limit (ADR 0014), with the same value slots.

Sixteen members as first drafted. What the rules **removed** is the more useful half of the exercise:

- **`DatagramDuplicate` and `PacketOutOfWindow`** are per-packet volume driven by loss and reordering.
  They fail the record test and become per-Peer counters.
- **`HorizonDeferred` and `ReadShareReached`** were the two I expected to be contested, and both resolve
  the same way: each can recur every Pump under sustained load, so both are counters. Neither loses its
  identity, because counters now have a per-Peer scope — which is that decision earning its place
  immediately.
- **`ReceiveIdleExpired`** gets no member at all. The transition to Interrupted is already an Event, and
  `Info` is explicitly for lifecycle that *never became* one.
- **The stream profile's accept cap has no member of its own**, because ADR 0012 unified it into the one
  admission cap. That consolidation pays out here, in a place we were not looking when we made it.
- **`SinkFaulted`** stays a counter, being the one record that has nowhere to go.

## Counters, and their two scopes

Counters exist at **two scopes that are different quantities, not the same quantity at two
resolutions.** The Reactor's figures run from construction; a subject's run from that association's
start and end with it. Neither is computable from the other — summing live Peers loses everything closed
Peers contributed, and a Reactor total cannot be split back out — which is how both survive the
derivability test honestly. Per-subject only was the more elegant answer and was rejected because it
makes every Reactor-lifetime figure unobtainable under Peer churn, which is exactly the situation being
investigated.

**A counter lives where its subject lives.** `SinkFaulted`, the arena high-water and datagrams
from an unknown Source have no Peer *by construction* and exist only on the Reactor. Retransmissions,
retention high-water and discards belong to a Peer. Very few appear in both.

| Scope | Counters |
| --- | --- |
| Reactor | `SinkFaulted`, `DatagramsSent`, `DatagramsReceived`, `UnknownSourceDatagrams`, `MalformedSessionDatagrams`, `ArenaHighWaterBytes`, `AdmissionDeferrals`, `AdmissionDiscards` |
| Per Peer | `BytesSent`, `BytesReceived`, `MessagesReceived`, `PacketsRetransmitted`, `DuplicatePackets`, `OutOfWindowPackets`, `UnreliableLossPackets`, `HorizonDeferrals`, `ReadShareExhaustions`, `RetentionHighWaterBytes`, `Resumes`, `SocketWrites` |
| Per Link (Raw) | `BytesSent`, `BytesReceived` |

**Counters are monotonic `ulong` and there is no reset.** A reset is a *write* to observation state, and
observation state is the one thing here with more than one plausible reader: a consumer's metrics export
and their debug overlay will both sample it, and whichever resets first silently destroys the other's
baseline — an invisible, intermittent bug in the component you reach for when something else is already
wrong. Diffing two snapshots gives everything a reset would, from state the consumer owns. `ulong` makes
wraparound moot at roughly 584 years of nanosecond increments.

**A per-subject counter's lifetime is the Peer's, not the transport's, so a Resume does not reset it.**
That follows from the Peer being the stable identity rather than being a nicety, and it is the more
useful behaviour: a retransmission count spanning three resumes is how a flapping path shows itself, and
a counter zeroing on every reconnect would hide the pattern it exists to reveal.

The per-subject snapshot is **fetched by value and keyed by `PeerId` or Link**, so nothing lets a
consumer hold a pointer into live state across a Pump. **There is no enumerate-all-subjects accessor in
1.0** — the consumer already holds the identities, and a roster-walking API invites sampling all 64 every
Pump.

At the design scale this is cheap: a per-subject counter block is a few dozen bytes inside an allocation
ADR 0011 already makes per Peer, so it adds no allocation and no per-Pump cost beyond the adds.

**Amended (issue #61, the Counter audit).** The table above now names the shipped members. Five
changes came out of the audit:

- **Two members were renamed** from `UnknownSourceDiscards` and `MalformedSessionDiscards` to
  `UnknownSourceDatagrams` and `MalformedSessionDatagrams`. `CONTEXT.md` defines a Discard as work held
  and let go because a cap or capacity was spent. A datagram that names nobody, or names a Peer and
  then does not parse, was never an Admission, so "Discard" meant two things in one record. The new
  names count datagrams, the same unit as `DatagramsSent` and `DatagramsReceived`.
- **`LinkCounters.ReadShareExhaustions` was removed.** Raw reads a ready Link once per Pump, so a Link
  never reaches a second reservation in one Frame and the field could only read zero. A field that
  cannot move fails the counter test.
- **`PeerCounters.ReadShareExhaustions` was added**, which is where the `ReadShareReached` candidate
  above was always meant to land. Both Session profiles can deliver several Messages from one Peer in
  a Frame, so both can spend a share. The stream profile keeps a refused Message for a later Pump. The
  datagram profile keeps nothing: an unreliable record is lost and a reliable one returns only by
  retransmission. Bytes received and Messages received show that delivery lagged, but not that the
  share was the cause rather than the Arena running out.
- **`ArenaHighWaterBytes` now moves on both Session profiles.** Only Raw fed it before, so a Session
  Reactor reported a false zero.
- **The stream profile now counts Peer bytes.** `BytesSent` is what each send call reported written and
  `BytesReceived` is every byte read on the Peer's connection, handshake and Resume records included,
  the same scope `SocketWrites` already had. The first record on an accepted connection is charged
  once it is admitted or matched as a Resume, because no Peer exists before that.

`AdmissionDeferrals` and `AdmissionDiscards` stay two members although they are never both non-zero
on one Reactor. Their units differ. A Deferral counts declinings and is coarse, because one endpoint
declines its whole backlog with one declining. A Discard counts pieces of work and is exact. One shared
field would invite a sum or comparison across profiles that means nothing. Tests pin both negatives,
the coarseness, and each member's "Not derivable" line in its XML doc.

## Consequences

- **The record/counter/Event split is now a stated rule rather than a habit**, and it is the first
  question asked of any future candidate. "Is it derivable?" is likewise the first question asked of any
  future counter.
- **ADR 0005's counter list was written before either wire format existed** and is superseded by the
  table above — retention high-water moves to the per-subject scope, and the discard counter is narrowed
  to unknown Sources, because a discard attributable to a Peer belongs to that Peer.
- **`DiagnosticRecord` grows to roughly 56 bytes and its subject becomes a tagged value.** This revises
  ADR 0008's profile-neutral subject a second time, and the justification is only sound because the
  admission rule emptied the path the original size was protecting.
- **Sixteen permanent public members, plus sixteen permanent counter fields.** That is the real cost of
  this ADR, and it is deliberate: every one of them was argued against a rule rather than added because
  it was easy.
- **`Error` ships empty.** If a member is ever proposed for it, that is a signal to check whether the
  condition should be a thrown exception instead, which is where local, must-fix states go.
- **A Sink written against the blocks keeps working as the enum grows**, and a Sink written against a
  `switch` over every member does not. The XML docs should say which of those we support.
- **Every value-slot meaning is a positional convention that will be miscopied**, which ADR 0005 already
  accepted; the table above exists so there is one place to check, and it must be kept in step with the
  XML docs rather than replacing them.
- **Binding and dual-stack is now the only open gap**, and the Source subject decided here is the first
  thing it will have to satisfy for IPv6.
