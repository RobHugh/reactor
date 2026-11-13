# The stream profile's construction surface

ADR 0010 named the knobs a `StreamSessionReactor` would need and settled none of them. This ADR settles
them, and the first decision is not a knob at all but the rule for having one.

**A knob may carry a default only when that default is derived — from machinery we own, from another
knob, or from a citable standard. A knob whose default could only come from predicting the consumer's
traffic, message sizes or network is required, with no default at all.** ADR 0009 already worked this
way without naming it: the RTO floor is the observed Pump interval, its ceiling is Linger, the receive
window defaults to ADR 0004's own floor. Naming the rule makes the two session profiles consistent and
gives a test that a future knob can be held against.

The alternative of defaulting everything would have been cheapest to adopt and is the shape most .NET
libraries take, but it reintroduces exactly the assumption this design has refused elsewhere — spread
over more numbers and made less visible. Requiring everything is honest and punitive: a library whose
smallest working construction is eleven arguments is met with a copied block from the README that
nobody reads, which is a default with worse ergonomics and no documentation.

## The surface

**Required — five, each of them either a memory bill or a statement of tolerance.**

| Knob | Why it cannot be defaulted |
|---|---|
| Capacity | The multiplier on every preallocation |
| `MaxMessageSize` | Floors two buffers and is announced as a receive limit |
| Retention bound, in bytes | The memory the profile exists to bound |
| Send-idle interval | How often you are willing to speak when you have nothing to say |
| Linger | How long the option to resume is worth holding |

**Defaulted, with the provenance stated.**

| Knob | Default | Derived from |
|---|---|---|
| Receive buffer | `MaxMessageSize + 4` | Its own floor: one incomplete maximum Message |
| Accept cap | Capacity | A Pump never spends more admission work than a full Reactor's worth |
| Missed-heartbeat count | 3 | RFC 4271, whose Hold Timer is three KeepAlive intervals |

**Supplied:** a `TimeProvider` and a Diagnostic Sink.

**Not knobs.** The receive-idle deadline is `peerSendIdle × count`. The re-dial schedule doubles from the
observed Pump interval to the effective Linger. Both are derived, and neither can be set.

## Capacity, and a superseded sentence

**ADR 0001's "capacity is fixed and small (default 64)" is superseded.** That sentence was written when
capacity was a scale hint about how many sockets `Select` would scan. Retention changed what it is: with
buffers preallocated per Peer, capacity multiplies every allocation in the library, and a consumer who
never thought about it would receive `64 × (retention + receive buffer)` — a quarter of a gigabyte at
generous settings, arrived at by not typing anything. Capacity is therefore **required**.

It is **not capped**. Above 64 nothing breaks: `Select` is O(n) and degrades smoothly, and the real
ceiling enforces itself, because a capacity large enough to matter fails to allocate at construction,
which is an immediate and honest signal. A hard cap would be permanent under ADR 0003, would defend a
number with no derivation behind it, and would refuse a consumer who has measured their own case and
found it fine — substituting our guess about their workload for their measurement. **64 survives as
documentation of intent: designed for capacity in the tens, unsuitable above the low hundreds.**

## Retention is a ring of framed bytes

The bound is expressed in **bytes**, over the framed form. A Send frames the Message once — `u32` prefix
plus payload — and appends it to a per-Peer ring. Retention *is* the send path: what goes to the socket
is a copy from the ring, and release is advancing the head past the prefixes the peer has confirmed.

A message-count bound was rejected because it bounds no memory: 256 retained Messages is a kilobyte or
four gigabytes depending on what was sent, so the only safe preallocation would be
`count × MaxMessageSize`. Bounding both bytes and count was rejected because the count has no
provenance under the rule above, and because one failure that could mean either is worse than one that
can only mean one thing. Under a byte ring **the message count is not a knob with a bad default; it is a
knob that does not exist**, since the descriptors live in the ring as the prefixes themselves.

The ring also makes resumption cheap in a way neither alternative does: after a re-dial the sender
reconstructs nothing, it rewinds a pointer and writes from that offset, because the framed bytes it
must resend are the bytes it already holds. That is what framing at Send rather than at flush buys, for
a copy the other designs pay anyway. A ring wrap makes one Send two copies, which is invisible to the
consumer and stated here rather than discovered.

**Retention is per Peer and preallocated**, so the bill is `capacity × bound` whether or not the Peers
exist. A shared pool would use far less memory on a Reactor whose Peers are mostly quiet, and was
rejected because it destroys the only backpressure signal the library has: ADR 0010 promised that `Send`
succeeds while a Peer is Interrupted *until retention fills*, and under a pool that promise silently
becomes "until someone's retention fills", which is a different promise and cannot be kept. A Send would
fail on a healthy Peer for a reason the consumer cannot see, in a way no single-Peer test can reproduce.
The arithmetic — `capacity × (retention + receive buffer)`, one line with no traffic term in it — belongs
in the options documentation, and the total belongs in the exception when the allocation fails.

## Liveness, expressed so that no network constant appears

ADR 0010 required construction to reject a receive-idle deadline that does not exceed the send-idle
interval "by enough for one heartbeat and one round trip". That check needs a round-trip figure, and
this profile deliberately has no RTT estimator, so the figure could only have been invented.

Instead, **the send-idle interval is an absolute duration and the receive-idle deadline is a count of
missed heartbeats.** The count is dimensionless and makes no claim about latency, so it may carry a
default while the duration stays required, and ADR 0010's validation becomes structural: any count of 2
or more satisfies it by construction, and 1 is rejected for a stated reason rather than an arithmetic
one. No round-trip term appears anywhere.

A count only means something relative to *the peer's* emission interval, so **each end announces its
send-idle interval in the handshake** and the receiver computes `peerSendIdle × count`. This is the move
`MaxMessageSize` already makes in ADR 0004: a receive-side property announced by the only party who
knows it.

The default of 3 is RFC 4271's ratio, and the precedent is unusually close — same transport, same
mechanism, same failure. Two observations make it the right one rather than a borrowed one. Over TCP a
heartbeat **cannot be lost**; the transport retransmits it, so the count guards against *lateness*, whose
real sources are the peer's own scheduling jitter and TCP's retransmission delay. And the minimum legal
value of 2 leaves no room for exactly that: a peer whose loop stalls slightly longer than one interval
is declared dead while healthy, and in .NET the stall that does it is a collection pause. The derived
deadline is also legible — a 10-second interval means you are told after 30 seconds, which a consumer
can check against their tolerance without doing our arithmetic.

The heartbeat cannot be disabled. A Session that is sending continuously never fires the timer, so it
already costs nothing; a Session that is not is the case the profile exists for.

## Linger is announced, and the minimum applies

Linger is the one knob both ends spend independently — the dialling side on re-dialling, the listening
side on holding a slot reserved — so a mismatch between two independently deployed applications is the
normal case, not a misconfiguration. **Linger is announced in the handshake and the effective value is
the minimum.**

Left local, a dialler with sixty seconds re-dialling into a listener with ten finds the slot released,
presents a secret matching nothing, and is admitted as a new Peer: the two ends disagree about whether a
Peer exists, one reporting Closed while the other shows Interrupted, and the consumer who configured
sixty seconds got ten without being told. Letting the listener's value win was rejected because it lets
a listener force a dialler to keep retrying against state that is gone, which is the wrong direction for
the mistake to run. Under the minimum, both ends abandon the Session at the same instant, and a consumer
whose generous value was reduced is not being overruled — the option genuinely stopped existing when the
other end stopped holding it.

The decisive gain is not symmetry for its own sake: a re-dial can still fail, but it can no longer fail
*because the peer had a different idea of how long the Session existed*. The only causes left are the
transport and a genuinely wrong secret, which is a diagnosis a consumer can act on.

## The re-dial schedule is derived, not configured

The delay starts at **the observed Pump interval** and doubles, ceilinged at **the effective Linger**,
with full jitter on each delay. This is ADR 0009's RTO derivation applied unchanged, for the same two
reasons: a retry sooner than the next Pump cannot happen, and a retry after Linger has nothing left to
reconnect to. Linger bounds the activity, so no attempt count is needed either. The conventional three
knobs — initial, maximum, jitter — would all have been required under the rule above, and requiring
three numbers to reconnect a socket is the punitive outcome the rule exists to avoid.

Jitter is stated rather than buried. A library used to talk to a shared remote will have many instances
re-dialling from the same event, and an undithered doubling schedule synchronises them into bursts that
arrive precisely when the far end is least able to absorb them. Randomising across `[0, computed]` costs
nothing. It makes the schedule non-deterministic, so tests assert the bounds and the doubling rather
than timestamps, and the randomness comes from a substitutable source — the discipline `TimeProvider`
already imposes on everything else here.

## Admission

**The accept cap is separate from capacity.** You might think capacity subsumes it, since only free
slots can be filled — but ADR 0010 refuses in-protocol with a framed Refusal, so a connection we will
not admit is still accepted, read, written to and closed. Accepts are therefore *not* bounded by
capacity, and a burst of a thousand inbound connections is a thousand accept-refuse-close cycles inside
one Pump, on the consumer's thread, at the moment we are already saturated. Draining the backlog every
Pump would hand an unbounded per-Pump cost to a caller running inside a simulation loop, and the cost is
remotely triggerable, which fails ADR 0005's rule that nothing on the network decides how much of the
consumer's Frame it occupies. The default of capacity has provenance rather than being chosen: a Pump
never spends more admission work in one call than a full Reactor's worth, and the remainder waits in the
OS backlog for the next Pump, which ADR 0007 already accepted on the Raw side.

**The resume match runs before the capacity check.** ADR 0010 has the listening side hold an Interrupted
Peer with its slot reserved, so a reconnecting Peer is not asking for a slot — it already owns one.
Checking capacity first would refuse a reconnection into a Reactor that is holding the very state being
reconnected to, which is the failure reserving was introduced to prevent. This is stated here because
getting it backwards is a bug that appears only under load.

## The delivery counter is emitted once per Pump

ADR 0010 said the counter is "sent periodically" and that the send-idle timer emits it. That is not
sufficient, and the gap is a bug rather than a refinement. Under **one-way traffic** the receiving end
is idle and emits on a multi-second timer while the sender fills its ring at line rate; nothing
releases, `Send` begins failing with `RetentionExhausted`, and the Session is entirely healthy.
Retention would be bounded by the idle interval rather than by the round trip.

**The counter is emitted once per Pump in which the received count advanced**, in addition to the
send-idle timer. No new constant appears. This is ADR 0009's reasoning for flushing a datagram once per
Peer per Pump, applied again: the protocol borrows the consumer's loop boundary rather than inventing a
timer, and retention is released within a round trip of delivery rather than within an interval chosen
for something else.

A trigger every N received Messages was rejected because N is a constant about someone's traffic, and it
is one the consumer could not choose even if asked — what it really controls is the *peer's* retention,
which they cannot see.

The cost is bounded and worth stating precisely, because the alarming version of it is wrong. A Pump
receiving from 64 Peers at 1000 Hz already makes 64 000 read syscalls a second; the counter adds nothing
to a two-way Peer, because it rides a write that was happening anyway, and at most doubles the syscall
count for a purely receiving one. It cannot exceed work already being done, and cannot exceed the rate
the consumer chose.

**Nagle is off, and that is a documented property of the profile rather than an implementation detail.**
Nagle is the mechanism TCP already offers for this exact problem, and unlike delayed acknowledgement it
solves it without a constant, self-clocked by the connection's own acknowledgements. It was still
rejected, because per-Pump batching already captures most of its benefit — we write once per Pump with
everything in it, rather than once per Send, which is a better version of what Nagle was invented for —
while its cost is not captured at all: a small Message written with data outstanding waits up to a round
trip, which taxes the payload in order to pace a control record. A consumer measuring latency needs to
know which of us is batching.

We could not have taken Nagle's clock without taking Nagle. It is a socket-wide switch, and from user
space we cannot observe TCP's acknowledgements, so an ack-clocked rule of our own is not available: any
rule of the form "emit when something changed" degenerates to per-Pump under continuous inbound traffic,
because something always has.

## What the handshake carries, and for how long

Version, `MaxMessageSize`, send-idle interval and Linger. **A Resume carries only the secret and the
delivery counter; the announced parameters are immutable for the Session's lifetime.**

Re-announcing them would break the mechanism underneath: retention holds framed bytes admitted against
the `MaxMessageSize` in force when they were sent, and a peer announcing a smaller one on resume has
made retained Messages illegal — leaving only dropping them, which silently breaks reliable delivery
exactly where the consumer cannot observe it, or sending them anyway, which makes the announced limit a
lie.

This is provable rather than preferable. The only party that can present a valid resume secret is a
process that still holds the Session's state, because the secret lives in memory and dies with it, as do
the ring and the counter. A process that restarted cannot resume at all; it dials fresh and negotiates
fresh, which is the reconfiguration path and the correct one. A re-announcement could therefore only
come from a party agreeing with itself. **The way to change a Session's parameters is to end it.**

## Options types and failure

**Each profile has its own options type with no shared base.** `StreamSessionOptions`,
`DatagramSessionOptions` and `RawOptions` overlap on a `TimeProvider`, a Sink and a capacity, and that
duplication is three property declarations. The alternative is a permanent public abstraction, under
ADR 0003, over a surface still being discovered — this ADR alone added an announced Linger, a
missed-heartbeat count and a per-Pump accept cap that did not exist when ADR 0010 was written. It also
runs against ADR 0010's own argument that conditional construction options are worse than conditional
calls, since a base is how conditional options creep back. The sharing that matters is not the
declarations but the **validation** — floors, cross-checks, allocation arithmetic — which is one
internal implementation in L1 that all three call into: public types separate and permanent, shared
logic internal and free to change.

**Construction throws.** `ArgumentOutOfRangeException` for a single bad value, `ArgumentException` for a
combination. The distinction that matters is who caused it: `Send` returns a `Result` because its
failures are caused by the peer or the network and are ordinary events in a correct program, while every
check here is deterministic, data-independent and entirely the caller's — the same options fail
identically on every run, on every machine, forever. A `Result` would put a branch on a path that can
only be taken by a bug.

The cross-checks are: retention ≥ `MaxMessageSize` + 4; receive buffer ≥ `MaxMessageSize` + 4;
missed-heartbeat count ≥ 2; capacity ≥ 1; accept cap ≥ 1; send-idle > 0; Linger > 0; and the allocation
succeeding.

**The messages state the arithmetic, not the predicate.** "Retention must be at least `MaxMessageSize`
plus four" restates the failure; "retention of 64 KB cannot hold one Message of 1 MB — raise retention or
lower `MaxMessageSize`" diagnoses it. These messages are the only documentation a consumer reads at the
moment they need it.

**Amended (issue #488).** `MaxMessageSize` is at most `Array.MaxLength` less the 4-byte length prefix,
2 147 483 587, so one framed maximum Message fits one array and no floor is computed past
`int.MaxValue`. The memory bill's total is summed as `Int128`, because each of its three lines can
approach 2^62 bytes.

## Consequences

- **ADR 0001's "default 64" is superseded.** Capacity is required and uncapped; 64 becomes a documented
  design scale rather than a value anyone receives by accident. The same argument applies to Raw's
  per-Link buffers, which is the first place this ADR's rule reaches outside its own profile.
- **`MaxMessageSize` now appears in three places at once** — announced as a receive limit, flooring the
  receive buffer, flooring the retention ring — making it the one knob whose careless enlargement
  multiplies memory in two directions. It needs a sentence in the options documentation, not a
  discovery.
- **The default-provenance rule is retroactive and should be applied to the other two profiles** when
  their surfaces are written. It is a rule about defaults, not about this Reactor.
- **A Session's negotiated parameters are immutable for its lifetime.** That is a stronger statement than
  "we do not renegotiate", and it is what makes the ring's sizing sound: the ring is admitted against a
  limit that cannot move underneath it.
- **There is no way to write code generic over a Reactor's options.** That sits behind the same boundary
  as `ISessionReactor`, and if that interface is ever added the options question returns with it — to be
  answered then with three shipped profiles to look at rather than now with none.
- **Nagle being off is part of the public contract**, because it determines who is responsible for
  batching and therefore what a latency measurement means.
- **The re-dial schedule is non-deterministic by design**, so its tests assert bounds and doubling rather
  than timestamps, and the jitter source is substitutable like `TimeProvider`.
- **The diagnostic roster grows again** — the re-dial attempts, the resume match and its failure, the
  accept cap being reached, and the receive-idle expiry all want records. Binding and dual-stack remain
  the other open gap.

## Amendment 2026-11-12: a re-dial resumes only when the listener answers

Issue #481. A dialled Peer left Interrupted as soon as its re-dial connected over TCP. A listener that no
longer held the Session closed each such connection. Each close was a new interruption with a new Linger
and a reset schedule, so the dialler re-dialled forever and never closed. "Linger bounds the activity",
above, was false for that case.

The listener's first delivery-counter record on the new connection confirms the Resume. A matched
listener always sends it first, because `ResumeHeldPeer` sends it before any retention. Until it arrives
the dialled Peer stays Interrupted and its Linger and schedule keep running.

Before that confirmation, each of these is a failed attempt: end of stream, a failed read, a failed
Resume send, and silence past the receive-idle deadline. A failed attempt raises no event and logs
`RedialFailed`. Its `Value0` is `SocketError.Disconnecting` for an end of stream, the read's error for a
failed read, `SocketError.SocketError` for a failed Resume send and `SocketError.TimedOut` for silence.
`Value1` is the attempt number. The next delay comes from the same schedule, and Linger is still counted
from the original interruption.

A dialler whose listener no longer holds the Session therefore raises one `PeerInterrupted` and closes
with `LingerExpired` on time. "Linger bounds the activity" holds.

The Resume record, the delivery counter's wire format, the listening side's match and refusal paths, and
the schedule's derivation do not change.

## Amendment 2026-11-12: a pending dial times out

Issue #483. A dial had no deadline once TCP connected. The idle timers cover established Peers only, so a
far end that accepted the connection and sent no handshake record left the dial waiting for a Hello,
holding its Slot, with `Send` returning `NotEstablished`, until the consumer closed it. The TCP connect
phase was bounded only by the operating system's connect retries, which differ by platform.

**Timeout.** A dial that has not established one `Linger` after `Connect` closes, whether its TCP connect is
still pending or the far end connected and sent nothing. No knob is added. `Linger` is this end's declared
bound on how long it waits for a silent remote, it is the only `Linger` a dial knows before the far end's
Hello announces one, and it is the bound ADR 0009's amendment of 2026-11-10 puts on a pending datagram
dial, so both profiles behave alike. The check runs in the Pump's idle-timer scan, after the readiness
pass, so a Hello already in the socket buffer establishes the Peer first.

**Close reason.** `TransportFailed`, for the reason ADR 0009's amendment gives: it already names a dial that
could not connect, and a new public member would be permanent for a condition an existing one covers.

**No diagnostic.** The `PeerClosed` event names the Peer the consumer dialled, and the consumer knows the
endpoint it gave `Connect`. The datagram profile logs nothing for the same condition, and a member added to
one profile alone would make the two disagree. Telling a timed-out dial from a refused connect would be one
member for both profiles, decided in its own issue.

**Accepting end.** No change. An accepted connection gets a Slot only once its whole Hello has arrived, and
it establishes in the same call, so no accepted Peer waits in the handshake. A connection held before that
is covered by issue #482, with its own deadline of `SendIdleInterval` × `MissedHeartbeats`: a held
connection has no Slot, and a dial has one.

The cost is that a `Linger` shorter than the round trip plus the far end's Pump interval closes a dial
before its answer arrives, as it already does on the datagram profile.
