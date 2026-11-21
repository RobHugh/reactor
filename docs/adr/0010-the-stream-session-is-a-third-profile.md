# The stream Session is a third profile

The datagram Session protocol is not carried over TCP. Running it there would put retransmission on top
of retransmission and ordering on top of ordering, and — worse than the waste — ADR 0009's adaptive RTO
would be measuring a path that already contains TCP's own recovery delays, tuning itself against a
signal it cannot interpret. Letting the delivery modes degenerate instead, so that `Unreliable` becomes
reliable when the Session happens to be a stream, was rejected for the reason this design keeps
rejecting things: a public contract whose meaning depends on invisible construction state.

What a stream does need is a layer above L1 that TCP does not provide — Message framing, liveness on a
connection that can be half-open indefinitely, and graceful resumption when one side drops. That is a
different protocol sharing the Session vocabulary, and it ships as a **third profile**.

**The three public faces are `DatagramSessionReactor`, `StreamSessionReactor` and `RawReactor`.**
`SessionReactor` cannot stay unqualified once there are two session protocols. `StreamReactor` alone
would be wrong rather than merely short, since `RawReactor` carries TCP too and "the stream one" does
not identify it. Naming them `Udp-` and `Tcp-` would be plainest and is what a consumer searches for,
but it puts transport names into the contract that `Peer` and `Handle` both keep on their avoid-lists,
undoing at the most visible point what ADR 0006 protected. `Datagram` and `Session` are terms the
glossary already owns.

The split is at construction, and that is the argument for it. ADR 0008 accepted runtime checks on
*calls* — `Send` against `SendTo` — because the compile-time alternative cost a per-event dictionary.
There is no comparable cost here, and a unified session Reactor would take fragment size,
`MaxDatagramSize`, cookie epoch and RTO bounds at construction, every one of them meaningless for a
stream Peer. Conditional construction options are worse than conditional calls: they are validated
once, far from the mistake, and a consumer who sets one that does nothing gets no signal at all.

The price is transport fallback. An application cannot switch from UDP to TCP by changing one
constructor. It is a smaller price than it looks, because what does not port is the delivery modes, and
those do not port under a unified Reactor either — they merely fail later.

## What the stream profile is

Much less than the datagram one. There is no cookie, because TCP's own handshake has already confirmed
the address and there is no amplification to defend. There are no acks, no sequence spaces, no
fragments and no RTO, because TCP provides what they were built for.

- **`u32` length-prefixed Messages**, with `MaxMessageSize` as a receive limit, so an oversized prefix is
  a protocol violation rather than an allocation.
- **A `u64` cumulative received-Message counter**, sent periodically as a control record.
- **A framed capacity Refusal**, carrying the `DisconnectReason` ADR 0007 deferred.
- **Goodbye, Closing, Interrupted, Linger and Resume**, meaning exactly what they mean in the glossary.

**Amended (issue #484).** `MaxMessageSize` is also a send limit. `Send` refuses a Message longer than
the limit the peer announced in its Hello with `RemoteMessageTooLarge` and copies nothing, the outcome
the datagram profile gives for the same limit (ADR 0004, #449). The check runs after `NotEstablished`,
because the limit arrives in the Hello. A Message longer than this end's own `MaxMessageSize` still
throws `ArgumentException`: the caller's own options predict it, and retention is floored by that
limit. The main cost is that `Send` is no longer decided by this Reactor's options alone, so a stream
consumer handles `RemoteMessageTooLarge` too.

## Resumption, on the premise that drops are ordinary

This profile is designed for connections that half-open and drop often, not rarely. That premise is
load-bearing and changes several answers from what they would otherwise be.

**A FIN is not a Goodbye.** A collapsing half-open pipe produces a FIN with no Goodbye, and Goodbye is
the only thing the glossary permits to forbid a Resume. So an inbound FIN without one means
`Interrupted`, and resumable. `Closing` is reached only when a Goodbye arrives with data still to
deliver.

**Resumption is by delivery counter, not by byte offset.** TCP reports what the kernel accepted, never
what the peer received, so a connection can die with megabytes in send buffers that the far application
never saw. Each end therefore reports the count of Messages it has fully received, and the sender
retains from that point. One number is a complete description, because TCP has already guaranteed that
everything below it arrived in order — no bitfield, no selective acknowledgement. **The counter
increments when a Message is handed to the frame arena, not when bytes are read**; getting that
backwards produces a Resume that silently drops exactly the Messages that were in flight when the
connection died, which is the failure the mechanism exists to prevent, appearing only during a
reconnect.

**Liveness is two independent timers.** Over TCP, sending proves nothing — the kernel accepts bytes into
a buffer whether or not anything is at the far end — so only receiving is evidence. A send-idle timer
emits the control record unconditionally; a receive-idle deadline declares the transport lost. They must
be independent: a heartbeat triggered by receive idleness alone deadlocks two quiet peers, because A's
probe resets B's timer, so B stays silent, so A hears nothing and declares a healthy peer dead.
Construction rejects a receive-idle deadline that does not exceed the send-idle interval by enough for
one heartbeat and one round trip.

The heartbeat is never a wasted packet: it is the delivery counter, so one idle record proves liveness,
advances the resume point and releases retention.

**The library re-dials; the listener reserves.** Only the side that dialled has an address to dial, and
under a premise of frequent drops, leaving recovery to the consumer means every consumer reimplements
retry-with-backoff under exactly the conditions that make it hard to test. The dialling side retries
during a Pump within Linger, with a backoff whose default is stated in consumer terms. This is not a new
kind of behaviour: ADR 0007 already accepted that a Pump accepts inbound connections unasked. The
listening side holds the Interrupted Peer **with its slot reserved for the length of Linger** and
matches an incoming connection presenting a valid resume secret onto the existing Peer. Reserving is
what stops a busy Reactor refusing a reconnection for capacity while still holding the state it was
reconnecting to.

The consumer sees `Interrupted`, then `Resumed`, with the same `PeerId`, and the Messages they had
already sent are still in flight.

## Backpressure

`Send` continues to succeed while a Peer is Interrupted — that is the point of holding it — until
retention fills. **Then `Send` fails with `RetentionExhausted` and the Peer stays Interrupted.** Closing
the Peer instead would discard a Session that might have resumed a moment later, to reclaim memory that
was preallocated to that slot and cannot be given to anyone else. Dropping the oldest retained Messages
would silently break reliable delivery on the exact path where the consumer is least able to observe it.

So the rule across all three profiles is one rule: **`Send` is the only place backpressure appears, and
it appears as a failed `Result`, never as blocking, dropping or growing.** ADR 0008 reached it from the
other direction with "the Reactor holds no outbound bytes", and a consumer's send path handles one
failure kind whichever profile they chose.

## Consequences

- **ADR 0007's "two public entry points to document, test and keep stable" becomes three.** That is a
  real cost against ADR 0003's permanence rule, and it is the second time the entry-point count has
  risen.
- **`SessionEvent` and `PeerId` are shared by both session profiles.** Re-running ADR 0008's test — does
  a shared struct carry fields dead in one profile — it comes out the other way here: the kinds and
  fields are identical. That the events did not diverge is a check on this ADR, since a split in the
  wrong place would have shown up as different events.
- **`ISessionReactor` is a roadmap item, not part of 1.0.** The shared event struct already makes the
  draining half of an application portable; an interface adds portable construction and `Send` at the
  cost of permanent public abstraction and an indirect call on the hottest path. It can be added later
  without breaking anything.
- **`Segment` narrows to Raw.** Its glossary definition — what a stream transport can honestly report —
  stops describing a stream Session, which delivers Messages.
- **Data is buffered twice**, once in our retention and once in the kernel's send buffer, so a stream
  Peer's footprint is our bound plus the socket's, and the socket's is not fully ours to control.
- **The version is per-profile.** The two session protocols are separate formats on separate transports
  with separate handshakes, and one shared number would force datagram Peers to refuse each other over a
  change that touched only framing.
- **`RetentionExhausted` is now reachable three ways** — one large Message, sustained message rate, and
  an interruption outlasting the send rate — and must be distinguishable from an ordinary refusal, or a
  consumer treating every failed `Send` alike will hammer a Peer that has said it is full.
- The stream profile's full construction surface, binding and dual-stack, and the diagnostic roster all
  remain open. The roster has grown again: the re-dial, the resume match, the counter and the two idle
  timers all want records.
