Closes #17
Closes #18
Closes #19

Part of #16

## What landed

A consumer now has a third public entry point. `StreamSessionReactor` is constructed from a
`StreamSessionOptions` — its own options type with **no shared base** with `DatagramSessionOptions`,
composing the shared `ReactorOptions` value rather than restating its members (ADR 0011). Five knobs
are required with no defaults (`Reactor.Capacity`, `MaxMessageSize`, `RetentionBytes`,
`SendIdleInterval`, `Linger`); three are defaulted with stated provenance (receive buffer to
`MaxMessageSize + 4`, admission cap to capacity, missed heartbeats to 3, citing RFC 4271). Every
cross-check throws at construction, and every message states the arithmetic rather than the
predicate — the retention failure names the bytes supplied, the floor, and `MaxMessageSize`, and the
memory-bill failure names both lines and the grand total.

The endpoint set is part of the options, may be empty (dial-only is an ordinary configuration), and
is bound once at construction. `BoundEndpoints` reads back what was actually bound, so port zero in a
test is normal. There is no rebinding: no public member begins, stops or moves listening, and no
public member of this profile exposes a socket, a Handle or a Link — a reflection test over every
public member's parameter and return types pins that. `Connect`, `Close`, `Pump`, `Drain`, `Counters`
and `Dispose` are the rest of the surface. Peers live in `Core/ShareHolderTable<PeerSlot>` — the
generational Slot table PR #69 lifted into `Core/` — converted to `PeerId` only at the public edge,
so L0 still does not know whether it holds Links or Peers, and a stale `PeerId` whose Slot has been
reused addresses nothing rather than the new occupant.

Both ends run a symmetric handshake: one fixed 41-byte record each, sent the instant the TCP
connection exists, carrying version, `MaxMessageSize`, send-idle interval, Linger and a resume secret.
Version must match exactly; a mismatch produces a **framed, in-protocol** Refusal naming both versions
and a `PeerClosed(VersionMismatch)`, not a bare close. A dialler arriving with every capacity Slot
occupied is accepted, told `RefusalReason.Capacity` on the wire and closed cleanly — never dropped
silently, never left in the backlog. Linger is negotiated to the minimum of the two announced values.
`PeerRefused` (300, `Info`, **Source** subject) and `LingerNegotiated` (301, `Info`, **Peer** subject)
record both; `DiagnosticSubjectKind` gained exactly one appended member, `Peer`.
`SessionEvent`, `SessionEventKind`, `SessionEventEnumerator` and `PeerId` are the **shared** types —
no stream-only twin appeared, and no stream-only event kind was needed.

**This profile carries no application data yet.** It establishes and refuses Sessions and nothing
else; `Send`, framing, retention, batching and Goodbye land in #20–#23.

`dotnet format --verify-no-changes` clean, `dotnet build` 0 warnings / 0 errors, `dotnet test`
**347 passed, 0 failed** — re-verified on this branch, not taken from the sub-agent reports.

## Sub-issue status

| Issue | Title | AC met | Notes |
|---|---|---|---|
| #17 | 3.1 `StreamSessionOptions`, cross-checks, and the allocation bill | 6/6 | 26 tests. `Endpoints` deliberately deferred to 3.2 per #17's own Scope. `StreamSessionReactor` stores nothing at this point — validation only. |
| #18 | 3.2 Peers, `SessionEvent`, and the declared endpoint set | 6/6 | 20 tests. Added `ISessionEventSource` so `SessionEventEnumerator` is genuinely shared rather than twinned; added `DisconnectReason.TransportFailed`. No version or capacity guarding here by design — that is 3.3's. |
| #19 | 3.3 The stream handshake, announced parameters, and refusal | 5/6 | 13 + 8 tests. The unmet AC is "a `Send` of an oversized Message fails at `Send`" — `Send` does not exist until #20, and the run explicitly forbade reaching forward. `PeerSlot.RemoteMaxMessageSize` is captured and waiting for it. |

## Still to come in slice 3

- **#20 (3.4)** — `u32` length-prefixed framing in both directions, the oversized-prefix protocol violation, and the delivery counter's increment point (at hand-off to the frame arena, not at byte read).
- **#21 (3.5)** — the per-Peer preallocated retention ring of framed bytes, wrap included; retention *is* the send path, and `Send` fails with `RetentionExhausted` rather than blocking, dropping or growing.
- **#22 (3.6)** — write batching once per Pump rather than once per `Send`, and Nagle off as a documented property of the profile.
- **#23 (3.7)** — `Close` as an announced Goodbye, the `Closing` state for a Peer still holding deliverable Messages, and Goodbye forbidding a Resume.

## Review this carefully

Ordered by how much I think a human needs to decide rather than confirm. Findings marked *(review)*
came from `/code-review`'s Standards or Spec pass and I have confirmed them against the code.

1. **The capacity refusal touches neither admission counter — and #16 arguably asked for the
   opposite.** `src/ReactorSockets/StreamSessionReactor.cs:353` (`RefuseForCapacity`), `:720`
   (`ReportDeclinedAdmissions`). #16's Amended section says 3.3 has *"both a coarse pre-accept and an
   exact post-accept declining… told apart by `AdmissionCapReached` versus `CapacityExhausted`"*;
   #19's own acceptance criteria say a capacity refusal *"must not touch either admission counter"*.
   #19 took the acceptance criteria as authoritative, removed 3.2's `_capacityDeferralsThisFrame`
   as dead, and asserted the negative explicitly
   (`StreamSessionHandshakeGuardTests.cs:161`). Consequence as shipped:
   **`DiagnosticId.CapacityExhausted` is unreachable from this profile.** #19 flagged the ambiguity
   honestly rather than picking silently. **This is a decision, not a fact — a human should pick the
   reading.** I read the same two paragraphs and find them genuinely contradictory; I lean toward
   #19's reading (a refusal says something on the wire and ends the connection, which is neither a
   Deferral nor a Discard under #74's fate-of-the-declined-work split), but I could be inheriting
   the implementer's misreading.

2. **`Connect` leaks a socket when the Peer table is full.** `StreamSessionReactor.cs:169-178`
   *(review — both passes)*. `TcpLink.Dial(destination)` runs *before* `_peers.TryAdd`, so the
   capacity `InvalidOperationException` at `:177` abandons an already-created connecting socket
   undisposed. `RawReactor` does not have this shape — it checks capacity before creating anything
   (`RawReactor.cs:980`). Genuine defect, not a judgement call. No test covers dial-at-capacity.

3. **A Refusal can be interleaved into an unfinished Hello.** `StreamSessionReactor.cs:532`
   (`SendRecordOrArm`) *(review — Spec pass)*. `RefuseVersionAndClose` calls `SendRecordOrArm`, which
   calls `Link.Send` and then overwrites `slot.PendingHandshakeBytesToSend` unconditionally. If this
   end's *own* Hello short-sent and its remainder is still pending, that remainder is silently
   dropped and Refusal bytes go onto the wire mid-record. 41 bytes makes this improbable, not
   impossible — and the short-send machinery exists precisely because #18 declined to assume it never
   happens. Same window, related: `HandshakeBytesFilled` is never reset, so if more bytes arrive
   while it already equals `RecordSize`, `OnPeerReadable` calls `Receive` on a **zero-length span**
   and can re-enter `OnHandshakeRecordFilled` on the same buffer. Both are narrow; both are in the
   region #20 will rewrite hardest.

4. **Cross-platform behaviour is the thing I could verify least.** I ran the suite on Windows only;
   CI runs both. Two specific risks:
   - `Thread.Sleep(20)` at `StreamSessionHandshakeGuardTests.cs:270` and `:297` *(review — Spec
     pass)*. The assertions are work-shaped (N genuine Hello replies received, not elapsed time),
     which is right, but the *stimulus* is wall-clock and will flake on a loaded runner.
   - `ReadinessDispatcher.Pump` passes a **zero-microsecond** `Select` timeout, so
     `PumpBothUntil(..., maxRounds: N)` is N tight non-blocking polls with no yield between them.
     On Windows loopback the connect and record exchange settle well inside the budget; whether they
     do under Linux CI load, I cannot tell from here. The failure mode is a clean assertion failure
     rather than a hang, which is the right one — but the round budgets are unverified on Linux.

5. **`StreamSessionOptions`' remarks describe a preallocation that has not happened yet.**
   `StreamSessionOptions.cs:27-33` *(review — Spec pass)*. *"A Reactor built from these options
   preallocates `Capacity × (RetentionBytes + receive buffer)` bytes"* — nothing is preallocated on
   this branch, and `ResolveArenaSize()`'s return value is discarded at
   `StreamSessionReactor.cs:57`. Correct as documentation of the *finished* profile, forward-looking
   as documentation of *this* branch. A decision about doc tense, not a defect.

6. **Two shared types carry datagram-only prose that the stream profile now also emits.**
   `SessionEventKind.cs` — `PeerConnected` says *"on the accepting end when a cookie-validated Hello
   is admitted"*; `DisconnectReason.cs` — `RemoteAtCapacity` says *"a Reactor at capacity answers a
   cookie-validated Hello"*. There is no cookie on this profile. Pre-existing text, newly wrong by
   this PR making the types genuinely shared. #16 says a wanted stream-only event kind is a signal to
   stop on — none was wanted, which is the good news; this is only the doc half.

7. **Nagle is not addressed anywhere.** No `NoDelay` in `src/`, and `StreamSessionReactor`'s doc does
   not mention it. Legitimately #22's, but #16 calls Nagle *"a documented property of the profile"*
   rather than an implementation detail, so nothing on this branch yet states it. Confirming this is
   deliberate rather than forgotten is worth a moment.

8. **`[NotNullWhen]` was not adopted; `result.Socket!` was.** `StreamSessionReactor.cs:304`
   *(review — both passes)*. #16 says *"`[NotNullWhen]` rather than the null-forgiving operator"*, and
   `src/CLAUDE.md` says no `!` without an inline comment. This copies `RawReactor.cs:861` verbatim and
   `TcpAcceptResult` carries no annotation, so it is house-consistent and pre-existing — fixing it
   properly means annotating the L1 type, which touches Raw. Left as-is; worth a decision about
   whether consistency or the stated rule wins.

9. **Duplication the interface extraction stopped one member short of removing.** *(review —
   Standards pass, judgement call.)* `PendingEvent`, the `GetPendingEvent` switch, `EnsureUsable`,
   `EnsureNotInsideAPump`, `EnsureCallingThreadIsConstructingThread`, `ToPeerId`/`ToHandle` and
   `DisposeLinks` now exist verbatim in both `StreamSessionReactor.cs` and
   `DatagramSessionReactor.cs`. Separately, `SendRefusalRecord`/`OnRefusalWritable` and
   `SendRecordOrArm`/`OnPeerWritable` are the same four-step short-send shape twice, and `RefusalSlot`
   is a strict subset of `PeerSlot`. Not a defect; a real invitation to a small shared "pending record
   send" helper before #22 adds a third caller.

10. **Minor, from the review passes.** `DiagnosticRecord`'s constructor is now 8 positional params
    and every existing factory gained a third `default` — the `(SubjectKind, Link, Source, Peer)`
    tagged union wants a small struct before a fifth subject kind arrives. The announced-parameter
    quintuple travels together through `StreamHandshakeCodec.Write`, five `TryRead` out-params, and
    five `PeerSlot` fields. `StreamSessionOptions.ReceiveBufferFloor` is used to validate
    `RetentionBytes` too, so the name is narrower than the concept.

### Things I checked and found clean

No `InternalsVisibleTo` (`AssemblySkeletonTests` still enforces it). `src/ReactorSockets` carries
zero NuGet dependencies. No hand-rolled Peer table. No stream-only `SessionEventKind`,
`SessionEvent` or `PeerId` twin. `DiagnosticSubjectKind` gained exactly one member, appended;
`PeerRefused` kept its Source subject and `LingerNegotiated` took the new Peer subject. No sibling
admission-counter member was added. The refusal rules are stated once as a table — `RefusalReasonMap`
in the `RawLinkLegality` shape — and every discard arm over an enum refuses rather than permits
(`RefusalReasonMap.DisconnectReasonFor` → `null`, `TryReadMessageType` → `false`,
`OnHandshakeRecordFilled`'s `default` → close). No test reads a private field, reaches into L0/L1, or
asserts on an internal call: everything asserts on drained `SessionEvent`s, Sink records or Counter
snapshots, and the counterparty instruments are a `RawReactor`-backed `HostilePeer` and a bare BCL
socket, which is the same house pattern `DatagramSessionRefusalTests` already uses.

### The forward edge

Slice 4 needs three things right in advance. **Immutability of negotiated parameters** holds, but by
construction rather than by type: `PeerSlot`'s `Remote*` and `EffectiveLinger` are settable
properties written once in `OnHelloRecordReceived`, and nothing reassigns them because
`OnPeerReadable` returns early once `Established`. Item 3 above is the one narrow path that could
re-enter that method, so it is worth closing for this reason as much as its own. **The ring's shape**
and **the delivery counter's increment point** are both untouched here — nothing was built that
presumes either, and `RemoteMaxMessageSize` (a `uint` against a local `int` `MaxMessageSize`, so #20
will need one cast) is captured and waiting. Neither is foreclosed.

### Red-green: my honest read

**The tests read as written alongside the code, not before it — and all three reports say so
themselves rather than claiming otherwise.** #17 wrote 26 tests as one red pass; #18 built the codec,
options, table and handshake state machine first because nothing would compile otherwise, then drove
the test file against it; #19 did the same for the wire-format-plus-state-machine change. Each
recorded genuine red→green cycles where a first attempt was actually wrong — #18's two
`PeerConnected`-events-in-a-stale-Frame failures and its same-`PeerId`-for-two-dials failure, #19's
`reply[0]` expecting `0x02` and getting `0x01` because the accepter's unconditional Hello precedes
its Refusal on the wire. Those are real failures read from real messages, not compile errors dressed
up.

What the tests *are*: behaviour-shaped names throughout, one behaviour each, AAA layout, and
assertions exclusively on the public surface. What they are not: derived one assertion at a time
from a failing test. The distinction shows in one place — the coverage tracks the shipped design's
seams (a `RefusalSlot` outside `_peers`, a `PendingCloseReason` on `PeerSlot`) closely enough that a
different-but-equivalent design would need different tests, which #16's "a test that would fail if
the ring were replaced by an equivalent structure is testing the wrong thing" warns about. Not badly
so, and not on any single test I would name — but backfill-shaped at the margin.

## Deferred

Flagged by the sub-agents for later slices, carried forward here so it is not lost:

- **No per-Peer counter storage exists on either Session profile.** #18 declined to build
  `PeerCounters` with nothing to increment it; `DatagramSessionReactor`'s `PeerSlot` has none either.
  #20/#21 (or a dedicated subtask) will need to add it to **both** profiles together.
- **`OnPeerReadable` deliberately stops reading once `Established`.** Bytes sent after the handshake
  sit in the kernel receive buffer. The first thing #20 must change. Note the consequence while it
  stands: a remote that dies *after* establishment is never noticed and produces no `PeerClosed`,
  and its socket stays readable-at-zero so the callback fires (and returns immediately) every Pump.
- **A third handshake-phase message has nowhere to go.** The Hello/Refusal records are fixed-size and
  padded to match each other. If Resume needs a third shape, it needs either a new fixed size shared
  by all three or a length-prefixed handshake frame — #19 asks for that to be decided deliberately
  rather than bolted on.
- **No per-Pump batching for Hello or Refusal sends.** Each is its own `Socket.Send`. #22 should look
  at `SendRecordOrArm` and `SendRefusalRecord` alongside whatever it adds for data Messages.
- **`DrainRefusal` treats `EndOfStream` and `Failed` the same as "nothing more to read"** and then
  sends the Refusal anyway. Harmless today because the send failure closes the slot; worth tidying
  when the refusal path is next touched.
- **ADR 0015 raise, per #16's instruction to report rather than decide:** every invariant this slice
  needed was reachable through the public seam, and no test wanted an internal one. Nothing here
  triggers the three conjunctive conditions that would reopen it.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
