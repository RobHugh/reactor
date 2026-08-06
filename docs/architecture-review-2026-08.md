# Architecture Review — August 2026

A record of the deepening opportunities surfaced against `RawReactor` after slice 2, what was
implemented, and what was rejected. Kept so a future review does not re-suggest work that was
already considered and turned down for a reason.

Vocabulary is `/codebase-design`'s (module, interface, depth, seam, adapter, leverage, locality) for
architecture, and `CONTEXT.md`'s for the domain.

**Scope of the survey.** `RawReactor.cs` was the codebase's hot spot by a wide margin — the largest
source file and the most-churned across slices 1 and 2 — so the survey weighted it heavily, as YAGNI
demands: deepening pays off where change keeps landing.

---

## Implemented

### 1 — Lift the Share-holder table out of `RawReactor`

**Strength at survey:** Strong · **Shipped:** `fe5a024`, PR #69

`RawReactor` hand-rolled a generational slot table inline: a `LinkSlot?[]`, a parallel `int[]` of
generations, `FindFreeSlot`/`TryFindFreeSlot`, and generation bookkeeping spread across `CloseSlot`,
`Dispose` and the accept path. Identity mechanism and Reactor policy were interleaved in the same
method bodies.

Extracted to `ShareHolderTable<T>` and `SlotHandle` in `Core/`, generic over the occupant because
Capacity counts Share-holders — Links in Raw, Peers in a Session (ADR 0014) — and ADR 0007 records
that `PeerId` is index-plus-generation *"like `LinkId`"*. All three Reactors need this table; only the
occupant type differs.

**Mechanism, not policy.** `TryAdd` answers `null` when full and never throws, so Raw keeps both of
its policies — throw on `Bind`, defer on accept — at L2, where a message naming "the Reactor"
belongs. Dispatcher registration, disposal and Event queuing stayed with the Reactor.

**Behaviour change:** generations begin at 1. Previously the first Link bound was `LinkId(0, 0)` —
bit-identical to `default(LinkId)`, which every non-`Accepted` Event carries in `ListeningLink`. Two
tests pin it, through both channels a bad `LinkId` is reported on (`SendTo`'s `Result`, `LocalOf`'s
throw); both were confirmed to fail with generations at 0.

Shipped alongside a vocabulary pass making the code obey `CONTEXT.md`: `CloseReason.ResetByPeer` →
`ResetByRemote` (Peer is a Session-only word) and `RawEvent.Listener` → `ListeningLink` (listening is
a role a Link plays; there is no such term as Listener). `CONTEXT.md` gained the resource-budgeting
vocabulary slice 2 had shipped without ever passing through the glossary — L0/L1/L2, Arena,
Share-holder, Capacity, Read share, Admission, Deferral, Slot, Payload, Dial.

**A claim corrected during the survey.** The original report justified this partly on "the interface
is the test surface — no sockets." That is false for this repo: commit `1aeffe6` deliberately deleted
five direct L0 unit-test files (88 → 73 tests), there is no `InternalsVisibleTo` anywhere, issue #1
forbids it, and `AssemblySkeletonTests` enforces a namespace guard. The candidate was re-based on
locality and leverage alone, and the testing policy split out to #68.

### 2 — State the Link legality matrix once

**Strength at survey:** Strong · **Shipped:** `6da3671`, PR #70

`Send`, `SendTo`, `Shutdown` and `RemoteOf` each established a Link's kind **by elimination** — check
for one kind, then name the remaining one in the `is not` arm:

```csharp
if (slot is DatagramLinkSlot)          { return ...("use SendTo"); }
if (slot is not StreamLinkSlot stream) { return ...("illegal on a listening Link."); }
```

Correct only while exactly three kinds exist. A fourth would have made all four members confidently
misname the kind in a **public error message**. The matrix itself was written down nowhere in the
source — only in `RawReactorLegalityTests.cs`'s doc comment.

`RawLinkLegality` now states all twelve `(operation, kind)` pairs explicitly, so no kind is inferred
from the absence of the others. `TryResolveFor<TSlot>` folds the three failure modes — not open,
illegal for this kind, wrong slot type — into one call, collapsing each member's checks from twelve
lines to three.

Two safety properties, of different kinds:

- **`LinkSlot.Kind` is abstract** — a genuine compile-time hole. A new kind of Link cannot compile
  without declaring itself, which sends its author to the matrix.
- **The discard arm refuses rather than permits** — a runtime net. C# cannot prove a switch over
  enums exhaustive (an enum can hold unnamed values), verified against the compiler rather than
  assumed, so a discard is mandatory whatever shape the table takes. Making it refuse means a missing
  row fails closed. Demonstrated by deleting the `(Send, Datagram)` row: still compiles, call still
  refused with the generic message, never wrongly permitted.

**This grew the codebase** — roughly +31 lines of code and +89 of documentation. It bought legibility
and correctness under growth, not brevity. Recorded here because the survey's original framing
oversold it as a net deletion.

### 3 — State the arena's reservation policy once

**Strength at survey:** Worth exploring · **Shipped:** `62e6441`, PR #71

`DeliverDatagram` and `DeliverSegment` ran the same reserve/refuse sequence over the frame arena,
differing only in the event shape queued on success — a duplication the source itself already admitted
to:

> "The reservation logic is identical to `DeliverDatagram`'s; only the event shape and the absence of
> a claimed Source differ." — `RawReactor.cs`

`TryReserve` now holds everything that is a fact about **bytes entering the arena**: the reservation,
both refusals with their accounting, and the two counters true of any admitted Payload. What kind of
Event those bytes become — and whether a Source is claimed — stays at the call site, which is why
`RecordDatagramReceived` sits outside: it counts whole Datagrams, not bytes. Each `Deliver` fell from
about 31 lines to 10, for a net **−7 lines of code**. Modest, and not the point; the point is that the
refusal policy is now stated once.

Its fall-through returns `false` rather than handing back an uninitialised offset — the same
fail-closed rule the legality matrix follows for a kind with no row.

**It stayed in `RawReactor` rather than moving to `Core/`,** which the survey had floated so the
Session profiles could reuse it. They cannot, on two ADRs:

- **ADR 0010** puts the Session reservation at a different point in the pipeline — *"the counter
  increments when a Message is handed to the frame arena, **not when bytes are read**"* — and calls
  getting that backwards the exact failure the mechanism exists to prevent. Slice 3 accumulates until
  a u32-prefixed Message completes; slice 5 branches on a control header first; slice 7 reassembles.
- **ADR 0007** requires the refusal policy itself to diverge: *"At capacity the two profiles diverge,
  because one of them owns a protocol and the other does not."* Raw drops what it cannot hold and says
  nothing on the wire; a Session answers with `RetentionExhausted` backpressure.

`FrameArena` is the module both profiles genuinely share, and it was already in `Core/`. A general
lesson for the next review: **shared-looking code is not the same as a shared seam.** Check where in
the pipeline the other caller sits before lifting anything for its benefit.

**Two claims corrected.** The survey said "three of the four switch arms are byte-identical" — there
are three arms and two are identical. It also proposed the name `TryAdmit`, which collides with
`CONTEXT.md`'s **Admission** (taking on a new Share-holder). No glossary change was made: this is
internal mechanics, and `CONTEXT.md` is a domain glossary.

---

## Rejected on re-derivation

The two remaining candidates were re-examined against the bar the first two set. Neither clears it,
and one is not an architecture change at all.

### 4 — Collapse the shallow L1 result types

**Verdict: rejected.** The survey counted seven result types in `Transports/` and called them
uniformly shallow. On inspection only three share a shape (`TcpLinkSendResult`, `UdpLinkSendResult`,
`TcpLinkShutdownResult`); the other four carry genuinely different outcome rosters —
`NoPendingConnection`, `Pending`, `Truncated`, `Unreachable`, `EndOfStream` — and collapsing them
would erase distinctions the Reactor switches on.

Of the three that do share a shape, merging the two send results would flatten a **load-bearing
semantic difference**:

```csharp
TcpLinkSendResult.ForSent(bytesSent)                 => new(true, ...)              // partial is success
UdpLinkSendResult.ForSent(bytesSent, bytesOffered)   => new(bytesSent == bytesOffered, ...)  // all-or-nothing
```

A datagram send is all-or-nothing (ADR 0008); a stream send reporting fewer bytes than offered is
normal and arms exactly one `Writable`. One type cannot hold both meanings of `IsSuccess` without a
flag that re-splits it. The types are shallow by measure and correct by design; the distinctness *is*
the interface.

### 5 — Give `Drain` a value-shaped projection

**Verdict: rejected as architecture; it is a feature.** `RawEvent` and `RawEventEnumerator` are
`ref struct`s that alias the frame arena, which is what makes `Drain` allocation-free and what
enforces that nothing escapes the Frame. A heap-shaped projection would not deepen an existing
module — it would add public surface and allocate, against the grain of the whole design.

If a consumer needs an Event to outlive its Frame, copying the payload is their call to make, and
offering to make it for them is a product decision for a future slice, not architecture upkeep.

---

## Newly surfaced, and not recommended

Found during re-derivation; recorded so they are not re-proposed as discoveries.

**The `Bind`/`Listen`/`Connect` preamble.** All three open with the same four lines
(`EnsureUsable`, `EnsureCallingThreadIsConstructingThread`, `ThrowIfNull`,
`EnsureCapacityForAnotherLink`) before diverging entirely — different link factory, different slot
type, different dispatcher registration, and `Connect` registers nothing until its handshake
resolves. Extracting the preamble saves about eight lines and buys indirection over three call sites.
Below the bar.

**The eight `IsEnabled` diagnostic guards.** `if (_diagnostics.IsEnabled(Warn)) { _diagnostics.Log(...) }`
appears eight times in `RawReactor`. The guard looks like duplication but is load-bearing: its purpose
is to avoid *constructing* the `DiagnosticRecord` when no sink wants it. Folding the level check into
`DiagnosticGateway.Log` would construct the record unconditionally; passing a factory would allocate a
closure per call. This is the idiomatic `ILogger` pattern and should stay.

**No test seam at L0.** `RawReactor` constructs all six of its collaborators directly
(`RawReactor.cs:110-116`), so `ReadinessDispatcher` cannot be substituted and every one of the 165
tests binds real loopback sockets, serialised.

This is the largest leverage item in the codebase — and it is **not actionable as architecture**. It
is worth being precise about why, because it is easy to misread as a violation:

- ADR 0003 is **satisfied**, not breached. Its replaceability claim is about internal *source*
  replacement — L0 is `internal`, so swapping the dispatcher needs a recompile, not a major version.
  It never promised runtime substitution.
- ADR 0012 chose these construction surfaces deliberately. There is no DI here by decision.
- By `/codebase-design`'s own rule — *one adapter means a hypothetical seam, two means a real one* —
  there is exactly one readiness mechanism today. A second exists only as a test fake, and whether
  this repo may reach for one is the open question in **#68**.

So the seam is blocked on a policy decision, not on design work. It should not be built until #68 is
settled either way.

---

## Open

**Nothing from this survey.** All five candidates are settled: three shipped, two rejected with
reasons recorded above.

---

## Standing conclusion

After three implemented rounds, **the well is dry**. Every candidate this survey raised has been built
or turned down, and the third was small enough that finding a fourth of its size would mean lowering
the bar rather than finding friction.

The remaining friction in this codebase is not shallow modules — it is the test-seam policy question
in **#68**, which no refactor can settle. A future review should treat that as the finding rather than
manufacture a sixth card.

The bar these rounds set: a deepening earns its place by fixing a real defect or concentrating a
policy that will genuinely be repeated — not by reducing line count, and not by satisfying a shape.
Two of the three that shipped did not reduce line count at all.

And the lesson candidate 3 added: **check where the other caller sits in the pipeline before lifting
anything for its benefit.** Code that looks shared is not the same as a seam that is.
