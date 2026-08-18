# Issue #82 — 3.4.4 The delivery counter and its increment point

## Done (by file)

- `src/ReactorSockets/PeerCounters.cs` — new. `PeerCounters(ulong MessagesReceived)`, a
  point-in-time snapshot record mirroring `LinkCounters`'s Raw-profile shape (ADR 0013).
- `src/ReactorSockets/Core/PeerCounterSet.cs` — new. The mutable per-Peer counter storage,
  mirroring `Core/LinkCounterSet`: `RecordMessageReceived()` and `Snapshot()`.
- `src/ReactorSockets/StreamSessionReactor.cs` — `PeerSlot` gets a `Counters` field
  (`PeerCounterSet`); a new public `CountersOf(PeerId)`, mirroring
  `RawReactor.CountersOf(LinkId)` exactly (throws `ArgumentException` if the Peer is not open);
  and one new line in `DeliverCompleteMessages`'s `Ready` arm — `slot.Counters.RecordMessageReceived();`
  — placed immediately after the `PendingEvent` is queued for the arena-committed Message, mirroring
  `RawReactor.DeliverDatagram`'s placement of `_counters.RecordDatagramReceived()`.
- `tests/ReactorSockets.Tests/StreamSessionDeliveryCounterTests.cs` — new. Two tests against a
  hand-built raw `Socket` counterparty (`CounterPeer`, mirroring `StreamSessionReadShareTests`'s
  `SharePeer`).
- `tests/ReactorSockets.Tests/AssemblySkeletonTests.cs` — added `nameof(PeerCounters)` to the
  expected L2-surface type list; this is the only place a new public type must be declared for
  that test to keep passing.

## Acceptance criteria

- **The counter after N delivered Messages reads N.** Checked —
  `CountersOf_AfterThreeDeliveredMessages_MessagesReceivedReadsThree`: three Messages, each sent
  and delivered in its own Pump, asserts `CountersOf(id).MessagesReceived == 3` at the end.
- **A Message whose bytes have all arrived but which has not yet been handed to the frame arena
  has not incremented it — held back with an exhausted read share, observed across two Pumps.**
  Checked — `CountersOf_AMessageHeldBackByAnExhaustedReadShare_DoesNotAdvanceUntilThePumpThatDeliversIt`:
  reuses #81's exact setup (capacity 4, 200-byte arena → 50-byte per-Peer share), sends three
  20-byte Messages (60 bytes total) in one write before any Pump runs, so the third Message's
  bytes are fully present at the front of the receive buffer from the very first read. Asserts
  `MessagesReceived == 2` after the Pump that delivers what the share allows (not 3), then Pumps
  further with nothing more sent by the peer and asserts `MessagesReceived == 3` only once the
  retry mechanism (#81) actually hands the third Message to the arena. This is asserted purely
  through `CountersOf`, never by inspecting `PeerSlot` or the buffer.
- **The counter is `u64` and cumulative from the Peer's establishment, with no reset.**
  `PeerCounters.MessagesReceived` is `ulong`; `PeerCounterSet` exposes no reset method — the same
  shape as `LinkCounterSet`/`ReactorCounterSet`, which ADR 0013 already establishes as
  monotonic-by-construction.
- **Reachable through the public seam, no `InternalsVisibleTo`.** `CountersOf(PeerId)` is public
  on `StreamSessionReactor`; `PeerCounters` is a public record; `PeerCounterSet` stays `internal`
  in `Core/`, reached only through the snapshot. `AssemblySkeletonTests` (both the exported-types
  list and the no-`InternalsVisibleTo` check) passes.
- **The chosen surface is stated as a decision, not left implicit.** See Decisions below.

## TDD cycles

1. **Red (compile-time).** Wrote both tests in `StreamSessionDeliveryCounterTests.cs` against
   `Reactor.CountersOf(id)`, which did not exist yet. `dotnet build` failed with six
   `CS1061: 'StreamSessionReactor' does not contain a definition for 'CountersOf'` errors — the
   right reason, not an assertion failure.
2. **Green (production code).** Added `PeerCounters`, `Core/PeerCounterSet`, the `Counters` field
   on `PeerSlot`, `CountersOf(PeerId)`, and the single `RecordMessageReceived()` call site. Build
   succeeded; filtered run of the two new tests: the positive-count test passed immediately, the
   held-back test initially failed once (`MessagesReceived` read `0` where `2` was expected) using
   a first draft of the test that drove `PumpUntil` with a condition that itself drained events
   (`DrainToListWithPayload().Count > 0 || …`) — a test-harness ordering issue in the test's own
   polling condition, not a production bug (confirmed by adding a per-Pump counter trace and
   observing the drain-based condition exit before the delivering Pump's effects were the ones
   being read). Rewrote the condition to poll `CountersOf(id).MessagesReceived` directly instead of
   draining as a side effect of the loop condition; reran 5× in a row with zero flakes.
3. **Refactor.** `dotnet format` made no further changes. Ran the full suite: one failure —
   `AssemblySkeletonTests.ReactorSocketsAssembly_ExportedTypes_AreOnlyTheL2SurfaceAConsumerDrives`,
   expected (a new public type joined the L2 surface); added `nameof(PeerCounters)` to that test's
   expected list, the only change that test permits for a legitimate new public type. Full suite
   re-run: 367/367 green, re-run twice more for stability with the TCP-timing test present (all
   green, ~6-7.5s each).

## Verification

- `dotnet format` — no changes (already clean).
- `dotnet build` — 0 warnings, 0 errors.
- `dotnet test --verbosity normal` — **367/367 passed**, 0 failed (365 baseline + 2 new), run three
  times total for stability given the counter test drives a real TCP socket. No
  `AssemblySkeletonTests` regression beyond the expected, intentional addition of `PeerCounters` to
  the exported-type list; the no-`InternalsVisibleTo` assertion is untouched and still passes.

## Decisions

- **Where the counter surfaces: a new `PeerCounters` snapshot record plus `CountersOf(PeerId)` on
  `StreamSessionReactor`, mirroring `RawReactor.CountersOf(LinkId)` exactly, rather than folding it
  into `SessionEvent` or a new field on an existing public type.** This is the explicit decision
  the issue asks for. #18 (3.2) allocated per-Peer state alongside the Peer's slot but never
  actually surfaced a per-Peer *counter* publicly — `PeerSlot` today carries no counters at all in
  either Session profile, so there was no existing per-Peer counter shape to extend. The nearest
  precedent is Raw's `LinkCounters`/`CountersOf(LinkId)` pair (ADR 0013): a `readonly record struct`
  snapshot, keyed by the entity's public identity, fetched by value with no enumerating accessor,
  backed by an `internal` mutable set living on the slot. `PeerId` is already shared by both Session
  profiles (ADR 0010), so the same shape generalises without forcing anything on the datagram
  profile — `PeerCounters` today has one member that only the stream profile ever moves, exactly as
  `ReactorCounters`' own doc remarks a member can sit at true zero on a profile that never touches
  it. The alternative considered and rejected: putting the count on `SessionEvent` itself (e.g. a
  running total attached to each `MessageReceived` event). Rejected because a Counter answers "how
  often", independent of any single Frame's events (ADR 0013's snapshot-by-value pull model), while
  an event answers "what happened this Frame" — conflating the two would mean a consumer who missed
  draining one Frame's events loses visibility into a total that a Counter is supposed to make
  recoverable by diffing two snapshots at any time, not just at delivery time.
- **Increment site: inside `DeliverCompleteMessages`'s `Ready` arm, immediately after the
  `PendingEvent` is queued, not inside `TryReserveMessage`.** This matches the exact separation
  `RawReactor.DeliverDatagram` already makes from `TryReserve` — `TryReserve`/`TryReserveMessage`
  answer "did the arena accept these bytes"; the counter answers "was a whole unit delivered". Doing
  it at the `DeliverCompleteMessages` level rather than `TryReserveMessage` is also what makes the
  negative acceptance criterion true by construction: `TryReserveMessage` returning `false`
  (`ShareExhausted`/`ArenaExhausted`) never reaches the increment, and the retry path
  (`RetryPeersWithBufferedMessages`) calls back into the very same `DeliverCompleteMessages`, so the
  eventual successful delivery increments exactly once with no separate wiring — as #81's own report
  already predicted.
- **No `CountersOf` throw is exercised by a new test here.** `RawReactorCountersTests` already
  establishes the house pattern for `CountersOf` (`ArgumentException` on an unknown/closed id) and
  this method's body is copy-shaped from it; adding a redundant "throws when unknown" test for the
  same shape on a second type was judged to test the pattern twice rather than this issue's actual
  behaviour, so it was not added.

## For later sub-issues

- This is the last of the four #20 sub-issues in this run (#79–#82 complete). No `Send` path,
  retention ring, or write-batching was touched, per the scope discipline.
- **For slice 4 (#25), which wires counter *emission* and retention release**: the increment point
  is exactly `DeliverCompleteMessages`'s `Ready` arm (`slot.Counters.RecordMessageReceived()`,
  `src/ReactorSockets/StreamSessionReactor.cs`) — nothing else in this Reactor increments
  `MessagesReceived`, so that is the one place to look if retention accounting needs to observe the
  same event. `PeerCounters` is deliberately a single-member record today (`MessagesReceived` only);
  extending it with, say, `BytesReceived` or a retention-specific counter is additive — follow
  `LinkCounters`'/`ReactorCounters`' existing shape (append fields to the `readonly record struct`
  and the matching `Core` counter set) rather than inventing a new surface.
- `PeerCounterSet` and `CountersOf(PeerId)` exist only on `StreamSessionReactor`. The datagram
  Session profile (`DatagramSessionReactor`) has no equivalent yet; if a future slice needs one
  there, `PeerCounters`'s shape is already generic enough to reuse (it is keyed on `PeerId`, shared
  by both profiles per ADR 0010), but the `Counters` field itself was added only to the stream
  profile's `PeerSlot`, not to `DatagramSessionReactor`'s.
