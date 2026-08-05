# Slice 1 — review state and handover

Written 2026-08-04. Everything here concerns branch `agent/slice-1` and **draft PR #65**
(`agent/slice-1` → `dev`). Read this first when picking the slice back up.

## Where it stands

All eight sub-issues of #1 (#2–#9) are implemented, gate-verified and pushed. The slice was built by
the sandcastle orchestrator over three sessions (see `.sandcastle/RESUME.md` for that side of it),
reviewed by an Opus agent that wrote `PR.md` and opened #65, and has since had **ten of the eleven
review items closed** by hand.

**Only item 5 remains open, and it cannot be closed from Windows** — it needs the suite run on
Linux. Everything else is decided and implemented.

| | |
|---|---|
| Branch | `agent/slice-1` @ `174ce5f` |
| PR | #65, **draft**, base `dev` |
| Gates | `dotnet format --verify-no-changes`, `dotnet build` (0 warnings), `dotnet test` — all exit 0 |
| Tests | 92 passing |
| Platform verified | Windows 11 / .NET 8 only |

Commit history on the branch:

```
174ce5f  Close review items 4, 8, 9, 11 and the rest of 10
aa35e27  Repair the mojibake the L0 move left in Core/ XML docs
05e2401  Record slice 1's review state for the next session
2fd1104  Guard Sink re-entrancy and make RawReactor IDisposable
1aeffe6  Make L0 internal and re-express its tests at the observable layer
8aaccf3  Slice 1 review: PR body with reviewer checklist
a809982  9: 1.8 Binding, address family policy, and the admission cap
74ca6be  8: 1.7 L1 UDP transport and the Raw datagram surface
c5cd95a  7: 1.6 Diagnostics facade, DiagnosticRecord, and pull-only Counters
6f26d51  6: 1.5 L0: readiness dispatch on Socket.Select, Pump, and thread affinity
84aef31  5: Frame arena, Frame lifetime, and the derived read share
72367b9  4: The Source value type and the two-type address vocabulary
a17a5c8  3: Shared options value, the default-provenance rule, and construction failure
47310b8  2: Solution skeleton: ReactorSockets assembly, test project, and package identity
```

The full reviewer assessment is `.sandcastle/reports/PR.md`, which is also #65's body. Per-sub-issue
reports are `.sandcastle/reports/issue-2.md` … `issue-9.md`. This file is the index over them.

---

## Resolved

### 1. L0 was public at the assembly root — fixed in `1aeffe6`

Issue #1 requires `Core/` and `Transports/` to be internal (user story 28: L0 and L1 replaceable
wholesale without a major version). Every sub-issue from #3 on shipped its types `public` instead,
each citing the same reason — no `RawReactor` existed yet to wrap them, `InternalsVisibleTo` is
forbidden, so the tests had no other way in — and each promised a later sub-issue would move them
back. `RawReactor` landed in #8 and nobody went back.

`FrameArena`, `ArenaReservation`, `ArenaReservationOutcome`, `ReadinessDispatcher`,
`DiagnosticGateway`, `ReactorCounterSet` and `LinkCounterSet` now live in
`src/ReactorSockets/Core/` under `namespace ReactorSockets.Core`, all `internal`.
`ReactorOptions.Validate()` and `ResolveArenaSize()` are internal too — the reviewer did not list
them under this item, but they were public for the identical reason and #1 says construction is
where validation throws.

The exported surface is now seventeen types, all in the root namespace: `RawReactor`, `RawEvent`,
`RawEventKind`, `RawEventEnumerator`, `CloseReason`, `Result`, `LinkId`, `Source`, `SourceFamily`,
`ReactorOptions`, `IReactorDiagnostics`, `DiagnosticRecord`, `DiagnosticLevel`, `DiagnosticId`,
`DiagnosticSubjectKind`, `ReactorCounters`, `LinkCounters`.

### 2. The seam test recorded the leak instead of catching it — fixed in `1aeffe6`

`AssemblySkeletonTests` maintained an allow-list of exported type names that each sub-issue widened
in turn. It is replaced by `ReactorSocketsAssembly_ExportedTypes_ContainNothingFromL0OrL1`, which
asserts every exported type's namespace is `ReactorSockets` — so a new public type in `Core/` or
`Transports/` fails without anyone having to remember to extend a list.

The five direct L0 unit-test files were deleted and re-expressed against `RawReactor`:
`RawReactorPumpTests`, `RawReactorFrameTests`, `RawReactorDiagnosticsTests`,
`RawReactorCountersTests`. `ReactorOptionsTests` now drives every options failure through
`new RawReactor(...)`.

**Coverage genuinely lost** — no test can reach an internal type without `InternalsVisibleTo`:

- the arena's `ReadShare` arithmetic across several (arena, capacity) pairs; one configuration is
  now pinned, through the escape test
- `ShareExhausted` — already unreachable through Raw, see open item 4
- `FrameArena`'s constructor and `shareHolderId` range guards, unreachable because `ReactorOptions`
  validates first; they remain as defensive code
- `ReadinessDispatcher.ObservedPumpInterval` — see open item 11

### 6. `RawReactor` was not `IDisposable` — fixed in `2fd1104`

`Dispose` unregisters and closes every open Link, is idempotent, and makes every other member throw
`ObjectDisposedException`. It queues no `Closed` events, since nothing can drain a disposed Reactor.

**There is deliberately no finalizer**, which is a departure from the review item as written: this
type holds nothing unmanaged of its own, and each `Socket` already finalizes itself, so a finalizer
would add GC cost to every Reactor to duplicate a guarantee the runtime already makes. The reasoning
is in the XML docs on `Dispose` — revisit it if you disagree rather than assuming it was overlooked.

Release is asserted the only way a consumer can observe it: bind a pinned port in one Reactor,
dispose it, bind the same port in another. Binds are exclusive, so it only passes if the socket was
really closed. `RawReactorHarness` now disposes the Reactor, so the tests no longer leak sockets.

### 7. Sink re-entrancy was unguarded — fixed in `2fd1104`

This was **the slice's one unmet acceptance criterion** (#7's fifth, marked ❌ in `issue-7.md`).
`RawReactor` carries an `_inPump` flag set for the whole of `Pump`, and every public member starts
with a guard rejecting a call arriving from inside a Frame. A Sink that calls back gets an
`InvalidOperationException` naming the rule; if it lets that escape it has thrown, so the gateway
faults and disables it as for any other Sink bug and the Frame still completes.

**Do not weaken these tests to type-only assertions.** Without the guard, a nested `Pump` throws
`InvalidOperationException` anyway, once it mutates the dispatcher's scan list mid-iteration — so
asserting only the exception type passes against no guard at all. The tests assert the message.

### 3. The read-cap escape test #1 asks for — partly fixed in `1aeffe6`

`Drain_DatagramLargerThanTheReadShare_IsDeliveredWholeViaTheEscape` pins the escape at the
observable layer, and `Drain_SeveralLinksEachSentTwoOversizedDatagrams_YieldsExactlyOnePerLinkPerPump`
pins the one-datagram-per-Link-per-Pump behaviour a consumer sees at the defaults. **Read open item 4
before treating this as closed** — the second test passes for a reason that is not the read cap.

### 10 (part). Duplicate recording sinks — fixed in `1aeffe6`

Three separate recording-Sink implementations across the test project are consolidated into
`RecordingDiagnosticsSink` plus `ThrowingDiagnosticsSink` (and `ReentrantDiagnosticsSink`, added in
`2fd1104`), all in `tests/ReactorSockets.Tests/RecordingDiagnosticsSink.cs`.

---

## Resolved in this session — the five open decisions

The repo owner took all five decisions on 2026-08-05. Items 4, 8, 9, 11 and the rest of 10 are
implemented in `174ce5f`; a separate `aa35e27` repairs an encoding defect found while doing it.

### 4. One datagram per Pump does not come from the read cap — decided: keep the single read

`RawReactor.OnLinkReadable` calls `slot.Link.Receive` exactly once per readiness callback, so a Link
can never reach a second `Reserve` in one Frame and `ArenaReservationOutcome.ShareExhausted` is
unreachable through the public surface. **This is the intended design for the Raw profile**, not an
oversight: one read per callback is the strongest fairness guarantee available — every ready Link is
served before any Link is served twice — and it costs one syscall per Link per Pump, which the
consumer already controls by choosing how often to Pump.

The reasoning now lives on `OnLinkReadable` rather than in this file, and the consequence is recorded
on `ShareExhausted` itself: it is retained rather than deleted because it is the bound a profile that
reads a Link in a loop will need, and the arena is already sized on the assumption that it exists.
Changing `OnLinkReadable` to loop until `ShareExhausted` is what makes that switch.

**What this closes and what it does not.** The design question is settled. `ShareExhausted` is still
unexercised code, and stays that way until a profile exists that can reach it — that is now a stated
property rather than an unnoticed gap.

### 8 and 9. `Send`/`Shutdown` — decided: resolve the LinkId first, and guard the thread

Both now resolve the `LinkId` and answer `"Link {id} is not open."` for a stale one, reaching the
datagram-illegality message only for a Link that really is open. Both now call
`EnsureCallingThreadIsConstructingThread`.

The old tests asserted only that the `Result` failed, which is exactly the trap the reviewer
described: they would have kept passing when slice 2 makes "illegal on a datagram Link" the wrong
answer for a stream Link. The replacements assert the *message*, so the two rules are distinguishable
— `Send_OnAnOpenDatagramLink_ReturnsFailedResultNamingTheIllegality` and
`Send_OnALinkIdThatIsNotOpen_ReturnsFailedResultSayingSo`, and the same pair for `Shutdown`.

Thread affinity now covers every mutating member and no pure read. `Send`, `Shutdown` and `SendTo`
each have a foreign-thread test; `CountersOf_CalledFromAnotherThread_DoesNotThrow` pins the other
side, so the rule cannot be widened to the reads by accident. The `Pump` foreign-thread test was
strengthened from a type-only assertion to a message assertion, for the same reason the re-entrancy
tests assert theirs.

### 11. `ObservedPumpInterval` — decided: deleted

It was RTO groundwork nothing consumed, and it lost its test when L0 went internal. Untested
unconsumed code is the worst of the three options, so it is gone, taking `ReadinessDispatcher`'s
`TimeProvider` parameter, `_lastPumpTimestamp` and `RecordObservedInterval` with it. **Slice 3
re-adds it with a consumer and a test in the same commit.**

`ReactorOptions.TimeProvider` stays — it is the shared options value every profile takes — and its
XML doc now says outright that nothing in the Raw profile reads it and which slices will.

`RawReactor.Shutdown`, the other half of this item, stays as it is: user story 9 asks for a failed
`Result` from it, which outranks `Shutdown` appearing in #1's Out of Scope list.

### 10 (rest). Standards items — all done

- **NSubstitute dropped.** The test doubles are hand-written, which `tests/CLAUDE.md` permits for
  non-boundary types, so the reference went rather than growing a use for it.
- **The duplicated thread guard is gone from `ReadinessDispatcher`**, not extracted into a shared
  helper. The dispatcher is internal and reachable only through the Reactor that owns it, which
  guards every public member before delegating — so the second check could only ever duplicate the
  first, and the exception a consumer sees should name the Reactor rather than a type they cannot
  reference. The reasoning is in a `<remarks>` on `Pump`.
- **The six `!` sites carry the documented-limitation comment** `src/CLAUDE.md` requires
  (`Socket.LocalEndPoint`, `Type.FullName`, `ParameterInfo.Name`). One of them was not a framework
  limitation at all — `ReentrantDiagnosticsSink.Reactor` genuinely can be null if a test forgets to
  set it, so it now throws a named exception instead, which stops that mistake surfacing as a
  `NullReferenceException` in a faulted Sink and being mistaken for the behaviour under test.
- **The non-atomic tests are split**, each asserting one fact, with the shared arrange in a named
  private helper rather than duplicated: the truncation pair, the closed-port pair, the throwing-Sink
  pair, the arena-exhaustion pair, and the escaping-re-entrant-Sink trio.
- **The error policy is documented rather than made uniform.** A member that already returns a
  `Result` reports a bad `LinkId` as a failed Result; a member that returns data throws
  `ArgumentException`, because it has no channel that could carry it and the alternative is a
  sentinel or an unwrap at every call site. `Close` is neither — it is idempotent, so a stale
  `LinkId` is a no-op. The rule is on `RawReactor` itself, where a consumer meets it.

### Encoding repair — found while doing the above, not a review item

`1aeffe6` (the L0 move) rewrote five `Core/` files through a CP1252 round trip, corrupting 24 em
dashes into `â€”` and one division sign into `Ã·` — including the `arena ÷ capacity` formula in
`FrameArena`'s summary. The originals at `84aef31` were clean. Repaired byte-wise in `aa35e27` with
no wording changes. This matters more than it looks: XML doc comments are what a consumer sees in
IntelliSense and what CodeMap indexes.

A non-ASCII audit over `src`, `tests` and `docs` now shows only `—`, `−`, `÷` and the legitimate
symbols in the design docs. **Worth re-running after any bulk file move on this repo.**

---

## Unresolved

### 5. Truncation detection is verified on one platform

`UdpLink.Receive` probes `Socket.Available > scratch.Length` before reading
(`src/ReactorSockets/Transports/UdpLink.cs:90`). #8 changed to this because on Windows 11 / .NET 8 an
oversized UDP receive neither threw `SocketError.MessageSize` nor set `SocketFlags.Truncated` — it
silently returned exactly `scratch.Length` bytes.

Winsock's `FIONREAD` documentation says it returns "the amount of data that can be read in a single
call to `recv`, which may not be the same as the total amount of data queued". If any platform or
socket state makes `Available` report the cumulative queue instead of the next datagram, several
small queued datagrams are misreported as one truncation and the payload is silently dropped. User
story 13 asks for identical truncation behaviour on Windows and Unix; **this slice has only ever run
on Windows.** Running the suite on Linux is the cheapest way to close or confirm this.

This is the **only** open item, and nothing in this session touched it — `UdpLink.Receive` is
unchanged. It needs a Linux run, not a decision. Until then the slice's claim to user story 13 is
unverified on half the platforms it names.

### Deferred by design (not defects)

Recorded so they are not rediscovered as bugs:

- **The admission cap has no consumer.** `ResolvedAdmissionCap`, `RecordAdmissionDeferred` and
  `ForAdmissionCapReached` are reachable only from tests. #1 establishes the cap here on purpose;
  slice 2's accepts are its first caller.
- **`DiagnosticSubjectKind` ships without a `Peer` case** — no `Peer`/`PeerId` type exists in this
  slice. The enum is additive-safe.
- **`CloseReason.ResetByPeer`, `Refused`, `Unreachable` are never produced** — only `LocalClose` and
  `Failed` have paths. The stream half of ADR 0008 supplies the rest in slice 2.
- **No per-`LinkId` counters registry.** `LinkCounterSet` is held per slot inside `RawReactor` and
  reached through `CountersOf(LinkId)`.
- **`Socket.Available` as a next-datagram size must not be carried into slice 2's stream transport.**
- **`DiagnosticFormatter` and the `ReactorSockets.Logging` adapter** are slice 9, per #1's Out of
  Scope.
- **#2 corrected the spec**: #1 names the test package `Microsoft.Extensions.Time.Testing`; the real
  NuGet id is `Microsoft.Extensions.TimeProvider.Testing`. The issue text is wrong, not the code.

---

## Verification state — what has and has not been checked

- All three gates pass at `174ce5f`, run from the worktree, outside any agent's session: `dotnet
  format --verify-no-changes` exit 0, `dotnet build` 0 warnings, `dotnet test` 92/92.
- The suite has been run a handful of times on one machine. **Nothing here speaks to flakiness under
  load or on another platform.** `Pump_SteadyStateAfterWarmupWithNothingReady_AllocatesNothingOnTheManagedHeap`
  asserts an exact `0` from `GC.GetAllocatedBytesForCurrentThread()` over 1 000 Pumps and is the most
  likely to turn brittle under a different runtime or a debugger.
- The standing spec checks were verified mechanically by the reviewer and still hold: no
  `InternalsVisibleTo`, no `DualMode`, no `MaxMessageSize` in the shared options value, no type named
  `Reactor`, no `PackageReference` in the src csproj.
- The orchestrator's **repair path has never fired** — no gate failed across all eight sub-issues, so
  that branch of `.sandcastle/orchestrate.ts` is untested code.

### Not covered by this session's work

- **Item 5 is untouched.** No Linux run happened; `UdpLink.Receive` is unchanged.
- **The new thread-affinity tests prove the guard fires, not that the Reactor is thread-safe.** They
  call one member from one foreign thread and assert it is rejected. Nothing here exercises genuine
  concurrent access, and the counter reads are explicitly documented as samples rather than
  consistent cuts — the fields behind them are written without a barrier.
- **`ShareExhausted` remains unexercised**, now by decision rather than by oversight. No test asserts
  it, because no profile can reach it.
- **The 12 new tests all came from splitting existing ones or from items 8/9.** Coverage of the arena
  arithmetic lost when L0 went internal (recorded under resolved item 2) has not been recovered.

## Picking this up next session

1. Read this file first. `.sandcastle/reports/PR.md` has the original reviewer's own words on items
   3–11, but its "Review this carefully" section describes the slice as it stood when #65 was opened
   — this file is the current state.
2. The branch may need a worktree: `git worktree add .sandcastle/worktrees/agent-slice-1 agent/slice-1`
   (sandcastle's teardown removes it after a clean run).
3. **Item 5 is the only thing standing between this slice and a mergeable state**, and it needs a
   Linux run of the suite, not a decision. Everything else is closed.
4. #65 is a **draft** and nothing auto-merges. Merging it closes #2–#9 via the `Closes` lines, so the
   sub-issues stay open until a human marks it ready and merges.
5. Slice 2 (#10–#15) is the next orchestrator run. Before launching it, re-read
   `.sandcastle/RESUME.md` — in particular `git switch dev` in the host checkout first, and note that
   `Socket.Available` must not be carried into the stream transport.
