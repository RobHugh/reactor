# Slice 1 — review state and handover

Written 2026-08-04. Everything here concerns branch `agent/slice-1` and **draft PR #65**
(`agent/slice-1` → `dev`). Read this first when picking the slice back up.

## Where it stands

All eight sub-issues of #1 (#2–#9) are implemented, gate-verified and pushed. The slice was built by
the sandcastle orchestrator over three sessions (see `.sandcastle/RESUME.md` for that side of it),
reviewed by an Opus agent that wrote `PR.md` and opened #65, and has since had four of the eleven
review items fixed by hand.

| | |
|---|---|
| Branch | `agent/slice-1` @ `2fd1104` |
| PR | #65, **draft**, base `dev` |
| Gates | `dotnet format --verify-no-changes`, `dotnet build` (0 warnings), `dotnet test` — all exit 0 |
| Tests | 80 passing |
| Platform verified | Windows 11 / .NET 8 only |

Commit history on the branch:

```
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

## Unresolved

Ordered by how much judgement they need, not by review-item number.

### 4. One datagram per Pump does not come from the read cap — decision needed

`RawReactor.OnLinkReadable` calls `slot.Link.Receive` exactly once per readiness callback
(`src/ReactorSockets/RawReactor.cs:293`), so a Link can never reach a second `Reserve` in one Frame
and `ArenaReservationOutcome.ShareExhausted` is unreachable through the public surface. One datagram
per Link per Pump is a property of the single read, not of the escape. Stories 20/21's per-Pump bound
is therefore currently enforced by the read loop's absence rather than by the arena.

Both designs are defensible. The reviewer called this the single most consequential thing in the
slice to get a second opinion on, and flagged that it read the same #1 the implementers read, so a
shared misreading would have passed it too. #5's own report predicted the outcome and shipped the
mechanism anyway per ADR 0014. **Nobody has decided whether this is the intended design.** The test
comment in `RawReactorFrameTests` states the situation rather than leaving it to be rediscovered.

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

### 8. Thread affinity is enforced unevenly, and story 3 has thin L2 coverage

`EnsureCallingThreadIsConstructingThread` is called from `Pump`, `Bind`, `SendTo`, `Close` and
`Dispose`, but not from `Send` (`RawReactor.cs:213`), `Shutdown` (`:223`), `LocalOf` (`:234`),
`CountersOf` (`:264`) or `Counters`. The reads are arguably fine; `Send`/`Shutdown` are less
obviously so. The re-entrancy work deliberately did **not** change this — `EnsureUsable` covers
disposal and re-entrancy on every member, thread affinity was left exactly as it was, because
widening it is this open decision.

`Pump_CalledFromAnotherThread_ThrowsInvalidOperationException` now drives a foreign thread at a real
`RawReactor` (it previously only existed against the dispatcher), so story 3 is pinned at L2 for
`Pump` — but for no other member.

### 9. `Send`/`Shutdown` ignore their arguments

Both return a constant `Result.Failure` without resolving the `LinkId`
(`src/ReactorSockets/RawReactor.cs:213` and `:223`). A stale or garbage `LinkId` gets "Send is illegal
on a datagram Link" rather than "not open". Correct for this slice, where every Link is datagram; it
becomes wrong the moment slice 2 adds stream Links, and `Send_OnDatagramLink_...` /
`Shutdown_OnDatagramLink_...` would keep passing straight through that change.

### 11. `ObservedPumpInterval` is unconsumed and now untested

`src/ReactorSockets/Core/ReadinessDispatcher.cs:71`. It is RTO groundwork for slice 3+ that nothing
in this slice consumes, and since L0 went internal it has no test either. **Three options: consume it
in slice 3, expose it on `RawReactor`, or delete it.** Untested unconsumed code is the worst of the
three, so this wants a decision rather than drift.

`RawReactor.Shutdown` existing at all is the other half of this item — `Shutdown` appears by name in
#1's Out of Scope list, though returning a failed `Result` for it is what user story 9 asks for.

### 10 (rest). Standards items

- `NSubstitute` 5.3.0 is referenced by `tests/ReactorSockets.Tests/ReactorSockets.Tests.csproj:21`
  and never used. The test doubles are hand-written, which `tests/CLAUDE.md` permits for non-boundary
  types — so either drop the package or start using it.
- `EnsureCallingThreadIsConstructingThread` is duplicated verbatim between `RawReactor.cs:435` and
  `Core/ReadinessDispatcher.cs:137`.
- `!` is used without the documented-limitation comment `src/CLAUDE.md` requires —
  `Transports/UdpLink.cs:67` (`socket.LocalEndPoint!`) and several test sites.
- Several test names bundle two assertions with an "And", against `tests/CLAUDE.md`'s atomic-tests
  rule: `Log_ThrowingSink_IsInvokedExactlyOnceAndTheFrameStillCompletes`,
  `Drain_ArenaWithNoRoomForASecondLinksDatagram_DeliversOneAndReportsArenaExhausted`,
  `Pump_SendToClosedPort_ProducesNoClosedEventAndOneDestinationUnreachableRecord`, and others.
- Error policy is inconsistent: "Link is not open" is a `Result.Failure` in `SendTo` but an
  `ArgumentException` in `LocalOf`/`CountersOf`.

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

- All three gates pass, run from the worktree, outside any agent's session.
- The suite has been run a handful of times on one machine. **Nothing here speaks to flakiness under
  load or on another platform.** `Pump_SteadyStateAfterWarmupWithNothingReady_AllocatesNothingOnTheManagedHeap`
  asserts an exact `0` from `GC.GetAllocatedBytesForCurrentThread()` over 1 000 Pumps and is the most
  likely to turn brittle under a different runtime or a debugger.
- The standing spec checks were verified mechanically by the reviewer and still hold: no
  `InternalsVisibleTo`, no `DualMode`, no `MaxMessageSize` in the shared options value, no type named
  `Reactor`, no `PackageReference` in the src csproj.
- The orchestrator's **repair path has never fired** — no gate failed across all eight sub-issues, so
  that branch of `.sandcastle/orchestrate.ts` is untested code.

## Picking this up next session

1. Read this file, then `.sandcastle/reports/PR.md` for the reviewer's own words on items 3–11.
2. The branch may need a worktree: `git worktree add .sandcastle/worktrees/agent-slice-1 agent/slice-1`
   (sandcastle's teardown removes it after a clean run).
3. Open items 4, 5, 8, 9 and 11 need a decision before code. Items under 10 are mechanical and can be
   done without one.
4. #65 is a **draft** and nothing auto-merges. Merging it closes #2–#9 via the `Closes` lines, so the
   sub-issues stay open until a human marks it ready and merges.
