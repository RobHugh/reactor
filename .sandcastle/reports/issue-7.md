# Issue 7: 1.6 Diagnostics facade, DiagnosticRecord, and pull-only Counters

## Done

- `src/ReactorSockets/DiagnosticId.cs` — the closed cause enum with explicit reserved-block values:
  `AdmissionCapReached = 1`, `ArenaExhausted = 2`, `LinkFailed = 100`, `DestinationUnreachable = 101`,
  `DatagramTruncated = 102`. No `None`/`Unknown`, zero unused. `Value0`/`Value1` meanings documented
  per member's XML doc.
- `src/ReactorSockets/DiagnosticSubjectKind.cs` — tag enum `{ None, Link, Source }` (see Decisions on
  why `Peer` is not a case yet).
- `src/ReactorSockets/LinkId.cs` — `readonly record struct LinkId(int Index, int Generation)`,
  introduced as a prerequisite for the tagged subject (see Decisions).
- `src/ReactorSockets/DiagnosticRecord.cs` — rewritten from the issue-2 stub into the full readonly
  record struct: `Id`, `Level`, `SubjectKind`, `Link`, `Source`, `Value0`, `Value1`, with a private
  constructor and five static factories (`ForAdmissionCapReached`, `ForArenaExhausted`,
  `ForLinkFailed`, `ForDestinationUnreachable`, `ForDatagramTruncated`) that bake in each id's fixed
  level and subject kind so a caller cannot mismatch them.
- `src/ReactorSockets/ReactorCounters.cs` / `ReactorCounterSet.cs` — the Reactor-scope snapshot struct
  (`SinkFaulted`, `DatagramsSent`, `DatagramsReceived`, `ArenaHighWaterBytes`, `AdmissionsDeferred`)
  and its mutable, monotonic, no-reset accumulator.
- `src/ReactorSockets/LinkCounters.cs` / `LinkCounterSet.cs` — the per-Link snapshot struct
  (`BytesSent`, `BytesReceived`, `ReadShareExhaustions`) and its accumulator.
- `src/ReactorSockets/DiagnosticGateway.cs` — the facade proper: wraps a nullable `Sink` +
  `MinimumLevel`, exposes `IsEnabled(level)` (Sink present, not faulted, level ≥ configured minimum,
  and the Sink's own `IsEnabled` agrees) as the single pre-construction gate, and `Log(in record)` as
  the only call site that invokes the Sink — catching a throw, counting `SinkFaulted`, and disabling
  itself permanently.
- `IReactorDiagnostics.cs` left unchanged; it already matched this issue's contract.
- Tests: `DiagnosticIdTests.cs`, `DiagnosticRecordTests.cs`, `ReactorCounterSetTests.cs`,
  `LinkCounterSetTests.cs`, `DiagnosticGatewayTests.cs` (30 new test cases).
- `tests/ReactorSockets.Tests/AssemblySkeletonTests.cs` — extended the exported-types allow-list with
  the seven new public types.

## Acceptance criteria

- ✅ No Sink installed ⇒ no record constructed — `DiagnosticGatewayTests.IsEnabled_NoSinkInstalled_IsFalseForEveryLevel`
  and `Log_NoSinkInstalled_NoRecordIsConstructedAndLogIsANoOp` pin that `IsEnabled` is the gate every
  caller must pass first; production call sites never reach `Log` because `IsEnabled` already
  answered `false`.
- ✅ Throwing Sink invoked exactly once, `SinkFaulted` reads 1, no subsequent record reaches it, that
  call still completes normally — `Log_ThrowingSink_IsInvokedExactlyOnceAndSinkFaultedReadsOne` and
  `Log_AfterSinkFaulted_NoSubsequentRecordReachesIt`: three `Log` calls, `ThrowingSink.CallCount == 1`,
  `counters.Snapshot().SinkFaulted == 1`, and none of the three calls throw out of `Log`.
- ❌ **A Sink calling back into the Reactor throws** — cannot be satisfied in this sub-issue. There is
  no `RawReactor` on this branch yet (that is a later sub-issue's UDP-transport/Link work); re-entrancy
  detection is necessarily a property of `Pump`'s call-stack state, which only `RawReactor` will have.
  `IReactorDiagnostics`'s XML doc already states the requirement for whoever wires `DiagnosticGateway`
  into `RawReactor` next.
- ✅ Two snapshots around known traffic differ by exactly the expected amounts, no reset method exists
  — `ReactorCounterSetTests.Snapshot_TwoSnapshotsAroundKnownTraffic_DifferByExactlyTheExpectedAmounts`
  and the `LinkCounterSet` equivalent; neither type exposes a reset method (compile-time check, not a
  runtime one).
- ✅ `DiagnosticId` values assert against their block numbers — `DiagnosticIdTests.Values_EveryMember_MatchesItsReservedBlockNumber`
  pins all five members' underlying `int` values; `Values_NoMemberIsZero_...` confirms no member is 0.

## Verification

- `dotnet format` — clean, no diff.
- `dotnet build` — Build succeeded, 0 warnings, 0 errors.
- `dotnet test --no-build --verbosity normal` — Total tests: 75, Passed: 75 (30 new; 45 carried over
  from earlier sub-issues, unchanged).

## Decisions

- **`DiagnosticSubjectKind` ships without a `Peer` case.** ADR 0013 names the subject as "none, Peer,
  Link, or Source", but Peers belong entirely to the Session profiles, explicitly out of scope for
  this slice (no `Peer`/`PeerId` type exists anywhere in the codebase, and none of this slice's five
  shipped `DiagnosticId` members ever populate a Peer subject — `ArenaExhausted`'s "Peer or Link" is
  always Link in Raw, since Raw has no Peers). Adding a case with no type to carry and no record that
  uses it would be exactly the speculative design the project guidelines warn against. The enum is
  additive-safe: a Session-profile sub-issue can add `Peer` alongside whatever `PeerId` shape it
  defines without touching this issue's members.
- **`LinkId` is introduced here**, one sub-issue ahead of the Link/UDP-transport work, because
  `DiagnosticRecord`'s tagged subject cannot carry a Link without it. Kept to exactly ADR 0014's
  shape (`{ Index, Generation }`) and nothing else — no formatting, no `IsStale` helper — so the
  Link-lifecycle sub-issue is free to add behaviour without inheriting assumptions from here.
- **The static factories bake in each id's fixed `Level` and `SubjectKind`** rather than taking them as
  parameters, so a call site cannot construct e.g. `LinkFailed` at `Info` or with a `Source` subject —
  the roster's level-per-id and subject-per-id pairings become compile-time-enforced rather than a
  convention callers must remember.
- **`DiagnosticGateway.IsEnabled` ANDs the configured `MinimumLevel` with the Sink's own `IsEnabled`.**
  Both `ReactorOptions.MinimumLevel` (a construction-time floor) and `IReactorDiagnostics.IsEnabled`
  (the Sink's own say) exist in the surface this issue inherited; treating them as independent gates
  is the only reading that gives the interface method a reason to exist rather than being dead
  surface a consumer must implement but the library never consults.
- **`DiagnosticGateway`, `ReactorCounterSet`, `LinkCounterSet` are `public`, not `internal`**, following
  every prior sub-issue's precedent (`FrameArena`, `ReadinessDispatcher`): no `RawReactor` exists to
  wrap them, and `InternalsVisibleTo` is forbidden, so this issue's own tests need direct access. They
  belong in `Core/` and should become `internal` once `RawReactor` wraps them and re-exposes only the
  snapshot structs and the Sink/`MinimumLevel` construction surface.
- **No keyed-by-`LinkId` counters registry (e.g. `Dictionary<LinkId, LinkCounterSet>`) is built here.**
  The issue's "fetched by value and keyed by Link" is a property of whatever holds Link lifecycle —
  which belongs to the Link/UDP-transport sub-issue, since only it knows when a `LinkCounterSet` is
  created (on registration) and discarded (on close/slot reuse). Building the registry now would be
  guessing at that lifecycle's shape.

## For later sub-issues

- The Link/UDP-transport sub-issue should hold one `LinkCounterSet` per registered Link (likely
  alongside the arena's share-holder index) and expose a `LinkCounters GetCounters(LinkId)` style
  accessor once `RawReactor` exists — there is deliberately no such registry yet.
- `RawReactor`'s constructor should build one `DiagnosticGateway` from `ReactorOptions.Sink` +
  `ReactorOptions.MinimumLevel` and one `ReactorCounterSet`, call `gateway.IsEnabled(level)` before
  building any `DiagnosticRecord` anywhere in L0/L1, and enforce "calling back into the Reactor from a
  Sink throws" — the one acceptance criterion this sub-issue could not close, since it requires
  `Pump`'s call-stack state to exist.
- `FrameArena.Reserve`'s `ArenaReservationOutcome.ArenaExhausted` path is exactly where
  `DiagnosticRecord.ForArenaExhausted` and `ReactorCounterSet.ObserveArenaUsage` are meant to be
  called from once wired into `RawReactor`.
- No issues found with earlier sub-issues' work; nothing outside this issue's scope was touched.
