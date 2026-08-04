# Issue 5: Frame arena, Frame lifetime, and the derived read share

## Done

- `src/ReactorSockets/FrameArena.cs` — `FrameArena(arenaSize, capacity)`: one preallocated
  `byte[]` buffer plus a per-share-holder `int[]` usage table, both allocated once at construction.
  `ReadShare` is `arenaSize / capacity` (ADR 0012/0014). `Reserve(shareHolderId, data)` commits bytes
  on behalf of a share-holder: a share-holder's first request this Frame always escapes its fair
  share (bounded only by the arena's own free space); a second request that would push it past
  `ReadShare` is refused as `ShareExhausted`; any request that does not fit in `BytesFree` is refused
  as `ArenaExhausted`. `Reset()` wipes usage for the next Frame by reusing the same backing memory
  (no clear, no reallocation).
- `src/ReactorSockets/ArenaReservation.cs` — `readonly ref struct` result of `Reserve`: `Outcome`,
  `Payload` (the committed span, aliasing the arena directly), `BytesRequested`, `BytesFree`. It is a
  `ref struct` because `Payload` must never outlive the Frame.
- `src/ReactorSockets/ArenaReservationOutcome.cs` — `Committed` / `ShareExhausted` / `ArenaExhausted`.
- `tests/ReactorSockets.Tests/FrameArenaTests.cs` — 13 tests (17 cases with theories) covering
  construction validation, the `ReadShare` arithmetic, the escape, `ShareExhausted`, `ArenaExhausted`
  with its carried bytes, Frame-to-Frame reuse, and the composed default behaviour.
- `tests/ReactorSockets.Tests/AssemblySkeletonTests.cs` — extended the exported-types allow-list with
  `FrameArena`, `ArenaReservation`, `ArenaReservationOutcome`.

## Acceptance criteria

- ✅ Composed default behaviour pinned: `Reserve_EachShareHolderExceedingItsShareOnce_...` — at
  arena 65 507 / capacity 4 (`ReadShare` = 16 376), two share-holders each escape a 20 000-byte unit
  on their first request and are refused (`ShareExhausted`) on their second, in the same Frame.
- ✅ Larger arena → proportional share, asserted arithmetically:
  `ReadShare_ArenaSizeAndCapacity_IsArenaSizeDividedByCapacity` asserts `readShare == arenaSize /
  capacity` across four (arena, capacity) pairs; `Reserve_LargerArenaAtSameCapacity_...` asserts a
  doubled arena at fixed capacity doubles the share.
- ✅ Arena full → refused with `ArenaExhausted` carrying bytes requested and bytes free, Frame still
  completes: `Reserve_RequestExceedingArenaFreeSpace_...` (`BytesRequested`=150, `BytesFree`=100) and
  `Reserve_AfterArenaExhausted_FrameStillCompletesAndResetRecoversFullCapacity` (`Reset()` afterward
  returns to `BytesUsed`=0, `BytesFree`=1000 — the arena-level refusal doesn't corrupt subsequent
  Frames).
- ✅ Frame N payload invalid in Frame N+1, by observing reuse not a crash:
  `Reset_BytesCommittedInPriorFrame_AreOverwrittenByTheNextFrame` keeps the `Span<byte>` from Frame
  1's reservation, resets, commits different bytes at the same share-holder in Frame 2, and asserts
  the *held* span now reads Frame 2's bytes — proving the same backing memory was reused, not
  reallocated.

## Verification

- `dotnet format --verify-no-changes` — clean, no diff.
- `dotnet build` — Build succeeded, 0 warnings, 0 errors.
- `dotnet test --no-build --verbosity normal` — Total tests: 41, Passed: 41 (17 new/expanded
  `FrameArenaTests` cases; 24 carried over from earlier sub-issues, unchanged).

## Decisions

- **`FrameArena` is `public`, not `internal`, matching issue 1.2's precedent.** The parent slice
  states L0 stays internal, but no `RawReactor` exists yet on this branch to expose the arena through
  `Pump`/Drain, and the rules forbid `InternalsVisibleTo`. Issue 1.2 resolved the identical tension by
  making `ReactorOptions.Validate`/`ResolveArenaSize` directly callable; this issue follows the same
  shape. `FrameArena`, `ArenaReservation` and `ArenaReservationOutcome` should become `internal` once
  a later sub-issue's `RawReactor` wraps the arena inside `Pump` — at that point these types stop
  needing to be part of the public surface and the assembly allow-list test should shrink back down.
- **Share-holder identity is a bare `int` (a zero-based index below `Capacity`), not a `LinkId`.**
  `LinkId` (index + generation) doesn't exist yet; a plain index is exactly what a future dispatcher
  needs to key the per-share-holder usage array, and a stale-handle generation check belongs to
  whoever owns `LinkId`, not to the arena.
- **The escape/share-cap mechanics are exercised directly with multiple `Reserve` calls per
  share-holder per Frame**, even though the eventual Raw dispatcher (a later sub-issue: readiness
  scan does one Select and one read per ready Link per Pump) will likely never call `Reserve` more
  than once per share-holder per Frame in practice — meaning `ShareExhausted` may be effectively
  unreachable through Raw as actually wired. The mechanism is still built and tested to spec because
  ADR 0014 states it generally (it also covers Peers in a Session profile, where multiple reads per
  Pump per Peer are plausible) and because "removing the cap entirely was rejected" is an explicit
  design decision this issue owns.
- **`ArenaExhausted` is reported only as the `ArenaReservationOutcome` enum value plus the
  `BytesRequested`/`BytesFree` fields on `ArenaReservation`**, not routed through
  `IReactorDiagnostics`/`DiagnosticRecord`. `DiagnosticRecord` is still the field-less placeholder
  issue 1.2 left it as ("issue 1.6 owns the full `DiagnosticId` roster and tagged subject"), so there
  is nothing to populate yet. A later sub-issue that wires the diagnostics facade should have its
  `Pump` implementation translate `ArenaReservationOutcome.ArenaExhausted` into a `DiagnosticRecord`
  with id 2, carrying these same two numbers.
- **Reserve's byte-cap check order is arena-first, then share.** A request that doesn't fit in the
  arena's free space is `ArenaExhausted` even on a share-holder's very first request of the Frame —
  the escape entitles a share-holder to exceed its *share*, never to manufacture arena space that
  isn't there ("the escape does not weaken the bound... reading still stops when the arena fills").

## For later sub-issues

- The readiness dispatcher (likely 1.5) should call `FrameArena.Reserve` once per ready share-holder
  per Pump with the actual bytes read off the scratch buffer, and translate `ArenaExhausted` into the
  `AdmissionCapReached`/`ArenaExhausted` diagnostic once `DiagnosticRecord` carries the id/subject
  roster.
- `RawReactor`'s constructor is where `FrameArena` gets constructed, using `ReactorOptions
  .ResolveArenaSize(floor, description)` for `arenaSize` and `ReactorOptions.Capacity` for `capacity`
  — both already public per issue 1.2.
- `RawReactor.Pump` should call `FrameArena.Reset()` exactly once, at the start of every Pump (not at
  the end), so that Drain surfaces the *previous* Pump's Events until the next Pump wipes them —
  matching "the Frame — the span from one Pump to the next."
- Once `RawReactor` wraps the arena, revisit whether `FrameArena`/`ArenaReservation`/
  `ArenaReservationOutcome` can move to `internal` and out of the assembly's exported-types allow-list.
