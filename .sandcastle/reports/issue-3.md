# Issue 3: Shared options value, the default-provenance rule, and construction failure

## Done

- `src/ReactorSockets/ReactorOptions.cs` — the shared value (ADR 0012): `Capacity` (no default),
  `AdmissionCap` (defaults to `Capacity` via `ResolvedAdmissionCap`), `ArenaSize` (profile-resolved,
  no universal default), `TimeProvider` (defaults to `TimeProvider.System`), `Sink` +
  `MinimumLevel` (one argument, both or neither). `Validate()` checks everything that does not
  depend on a profile floor; `ResolveArenaSize(floor, description)` applies and checks the
  profile-specific arena floor, with the failure message stating the arithmetic.
- `src/ReactorSockets/DiagnosticLevel.cs` — the four-level enum ADR 0005 names (`Trace`, `Info`,
  `Warn`, `Error`), ascending severity.
- `src/ReactorSockets/IReactorDiagnostics.cs` — the Sink contract (`IsEnabled`/`Log`) so
  `ReactorOptions.Sink` has a real type to compose. Minimal on purpose: the diagnostic roster and
  Sink-fault semantics belong to issue 1.6.
- `src/ReactorSockets/DiagnosticRecord.cs` — an empty `readonly record struct` placeholder for
  `Log(in DiagnosticRecord)` to compile against. Field roster is issue 1.6's (ADR 0013).
- `tests/ReactorSockets.Tests/ReactorOptionsTests.cs` — 14 tests covering every AC scenario plus
  `AdmissionCap`/`ArenaSize`/`TimeProvider` default resolution.
- `tests/ReactorSockets.Tests/AssemblySkeletonTests.cs` — updated the now-obsolete
  "public types is empty" assertion to assert the expected public surface by name (see Decisions).

## Acceptance criteria

- ✅ Every default has stated provenance in XML docs — `AdmissionCap`, `ArenaSize`, `TimeProvider`
  doc comments each cite the derivation and the ADR.
- ✅ Error messages state the arithmetic — `ResolveArenaSize` message: "arena of {n} bytes cannot
  hold {description} — raise ArenaSize to at least {floor}."; pinned by
  `ResolveArenaSize_ArenaSizeBelowProfileFloor_ThrowsArgumentExceptionStatingArithmetic`.
- ✅ Tests cover capacity omitted / capacity < 1 / admission cap < 1 / Sink without level / level
  without Sink / arena below profile floor — all present in `ReactorOptionsTests.cs`, 16/16 passing.

## Verification

- `dotnet format` — clean, no diff on final run.
- `dotnet build` — Build succeeded, 0 warnings, 0 errors.
- `dotnet test --no-build --verbosity normal` — Total tests: 16, Passed: 16.

## Decisions

- **`ReactorOptions` is a plain class with `init` properties, not a `record`.** Records' generated
  equality/`ToString` aren't needed here and a class keeps `Validate()`/`ResolveArenaSize` reading
  as ordinary methods rather than fighting record semantics.
- **No C# `required` modifier on `Capacity`.** `required` would make "capacity omitted" a compile
  error, which is untestable at runtime and would also block a profile from ever supplying a
  post-hoc default. Instead it defaults to `0` and `Validate()` rejects it — "omitted" and
  "explicitly 0" are the same runtime state and both are exercised as separate tests per the AC
  wording.
- **Validation is two public methods, not one, and neither runs implicitly at construction.**
  `Validate()` covers everything the shared value can check on its own; `ResolveArenaSize` is the
  one check ADR 0012 says belongs to "each Reactor's constructor" because the floor is
  profile-specific. Per the parent issue's testing rule (no `InternalsVisibleTo`, tests observe only
  the public surface) and the fact that no `RawReactor` exists yet in this branch, both had to be
  public and directly callable so this issue's own tests could exercise them. A later Reactor
  constructor calls both from its own constructor, which is where "construction throws" becomes
  true end-to-end.
- **Diagnostics types are minimal stand-ins**, not full implementations. `DiagnosticRecord` is
  field-less and `IReactorDiagnostics` only carries the two methods ADR 0005 names. They exist
  solely so `ReactorOptions.Sink`/`MinimumLevel` have real types to compose and validate against.
  Issue 1.6 owns the full `DiagnosticId` roster, the tagged subject, and Counters — expect it to add
  fields to `DiagnosticRecord` rather than replace it, since `ReactorOptions` already depends on its
  identity.
- **`AssemblySkeletonTests.ReactorSocketsAssembly_PublicTypes_IsEmpty` was rewritten**, not left
  failing. It asserted zero public types, which issue 1.1 wrote before any public API existed; this
  issue is the first to add one. Replaced with an explicit allow-list by type name so the next
  sub-issue that adds public API gets a pointed failure here rather than a silent pass.

## For later sub-issues

- `RawOptions` (and the two Session options types) should compose `ReactorOptions` as a member and,
  in their own constructors, call `Validate()` then `ResolveArenaSize(floor, description)` with
  their profile's floor — that is where "construction throws" becomes observable through a real
  Reactor.
- Issue 1.6: `DiagnosticRecord` and `IReactorDiagnostics` already exist with minimal shape; extend
  rather than redefine, since `ReactorOptions` is compiled against them.
- Issue 1.8 (binding/admission cap) will want `ReactorOptions.ResolvedAdmissionCap`.
