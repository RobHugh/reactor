# Issue 2: Solution skeleton — ReactorSockets assembly, test project, package identity

## Done

- `ReactorSockets.slnx` — solution file at repo root (slnx format, matching `.editorconfig`'s
  `[*.{csproj,props,targets,slnx}]` section), referencing both projects in `/src/` and `/tests/`
  solution folders.
- `src/ReactorSockets/ReactorSockets.csproj` — net8.0, `Nullable=enable`, `ImplicitUsings=enable`,
  `RootNamespace`/`AssemblyName`/`PackageId` all `ReactorSockets`. No `PackageReference`s. Package
  metadata (`Description`, `Authors`) set; `GeneratePackageOnBuild=false`, nothing published.
- `src/ReactorSockets/Core/`, `src/ReactorSockets/Transports/` — empty folders (`.gitkeep`) for L0/L1,
  ready for the internal types later sub-issues add. No L2 public types exist yet — nothing in this
  sub-issue's scope requires one.
- `tests/ReactorSockets.Tests/ReactorSockets.Tests.csproj` — net8.0 test project referencing xUnit
  2.9.2, `xunit.runner.visualstudio`, FluentAssertions 6.12.1, NSubstitute 5.3.0,
  `Microsoft.Extensions.TimeProvider.Testing` 8.10.0, plus a `ProjectReference` to
  `ReactorSockets.csproj`. No `InternalsVisibleTo`.
- `tests/ReactorSockets.Tests/AssemblySkeletonTests.cs` — two smoke tests: the `ReactorSockets`
  assembly loads by name, and its exported (public) type set is empty.

## Acceptance criteria

- ✅ `dotnet build` and `dotnet test` both pass on an empty test suite — 2/2 smoke tests pass (see
  Verification).
- ✅ `dotnet format` produces no changes — `dotnet format --verify-no-changes` exits 0.
- ✅ No project file in `src/ReactorSockets` references a NuGet dependency — `ReactorSockets.csproj`
  has zero `PackageReference` entries.
- ✅ `Core` and `Transports` types are internal; no public type exists outside the root namespace —
  review note: `Core/` and `Transports/` contain no types yet (only placeholder folders), so this is
  vacuously true; `AssemblySkeletonTests.ReactorSocketsAssembly_PublicTypes_IsEmpty` pins the empty
  public surface as a regression guard for later sub-issues.

## Verification

- `dotnet format --verify-no-changes` — exit 0, no changes.
- `dotnet build` — Build succeeded, 0 Warning(s), 0 Error(s).
- `dotnet test --no-build --verbosity normal` — Total tests: 2, Passed: 2, Failed: 0.

## Decisions

- **Package ID for the fake time provider.** The issue's parent spec calls it
  `Microsoft.Extensions.Time.Testing`; the real NuGet package is
  `Microsoft.Extensions.TimeProvider.Testing`. Used the correct id (verified against the NuGet
  registration API) so restore succeeds; `src/CLAUDE.md`/`tests/CLAUDE.md` both point at
  `FakeTimeProvider`, which ships in this package.
- **Solution file format.** `dotnet new sln` on this SDK (10.0.301) emits `.slnx` directly (the new
  XML solution format), so no separate `.sln`→`.slnx` migration step was needed.
- **No types in `Core`/`Transports` yet.** The sub-issue scope only asks for the folder layout; adding
  speculative internal types with no behaviour would violate the "no placeholder implementations"
  rule. `.gitkeep` files hold the empty directories in git until the next sub-issue populates them.

## For later sub-issues

- `Core/` and `Transports/` are empty except for `.gitkeep` — delete the `.gitkeep` once the first real
  file lands in each folder.
- The `AssemblySkeletonTests.ReactorSocketsAssembly_PublicTypes_IsEmpty` test will start failing the
  moment `RawReactor` (or any other L2 public type) is added — that is expected and the test should be
  deleted or repurposed at that point, not patched to special-case new types.
- This sandbox's default curl/TLS path fails certificate revocation checks against nuget.org; `dotnet
  restore`/`build` still succeeded through the configured NuGet client, so this only matters if you
  need direct HTTP access to nuget.org for other reasons.
