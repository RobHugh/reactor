# Issue 4: The Source value type and the two-type address vocabulary

## Done

- `src/ReactorSockets/SourceFamily.cs` — explicit `IPv4`/`IPv6` tag enum.
- `src/ReactorSockets/Source.cs` — `readonly record struct Source(SourceFamily Family, UInt128 AddressBytes, long ScopeId, int Port)`, with `FromIPEndPoint`/`ToIPEndPoint` conversions. Address bytes are stored in a `UInt128` (a genuine 16-byte blittable value, not a heap-allocated `byte[]`) via `BinaryPrimitives.Read/WriteUInt128BigEndian`, so the raw byte layout the OS reported round-trips exactly regardless of numeric interpretation.
- `tests/ReactorSockets.Tests/SourceTests.cs` — round-trip, equality/hash-code and size-budget tests.
- `tests/ReactorSockets.Tests/AssemblySkeletonTests.cs` — extended the exported-types allowlist to include `Source` and `SourceFamily`.

## Acceptance criteria

- ✅ Round-trip `IPEndPoint` → `Source` → `IPEndPoint` preserves family, address, scope and port for IPv4, IPv6 global and IPv6 link-local — `SourceTests.ToIPEndPoint_RoundTrippedFromFromIPEndPoint_PreservesFamilyAddressScopeAndPort`, 3 theory cases.
- ✅ Two `Source` values differing only in scope id are not equal — `Equality_SourcesDifferingOnlyInScopeId_AreNotEqual`.
- ✅ An IPv4 `Source` and an IPv6 `Source` holding the same 16 bytes are not equal — `Equality_IPv4AndIPv6SourcesHoldingSameBytes_AreNotEqual` (the record struct's compiler-generated equality compares `Family` alongside the bytes, so this holds without a custom `Equals`).
- ✅ `GetHashCode` agrees with equality on all of the above — paired `HaveDifferentHashCodes`/`Equality_IdenticalSources_AreEqualWithMatchingHashCodes` tests.
- ✅ The struct's size is asserted — `Source_SizeOf_MatchesTheDocumentedBudget` pins `Unsafe.SizeOf<Source>()` at 48 bytes.

## Verification

- `dotnet format` — clean, no changes.
- `dotnet build` — Build succeeded, 0 Warning(s), 0 Error(s).
- `dotnet test --no-build --verbosity normal` — Total tests: 25, Passed: 25.

## Decisions

- **Storage for the 16 address bytes**: chose `UInt128` over `byte[16]` or four `uint`s. `UInt128` is a genuine value type (no heap allocation, matching the record-path load-bearing requirement) and `BinaryPrimitives` gives an exact byte-for-byte round trip via big-endian read/write, so the field's numeric interpretation never matters — only its byte layout does.
- **Struct size came out to 48 bytes, not exactly 56.** The issue's "roughly 56 bytes" is ADR 0013's budget, not a hard requirement, and the acceptance criterion only asks that the size be *asserted* so a future field addition is deliberate. 48 bytes (enum padded to 8-byte alignment for the `UInt128`, 16 bytes of address, 8-byte scope id, 4-byte port padded to 8) is under budget, which is fine — I pinned the actual value rather than padding artificially to hit 56.
- **IPv4 storage convention**: `FromIPEndPoint` writes the 4 IPv4 bytes into the first 4 bytes of a zeroed 16-byte buffer before reading it as `UInt128`; `ToIPEndPoint` reads only the first 4 bytes back out for `SourceFamily.IPv4`. This is an internal convention only — nothing outside `Source` inspects `AddressBytes` directly — so it doesn't need to match any wire format.
- **`ScopeId` on IPv4** is always `0`; `IPAddress.ScopeId` throws for `AddressFamily.InterNetwork`, so `FromIPEndPoint` special-cases IPv4 to avoid touching that property, and the round-trip test skips asserting scope-id equality for non-IPv6 cases.

## For later sub-issues

- `Source.FromIPEndPoint`/`ToIPEndPoint` throw `ArgumentException`/`ArgumentOutOfRangeException` for anything outside IPv4/IPv6 (e.g. `AddressFamily.Unspecified`) — later slices that bind or receive should never feed those in, since `DualMode` is never set (ADR 0014), but the guard is there if a bug elsewhere produces one.
- No issues found with earlier sub-issues' work; nothing outside this issue's scope was touched.
