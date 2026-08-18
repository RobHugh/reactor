# Issue #17 (3.1) — StreamSessionOptions, cross-checks, and the allocation bill

## Done

- `src/ReactorSockets/StreamSessionOptions.cs` — new options type, composed (not inherited) from
  the shared `ReactorOptions`. Five required knobs (`MaxMessageSize`, `RetentionBytes`,
  `SendIdleInterval`, `Linger`, plus `Reactor.Capacity`), two defaulted knobs
  (`ReceiveBufferSize` → `MaxMessageSize + 4`, `MissedHeartbeats` → 3), all cross-checks, and the
  memory-bill arithmetic (`ReceiveBudgetBytes`, `SendBudgetBytes`, `ValidateMemoryBill`).
- `src/ReactorSockets/StreamSessionReactor.cs` — a minimal constructor: null-checks, calls
  `options.Validate()` then `options.ResolveArenaSize()`, and stores nothing else. No fields, no
  `IDisposable` — there is nothing yet to hold or release; 3.2 and 3.3 build on this same
  constructor.
- `tests/ReactorSockets.Tests/StreamSessionOptionsTests.cs` — 26 tests, one baseline builder with
  every knob overridable by name, mirroring `DatagramSessionOptionsTests`'s shape.
- `tests/ReactorSockets.Tests/AssemblySkeletonTests.cs` — added `StreamSessionReactor` and
  `StreamSessionOptions` to the exported-types allow-list; this test failed once the two new public
  types existed, and is the correct place to declare them.

## Acceptance criteria

- ✅ Construction throws `ArgumentOutOfRangeException` for a single bad value — 12 tests cover
  Capacity, AdmissionCap, MaxMessageSize, MissedHeartbeats, SendIdleInterval, Linger.
- ✅ Construction throws `ArgumentException` for a combination — RetentionBytes below floor,
  ReceiveBufferSize below floor, ArenaSize below floor, memory bill too large, Sink/MinimumLevel
  mismatch.
- ✅ Messages state the arithmetic, not the predicate — e.g. retention message names both the
  supplied bytes and `MaxMessageSize`; asserted by `.Contain("4099").And.Contain("4096")`.
- ✅ XML docs carry `Capacity × (RetentionBytes + receive buffer)` as one line, plus the
  double-buffering note, on the class remarks.
- ✅ `MaxMessageSize` has its own sentence describing the three places it appears.
- ✅ Each cross-check asserted independently; `Constructor_MemoryBillExceedsMaxAllocatable_...`
  asserts the message contains both line totals and the grand total.

## TDD cycles

Written as one red pass across the whole test file rather than one assertion at a time — the 26
tests are all statements about one options type with one validation idiom already established by
`DatagramSessionOptionsTests`, so there was one design to implement, not 26. This departs from
strict single-assertion TDD; noted rather than hidden.

- Red: full test file added referencing `StreamSessionOptions`/`StreamSessionReactor` → build
  failed with `CS0246: The type or namespace name 'StreamSessionOptions' could not be found`.
- Green: added both production types → all 26 new tests passed on the first run.
- Refactor: `dotnet format` flagged `IDE0052` (unread private field) on `StreamSessionReactor`'s
  `_options` field; removed the field entirely rather than suppress, since nothing in this
  sub-issue's scope reads it yet.
- Follow-up red/green: full suite run surfaced `AssemblySkeletonTests.
  ReactorSocketsAssembly_ExportedTypes_AreOnlyTheL2SurfaceAConsumerDrives` failing (allow-list
  didn't know the two new public types) → added both names → suite green.

## Verification

- `dotnet format` — clean, no diffs.
- `dotnet build` — 0 Warnings, 0 Errors.
- `dotnet test` — 306 passed, 0 failed (280 pre-existing + 26 new).

## Decisions

- **No `Endpoints` on `StreamSessionOptions`.** The issue's own Scope section says "Plus the
  endpoint set (subtask 3.2)" — endpoints are explicitly out of 3.1's scope, unlike
  `DatagramSessionOptions` where endpoints and options landed together. Confirmed against the
  cross-check list in #17, which has no endpoint-related row.
- **`StreamSessionReactor` stores nothing.** 3.1's only job is validating options and the
  allocation bill; there is no arena, counter set, or Peer table to build yet (that starts
  arriving in 3.2/3.3), so the constructor does the two required calls and returns. This is a
  genuine (not stub) implementation of exactly this sub-issue's scope — no `Send`, no framing, no
  retention ring were added, per the run's explicit prohibition.
- **Retention/receive-buffer floor is `MaxMessageSize + 4`** (the `u32` length prefix), matching
  the issue's stated cross-checks verbatim; no fragment-size concept exists on this profile since
  TCP has no such unit.

## For later sub-issues

- 3.2 (#18) adds `Endpoints` to `StreamSessionOptions` and binding logic to
  `StreamSessionReactor`'s constructor — mirror `DatagramSessionOptions.Endpoints` and
  `DatagramSessionReactor`'s bind-and-register loop, including the try/catch that disposes any
  already-bound links on a later bind failure.
- 3.3 (#19) adds the handshake, `AdmissionCap`-bounded accepts, and the Peer table via
  `ShareHolderTable<T>` (do not hand-roll one) — `StreamSessionReactor` will need real fields at
  that point (arena, counters, diagnostics, peer table), none of which exist yet.
- No `IDisposable` on `StreamSessionReactor` yet — add it when 3.2 introduces bound sockets to
  release.
