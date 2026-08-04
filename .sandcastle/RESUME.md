# Slice 1 orchestrator run — resume state

Written 2026-08-04, updated after the second session. Both stops so far have been the same thing:
the Claude subscription session limit. Nothing was broken and nothing was lost either time — this
is a pause, not a failure. The second stop was at the *start* of #8, so no partial work exists.

## Where it got to

| Sub-issue | State | Commit |
|---|---|---|
| #2 — 1.1 Solution skeleton | done, gates green | `47310b8` |
| #3 — 1.2 Shared options value | done, gates green | `a17a5c8` |
| #4 — 1.3 `Source` value type | done, gates green | `72367b9` |
| #5 — 1.4 Frame arena and read share | done, gates green | `84aef31` |
| #6 — 1.5 L0 readiness dispatch | done, gates green | `6f26d51` |
| #7 — 1.6 Diagnostics and Counters | done, gates green | `c5cd95a` |
| #8 — 1.7 UDP transport | **not started** (limit hit on launch) | — |
| #9 — 1.8 | not started | — |
| review + draft PR | not reached | — |

Branch `agent/slice-1` is at `c5cd95a`, six commits ahead of the point it was cut from
(`dev@7c2ece3`). It has never been pushed. Reports for #2–#7 are committed under
`.sandcastle/reports/`. Each of those six passed `dotnet format --verify-no-changes`,
`dotnet build` and `dotnet test` run by the orchestrator, outside the agent's own session — 75
tests passing as of #7.

`dev` carries the orchestrator itself. Those commits are deliberately not on `agent/slice-1` —
the orchestrator runs from the host checkout, not from inside the worktree, so the branch does
not need them.

## To resume

1. **Check the main worktree is on `dev`, and that the agent worktree still exists.**
   ```bash
   git worktree list
   ```
   When the orchestrator *throws* (as on a session limit) the `await using` teardown does not
   run, so the worktree survives and the main checkout is left alone — the good case. When it
   exits cleanly, teardown leaves the main checkout on `agent/slice-1`, and git then refuses to
   create a worktree for an already-checked-out branch, so the next run fails at
   `createWorktree`. If that has happened, `git switch dev` first.
2. **Skip the finished sub-issues.** In `.sandcastle/orchestrate.ts`:
   ```ts
   const SUB_ISSUES = [8, 9];
   ```
   Re-running finished sub-issues is not harmful — the agents recognise completed work and no-op
   — but it costs session budget for nothing, which is the resource that keeps running out.
3. **Run it.**
   ```bash
   node .sandcastle/orchestrate.ts
   ```
   `createWorktree` reattaches to the existing `agent/slice-1` rather than recreating it, so the
   six finished commits are already in place.
4. **Restore `SUB_ISSUES` to all eight** once the slice is complete, so the file describes the
   whole job rather than one resumption.

Budget note: two session limits have bought six sub-issues. #8 (UDP transport + the Raw datagram
surface) is the largest single piece in the slice and will likely consume most of a session on
its own, with #9 and the review stage after it. Expect at least one more reset beyond the next.

## Carried forward for the reviewer

Things the sub-agents flagged that the final review stage should check, recorded here in case the
review runs in a later session that cannot see the earlier reports being discussed:

- **#3 widened the public surface.** `ReactorOptions.Validate()` and `ResolveArenaSize(floor,
  description)` are public and must be called explicitly; nothing throws at construction yet. The
  agent had no other way to test them, since no `RawReactor` existed on the branch and the slice
  forbids `InternalsVisibleTo`. #1 says *"Construction throws"* — #8/#9 must call both from the
  real constructor, and even then these two stay public.
- **#3 left placeholders by design.** `DiagnosticRecord` is field-less and `IReactorDiagnostics`
  carries only `IsEnabled`/`Log`. `ReactorOptions` is already compiled against both, so #7 must
  extend them rather than replace them.
- **#4's `Source.AddressBytes` is public.** Its report calls the IPv4-in-the-first-4-bytes layout
  "an internal convention only", but `Source` is a positional record struct, so the property — and
  therefore the convention — is public and inspectable.
- **#2's assembly guard has been rewritten once already.** `PublicTypes_IsEmpty` became an
  allow-list by type name in #3, so each sub-issue adding public API must extend it. Check it is
  still asserting something meaningful by the end rather than having become a rubber stamp.
- **#2 corrected the spec.** Issue #1 names the test package `Microsoft.Extensions.Time.Testing`;
  the real NuGet id is `Microsoft.Extensions.TimeProvider.Testing`. The issue text is wrong, not
  the code.
- **The public-surface drift is now the headline finding.** #1 requires `Core/` and `Transports/`
  to be internal. Every sub-issue from #3 onward has instead shipped its types `public`, each
  citing the same reason: no `RawReactor` exists to wrap them, and `InternalsVisibleTo` is
  forbidden, so the tests have no other way in. The list is now `ReactorOptions.Validate`,
  `ResolveArenaSize`, `FrameArena`, `ArenaReservation`, `ArenaReservationOutcome`,
  `ReadinessDispatcher`, `DiagnosticGateway`, `ReactorCounterSet`, `LinkCounterSet`, `LinkId`.
  Each report promises a later sub-issue will move them back to `internal`. **Check whether #8/#9
  actually did.** If not, the slice ships an entire L0 as public API and the assembly guard test
  is documenting that rather than preventing it. There are also no `Core/` or `Transports/`
  folders — every file sits flat in `src/ReactorSockets/`.
- **#7 closed with a failing acceptance criterion, honestly marked ❌.** "A Sink calling back into
  the Reactor throws" cannot be built without `Pump`'s call-stack state. #8/#9 must close it; if
  they did not, the slice has a known unmet criterion and the PR body must say so.
- **#7 introduced `LinkId` a sub-issue early**, to give `DiagnosticRecord` a subject to carry.
  Check the Link-lifecycle work uses that type rather than defining a second one.
- **`DiagnosticSubjectKind` ships without a `Peer` case**, deliberately — ADR 0013 names Peer, but
  Peers are Session-profile. Judge whether that omission is right or a spec deviation.
- **#5's `ShareExhausted` may be unreachable through Raw as wired.** Its own report says so: the
  Raw dispatcher does one read per ready Link per Pump, so a second `Reserve` never happens. The
  mechanism is built and unit-tested; whether the slice can demonstrate it end-to-end is open.
- **#6's allocation test asserts an exact `0`** from `GC.GetAllocatedBytesForCurrentThread()` over
  1000 Pumps. It passed, but it is the kind of test that turns flaky under a different runtime or
  a debugger — worth a judgement call rather than a rubber stamp.
- **Still unproven in the harness:** the repair path (no gate has failed yet) and PR creation
  end-to-end (the smoke test's reviewer correctly declined to open a PR for an incomplete slice,
  so `gh pr create` has never actually run).

## Harness quirks already worked around

Do not "fix" these back — see `f90ab0b` and `9cb4357` for the full reasoning.

- `.sandcastle/hostShell.ts` replaces `noSandbox()`. Sandcastle quotes agent commands POSIX-style
  but `noSandbox` runs them through `cmd.exe`, where single quotes are literal.
- `captureSessions: false` is mandatory. Sandcastle's session-path encoding does not match Claude
  Code 2.1.221, and capture throws *after* a successful commit.
- The branch is pushed by the orchestrator immediately before the review stage, because
  `gh pr create --head` cannot see a purely local branch.
