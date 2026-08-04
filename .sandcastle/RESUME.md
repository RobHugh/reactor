# Slice 1 orchestrator run — resume state

Written 2026-08-04. The run stopped part-way through sub-issue #5 because the Claude
subscription session limit was reached (`resets 1:40pm Europe/London`). Nothing was broken and
nothing was lost — this is a pause, not a failure.

## Where it got to

| Sub-issue | State | Commit |
|---|---|---|
| #2 — 1.1 Solution skeleton | done, gates green | `47310b8` |
| #3 — 1.2 Shared options value | done, gates green | `a17a5c8` |
| #4 — 1.3 `Source` value type | done, gates green | `72367b9` |
| #5 — 1.4 Frame arena and read share | **cut off mid-run**, no commit | — |
| #6 – #9 | not started | — |
| review + draft PR | not reached | — |

Branch `agent/slice-1` is at `72367b9`, three commits ahead of the point it was cut from
(`dev@7c2ece3`). It has never been pushed. Reports for #2–#4 are committed under
`.sandcastle/reports/`. Each of those three passed `dotnet format --verify-no-changes`,
`dotnet build` and `dotnet test` run by the orchestrator, outside the agent's own session.

`dev` is at `9cb4357` and carries the orchestrator itself. Those commits are deliberately not on
`agent/slice-1` — the orchestrator runs from the host checkout, not from inside the worktree, so
the branch does not need them.

## To resume

1. **Check the main worktree is on `dev`.** Sandcastle's teardown left it checked out on
   `agent/slice-1` last time, and git refuses to create a worktree for a branch that is already
   checked out somewhere — the resume fails at `createWorktree` if this is not done.
   ```bash
   git switch dev
   ```
2. **Skip the finished sub-issues.** In `.sandcastle/orchestrate.ts`:
   ```ts
   const SUB_ISSUES = [5, 6, 7, 8, 9];
   ```
   Re-running #2–#4 is not harmful — the agents recognise completed work and no-op — but it costs
   session budget for nothing, which is the resource that ran out.
3. **Run it.**
   ```bash
   node .sandcastle/orchestrate.ts
   ```
   `createWorktree` reattaches to the existing `agent/slice-1` rather than recreating it, so the
   three finished commits are already in place.
4. **Restore `SUB_ISSUES` to all eight** once the slice is complete, so the file describes the
   whole job rather than one resumption.

Budget note: #2–#4 consumed roughly one session limit between them. #5 (frame arena), #6
(`Socket.Select` dispatcher) and #8 (UDP transport) are the substantial ones, so expect the
remaining five to need more than one further reset.

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
