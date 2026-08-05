# Slice 1 orchestrator run — complete

Ran 2026-08-04 across three sessions, each of the first two ending on the Claude subscription
session limit rather than on any failure. All eight sub-issues of #1 landed on `agent/slice-1`,
every one gate-verified by the orchestrator outside the agent's own session, and the review stage
opened **draft PR #65** (`agent/slice-1` → `dev`).

| Sub-issue | Commit |
|---|---|
| #2 — 1.1 Solution skeleton | `47310b8` |
| #3 — 1.2 Shared options value | `a17a5c8` |
| #4 — 1.3 `Source` value type | `72367b9` |
| #5 — 1.4 Frame arena and read share | `84aef31` |
| #6 — 1.5 L0 readiness dispatch | `6f26d51` |
| #7 — 1.6 Diagnostics and Counters | `c5cd95a` |
| #8 — 1.7 L1 UDP transport and the Raw surface | `74ca6be` |
| #9 — 1.8 Binding, address family, admission cap | `a809982` |
| review — PR body and checklist | `8aaccf3` |

Final gate: `dotnet format --verify-no-changes` clean, `dotnet build` 0 warnings, `dotnet test`
88/88. Per-issue reports are in `.sandcastle/reports/`; the reviewer's assessment is
`.sandcastle/reports/PR.md`, which is also the PR body.

**Since then (2026-08-05), ten of the eleven review items have been closed by hand** — branch is at
`30b86d2`, suite at 92 tests. `.sandcastle/reports/review_slice_1.md` is the current state; read it
rather than `PR.md`'s "Review this carefully" section, which describes the slice as it stood when #65
was opened and is kept only so each finding reads next to its fix.

**Item 5 is the one thing still open: truncation detection has only ever run on Windows.** It needs
the suite run on Linux, not a decision. Everything else — L0 internal, the seam test, disposal, Sink
re-entrancy, the read-cap decision, `Send`/`Shutdown`, `ObservedPumpInterval`, the standards items —
is done.

## What this run proved about the harness

- The two-layer gate works. Sub-agents run format/build/test themselves; the orchestrator re-runs
  all three via `exec` so completion is an exit code, not a claim.
- **The repair path is still unproven** — no gate ever failed across eight sub-issues.
- Reviewer agents will refuse a false premise. The smoke test's reviewer declined to open a PR for
  an incomplete slice and said why, which is the behaviour you want and cost nothing to get.
- A run that ends on a session limit *throws*, so `await using` teardown never runs: the worktree
  survives and the main checkout is left alone. A run that ends cleanly leaves the main checkout on
  the agent branch, and git then refuses a worktree for an already-checked-out branch — so the next
  run fails at `createWorktree` unless you `git switch dev` first.
- Uncommitted work survives a session-limit stop. #8's first attempt wrote twelve files and never
  committed; the resumed agent found them in place and built on them. Worth checking
  `git status` in the worktree before assuming an interrupted sub-issue did nothing.

## Harness fixes — do not revert

- `.sandcastle/hostShell.ts` replaces `noSandbox()`. Sandcastle quotes agent commands POSIX-style
  but `noSandbox` runs them through `cmd.exe`, where single quotes are literal. See `f90ab0b`.
- `captureSessions: false` is mandatory. Sandcastle's session-path encoding does not match Claude
  Code 2.1.221, and capture throws *after* a successful commit. See `f90ab0b`.
- The orchestrator pushes the branch immediately before the review stage — `gh pr create --head`
  cannot see a purely local branch. See `9cb4357`.
- `review.md` builds the PR head from `{{SOURCE_BRANCH}}`, not `{{TARGET_BRANCH}}`. Despite the
  names, on this code path Sandcastle sets `TARGET_BRANCH` to the host checkout's branch (`dev`),
  so `--head {{TARGET_BRANCH}}` is rejected with "head branch is the same as base branch". The
  reviewer diagnosed and worked around this itself on the real run; the template is now correct.
