# Review Slice 1 and open a pull request

Eight sub-issues (#2–#9) of #1 have been implemented on this branch, one commit-set each. Every one
passed `dotnet format --verify-no-changes`, `dotnet build` and `dotnet test`, verified by the
orchestrator outside the implementing agent's session.

## The slice spec

!`gh issue view 1 --json body --jq .body`

## What each sub-agent reported

!`cat .sandcastle/reports/issue-*.md`

## The work

!`git log --oneline dev..HEAD`

!`git diff --stat dev...HEAD`

## Your job

Review the whole slice as one thing. Each sub-agent saw only its own issue, so cross-cutting problems
are exactly what they could not catch.

**Spec fidelity.** Walk #1's Implementation Decisions list and check each against the code. The
standing test cases the issue names itself:

- Nothing protocol-specific in the shared options value — `MaxMessageSize` must not have crept in.
- No `InternalsVisibleTo` anywhere.
- No type named `Reactor`; this slice ships `RawReactor` only.
- `DualMode` is never set.
- `Socket.Select`'s in-place list mutation is designed around, not used naively — the dispatcher must
  not allocate every Frame.
- The readiness scan starts at a rotating offset.
- `Core/` and `Transports/` types are `internal`; the only public surface is at the root namespace.
- `src/ReactorSockets` carries zero NuGet dependencies.

**Seams.** No test may reach into L0/L1, read a private field, or assert on an internal call.
Everything asserts on drained Events, returned `Result`s, Sink records, or Counter snapshots.

**Coherence.** Types introduced early — `Source`, the shared options value, `DiagnosticRecord` — are
used consistently by the later sub-issues rather than shadowed, duplicated, or quietly widened.

**The gap #1 flags about itself.** The read cap's escape fires every time at the default arena,
yielding exactly one datagram per Link per Pump. The issue says this "was not designed and is worth a
test that pins it". Confirm such a test exists. If it does not, say so prominently.

Then run `/code-review` against `dev...HEAD` and fold what it finds into your assessment.

**Do not fix what you find.** Fix only outright build or test breakage you introduce yourself.
Everything else goes into the PR body as a reviewer checklist — a human decides what to act on.

## Write the PR body

Write `.sandcastle/reports/PR.md` containing:

- **Closes** — `Closes #2` … `Closes #9`, then `Part of #1`
- **What landed** — two or three paragraphs describing the public surface a consumer now has
- **Sub-issue status** — a table: issue, title, acceptance criteria met (n/n), notes
- **Review this carefully** — the specific things you could not fully verify, or that you judged as
  decisions rather than facts, each with a file path. Be honest about the limits of your own review:
  you read the same spec the implementers read and can have inherited their misreadings.
- **Deferred** — anything a sub-agent flagged for a later slice

Commit it.

## Open the PR

```
gh pr create --draft --base dev --head {{SOURCE_BRANCH}} \
  --title "Slice 1: L0 readiness dispatch, L1 UDP, and Raw datagram Links" \
  --body-file .sandcastle/reports/PR.md
```

Draft, not ready-for-review — a human marks it ready. Do not merge it.

Emit `<promise>COMPLETE</promise>` once the PR exists.
