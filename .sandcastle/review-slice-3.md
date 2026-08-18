# Review the first half of Slice 3 and open a pull request

Three sub-issues — #17 (3.1 options), #18 (3.2 Peers and endpoints), #19 (3.3 handshake and
refusal) — have been implemented on this branch, one commit-set each. Every one passed
`dotnet format --verify-no-changes`, `dotnet build` and `dotnet test`, verified by the orchestrator
outside the implementing agent's session.

**The slice is deliberately partial.** #20–#23 (3.4 framing and the delivery counter, 3.5 the
retention ring, 3.6 write batching and Nagle, 3.7 Goodbye and Closing) were not in this run. A
`StreamSessionReactor` on this branch establishes and refuses Sessions and carries no application
data. That is the intended end state of this PR, not a gap — but a *stub* for the missing half is a
gap, and finding one is your job.

## The slice spec

!`gh issue view 16 --json body --jq .body`

## What each sub-agent reported

!`cat .sandcastle/reports/issue-1*.md`

## The work

!`git log --oneline dev..HEAD`

!`git diff --stat dev...HEAD`

## Your job

Review the three sub-issues as one thing. Each sub-agent saw only its own issue, so cross-cutting
problems are exactly what they could not catch.

**Spec fidelity.** Walk #16's Implementation Decisions list and check each against the code, taking
only the parts 3.1–3.3 own. The standing test cases:

- No `InternalsVisibleTo` anywhere — a test asserts this; confirm it still holds.
- `StreamSessionOptions` has **no shared base** with `DatagramSessionOptions` (ADR 0011), and
  composes the shared `ReactorOptions` value rather than restating its members.
- Peers live in `Core/ShareHolderTable<T>`; there is no hand-rolled Peer table, and L0 never learns
  whether it holds Links or Peers.
- `SessionEvent`, `SessionEventKind` and `PeerId` are the **shared** types from slice 5, not
  stream-only twins. A stream-only event kind is evidence the profile split is in the wrong place —
  flag it loudly if one appeared.
- `DiagnosticSubjectKind` gained exactly one member, `Peer`, appended. `PeerRefused` (300) keeps its
  **Source** subject; `LingerNegotiated` (301) takes the new **Peer** subject.
- No public method of this profile exposes a socket, a Handle or a Link.
- There is no rebinding: no public way to begin, stop or move listening after construction.
- The refusal rules are stated once as a table in the `RawLinkLegality` house pattern, and the
  discard arm **refuses rather than permits** so an unlisted reason fails closed. `[NotNullWhen]`
  rather than the null-forgiving operator.
- A refused dialler touches **neither** admission counter — a refusal is not a Deferral. #19 was
  told to assert this negatively; confirm the assertion exists.
- `AdmissionDeferrals` is wired, and no sibling member was added to tell the pre-accept and
  post-accept declinings apart.
- `src/ReactorSockets` still carries zero NuGet dependencies.

**Seams.** No test may reach into L0/L1, read a private field, or assert on an internal call.
Everything asserts on drained Events, returned `Result`s, Sink records, or Counter snapshots.

**Red-green.** Each report has a "TDD cycles" section. Judge whether the tests read as though they
were written before the code — behaviour-shaped names, one behaviour each, assertions on the public
surface — or as though they were backfilled to cover what was written. Say which, honestly.

**Coherence.** Types introduced by 3.1 are used by 3.2 and 3.3 rather than shadowed, duplicated or
quietly widened. The negotiated parameters really are immutable for a Session's lifetime.

**The forward edge.** Slice 4 is written against three things this half is supposed to get right in
advance: the ring's shape, the delivery counter's increment point, and the immutability of
negotiated parameters. Only the third is fully in this run. Check that nothing here forecloses the
other two.

Then run `/code-review` against `dev...HEAD` and fold what it finds into your assessment.

**Do not fix what you find.** Fix only outright build or test breakage you introduce yourself.
Everything else goes into the PR body as a reviewer checklist — a human decides what to act on.

## Write the PR body

Write `.sandcastle/reports/PR-slice-3.md` containing:

- **Closes** — `Closes #17`, `Closes #18`, `Closes #19`, then `Part of #16`
- **What landed** — two or three paragraphs describing the public surface a consumer now has, and
  one sentence stating plainly that the profile carries no data yet
- **Sub-issue status** — a table: issue, title, acceptance criteria met (n/n), notes
- **Still to come in slice 3** — #20–#23, one line each
- **Review this carefully** — the specific things you could not fully verify, or that you judged as
  decisions rather than facts, each with a file path. Be honest about the limits of your own review:
  you read the same spec the implementers read and can have inherited their misreadings.
- **Deferred** — anything a sub-agent flagged for a later slice

Commit it.

## Open the PR

```
gh pr create --draft --base dev --head {{SOURCE_BRANCH}} \
  --title "Slice 3 (part 1): StreamSessionReactor options, Peers, and the handshake" \
  --body-file .sandcastle/reports/PR-slice-3.md
```

Draft, not ready-for-review — a human marks it ready. Do not merge it.

Emit `<promise>COMPLETE</promise>` once the PR exists.
