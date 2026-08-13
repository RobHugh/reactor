# Implement sub-issue #{{ISSUE_NUMBER}} of #16

You are implementing one sub-issue of vertical slice 3 in a C#/.NET library.

## The parent slice (context — do not implement this whole thing)

!`gh issue view 16 --json body --jq .body`

## Your sub-issue — implement exactly this and nothing else

!`gh issue view {{ISSUE_NUMBER}} --json number,title,body --jq "\"\(.number): \(.title)\n\n\(.body)\""`

## This run covers only 3.1, 3.2 and 3.3 — the slice stays partial

Sub-issues #17 (3.1), #18 (3.2) and #19 (3.3) are being implemented, in that order, and nothing
else. **#20–#23 (3.4 framing and the delivery counter, 3.5 the retention ring, 3.6 write batching
and Nagle, 3.7 Goodbye and Closing) are out of scope and are not on this branch.**

That has a consequence you must design around rather than paper over: after #19 a
`StreamSessionReactor` establishes and refuses Sessions and **carries no application data**. Do not
build a `Send`, a framing codec, a retention ring or a Goodbye exchange "while you are in there",
and do not leave a stub for one. If an acceptance criterion of your own sub-issue appears to need
one, say so in your report rather than reaching forward.

The exception is what the spec explicitly places in this half: the **handshake's own** framing (it
is a framed, in-protocol exchange), the resume secret's establishment and storage, and the
negotiated parameters being immutable for the Session's lifetime.

## What already exists — reuse it, do not re-derive it

The slice-3 issue text was written before slice 5 landed. Slice 5 (`DatagramSessionReactor`,
#31–#37) is **merged into `dev` and is on this branch**, and it already built most of the shared
Session vocabulary that #18 in particular reads as though it is introducing:

- `SessionEvent`, `SessionEventEnumerator`, `SessionEventKind` — the shared `ref struct` surface.
- `PeerId` — `(ulong Origin, int Index, ulong Generation)`. Note the **`Origin`**, which post-dates
  the issue text; ADR 0016 covers it.
- `Core/ShareHolderTable.cs` and `Core/SlotHandle.cs` — the generational Slot table. **Put Peers in
  this. Do not hand-roll a Peer table.**
- `DisconnectReason`, `Session/RefusalReason.cs`, `Source`, `ReactorOptions` (the shared value),
  the diagnostics and counter machinery.
- `DatagramSessionOptions` and `DatagramSessionReactor` are the **house pattern for the stream
  analogue**: read them before writing `StreamSessionOptions` or `StreamSessionReactor`, and match
  their shape, their XML-doc density and their validation idiom. Match — do not inherit. Each
  profile has its own options type with **no shared base** (ADR 0011).
- The test side has the same: `DatagramSessionReactorHarness`, `RecordingDiagnosticsSink`,
  `HostilePeer`, `WireObserver`. Read them before writing a stream fixture.

If your sub-issue asks for something that already exists in a shared form, **use the shared form and
say so in your report**. Adding a stream-only twin of a shared type is the failure mode here.

## Work already on this branch

!`git log --oneline -20`

!`git ls-files src tests`

## Rules

Read `CLAUDE.md`, then `src/CLAUDE.md` and `tests/CLAUDE.md` before writing any code.
`.editorconfig` is the sole authority on formatting, indentation, braces and spacing.

**You are implementing, not investigating.** Write real code. Do not stop after producing an
analysis, a design document, or a list of what you would do. Do not leave `// TODO`,
`throw new NotImplementedException()`, or placeholder bodies.

Stay inside your sub-issue's Scope section. If you find something broken in earlier sub-issues' work,
note it in your report — do not fix it unless your own acceptance criteria cannot be met without
doing so.

Write `/// <summary>` XML doc comments on every public and internal type and member. They are not
decoration here: CodeMap indexes them and every later agent reads your code through them.

## Use CodeMap rather than grepping

`CODEMAP-AGENT-GUIDE.md` is loaded through `CLAUDE.md` and governs. In short, at the start of your
session:

```
index.ensure_baseline { repo_path: "." }
workspace.create { repo_path: ".", workspace_id: "issue-{{ISSUE_NUMBER}}" }
```

then `index.refresh_overlay { repo_path: ".", workspace_id: "issue-{{ISSUE_NUMBER}}" }` after every
batch of edits, and pass that `workspace_id` on every query. Use `symbols.get_context` to understand
a symbol, `code.search_text` for source-text searches, and `types.hierarchy` and `graph.callers` for
relationships — in preference to `Grep` over `src/` and `tests/`. `Grep` remains right for `.md`,
`.csproj`, `.editorconfig` and anything outside the index.

## Red-green-refactor, strictly

Every behaviour change goes through the cycle, and **you must actually observe the red**:

1. **Red.** Write one failing test for the next behaviour. Run it — `dotnet test --filter
   "FullyQualifiedName~<YourTest>"` — and read the failure. It must fail for the reason you intend:
   a wrong value or a missing member, not a typo, an unrelated exception, or a compile error in a
   different file. If it passes, the test is not testing what you think.
2. **Green.** Write the least code that makes it pass. Run the same filtered test.
3. **Refactor.** Clean up with the test still green, then run the full suite.

Do not write the production code first and backfill tests. Do not batch five tests and then
implement all five. In your report, name the cycles you drove.

### What the tests may observe

Tests observe only the public L2 surface: drained Events, returned `Result`s, records received by a
Diagnostic Sink, and Counter snapshots. Two Reactors on loopback, both pumped explicitly by the
test, is the normal shape; the `RawReactor` is the instrument for anything a well-behaved peer
cannot produce. Never add `InternalsVisibleTo`, never read a private field, never assert on which
internal method was called. A test that would fail if the dispatcher were rewritten but its
behaviour preserved is testing the wrong thing.

An invariant you cannot reach through that seam is a design signal, not a testing problem: change
the code until the invariant is trivially true rather than reaching past the seam to observe it. The
reasoning, and the only conditions that would reopen any of this, are in
[ADR 0015](../docs/adr/0015-l0-is-untested-through-and-an-unreachable-invariant-is-designed-away.md).
Raise it in your report if you think you have hit one — do not decide it yourself.

### Both platforms

CI runs Linux and Windows on every push. Three measured findings from PR #73 bear on this slice:

- Linux calls a socket writable only once roughly half the send buffer is free; Windows as soon as
  there is any room.
- Shrinking a counterparty's `SO_RCVBUF` after accepting is a trap on Linux — shrink on the
  listener, before `Listen`.
- Where the platforms genuinely diverge, take a per-platform **stimulus** and keep one identical
  **assertion**. Loosening an assertion to accept either outcome is not the accommodation.

## Verification cycle — repeat until all three pass

1. `dotnet format`
2. `dotnet build`
3. `dotnet test --verbosity normal`

Do not finish with a red build or a failing test. If you cannot make an acceptance criterion pass,
say so explicitly in your report rather than deleting or weakening the test.

The orchestrator re-runs all three commands after you finish, outside your session. Claiming they
passed when they did not will simply fail there.

## Finish

1. Commit to the current branch. Subject: `{{ISSUE_NUMBER}}: <your sub-issue's title>`, plus a
   one-line body.
2. Write `.sandcastle/reports/issue-{{ISSUE_NUMBER}}.md` containing:
   - **Done** — what you implemented, by file
   - **Acceptance criteria** — each criterion from the issue, ✅ or ❌, with one line of evidence
   - **TDD cycles** — the red-green cycles you drove, one line each: the test, and what the red
     failure said before you made it green
   - **Verification** — final `dotnet format` / `build` / `test` results, with test counts
   - **Decisions** — anything the issue left open that you had to settle, and why
   - **For later sub-issues** — anything the next author should know, including anything you had to
     leave undone because 3.4–3.7 are not in this run

   Keep it under a page. It is a record of what happened, not a substitute for having done it.
   Commit it too.
3. Emit `<promise>COMPLETE</promise>`.
