# Implement sub-issue #{{ISSUE_NUMBER}} of #1

You are implementing one sub-issue of a vertical slice in a C#/.NET library.

## The parent slice (context — do not implement this whole thing)

!`gh issue view 1 --json body --jq .body`

## Your sub-issue — implement exactly this and nothing else

!`gh issue view {{ISSUE_NUMBER}} --json number,title,body --jq "\"\(.number): \(.title)\n\n\(.body)\""`

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

Use TDD at the seams your sub-issue's "Testing notes" identify. Tests observe only the public L2
surface: drained Events, returned `Result`s, records received by a Diagnostic Sink, and Counter
snapshots. Never add `InternalsVisibleTo`, never read a private field, never assert on which internal
method was called. A test that would fail if the dispatcher were rewritten but its behaviour
preserved is testing the wrong thing.

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
   - **Verification** — final `dotnet format` / `build` / `test` results, with test counts
   - **Decisions** — anything the issue left open that you had to settle, and why
   - **For later sub-issues** — anything the next author should know

   Keep it under a page. It is a record of what happened, not a substitute for having done it.
   Commit it too.
3. Emit `<promise>COMPLETE</promise>`.
