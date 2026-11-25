# Verification runs at the edit, the push, the pull request and the week

Before this change, CI on a pull request was the only check, and it was advisory. `main` and `dev`
had no branch protection, so a red pull request could still be merged. The CLAUDE.md verification
gate ("`dotnet build` and `dotnet test` must pass before reporting a task complete") was only an
instruction to the agent, and it was skipped at times. Plain `dotnet format` exits 0 on IDE1006, so
a local check missed a failure the CI format job caught. A hung test ran into the 15-minute job
limit, and the runner died before it uploaded the logs. `AssemblySkeletonTests` guarded which types
are public, but not their members, although ADR 0003 makes every public member a permanent
commitment. The delivery promises in `docs/delivery-modes.md` were proved only by scripted faults.

## Decision

**Rulesets on `main` and `dev` require a pull request and passing CI.** The `format`,
`ubuntu-latest` and `windows-latest` checks must pass, and must be reported by GitHub Actions. The
branches cannot be deleted or force-pushed. There is no bypass, so a flaky test is re-run rather
than merged over. The rulesets live in GitHub's settings, not in this repository.

**CI kills a hung test host after five minutes.** `--blame-hang-timeout 5m` writes a mini dump and
fails the run well inside the job timeout, so the TRX files and the dump still upload.
`--blame-crash` dumps a host that crashes.

**The public API is tracked member by member.** `Microsoft.CodeAnalysis.PublicApiAnalyzers` reads
`PublicAPI.Shipped.txt` and `PublicAPI.Unshipped.txt` in each src project. Shipped is the surface at
the 1.0.0 release (`0bbcf62`), and Unshipped holds every change since. RS0016 fails the build for an
unlisted public symbol and RS0017 for a listed one that no longer exists, so every change to the
surface is an edit to these files that a reviewer sees. A removal is recorded as `*REMOVED*` in
Unshipped. At a release, Unshipped moves into Shipped.

**Git hooks in `.githooks` run before a commit and a push.** `commit-msg` rejects a
`Co-Authored-By` trailer. `pre-push` runs CI's gates: `dotnet format --verify-no-changes`, then the
Release build and the Release tests. It runs against the working tree, so it refuses a push it
cannot vouch for: uncommitted tracked changes, or a pushed commit other than HEAD. A HEAD that
already passed is not rebuilt. Each clone enables them with
`git config core.hooksPath .githooks`, and `.gitattributes` keeps them LF so `sh` can run them.

**Claude Code hooks enforce the verification gate.** `.claude/settings.json` and `.claude/hooks`
are committed, so worktrees under `.claude/worktrees` get them too. After each Edit or Write of a C#
file, `dotnet format whitespace` fixes that file. When the agent stops with code or build files
changed, the Stop hook runs `dotnet format`, the format check, the build and the tests. A failure
blocks the stop and hands the agent the failing lines. A tree that already passed is not checked
again, and after three blocked stops in a row the hook lets the agent stop and warns the user.

**A weekly workflow runs the checks too slow or too costly for every push.**

- *Random-fault scenarios.* `DatagramSessionRandomFaultScenarioTests` runs one Session through a
  `LossyProxy` with all four modes sending at once and a random drop, duplicate, reorder or delay
  queued in each direction every round. It asserts every promise in `docs/delivery-modes.md`, and
  that neither end interrupts or closes the Peer, logs an Error or refuses a Send other than for
  backpressure. Every CI run covers three fixed seeds. The weekly job runs 100 fresh seeds per OS
  from `REACTOR_SCENARIO_SEED` and `REACTOR_SCENARIO_COUNT`. A failing test's name carries its seed.
- *Agent evals.* `evals/agent/run-case.sh` replays a closed issue whose fix landed as one commit.
  Claude Code implements the issue from its text, starting at the fix's parent with this checkout's
  CLAUDE.md files and `.claude` rules and hooks copied in. The fix commit's tests then replace
  whatever the agent wrote at those paths. A case passes if the Release build, those tests and the
  format check pass, and the agent's diff adds no `// TODO`, no comment saying "today", no
  `InternalsVisibleTo` and no `Co-Authored-By` trailer. The full suite is reported but does not
  decide the score, because it carries known flaky tests. The job skips when neither
  `ANTHROPIC_API_KEY` nor `CLAUDE_CODE_OAUTH_TOKEN` is set, and the model is the
  `AGENT_EVAL_MODEL` repository variable.

## Rejected

- **A random seed on every CI run.** A pull request would fail on a seed it did not cause, and the
  rulesets would then block it until a re-run drew another. Fixed seeds keep pull request runs
  reproducible, and the weekly job does the exploring.
- **A probability for each fault in `LossyProxy`.** The proxy stays scheduled (its remarks give the
  reasons). The scenario draws from the seed which fault to schedule next.
- **The full `dotnet format` after every edit.** Between two edits it deletes work in progress:
  IDE0051 removes a private member not yet used, and IDE0005 removes a using not yet needed. It
  deleted a constant while this ADR's scenario suite was being written. Style and analyzer fixes run
  in the Stop hook, once the code is complete.
- **Grading an agent eval on its own tests.** An agent that writes weak tests would score a pass.
  The fix commit's tests are fixed, and the `reference` and `none` solvers check the grading itself:
  the fix must pass and an empty change must fail.
- **Every public type in Unshipped.** 1.0.0 shipped, so a change since then must show as a change,
  not as one more line in an undifferentiated list.

## Consequences

- **A pull request cannot merge red, and a direct push to `main` or `dev` is rejected.** Renaming a
  CI job or its OS matrix entries means updating the rulesets, or every pull request waits for a
  check that never reports.
- **Every public API change edits a PublicAPI file.** RS0016 and RS0017 are errors under
  `TreatWarningsAsErrors`.
- **A push takes the length of the suite**, a few minutes, unless that HEAD already passed.
  `git push --no-verify` skips it.
- **An agent turn that changed code ends with a full build and test run**, about 90 seconds on
  Windows.
- **The scenario suite adds about 15 seconds to CI.** The weekly scenario job takes a few minutes
  per OS.
- **Agent evals cost money.** Each case is one Claude Code session plus a build and two test runs.
  The issue text is fetched when the case runs, so an issue edited after its fix changes the task.
- **The agent evals run on Windows.** Only a Windows checkout matches `.editorconfig`'s CRLF, so
  only there can the format check pass.
