/**
 * Orchestrates the first three sub-issues of slice 3 (#16).
 *
 * Same shape as `orchestrate.ts`, which ran slice 1: one worktree, one branch,
 * one long-lived sandbox handle. Each sub-issue is handed to a fresh agent in
 * dependency order; after each, the orchestrator re-runs format/build/test
 * itself so the gate is a real exit code rather than the agent's claim about
 * one. A red tree gets one repair attempt and then stops the chain — the
 * sub-issues build on each other, so carrying a broken one forward produces
 * failures that all look like the next one's fault.
 *
 * Run:  node .sandcastle/orchestrate-slice-3.ts
 */

import { claudeCode, createWorktree } from "@ai-hero/sandcastle";
import type { Sandbox, SandboxRunResult } from "@ai-hero/sandcastle";
import { hostShell } from "./hostShell.ts";

/**
 * 3.1, 3.2, 3.3 — in dependency order. The slice's remaining four sub-issues
 * (#20–#23: framing, the retention ring, write batching, Goodbye) are
 * deliberately not in this run, and both prompt files say so, because an agent
 * told only "implement slice 3" would build half of them speculatively.
 */
const SUB_ISSUES = [17, 18, 19];

const PARENT_ISSUE = 16;

/**
 * The deliverable branch. It is created in a worktree off `BASE`, which is the
 * branch this orchestrator and its two prompt files live on — Sandcastle reads
 * `promptFile` relative to the *host* checkout, so the harness cannot sit on a
 * branch the host is not on, and `dev` is off limits for this work.
 */
const BRANCH = "agent/slice-3";
const BASE = "agent/slice-3-harness";

/** Re-run by the orchestrator after every sub-issue. Order matters: cheapest first. */
const GATES = [
  "dotnet format --verify-no-changes",
  "dotnet build",
  "dotnet test --verbosity normal",
];

/**
 * The agent runs on the host (see hostShell.ts), so an explicit permission mode
 * is required — without one an AFK run blocks on the first tool prompt. "auto"
 * gives AI-mediated per-tool approval, which is what the flag exists for on
 * unsandboxed host runs, and it takes precedence over the
 * `--dangerously-skip-permissions` Sandcastle would otherwise pass.
 *
 * `captureSessions: false` is required, not a preference. Sandcastle's
 * `encodeProjectPath` does not match what Claude Code writes, and capture
 * therefore throws ENOENT *after* the agent has finished and committed,
 * destroying an otherwise successful run. The cost is that
 * `SandboxRunResult.resume` is unavailable, so the repair path below starts a
 * fresh agent rather than continuing the session. See `.sandcastle/RESUME.md`.
 */
const AGENT_OPTIONS = {
  effort: "medium",
  permissionMode: "auto",
  captureSessions: false,
} as const;

const IMPLEMENTER = claudeCode("claude-sonnet-5", AGENT_OPTIONS);

const REVIEWER = claudeCode("claude-opus-5", AGENT_OPTIONS);

/** Runs every gate, returning the first command that failed. */
const firstFailingGate = async (sandbox: Sandbox): Promise<string | undefined> => {
  for (const command of GATES) {
    const result = await sandbox.exec(command);
    if (result.exitCode !== 0) {
      console.error(`  gate failed: ${command}\n${result.stderr || result.stdout}`);
      return command;
    }
  }
  return undefined;
};

await using worktree = await createWorktree({
  branchStrategy: { type: "branch", branch: BRANCH, baseBranch: BASE },
});

await using sandbox = await worktree.createSandbox({ sandbox: hostShell() });

console.log(`worktree ${sandbox.worktreePath} on ${sandbox.branch}`);

for (const issue of SUB_ISSUES) {
  console.log(`\n=== #${issue} ===`);

  const result: SandboxRunResult = await sandbox.run({
    agent: IMPLEMENTER,
    promptFile: ".sandcastle/subissue-slice-3.md",
    promptArgs: { ISSUE_NUMBER: String(issue) },
    name: `slice-3-issue-${issue}`,
    maxIterations: 4,
    idleTimeoutSeconds: 900,
    logging: { type: "file", path: `.sandcastle/logs/issue-${issue}.log`, verbose: true },
  });

  const failure = await firstFailingGate(sandbox);

  if (failure) {
    console.log(`  repairing #${issue}…`);
    await sandbox.run({
      agent: IMPLEMENTER,
      prompt:
        `The agent implementing sub-issue #${issue} of #${PARENT_ISSUE} finished and committed, but ` +
        `the orchestrator then ran \`${failure}\` and it failed.\n\n` +
        `Read .sandcastle/reports/issue-${issue}.md for what it believed it had done, and ` +
        `\`gh issue view ${issue}\` for what was asked. Reproduce the failure, fix the cause, ` +
        `and re-run \`dotnet format\`, \`dotnet build\` and \`dotnet test\` until all three pass.\n\n` +
        `Fix the code, not the test — if an acceptance criterion genuinely cannot be met, say so ` +
        `in the report rather than weakening the test. Sub-issues #20–#23 of #${PARENT_ISSUE} are ` +
        `not in this run, so do not reach forward into framing, retention, write batching or ` +
        `Goodbye to make something pass.\n\n` +
        `Append a "Repair" section to .sandcastle/reports/issue-${issue}.md explaining what was ` +
        `wrong, then commit.\n\n` +
        `Emit <promise>COMPLETE</promise> when done.`,
      name: `slice-3-issue-${issue}-repair`,
      maxIterations: 3,
      idleTimeoutSeconds: 900,
      logging: { type: "file", path: `.sandcastle/logs/issue-${issue}-repair.log`, verbose: true },
    });

    const stillFailing = await firstFailingGate(sandbox);
    if (stillFailing) {
      throw new Error(
        `#${issue} left the tree red after one repair attempt: \`${stillFailing}\`. ` +
          `Chain stopped. Branch ${BRANCH} kept at ${sandbox.worktreePath} for inspection.`,
      );
    }
  }

  console.log(`  #${issue} done — ${result.commits.length} commit(s)`);
}

// Every gate is green across all three sub-issues. Review the half-slice as a
// whole and open a draft PR for a human.
console.log(`\n=== review ===`);

// `gh pr create` needs the branch on the remote, and nothing has pushed it —
// the worktree and its branch are local until this point.
const push = await sandbox.exec(`git push -u origin ${BRANCH}`);
if (push.exitCode !== 0) {
  throw new Error(`Could not push ${BRANCH}: ${push.stderr || push.stdout}`);
}

await sandbox.run({
  agent: REVIEWER,
  // The head branch comes from the built-in {{SOURCE_BRANCH}}, not
  // {{TARGET_BRANCH}}. Despite the names, on this code path Sandcastle sets
  // TARGET_BRANCH to the *host* checkout's branch, so a `gh pr create
  // --head {{TARGET_BRANCH}}` is rejected. Neither key may be passed via
  // promptArgs; both are reserved.
  promptFile: ".sandcastle/review-slice-3.md",
  name: "slice-3-review",
  maxIterations: 2,
  idleTimeoutSeconds: 900,
  logging: { type: "file", path: ".sandcastle/logs/review-slice-3.log", verbose: true },
});

// Do not claim the PR exists — ask. The reviewer is instructed to stop rather
// than open a PR it cannot stand behind, and that outcome must be reported as
// what it is.
const pr = await sandbox.exec(`gh pr list --head ${BRANCH} --state open --json number,url`);
const opened: { number: number; url: string }[] = pr.exitCode === 0 ? JSON.parse(pr.stdout || "[]") : [];

if (opened.length === 0) {
  console.error(
    `\nAll gates passed, but no PR was opened from ${BRANCH}. The reviewer declined to open one — ` +
      `read .sandcastle/logs/review-slice-3.log for why.`,
  );
  process.exitCode = 1;
} else {
  console.log(`\nDone. Draft PR ${opened[0]!.url} — ${BRANCH} into dev.`);
}
