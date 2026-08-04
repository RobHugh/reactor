/**
 * Orchestrates the sub-issues of a slice.
 *
 * One worktree, one branch, one long-lived sandbox handle. Each sub-issue is
 * handed to a fresh agent in dependency order; after each, the orchestrator
 * re-runs format/build/test itself so the gate is a real exit code rather than
 * the agent's claim about one. A red tree gets one repair attempt and then
 * stops the chain — the sub-issues build on each other, so carrying a broken
 * one forward produces failures that all look like the next one's fault.
 *
 * Run:  node .sandcastle/orchestrate.ts
 */

import { claudeCode, createWorktree } from "@ai-hero/sandcastle";
import type { Sandbox, SandboxRunResult } from "@ai-hero/sandcastle";
import { hostShell } from "./hostShell.ts";

/**
 * Sub-issues of #1, in dependency order (1.1 … 1.8).
 *
 * Resuming: #2–#7 are done and gate-verified on `agent/slice-1` (see
 * .sandcastle/RESUME.md). Restore this to `[2, 3, 4, 5, 6, 7, 8, 9]` once the
 * slice is complete, so the file describes the whole job again.
 */
const SUB_ISSUES = [8, 9];

const BRANCH = "agent/slice-1";
const BASE = "dev";

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
 */
/**
 * `captureSessions: false` is required, not a preference. Sandcastle's
 * `encodeProjectPath` does not match what Claude Code 2.1.221 writes — it
 * leaves `_` and `.` intact where the real encoder turns both into `-` — and it
 * looks for the JSONL under a container home (`/home/agent`) that does not
 * exist on a host run. Capture therefore throws ENOENT *after* the agent has
 * finished and committed, destroying an otherwise successful run.
 *
 * The cost is that `SandboxRunResult.resume` is unavailable, so the repair path
 * below starts a fresh agent rather than continuing the session.
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
    promptFile: ".sandcastle/subissue.md",
    promptArgs: { ISSUE_NUMBER: String(issue) },
    name: `slice-1-issue-${issue}`,
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
        `The agent implementing sub-issue #${issue} of #1 finished and committed, but the ` +
        `orchestrator then ran \`${failure}\` and it failed.\n\n` +
        `Read .sandcastle/reports/issue-${issue}.md for what it believed it had done, and ` +
        `\`gh issue view ${issue}\` for what was asked. Reproduce the failure, fix the cause, ` +
        `and re-run \`dotnet format\`, \`dotnet build\` and \`dotnet test\` until all three pass.\n\n` +
        `Fix the code, not the test — if an acceptance criterion genuinely cannot be met, say so ` +
        `in the report rather than weakening the test. Append a "Repair" section to ` +
        `.sandcastle/reports/issue-${issue}.md explaining what was wrong, then commit.\n\n` +
        `Emit <promise>COMPLETE</promise> when done.`,
      name: `slice-1-issue-${issue}-repair`,
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

// Every gate is green across all eight sub-issues. Review the slice as a whole
// and open a draft PR for a human.
console.log(`\n=== review ===`);

// `gh pr create` needs the branch on the remote, and nothing has pushed it —
// the worktree and its branch are local until this point.
const push = await sandbox.exec(`git push -u origin ${BRANCH}`);
if (push.exitCode !== 0) {
  throw new Error(`Could not push ${BRANCH}: ${push.stderr || push.stdout}`);
}

await sandbox.run({
  agent: REVIEWER,
  // {{TARGET_BRANCH}} is built in — Sandcastle sets it to the worktree branch,
  // and passing it via promptArgs is rejected.
  promptFile: ".sandcastle/review.md",
  name: "slice-1-review",
  maxIterations: 2,
  idleTimeoutSeconds: 900,
  logging: { type: "file", path: ".sandcastle/logs/review.log", verbose: true },
});

// Do not claim the PR exists — ask. The reviewer is instructed to stop rather
// than open a PR it cannot stand behind, and that outcome must be reported as
// what it is.
const pr = await sandbox.exec(`gh pr list --head ${BRANCH} --state open --json number,url`);
const opened: { number: number; url: string }[] = pr.exitCode === 0 ? JSON.parse(pr.stdout || "[]") : [];

if (opened.length === 0) {
  console.error(
    `\nAll gates passed, but no PR was opened from ${BRANCH}. The reviewer declined to open one — ` +
      `read .sandcastle/logs/review.log for why.`,
  );
  process.exitCode = 1;
} else {
  console.log(`\nDone. Draft PR ${opened[0]!.url} — ${BRANCH} into ${BASE}.`);
}
