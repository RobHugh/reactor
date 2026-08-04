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
import { noSandbox } from "@ai-hero/sandcastle/sandboxes/no-sandbox";
import type { Sandbox, SandboxRunResult } from "@ai-hero/sandcastle";

/** Sub-issues of #1, in dependency order (1.1 … 1.8). */
const SUB_ISSUES = [2, 3, 4, 5, 6, 7, 8, 9];

const BRANCH = "agent/slice-1";
const BASE = "dev";

/** Re-run by the orchestrator after every sub-issue. Order matters: cheapest first. */
const GATES = [
  "dotnet format --verify-no-changes",
  "dotnet build",
  "dotnet test --verbosity normal",
];

/**
 * `noSandbox` runs the agent on the host, so Sandcastle does not pass
 * `--dangerously-skip-permissions`. Without an explicit permission mode an AFK
 * run would block on the first prompt. "auto" gives AI-mediated per-tool
 * approval, which is what this flag exists for on unsandboxed host runs.
 */
const IMPLEMENTER = claudeCode("claude-sonnet-5", {
  effort: "medium",
  permissionMode: "auto",
});

const REVIEWER = claudeCode("claude-opus-5", {
  effort: "medium",
  permissionMode: "auto",
});

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

await using sandbox = await worktree.createSandbox({ sandbox: noSandbox() });

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
    if (!result.resume) {
      throw new Error(
        `#${issue} left the tree red (\`${failure}\`) and the session cannot be resumed. ` +
          `Branch ${BRANCH} kept at ${sandbox.worktreePath}.`,
      );
    }

    console.log(`  repairing #${issue}…`);
    await result.resume(
      `The orchestrator ran \`${failure}\` after you finished, outside your session, and it failed. ` +
        `Reproduce it, fix the cause, re-run format/build/test until all three pass, ` +
        `amend .sandcastle/reports/issue-${issue}.md to say what went wrong, and commit.`,
      { name: `slice-1-issue-${issue}-repair` },
    );

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

console.log(`\nDone. Draft PR opened from ${BRANCH} into ${BASE}.`);
