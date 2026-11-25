// Stop hook: enforces the CLAUDE.md verification gate. When the working tree has uncommitted changes to
// code or build files, it runs dotnet format, then the format check, the build and the tests. A failure
// blocks the stop and hands the agent the failing output to fix.
//
// A working tree that already passed is not checked again, so a turn that changed nothing costs one
// git call. After three blocked stops in a row with no passing run in between, the hook lets the agent
// stop and warns the user instead, so a failure the agent cannot fix does not loop forever.

import { execFileSync, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const maxConsecutiveBlocks = 3;
const relevant = /\.(cs|csproj|props|targets|slnx)$|(^|\/)PublicAPI\.(Shipped|Unshipped)\.txt$|(^|\/)\.editorconfig$/;

const input = JSON.parse(readFileSync(0, "utf8"));
const root = input.cwd ?? process.cwd();

function git(...args) {
    return execFileSync("git", args, { cwd: root, encoding: "utf8", maxBuffer: 256 * 1024 * 1024 });
}

if (!existsSync(join(root, "ReactorSockets.slnx"))) {
    process.exit(0);
}

const changed = git("status", "--porcelain", "--untracked-files=all")
    .split("\n")
    .map(line => line.slice(3).trim().replace(/^"|"$/g, ""))
    .filter(path => path.length > 0 && relevant.test(path));

if (changed.length === 0) {
    process.exit(0);
}

/** HEAD, the tracked diff and every untracked relevant file's bytes, hashed. */
function fingerprintTree() {
    const hash = createHash("sha256");
    hash.update(git("rev-parse", "HEAD"));
    hash.update(git("diff", "HEAD", "--binary"));
    for (const path of git("ls-files", "--others", "--exclude-standard").split("\n").filter(p => relevant.test(p))) {
        hash.update(path);
        hash.update(readFileSync(join(root, path)));
    }

    return hash.digest("hex");
}

const fingerprint = fingerprintTree();
const passedFile = join(root, git("rev-parse", "--git-path", "claude-verify-passed").trim());
const blocksFile = join(root, git("rev-parse", "--git-path", "claude-verify-blocks").trim());

if (existsSync(passedFile) && readFileSync(passedFile, "utf8") === fingerprint) {
    process.exit(0);
}

const steps = [
    ["format", ["format", "ReactorSockets.slnx"]],
    ["format check", ["format", "ReactorSockets.slnx", "--verify-no-changes"]],
    ["build", ["build", "ReactorSockets.slnx"]],
    ["test", ["test", "ReactorSockets.slnx", "--no-build"]],
];

for (const [name, args] of steps) {
    const result = spawnSync("dotnet", args, { cwd: root, encoding: "utf8", maxBuffer: 256 * 1024 * 1024 });
    if (result.status === 0) {
        continue;
    }

    const output = `${result.stdout ?? ""}${result.stderr ?? ""}`;
    const failures = output
        .split("\n")
        .filter(line => /\berror\b|\bFailed\b|\[FAIL\]|Assert|Expected/.test(line))
        .slice(0, 60)
        .join("\n");
    const detail = failures.length > 0 ? failures : output.split("\n").slice(-40).join("\n");

    const blocks = (existsSync(blocksFile) ? Number(readFileSync(blocksFile, "utf8")) : 0) + 1;
    if (blocks > maxConsecutiveBlocks) {
        rmSync(blocksFile, { force: true });
        console.log(JSON.stringify({
            systemMessage: `Verification gate: dotnet ${name} still fails after ${maxConsecutiveBlocks} attempts. The task is not verified.`,
        }));
        process.exit(0);
    }

    writeFileSync(blocksFile, String(blocks));
    console.log(JSON.stringify({
        decision: "block",
        reason: `Verification gate (CLAUDE.md): dotnet ${args.join(" ")} failed. Fix it before reporting the task complete.\n${detail}`,
    }));
    process.exit(0);
}

// dotnet format may have rewritten files, so fingerprint what passed rather than what arrived.
rmSync(blocksFile, { force: true });
writeFileSync(passedFile, fingerprintTree());
