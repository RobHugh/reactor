// PostToolUse hook for Edit and Write: fixes whitespace in the one C# file the tool just changed. It
// never blocks the tool.
//
// Whitespace only. Style and analyzer fixes run in the Stop hook, once the code is complete, because
// between two edits they delete work in progress: IDE0051 removes a private member not yet used and
// IDE0005 removes a using not yet needed.

import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";

const input = JSON.parse(readFileSync(0, "utf8"));
const filePath = input.tool_input?.file_path ?? input.tool_response?.filePath;

if (typeof filePath !== "string" || !filePath.endsWith(".cs") || !existsSync(filePath)) {
    process.exit(0);
}

// Walk up from the file rather than trusting the session directory: a worktree under
// .claude/worktrees holds its own solution file.
let root = dirname(filePath);
while (!existsSync(join(root, "ReactorSockets.slnx"))) {
    const parent = dirname(root);
    if (parent === root) {
        process.exit(0);
    }

    root = parent;
}

try {
    execFileSync(
        "dotnet",
        ["format", "whitespace", "--folder", "--include", relative(root, filePath)],
        { cwd: root, stdio: "ignore", timeout: 60_000 });
} catch {
    // A failed format is reported by the Stop hook's verify step, not here.
}
