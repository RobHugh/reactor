/**
 * A host-shell sandbox provider: runs commands on the host, like `noSandbox()`,
 * but through a POSIX shell rather than `cmd.exe`.
 *
 * Why this exists
 * ---------------
 * Sandcastle composes agent commands with POSIX single-quoting — `shellEscape`
 * in its source is `s => "'" + s.replace(/'/g, "'\\''") + "'"`. Its Docker and
 * Podman providers run that string inside a container's `sh`, so the quoting is
 * honoured. `noSandbox()` on Windows runs it through `cmd.exe /d /s /c`, where
 * single quotes are ordinary characters: `--model 'claude-sonnet-5'` reaches the
 * CLI with the quotes still attached and fails as `model_not_found`. The same
 * bug hits `--resume '<session-id>'`, which is the repair path in
 * `orchestrate.ts`.
 *
 * Spawning Git Bash's `sh` instead makes the quoting mean what Sandcastle
 * intended. Everything else is identical to `noSandbox()`: the command runs on
 * the host, as the host user, with no container isolation. Isolation here comes
 * only from the git worktree and its branch.
 *
 * This is registered as a bind-mount provider because the worktree is already a
 * real host directory — "copying a file in" is a filesystem copy, and the
 * agent's `worktreePath` and ours are the same path.
 */

import { spawn } from "node:child_process";
import { copyFile, mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { createInterface } from "node:readline";
import { createBindMountSandboxProvider } from "@ai-hero/sandcastle";
import type { BindMountSandboxProvider, ExecResult } from "@ai-hero/sandcastle";

/** Bounds the output tail kept per stream, so a chatty agent cannot exhaust V8's string limit. */
const MAX_TAIL_CHARS = 64 * 1024;

const keepTail = (tail: string, chunk: string): string => {
  const joined = tail + chunk;
  return joined.length > MAX_TAIL_CHARS ? joined.slice(-MAX_TAIL_CHARS) : joined;
};

export interface HostShellOptions {
  /** Absolute path to a POSIX `sh`. Defaults to Git for Windows' shell. */
  readonly shellPath?: string;
  /** Environment variables injected into every command. */
  readonly env?: Record<string, string>;
}

export const hostShell = (options?: HostShellOptions): BindMountSandboxProvider =>
  createBindMountSandboxProvider({
    name: "host-shell",
    env: options?.env ?? {},
    create: async (createOptions) => {
      const shellPath = options?.shellPath ?? "C:\\Program Files\\Git\\bin\\sh.exe";
      const worktreePath = createOptions.worktreePath;
      const processEnv = { ...process.env, ...createOptions.env };

      return {
        worktreePath,

        exec: (command, opts): Promise<ExecResult> =>
          new Promise((resolve, reject) => {
            const child = spawn(shellPath, ["-c", command], {
              cwd: opts?.cwd ?? worktreePath,
              env: processEnv,
              stdio: [opts?.stdin !== undefined ? "pipe" : "ignore", "pipe", "pipe"],
            });

            if (opts?.stdin !== undefined) {
              child.stdin!.write(opts.stdin);
              child.stdin!.end();
            }

            let stdout = "";
            let stderr = "";

            // Streaming per line is a contract requirement: it is how Sandcastle
            // shows live output and enforces its idle timeout.
            if (opts?.onLine) {
              createInterface({ input: child.stdout! }).on("line", (line) => {
                stdout = keepTail(stdout, line + "\n");
                opts.onLine!(line);
              });
              createInterface({ input: child.stderr! }).on("line", (line) => {
                stderr = keepTail(stderr, line + "\n");
                opts.onLine!(line);
              });
            } else {
              child.stdout!.on("data", (d: Buffer) => {
                stdout = keepTail(stdout, d.toString());
              });
              child.stderr!.on("data", (d: Buffer) => {
                stderr = keepTail(stderr, d.toString());
              });
            }

            child.on("error", (error) => reject(new Error(`exec failed: ${error.message}`)));
            child.on("close", (code) => resolve({ stdout, stderr, exitCode: code ?? 1 }));
          }),

        interactiveExec: (args, opts) =>
          new Promise((resolve, reject) => {
            const child = spawn(args[0]!, args.slice(1), {
              cwd: opts.cwd ?? worktreePath,
              env: processEnv,
              stdio: "inherit",
            });
            child.on("error", reject);
            child.on("close", (code) => resolve({ exitCode: code ?? 1 }));
          }),

        // The "sandbox" is the host, so both directions are plain file copies.
        copyFileIn: async (hostPath, sandboxPath) => {
          await mkdir(dirname(sandboxPath), { recursive: true });
          await copyFile(hostPath, sandboxPath);
        },
        copyFileOut: async (sandboxPath, hostPath) => {
          await mkdir(dirname(hostPath), { recursive: true });
          await copyFile(sandboxPath, hostPath);
        },

        close: async () => {
          // Nothing to tear down — no container was started.
        },
      };
    },
  });
