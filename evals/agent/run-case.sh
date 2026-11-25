#!/usr/bin/env bash
# Runs one agent eval case from cases.json and grades the result.
#
#   evals/agent/run-case.sh <case-id> [agent|reference|none]
#
# A case is a closed issue whose fix landed as one commit. The solver starts from the fix's parent with
# this checkout's agent setup copied over it (CLAUDE.md files, .claude rules and hooks), so the eval
# measures today's setup on yesterday's task.
#
#   agent      Claude Code implements the issue from its title and body.
#   reference  The fix commit itself. It must score a pass, or the grading is wrong.
#   none       No change. It must score a fail, or the reference tests do not test the fix.
#
# Grading: the fix commit's test files replace whatever the solver wrote at those paths, then the Release
# build, those tests, the format check and the rule checks must all pass. The full suite is reported but
# does not decide the score, because it carries known flaky tests.
#
# Needs git, node and dotnet. The agent solver needs the claude CLI and ANTHROPIC_API_KEY or
# CLAUDE_CODE_OAUTH_TOKEN. The issue is fetched with gh unless <output>/issue.json already exists.
# Exits 0 on a pass and 1 on a fail.

set -uo pipefail

case_id=${1:?usage: run-case.sh <case-id> [agent|reference|none]}
solver=${2:-agent}
repo=$(git rev-parse --show-toplevel)
cases="$repo/evals/agent/cases.json"
out="${EVAL_OUTPUT_DIR:-$repo/evals/agent/results}/$case_id"
work="${EVAL_WORK_DIR:-${RUNNER_TEMP:-${TMPDIR:-/tmp}}}/agent-eval-$case_id"
model=${EVAL_MODEL:-claude-opus-5-5}
agent_timeout=${EVAL_AGENT_TIMEOUT:-3600}

field() {
    node -e '
        const [file, id, key] = process.argv.slice(1);
        const found = JSON.parse(require("fs").readFileSync(file, "utf8")).find(c => c.id === id);
        if (!found) { console.error(`no case ${id} in ${file}`); process.exit(2); }
        const value = found[key];
        console.log(Array.isArray(value) ? value.join("\n") : value);
    ' "$cases" "$case_id" "$1"
}

issue=$(field issue) || exit 2
base=$(field base)
fix=$(field fix)
mapfile -t tests < <(field tests | tr -d '\r')

mkdir -p "$out"
rm -f "$out"/*.log "$out/result.json" "$out/solution.diff"

if [ -d "$work" ]; then
    git -C "$repo" worktree remove --force "$work" >/dev/null 2>&1 || rm -rf "$work"
fi
git -C "$repo" worktree prune
git -C "$repo" worktree add --detach "$work" "$base" >/dev/null || exit 2

# Today's agent setup over the old tree, committed so the solver's diff leaves it out.
for path in CLAUDE.md CODEMAP-AGENT-GUIDE.md src/CLAUDE.md tests/CLAUDE.md .claude/settings.json .claude/hooks .claude/rules; do
    if [ -e "$repo/$path" ]; then
        mkdir -p "$work/$(dirname "$path")"
        rm -rf "${work:?}/$path"
        cp -R "$repo/$path" "$work/$path"
    fi
done
git -C "$work" add -A -f CLAUDE.md CODEMAP-AGENT-GUIDE.md src/CLAUDE.md tests/CLAUDE.md .claude >/dev/null 2>&1
git -C "$work" -c user.name=agent-eval -c user.email=agent-eval@localhost commit -q --allow-empty -m "Agent eval setup"
setup=$(git -C "$work" rev-parse HEAD)

agent_exit=0
case "$solver" in
    agent)
        if [ ! -f "$out/issue.json" ]; then
            (cd "$repo" && gh issue view "$issue" --json title,body) > "$out/issue.json" || exit 2
        fi

        node -e '
            const [file, issue] = process.argv.slice(1);
            const { title, body } = JSON.parse(require("fs").readFileSync(file, "utf8"));
            process.stdout.write(
                `Implement GitHub issue #${issue} in this repository, following CLAUDE.md.\n` +
                "Write the tests the issue asks for. Do not commit, push or open a pull request.\n" +
                "When you are done, reply with a short summary of the change.\n\n" +
                `# ${title}\n\n${body}\n`);
        ' "$out/issue.json" "$issue" > "$out/prompt.md"

        # The agent gets no GitHub token: it works from the issue text alone.
        (cd "$work" && env -u GH_TOKEN -u GITHUB_TOKEN timeout "$agent_timeout" claude -p \
            --model "$model" \
            --output-format json \
            --permission-mode acceptEdits \
            --allowedTools "Read,Edit,Write,Glob,Grep,Bash(dotnet *),Bash(git status*),Bash(git diff*),Bash(git log*),Bash(git show*),Bash(ls *),Bash(node *)" \
            < "$out/prompt.md" > "$out/agent.json" 2> "$out/agent.log")
        agent_exit=$?
        ;;
    reference)
        git -C "$work" diff "$setup" "$fix" -- . ':(exclude)CLAUDE.md' ':(exclude)CODEMAP-AGENT-GUIDE.md' \
            ':(exclude)src/CLAUDE.md' ':(exclude)tests/CLAUDE.md' ':(exclude).claude' | git -C "$work" apply --whitespace=nowarn
        agent_exit=$?
        ;;
    none)
        ;;
    *)
        echo "unknown solver: $solver" >&2
        exit 2
        ;;
esac

# The solution: everything the solver changed, committed or not, before the reference tests go in.
git -C "$work" add -A >/dev/null 2>&1
git -C "$work" diff --cached "$setup" > "$out/solution.diff"
git -C "$work" log --format=%B "$setup..HEAD" > "$out/commit-messages.txt"

added=$(grep -E '^\+[^+]' "$out/solution.diff" || true)
rule_todo=$(printf '%s\n' "$added" | grep -ciE '//+\s*TODO' || true)
rule_today=$(printf '%s\n' "$added" | grep -ciE '^\+\s*//.*\btoday\b' || true)
rule_ivt=$(printf '%s\n' "$added" | grep -c 'InternalsVisibleTo' || true)
rule_coauthor=$(grep -ciE '^\s*co-authored-by:' "$out/commit-messages.txt" || true)

git -C "$work" checkout "$fix" -- "${tests[@]}"

filter=$(for t in "${tests[@]}"; do printf 'FullyQualifiedName~%s|' "$(basename "$t" .cs)"; done)
filter=${filter%|}

step() {
    local name=$1
    shift
    (cd "$work" && "$@") > "$out/$name.log" 2>&1 && echo true || echo false
}

build=$(step build dotnet build ReactorSockets.slnx --configuration Release)
reference_tests=false
full_suite=false
if [ "$build" = true ]; then
    reference_tests=$(step reference-tests dotnet test ReactorSockets.slnx --no-build --configuration Release --filter "$filter")
    full_suite=$(step full-suite dotnet test ReactorSockets.slnx --no-build --configuration Release --blame-hang-timeout 5m)
fi
format=$(step format dotnet format ReactorSockets.slnx --verify-no-changes)

node -e '
    const fs = require("fs");
    const [file, id, issue, solver, agentExit, build, referenceTests, fullSuite, format, todo, today, ivt, coauthor, agentFile] = process.argv.slice(1);
    let agent = {};
    try { agent = JSON.parse(fs.readFileSync(agentFile, "utf8")); } catch { }
    const rules = { todo: +todo, today: +today, internalsVisibleTo: +ivt, coAuthoredBy: +coauthor };
    const result = {
        id, issue: +issue, solver, agentExit: +agentExit,
        costUsd: agent.total_cost_usd ?? null, turns: agent.num_turns ?? null,
        build: build === "true", referenceTests: referenceTests === "true",
        fullSuite: fullSuite === "true", format: format === "true", rules,
    };
    result.pass = result.build && result.referenceTests && result.format && Object.values(rules).every(n => n === 0);
    fs.writeFileSync(file, JSON.stringify(result, null, 2) + "\n");
    console.log(JSON.stringify(result));
' "$out/result.json" "$case_id" "$issue" "$solver" "$agent_exit" "$build" "$reference_tests" "$full_suite" "$format" \
    "$rule_todo" "$rule_today" "$rule_ivt" "$rule_coauthor" "$out/agent.json"

if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    node -e '
        const r = JSON.parse(require("fs").readFileSync(process.argv[1], "utf8"));
        const mark = b => (b ? "pass" : "FAIL");
        const broken = Object.entries(r.rules).filter(([, n]) => n > 0).map(([k, n]) => `${k} ${n}`).join(", ") || "clean";
        console.log(`| ${r.id} | ${r.solver} | ${r.pass ? "**pass**" : "**FAIL**"} | ${mark(r.build)} | ${mark(r.referenceTests)} | ${mark(r.format)} | ${broken} | ${mark(r.fullSuite)} | ${r.costUsd ?? "-"} | ${r.turns ?? "-"} |`);
    ' "$out/result.json" >> "$GITHUB_STEP_SUMMARY"
fi

if [ -z "${EVAL_KEEP_WORK:-}" ]; then
    git -C "$repo" worktree remove --force "$work" >/dev/null 2>&1 || true
fi

grep -q '"pass": true' "$out/result.json"
