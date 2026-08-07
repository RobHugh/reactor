# Slice 3 (part 4): per-Pump write batching and Nagle off as a public property

**Closes** `#22`, `#89`, `#90`, `#91`. Part of `#16` (slice 3).

## What landed

Writes to a Peer's socket are now batched **once per `Pump()`, with everything currently queued in
it** — never once per `Send`. `Send(PeerId, ReadOnlySpan<byte>)` no longer writes or arms the socket
itself; a successful append to the retention ring only sets a new `PeerSlot.HasPendingRetentionWrite`
marker. A new `Pump()` step, `FlushPendingRetentionWrites`, runs once per Pump immediately after the
readiness dispatcher and visits every marked Peer exactly once, issuing at most one
`slot.Link.Send` carrying everything currently readable from that Peer's retention ring
(`FlushRetention`, unchanged from `#86`), then clearing the marker. `OnPeerWritable`'s own retention
tail — the short-write re-arm path — no longer writes directly either; it re-sets the same marker, so
every retention write in a Frame routes through the single `FlushPendingRetentionWrites` call site
regardless of whether the socket became readiness-writable, a `Send` queued bytes, or both happened
in the same Frame. Nothing ticks unless the consumer calls `Pump`.

`Socket.NoDelay = true` is set unconditionally in both of `TcpLink`'s "configure a newly-created
socket" seams — `FromAccepted` (the accept path) and `Dial` (the connect path) — which are shared
with `RawReactor`'s TCP support (ADR 0007). Setting it unconditionally is correct rather than
conditional: `TcpLink.Send` never batches or queues for either profile (ADR 0008), so Raw TCP has no
less reason to want Nagle off than the stream Session profile does. Both properties — batching is
ours, Nagle is off — are now stated as public contract in `StreamSessionReactor`'s class-level XML
`<remarks>`, in the consumer's terms, per ADR 0011: a consumer measuring latency needs to know which
of the two is doing the batching, theirs (by calling `Pump` less often) or ours (always).

## Sub-issue status

| Issue | Title | AC met | Notes |
|---|---|---|---|
| #89 | 3.6.1 NoDelay on every stream Session socket | 2/2 | Proven through observable timing (no direct `socket.NoDelay` read — not reachable through the public L2 surface per `tests/CLAUDE.md`): two small Messages with the first still unacknowledged arrive in ~22ms, well under a Nagle-shaped stall. |
| #90 | 3.6.2 Batch retention writes once per Pump | 3/3 | First stream-profile use of `WireObserver` (extended with `SendOnAcceptedLink`): N Sends before one Pump produce exactly one wire Segment; a Send with no following Pump produces none; a fourth test added after review (below) proves the pending-write marker survives a real short write under a shrunk-receive-window peer without losing or duplicating bytes. |
| #91 | 3.6.3 Document batching and Nagle-off as Reactor contract | 2/2 | Doc-only, as scoped — no executable code touched; a new `<para>` on `StreamSessionReactor`'s class-level `<remarks>`, verified via `git diff` to be doc comments only. |

## Still to come in slice 3

- `#23` (3.7) — Goodbye, `Closing`, and the `DisconnectReason` roster.

## Review this carefully

Findings marked *(review)* came from `/code-review`'s Standards or Spec pass, run against
`9ccc63f...HEAD` (before the fix below was added).

- **Missing backpressure test, fixed in this PR** *(review — Spec)*: #90's third acceptance criterion
  — both write-batching criteria must hold on Linux and Windows, with a slow-reader stimulus applied
  by shrinking `SO_RCVBUF` on the listener before `Listen`/`Connect` — had no corresponding test. The
  three tests `#90`'s subagent added all used small payloads that never forced a short write, so the
  new pending-write marker's survival across `OnPeerWritable`'s re-arm was argued only in code
  comments. Added `Send_QueuedBurstAgainstASlowReader_DeliversEveryMessageWholeAndInOrderAcrossTheShortWriteRearmCycle`
  (`StreamSessionWriteBatchingTests.cs`) after the review: a real socket peer with a 1024-byte receive
  window, shrunk before `Connect` per commit `8426ff3`'s established pattern, forces the first flush
  attempt inside a Pump to be a short write, and the test asserts every queued Message still arrives
  whole and in order once the peer starts reading.
- **Duplicated test-support helpers** *(review — Standards, judgement call)*: `BuildHello`/
  `BuildFrame`/`PayloadOf` are copied near-verbatim into both new test files this half, continuing a
  pattern already flagged in part 3's report and present in ~7 other `StreamSession*Tests.cs` files.
  Not a correctness issue; a shared test-support fixture would remove it if the pattern continues into
  `#23`.
- **`FlushPendingRetentionWrites` mirrors `RetryPeersWithBufferedMessages`'s snapshot-clear-iterate
  shape** *(review — Standards, judgement call)*: copy-pasted rather than factored into one generic
  helper. The two loops do different post-resolve work (deliver a Message vs. clear-flag-then-flush),
  so the author's own comment reasoning — that a shared generic would trade clarity for a thin
  abstraction — was accepted rather than changed.

Both axes otherwise ran clean: the Spec pass traced the full `Send → FlushPendingRetentionWrites →
FlushRetention → OnPeerWritable` cycle directly in the code and confirmed a Peer that is both
readiness-re-armed and freshly `Send`-marked in the same Frame is still flushed exactly once; the
unconditional `NoDelay` on shared `TcpLink` was confirmed spec-sanctioned (the issue explicitly asked
for this judgement call) rather than scope creep.

## Deferred

- The Standards review's minor test-duplication smells (above) are candidate cleanups for whoever
  picks up `#23`, not blocking this PR.

## Verification

`dotnet format --verify-no-changes`, `dotnet build` (0 warnings/errors), and `dotnet test` all green:
**387/387** tests passing, up from 382 at the start of this half (5 new tests across #89–#91 plus the
post-review backpressure test).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
