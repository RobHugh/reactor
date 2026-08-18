# Slice 3 (part 3): the retention ring and RetentionExhausted backpressure

**Closes** `#21`, `#83`, `#84`, `#85`, `#86`, `#87`, `#88`. Part of `#16` (slice 3).

## What landed

`StreamSessionReactor` gains its first public `Send`. A per-Peer, preallocated `RetentionArena`
(`Core/RetentionArena.cs`) — sized `Capacity × RetentionBytes` and allocated in full at
construction, before any Peer exists — is the send path itself, not a buffer beside it (ADR 0011).
`Send(PeerId, ReadOnlySpan<byte>)` frames a Message as a `u32` big-endian length prefix plus
payload and appends the framed bytes into that Peer's fixed-size region; a full region fails the
call with `SendOutcome.RetentionExhausted`, a value with no relation to `DisconnectReason` and
directly asserted as such. The arena tracks each Peer's region with monotonically increasing
`ulong` written/released totals rather than wrapping head/tail indices, per ADR 0015 — this makes
counter wraparound unreachable by construction instead of leaving an edge case only checkable by
opening a test seam into `internal` state, and no `InternalsVisibleTo` was added anywhere. Retained
bytes reach the wire from the existing `OnPeerWritable` flush path, using the same short-write
re-arm shape already used for handshake bytes, and are released only once successfully handed to
the OS socket — not once the peer has acknowledged them, which is slice 4's delivery-counter
mechanism (`#25`) and explicitly out of scope here. A per-Peer retention high-water counter reads
through the existing pull-only `CountersOf(PeerId)` seam (ADR 0005), not Diagnostics. A Peer's
retention region is reset for reuse at the same single `ClosePeer` choke point every slot-release
path already funnels through, including the three hostile-peer paths that never complete a
handshake — confirmed to need no new wiring, since they were already reachable from there.

## Sub-issue status

| Issue | Title | AC met | Notes |
|---|---|---|---|
| #83 | 3.5.1 The `RetentionArena` core type | 1/1 | Unwired and untested-through by design; mirrors `FrameArena.cs`'s constructor-validation and preallocation shape. No arithmetic felt like it needed a direct seam. |
| #84 | 3.5.2 Wire `RetentionArena` into construction and slot teardown | 2/2 | `_retention` constructed alongside `_arena`; allocation proven via `GC.GetAllocatedBytesForCurrentThread()` with zero Peers connected. |
| #85 | 3.5.3 Public `Send` with framing and `RetentionExhausted` | 2/2 | `SendOutcome` enum, `Send` resolves `PeerId` the same way `Close`/`CountersOf` do; distinctness from `DisconnectReason` asserted directly. |
| #86 | 3.5.4 Flush retention to the socket and release on write | 2/2 | Release triggers on local flush only, scope boundary stated in the diff; wrap-crossing `Send` sequence delivers correct bytes; released space is reusable. |
| #87 | 3.5.5 Retention high-water counter and real-socket exhaustion test | 2/2 | High-water reads the true burst peak and does not decrease on drain; exhaustion test reuses the `8426ff3` `SO_RCVBUF`-shrunk-on-listener stimulus rather than re-deriving it, green on both platforms in CI. |
| #88 | 3.5.6 Hostile-peer coverage for retention slot lifecycle | 2/2 | All three scenarios (silent open, truncated handshake, garbage version) reclaim their slot without reaching into `ShareHolderTable`/`RetentionArena` — proven via a subsequent legitimate connection at full capacity. |

## Still to come in slice 3

- `#22` (3.6) — Write batching once per Pump and Nagle off as a documented property.
- `#23` (3.7) — Goodbye and Closing.

## Review this carefully

Findings marked *(review)* came from `/code-review`'s Standards or Spec pass, run against
`35982be...HEAD`.

- **Duplicated retry-loop shape** *(review — Standards, judgement call)*: the `Thread.Sleep(20)`
  Nagle-flakiness workaround (already landed in `8426ff3`, ahead of this half) is now duplicated
  near-verbatim across three test files with slightly different attempt bounds (5 vs 20). Not a
  correctness issue; a shared harness helper would remove it if this shape recurs into 3.6–3.7.
- **Miscited commit hash in a test comment** *(review — Spec, trivial)*: the new exhaustion test's
  doc comment attributes the `SO_RCVBUF`-shrunk-on-listener-before-`Listen` pattern to `8426ff3`;
  that commit is the unrelated Nagle-flakiness fix. The pattern is correctly reused regardless —
  only the citation is wrong. Worth a one-line comment fix, not a behavioural issue.
- **`8426ff3` sits inside the reviewed diff range** but is a pre-existing flakiness fix unrelated to
  retention, landed on this branch ahead of the 3.5 work. Flagged so it isn't mistaken for
  undisclosed scope creep in this half.

Both axes otherwise ran clean: every AC from #21 and each of #83–#88 traced correctly against the
code, `RetentionExhausted` is genuinely distinct from every `DisconnectReason`, the arena is sized
and allocated eagerly exactly as specified, and `ResetShareHolder` is reachable from every
slot-release path through the single `ClosePeer` choke point — including the hostile-peer paths
#88 asked to double-check.

## Deferred

- **Peer-acknowledged release** (moving the `_retention.Release` trigger from local-flush to
  peer-confirmed via the delivery counter) is explicitly slice 4's (`#25`), per #86's own scope
  boundary — the call this half added is expected to be reused as-is, just invoked from a different
  point.
- The Standards review's minor retry-loop duplication (above) is a candidate cleanup for whoever
  picks up 3.6, not blocking this PR.

## Verification

`dotnet format --verify-no-changes`, `dotnet build` (0 warnings/errors), and `dotnet test` all
green: **382/382** tests passing, up from 367 at the start of this half (15 new tests across
#83–#88).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
