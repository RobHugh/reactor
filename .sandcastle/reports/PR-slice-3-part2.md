# Slice 3 (part 2): u32 length-prefixed Message framing and the delivery counter

**Closes** `#79`, `#80`, `#81`, `#82`. Part of `#20` (3.4), part of `#16` (slice 3).

## What landed

A `StreamSessionReactor` Peer now carries and delivers application data. Bytes arriving on an
established Peer's transport are reassembled in a persistent per-Peer `ReceiveBuffer` (sized
`MaxMessageSize + 4`, surviving across Pumps), parsed as `u32` big-endian length-prefixed frames,
and — once a Message is complete — copied into the frame arena and delivered as exactly one
`SessionEventKind.MessageReceived` Event, valid until the next Pump. Delivery is bounded by the
same per-Peer read share `Core/FrameArena` already provides Raw Links (Arena ÷ Capacity, with the
escape that a Peer may always deliver one whole Message), and a Peer that loses that race for an
already-complete Message is retried on every subsequent Pump rather than left stalled. Malformed
framing — an oversized prefix, a zero-length prefix, a stream ending mid-prefix or mid-Message — is
stated once as a fail-closed table (`Session/MessageFramingLegality.cs`, the `RawLinkLegality`
house pattern) and closes the Session with a protocol-violation `DisconnectReason`, distinct from
the transport-failure reason a pending socket error produces through the same read path. A `u64`
cumulative `MessagesReceived` counter, reachable through a new public `CountersOf(PeerId)` seam
(mirroring `RawReactor.CountersOf(LinkId)`), increments only when a Message is handed to the frame
arena — never when its bytes finish arriving or while it is buffered waiting for share.

The profile still has no `Send` and no framing on the write side — that is 3.5 (`#21`), out of
scope here, and this half deliberately does not reach into it.

## Sub-issue status

| Issue | Title | AC met | Notes |
|---|---|---|---|
| #79 | 3.4.1 Per-Peer receive buffer and whole-Message delivery through the frame arena | 5/5 | `FrameArena.Reserve(handle.Index, …)` reused as-is, no second reservation path |
| #80 | 3.4.2 The malformed-framing table | 6/6 | 4GB-prefix non-allocation asserted via `GC.GetAllocatedBytesForCurrentThread()`; transport-failure detected for free through PR #73's error-set dispatch |
| #81 | 3.4.3 The per-Peer read share and the one-whole-Message escape | 4/5 | AC5 (share exhaustion visible via the per-Peer counter surface) explicitly deferred to #82, which lands it — see #81's report |
| #82 | 3.4.4 The delivery counter and its increment point | 4/4 | Counter surface decision stated explicitly per ADR 0015: `PeerCounters` record via `CountersOf(PeerId)`, generalising Raw's `LinkCounters`/`CountersOf(LinkId)` shape rather than attaching a running count to `SessionEvent` |

## Still to come in slice 3

- `#21` (3.5) — Send-side framing into the retention ring.
- `#22` (3.6) — Write batching and Nagle.
- `#23` (3.7) — Goodbye and Closing.

## Review this carefully

- **Retry ordering** (`#81`): `RetryPeersWithBufferedMessages` runs at the top of every `Pump()`,
  before the readiness dispatcher, so a Peer stalled from a previous Pump gets first claim on the
  freshly-reset arena/share ahead of Peers with genuinely new bytes this Frame. The spec does not
  order this explicitly; it is a disclosed decision, not a bug, but worth a second look if fairness
  ordering matters later.
- **Test duplication** (Standards review): `BuildHello`/`BuildFrame`/`EstablishSession`-shaped
  helpers are copy-pasted near-verbatim across the four new test files
  (`StreamSessionMessageFramingTests.cs`, `StreamSessionMalformedFramingTests.cs`,
  `StreamSessionReadShareTests.cs`, `StreamSessionDeliveryCounterTests.cs`). Not a correctness
  issue; a shared test-support fixture would remove it if this pattern continues into 3.5–3.7.
- **`MessageFramingLegality.Classify`**: written as sequential `if` guards ending in a ternary
  rather than a switch expression, despite `src/CLAUDE.md`'s preference for pattern
  matching/switch on state transitions. Cosmetic — the fail-closed structure itself is correct.

Both `/code-review` axes (Standards, Spec) ran clean otherwise: no missing or scope-creeping
requirements, and the delivery-counter increment point — the one piece #20 calls load-bearing —
was independently traced in the actual code, not just taken from the implementer's report.

## Deferred

- Counter **emission** and its role in releasing retention are explicitly slice 4's (`#25`) per
  `#82`; only the increment point is fixed here.
- The Standards review's minor test-duplication smell (above) is a candidate cleanup for whoever
  picks up 3.5, not blocking this PR.

## Verification

`dotnet format --verify-no-changes`, `dotnet build` (0 warnings/errors), and `dotnet test` all
green: **367/367** tests passing, up from 352 at the start of this half (15 new tests across
#79–#82).
