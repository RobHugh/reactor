# Issue #80 — 3.4.2 The malformed-framing table: oversized prefix, zero prefix, and truncation

## Done (by file)

- `src/ReactorSockets/Session/MessageFramingLegality.cs` — new. `MessageFramingOutcome`
  (`AwaitingPrefix`, `AwaitingMessage`, `Ready`, `OversizedPrefix`, `ZeroLengthPrefix`) and
  `MessageFramingLegality.Classify`, the malformed-framing table stated once, mirroring the
  `RawLinkLegality`/`TryReserve` house pattern.
- `src/ReactorSockets/StreamSessionReactor.cs` — `DeliverCompleteMessages` now switches over
  `MessageFramingOutcome`: `Ready` reserves and delivers exactly as before, `OversizedPrefix` and
  `ZeroLengthPrefix` log `DiagnosticId.MalformedFraming` and close with `DisconnectReason.ProtocolViolation`
  — grouped with a `default` arm so an outcome added later with no row here fails closed rather than
  being silently permitted. `OnEstablishedPeerReadable` now classifies `EndOfStream` through the new
  `ClassifyEndOfStream` (nothing buffered → `TransportFailed`; anything buffered → `ProtocolViolation`,
  since `DeliverCompleteMessages` already drains every whole/oversized/zero-length state before a read
  can report `EndOfStream`, so leftover bytes are always a mid-prefix or mid-Message truncation).
  `Failed` (a pending socket error) is unchanged: always `TransportFailed`, and still detected with
  nothing extra, through the same read path PR #73's readiness dispatcher already feeds.
- `src/ReactorSockets/DisconnectReason.cs` — added `ProtocolViolation`, appended at the end.
- `src/ReactorSockets/DiagnosticId.cs` — added `MalformedFraming = 302` in the stream-session block.
- `src/ReactorSockets/DiagnosticRecord.cs` — added `ForMalformedFraming(PeerId, announcedLength, maxMessageSize)`.
- `tests/ReactorSockets.Tests/DiagnosticIdTests.cs` — added the `MalformedFraming = 302` row.
- `tests/ReactorSockets.Tests/StreamSessionMalformedFramingTests.cs` — new. Eight tests against a
  hand-built raw `Socket` counterparty (`MalformedPeer`), mirroring `StreamSessionMessageFramingTests`'s
  `MessagePeer` and `StreamSessionHandshakeGuardTests`'s `RawTcpPeer`.

## Acceptance criteria

- **A prefix announcing 4 GB causes no allocation; the Session closes with a protocol-violation
  reason and the test asserts memory did not move.** Checked —
  `DeliverCompleteMessages_PrefixAnnouncing4GB_ClosesWithProtocolViolationAndAllocatesNothingSizedByTheClaim`
  measures `GC.GetAllocatedBytesForCurrentThread()` around sending just the four prefix bytes plus the
  Pumps that detect and close, and asserts the delta is under 1 MB — nowhere near the claimed 4 GB.
- **A prefix of zero closes the Session with a protocol-violation reason.** Checked —
  `DeliverCompleteMessages_ZeroLengthPrefix_ClosesWithProtocolViolationRatherThanAnEmptyMessage`.
- **A stream ending mid-prefix and a stream ending mid-Message each close the Session with a
  protocol-violation reason rather than throwing or hanging.** Checked —
  `OnEstablishedPeerReadable_StreamEndsMidPrefix_ClosesWithProtocolViolation` (2 of 4 prefix bytes,
  then a graceful FIN) and `OnEstablishedPeerReadable_StreamEndsMidMessage_ClosesWithProtocolViolation`
  (full prefix plus 3 of 10 body bytes, then a graceful FIN).
- **A peer that sends a Message one byte larger than our announced MaxMessageSize is disconnected.**
  Checked — `DeliverCompleteMessages_PrefixOneByteLargerThanMaxMessageSize_ClosesWithProtocolViolation`.
- **A pending socket error surfacing through the read path closes the Session with a
  transport-failure reason, distinct from the protocol-violation reasons.** Checked —
  `OnEstablishedPeerReadable_APendingSocketErrorOnRead_ClosesWithTransportFailedNotProtocolViolation`
  forces an RST via `LingerOption(true, 0)` (this codebase's existing house pattern, mirroring
  `RawReactorHarness.CloseCounterpartyAbruptly`) and asserts `TransportFailed`.
- **The rules live in one table; a reason with no row fails closed, asserted rather than assumed.**
  `MessageFramingLegality.Classify` is the one table. Fail-closed is structural, not merely
  asserted-by-inspection: `DeliverCompleteMessages`'s switch groups `OversizedPrefix`,
  `ZeroLengthPrefix` and `default` into a single refusing arm, so a future `MessageFramingOutcome`
  member added with no row there closes the Session rather than being read as legal — the same
  guarantee `RawLinkLegality`'s and `TryReserve`'s discard arms give. Also checked as a byproduct of
  every test above passing without a corresponding case existing to "assume" — see the Decisions
  section for the deliberate `TransportFailed`-on-clean-EOF case, the one branch this table
  distinguishes from a violation on purpose.

## TDD cycles

1. **Red (compile-time scaffold).** Added `DisconnectReason.ProtocolViolation`,
   `DiagnosticId.MalformedFraming` and `DiagnosticRecord.ForMalformedFraming` first — the minimal
   surface the test file needed to compile — then wrote the full production implementation
   (`MessageFramingLegality`, the `DeliverCompleteMessages` rewrite, `ClassifyEndOfStream`) and all 8
   tests together as one coherent design, the same shape issue #79's report used and justified: the
   parser's malformed branches and the tests asserting them are one decision, not eight independent
   ones discovered incrementally.
2. **Red (behavioural check).** With the implementation in place, temporarily reverted the two
   decision points to their pre-#80 shape — the `OversizedPrefix`/`ZeroLengthPrefix` arm returned
   silently instead of closing (the exact stall #79 left behind), and `ClassifyEndOfStream` always
   returned `TransportFailed` (the exact behaviour before this table existed) — then ran the filtered
   suite: **6 of 8 failed**, each for the intended reason (an empty drained-events collection where a
   `PeerClosed` was expected, or `TransportFailed` where `ProtocolViolation` was expected — never a
   compile error or an unrelated exception). The 2 that still passed were the two this change does not
   touch: the "one byte larger than MaxMessageSize" case (already closes via the pre-existing
   `messageEnd > buffer length` check, which is a strict subset this table now generalises) and the
   pending-socket-error case (already `TransportFailed` before #80, and meant to stay that way).
3. **Green.** Restored the real implementation; the full filtered suite passed 8/8, and the full
   suite passed 361/361 (352 baseline + 8 new + 1 new `DiagnosticIdTests` row).
4. **Refactor.** None needed beyond what was written in step 1 — `dotnet format` made no further
   changes.

## Verification

- `dotnet format` — no changes (already clean).
- `dotnet build` — 0 warnings, 0 errors.
- `dotnet test --verbosity normal` — **361/361 passed**, 0 failed, ~7.4s. No `AssemblySkeletonTests`
  regression (no `InternalsVisibleTo` was added); `DiagnosticIdTests` confirms `MalformedFraming = 302`
  sits in the stream-session reserved block and no member is 0.

## Decisions

- **One `DisconnectReason` for every violation, not one per case.** The issue's own wording
  repeats "a protocol-violation `DisconnectReason`" for all four malformed cases rather than naming
  four distinct reasons, so `ProtocolViolation` is a single new member. The `DiagnosticId.MalformedFraming`
  record still carries the offending length prefix in `Value0` (0 for a truncation, since no full
  prefix was ever read) for anyone who needs to tell the cases apart after the fact.
- **`ClassifyEndOfStream` does not call `MessageFramingLegality.Classify`.** By construction,
  `DeliverCompleteMessages` runs after every successful read and drains every `Ready` Message while
  closing immediately on `OversizedPrefix`/`ZeroLengthPrefix` the instant the 4-byte prefix is legible
  — so by the time a read reports `EndOfStream`, the only two states leftover bytes can be in are
  `AwaitingPrefix` or `AwaitingMessage`. Both map to the same `ProtocolViolation` outcome, so the EOF
  decision is the two-way `slot.ReceiveBytesFilled == 0` check rather than a second call into the
  table — simpler, and the doc comment states the invariant that makes it sound rather than leaving it
  implicit.
- **The oversized check compares against `MaxMessageSize`, not the receive buffer's length.** #79
  left the check as `messageEnd > slot.ReceiveBuffer.Length` (the buffer's own floor, which happens to
  equal `MaxMessageSize + 4` by default but need not with a widened `ReceiveBufferSize`). This slice
  compares the announced length directly against `_options.MaxMessageSize`, which is the actual
  contract ("`MaxMessageSize` is a receive limit... the whole reason it is announced") and is what
  makes the "one byte larger than MaxMessageSize" acceptance criterion correct independent of how the
  receive buffer happens to be sized.
- **Test counterparty.** `MalformedPeer`, a third hand-built raw `Socket` type in this test area
  (after `RawTcpPeer` and `MessagePeer`), rather than extending either existing one — it needed both
  `Shutdown(SocketShutdown.Send)` (a graceful half-close, for the two truncation cases) and the
  `LingerOption(true, 0)` reset (for the transport-failure case), neither of which the existing two
  types expose, and #80's own text calls for "a `RawReactor` hostile peer — an ordinary Raw consumer".
- **`DiagnosticId.MalformedFraming` is logged only for `OversizedPrefix`/`ZeroLengthPrefix`, not for
  truncation.** Matches the existing house pattern: a plain `TransportFailed` from `EndOfStream`/`Failed`
  has never logged a Diagnostic in this Reactor (see the unchanged `Failed` arm and the pre-#80
  `EndOfStream` arm), so a truncation — which now shares `ProtocolViolation`'s reason but arrives
  through the same code path as an ordinary transport ending — keeps that same absence of a Diagnostic
  rather than inventing a new one this issue's acceptance criteria never asked for.

## For later sub-issues

- **#81 (per-Peer read share and one-whole-Message escape).** Untouched by this issue: the
  `ShareExhausted`/`ArenaExhausted` retry-on-buffer path in the `Ready` case is exactly what #79 left
  it as, just now reached through the table's `Ready` arm instead of an inline length check. The
  "Message stays buffered for retry with no guaranteed re-delivery trigger" gap #79's report flagged is
  still open and still #81's to close.
- **#82 (delivery counter and its increment point).** Still untouched — no counter was added or
  incremented anywhere in this issue. `DeliverCompleteMessages`'s `Ready` arm remains the natural
  increment point #79's report already named; nothing here changes that recommendation.
- **A structural note for whoever next edits `MessageFramingOutcome`.** The fail-closed guarantee
  lives in `DeliverCompleteMessages`'s switch shape — `OversizedPrefix`, `ZeroLengthPrefix` and
  `default` grouped into one arm — not in the enum itself. Anyone adding a new `MessageFramingOutcome`
  member that is meant to be legal (not a violation) must add its own `case` above that group,
  exactly as `AwaitingPrefix`/`AwaitingMessage`/`Ready` do; forgetting to is safe (it fails closed as a
  violation) but wrong for a legal case, and there is no compiler check that would catch a missing
  "this one's fine" row the way there is for the outcomes that must refuse.
