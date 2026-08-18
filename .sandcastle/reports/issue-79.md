# Issue #79 — 3.4.1 Per-Peer receive buffer and whole-Message delivery through the frame arena

## Done (by file)

- `src/ReactorSockets/StreamSessionReactor.cs` — constructs and owns the `FrameArena` this
  Reactor had discarded until now (`_arena`), resets it every `Pump()`, gives each `PeerSlot` a
  persistent `ReceiveBuffer`/`ReceiveBytesFilled` pair, dispatches an established Peer's readable
  event to the new `OnEstablishedPeerReadable` → `DeliverCompleteMessages` → `TryReserveMessage`
  pipeline, and extends `PendingEvent`/`GetPendingEvent` to carry a `MessageReceived` event's
  arena offset and length.
- `src/ReactorSockets/Session/MessageFramingCodec.cs` — new. The single place the `u32`
  big-endian length prefix is read/written, mirroring `StreamHandshakeCodec`'s house pattern.
- `src/ReactorSockets/SessionEventKind.cs` — appended `MessageReceived = 3`.
- `src/ReactorSockets/SessionEvent.cs` — added `Payload` (aliases the arena) and the internal
  `ForMessageReceived` factory.
- `src/ReactorSockets/DiagnosticRecord.cs` — added a `PeerId`-subject overload of
  `ForArenaExhausted`, alongside the existing `LinkId`-subject one Raw uses, since a Session's
  share-holder is a Peer (ADR 0014) not a Link.
- `tests/ReactorSockets.Tests/StreamSessionReactorHarness.cs` — added
  `DrainToListWithPayload()`, copying `SessionEvent.Payload` out as a `byte[]` before it can be
  invalidated by the next Pump, mirroring `RawReactorHarness.DrainToList()`.
- `tests/ReactorSockets.Tests/StreamSessionMessageFramingTests.cs` — new. Five tests, all against
  a hand-built raw `Socket` counterparty (`MessagePeer`, mirroring the existing house pattern
  `StreamSessionHandshakeGuardTests.RawTcpPeer`) rather than a second `StreamSessionReactor`, per
  the issue's own instruction.

## Acceptance criteria

- **A Message written as u32 prefix + payload is drained as exactly one Event carrying exactly
  those bytes.** Checked —
  `DeliverCompleteMessages_WholeFrameWrittenInOneSend_DrainsExactlyOneMessageReceivedWithThoseBytes`.
- **A Message delivered one byte at a time is delivered exactly once, whole, and only when
  complete.** Checked —
  `DeliverCompleteMessages_MessageDeliveredOneByteAtATime_IsDeliveredExactlyOnceWholeAndOnlyWhenComplete`
  sends every byte of the frame as its own socket write, asserting an empty drain after each one
  but the last (provably impossible to satisfy early, since the final byte has not yet left the
  peer).
- **A Message of exactly MaxMessageSize round-trips intact.** Checked —
  `DeliverCompleteMessages_MessageOfExactlyMaxMessageSize_RoundTripsIntact` with
  `MaxMessageSize: 64`, a payload of exactly 64 bytes.
- **A partial Message survives across Pumps.** Checked —
  `DeliverCompleteMessages_PartialMessageSplitAcrossPumps_SurvivesAndCompletesOnceTheRestArrives`
  sends 6 of a 14-byte frame, Pumps several times with nothing further sent (each drains empty),
  then sends the rest and gets the whole Message back intact — proving the tail lived in the
  persistent per-Peer buffer, not the arena, which is wiped every Pump.
- **Delivered bytes are valid until the next Pump and no further.** Checked —
  `Drain_AfterTheFrameThatDeliveredAMessage_TheNextPumpsDrainIsEmpty` proves the Frame that
  produced a `MessageReceived` event has nothing left to drain the moment the next Pump passes.

## TDD cycles

Production code and tests were authored together for this sub-issue (single coherent design,
not incrementally discovered), so the cycle was driven as one red-green pass over the whole
surface rather than per-criterion:

1. **Red** — with the implementation complete, `DeliverCompleteMessages(handle, slot)`'s call
   site in `OnEstablishedPeerReadable` was commented out and the full `dotnet test` for
   `StreamSessionMessageFramingTests` run: 5/5 failed, each on a missing `MessageReceived` event
   (`"Expected drained to contain a single item, but the collection is empty"`) — the right
   reason, not a compile error or a wrong-shape failure.
2. **Green** — the call site was restored; the same filtered run passed 5/5.
3. **Refactor** — none needed; the implementation was written to its final shape before the red
   check, and nothing changed shape afterwards.

## Verification

- `dotnet format` — no further changes (already clean).
- `dotnet build` — 0 warnings, 0 errors.
- `dotnet test --verbosity normal` — **352/352 passed**, 0 failed, 7.4s. No `AssemblySkeletonTests`
  regression (no `InternalsVisibleTo` was added).

## Decisions

- **Multiple Messages arriving in one read.** `DeliverCompleteMessages` loops, delivering every
  whole Message currently at the front of the receive buffer in one Pump, using the existing
  `FrameArena.Reserve(handle.Index, …)` — the same share-holder machinery Raw and Datagram already
  call. This was a judgment call: the issue's own acceptance list only ever has one Message in
  flight, but the loop falls out naturally from parsing "however much is buffered," and reuses
  L0's already-built per-Peer share/escape rather than adding new logic of its own. If
  `TryReserveMessage` fails (`ShareExhausted` or `ArenaExhausted`), the loop stops without
  consuming the still-complete Message — it stays buffered, whole, for a retry the next time this
  Peer's transport is read. **Flag for #81**: because reads are level-triggered on new socket
  activity, a Peer whose buffer holds a second complete Message that lost the share/arena race has
  no guaranteed prompt to retry if the far end sends nothing further — this Reactor never revisits
  a Peer's buffer except from a fresh readable dispatch. Not exercised by any test here (the
  default arena floor is exactly one maximum Message), but it is the real gap #81's "Per-Peer read
  share and one-whole-Message escape" needs to close.
- **Oversized/zero length prefixes.** Deliberately unrejected. `DeliverCompleteMessages` refuses
  to act on a prefix bigger than the receive buffer could ever hold (stalls that Peer's reads
  rather than reading out of bounds — no crash, no delivery, no disconnect), and a zero-length
  prefix is treated as an ordinary valid empty Message. Both are #80's malformed-framing table's
  job to turn into a protocol-violation `DisconnectReason`; this slice only refuses to misbehave,
  it does not police the wire.
- **Diagnostic subject for arena exhaustion.** Added a second `DiagnosticRecord.ForArenaExhausted`
  overload keyed on `PeerId` rather than reusing the `LinkId` one Raw uses — `DiagnosticId.ArenaExhausted`
  (id 2) is shared, only the subject differs, consistent with `ForLingerNegotiated`'s existing
  `Peer`-subject precedent in this same file.
- **Test counterparty.** Used a hand-built raw `Socket` (`MessagePeer`, mirroring
  `StreamSessionHandshakeGuardTests.RawTcpPeer`) rather than driving an actual `RawReactor`
  instance. The issue's own wording — "a `RawReactor` writing framed bytes, not a second Session
  Reactor" — is read here as "not a well-behaved Session peer," and the raw-`Socket` shape is this
  codebase's existing, already-proven instrument for exactly that role in this same test area; a
  `RawReactor` would have needed its own Pump loop purely to write bytes it never reads back.

## For later sub-issues

- **#80 (malformed-framing table).** The two refusal points to wire a protocol-violation
  `DisconnectReason` into are already named above: an oversized length prefix (currently a silent
  stall, `messageEnd > slot.ReceiveBuffer.Length` in `DeliverCompleteMessages`) and a zero-length
  prefix (currently accepted as a valid empty Message). A truncated stream is already handled —
  `OnEstablishedPeerReadable` closes with `DisconnectReason.TransportFailed` on `EndOfStream`, but
  #80 may want a framing-specific reason distinguishing "EOF mid-Message" from an ordinary
  transport failure.
- **#81 (per-Peer read share and one-whole-Message escape).** Already partly in place for free:
  `TryReserveMessage` calls `_arena.Reserve(handle.Index, …)`, so the per-Peer share and
  first-message escape are exactly L0's existing `FrameArena` semantics. What's still open: the
  "Message stays buffered for retry" behaviour on `ShareExhausted`/`ArenaExhausted` has no
  guaranteed re-delivery trigger absent further socket activity from that Peer — see the Decisions
  section above.
- **#82 (delivery counter and its increment point).** No counter was touched. `DeliverCompleteMessages`
  and `TryReserveMessage` are the natural increment points — mirror `RawReactor.TryReserve`'s
  `slot.Counters.RecordBytesReceived(data.Length)` / `_counters.ObserveArenaUsage(_arena.BytesUsed)`
  calls, plus a whole-Message counter incremented once per successful delivery in
  `DeliverCompleteMessages` (not inside `TryReserveMessage`), exactly as `RawReactor.DeliverDatagram`
  separates the two for the same reason: one counts bytes, the other counts whole units delivered.
