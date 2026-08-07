# Issue #81 — 3.4.3 The per-Peer read share and the one-whole-Message escape

## Done (by file)

- `src/ReactorSockets/StreamSessionReactor.cs` — the per-Peer read share and the one-whole-Message
  escape were already in place as a side effect of #79/#80: `TryReserveMessage` calls
  `_arena.Reserve(handle.Index, message)`, so `Core/FrameArena` already divides the arena by Peer
  slot index and already implements the escape. What was missing, and is this issue's actual
  change, is a guaranteed re-delivery trigger: a new `_peersWithBufferedMessages` set records any
  Peer whose `DeliverCompleteMessages` stalled on `ArenaReservationOutcome.ShareExhausted` or
  `ArenaExhausted` with a whole Message still sitting at the front of its buffer, and a new
  `RetryPeersWithBufferedMessages`, called once at the start of every `Pump()` right after
  `_arena.Reset()` and before the readiness dispatcher runs, gives every such Peer a fresh
  `DeliverCompleteMessages` pass under the new Frame's reset share — independent of whether the
  far end ever sends anything more. The set is drained (snapshot-and-clear) at the start of each
  retry pass so it only ever holds Peers that stalled *this* Frame, and is also cleared in
  `ClosePeer` and `Dispose` alongside the Reactor's other per-Peer collections.
- `tests/ReactorSockets.Tests/StreamSessionReadShareTests.cs` — new. Four tests against a
  hand-built raw `Socket` counterparty (`SharePeer`), mirroring `StreamSessionMessageFramingTests`'s
  `MessagePeer`.

## Acceptance criteria

- **Several Messages arriving inside one TCP read are delivered as several Events in the same Pump,
  subject to the read share.** Checked —
  `DeliverCompleteMessages_SeveralMessagesInOneReadWithinTheShare_DeliversAllInTheSamePump`: two
  20-byte Messages against a 50-byte share both land in the first non-empty Pump.
- **Messages beyond the Peer's share are delivered in a following Pump, in order, with none lost.**
  Checked —
  `DeliverCompleteMessages_MessagesBeyondTheShare_DeliverOnALaterPumpInOrderWithoutFurtherDataFromThePeer`:
  three 20-byte Messages (60 bytes against a 50-byte share) deliver 2 in the first Pump and the
  third on a later Pump, with **no further `Send` call from the peer between the two** — this is the
  test that would have failed without this issue's fix, and did (see TDD cycles).
- **A Peer whose share is smaller than one whole Message still delivers that Message whole, in one
  Pump.** Checked — `DeliverCompleteMessages_MessageLargerThanTheShare_StillDeliversWholeInOnePump`:
  an 8-byte share (capacity 8, arena defaulted to the 64-byte floor), one 50-byte Message, delivered
  whole in the first Pump via the escape.
- **Two Peers each get their own share: a Peer saturating its share does not reduce what another
  Peer delivers in the same Pump.** Checked —
  `DeliverCompleteMessages_TwoPeers_OneSaturatingItsShareDoesNotReduceTheOthersDeliveryInTheSameFrame`:
  Peer A sends 3×20-byte Messages (exceeding its 50-byte share), Peer B sends 2×20-byte Messages
  (fitting its own share) in the same Frame; Peer B's both land, Peer A gets only 2, and Peer A's
  third lands on a later Pump with nothing further sent.
- **A read share exhaustion is visible to the consumer through the per-Peer counter surface.** **Not
  covered by #81.** Per the orchestrator's scope discipline, the delivery counter is #82's — this
  issue does not add or touch counter logic. The mechanism this criterion depends on
  (`ArenaReservationOutcome.ShareExhausted`/`ArenaExhausted` already distinguishing a stalled
  reservation from a committed one) is in place and unchanged by this issue; #82's counter, once it
  increments only on `Committed`, will make an exhaustion visible as "the counter did not move on a
  Frame where bytes plainly arrived" without any further change here.

## TDD cycles

1. **Red.** Wrote all four tests in `StreamSessionReadShareTests.cs` against the reservation/escape
   machinery already present from #79. Ran filtered: 2/4 failed —
   `DeliverCompleteMessages_MessagesBeyondTheShare_…` and
   `DeliverCompleteMessages_TwoPeers_…`, both on `Expected […] to contain a single item, but the
   collection is empty` at the assertion checking the later Pump's delivery of the deferred
   Message. The other two (single-Pump-within-share, and the escape) already passed, confirming
   `FrameArena`'s existing per-share-holder division and escape needed no change — exactly what
   #79's and #80's reports predicted. The two failures were the flagged gap, reproduced concretely
   rather than assumed.
2. **Green.** Added `_peersWithBufferedMessages`, the `_peersWithBufferedMessages.Add(handle)` call
   in `DeliverCompleteMessages`'s stall branch, `RetryPeersWithBufferedMessages()`, and its call
   site at the top of `Pump()`. Filtered run: 4/4 passed.
3. **Refactor.** Added cleanup of the set in `ClosePeer` and `Dispose` for consistency with the
   Reactor's other per-Peer collections (`_connectingPeers`, `_pendingEvents`); `dotnet format`
   made no further changes. Full suite re-run: still 365/365.

## Verification

- `dotnet format` — no changes (already clean).
- `dotnet build` — 0 warnings, 0 errors.
- `dotnet test --verbosity normal` — **365/365 passed**, 0 failed, ~7.5s (361 baseline + 4 new). No
  `AssemblySkeletonTests` regression (no `InternalsVisibleTo` was added).

## Decisions

- **No second share mechanism.** Per the orchestrator's scope discipline, `Core/FrameArena`'s
  existing Arena ÷ Capacity division and one-whole-unit escape are untouched; the only change is
  making sure every Peer holding a whole, undelivered Message gets revisited every Pump, not just
  on new socket readiness.
- **Retry runs before the dispatcher, not after.** Placed right after `_arena.Reset()` so a
  redelivered Peer's fresh share is spent first, in handle order, before any Peer with genuinely
  new bytes this Frame competes for the same reset arena. Order between two starved Peers in the
  same Frame is not specified by the issue and was not asserted on.
- **Snapshot-and-clear rather than an in-place mutable iteration.** `DeliverCompleteMessages` can
  re-add a handle to `_peersWithBufferedMessages` while `RetryPeersWithBufferedMessages` is
  iterating it (if the retried Peer stalls again this Frame); copying to an array and clearing the
  field first before the loop keeps that sound without a defensive copy check inside the loop body.
- **`HashSet<SlotHandle>` rather than a `List`.** A Peer can stall more than once in a Frame
  (once via the retry pass, again via a dispatcher-triggered read later the same Frame if new bytes
  also arrived); a set makes the "retry once per Peer per Frame" property structural rather than
  something a duplicate-check has to maintain.
- **Test harness knobs.** Used the harness's existing `arenaSize`/`capacity`/`receiveBufferSize`
  parameters to derive small, exact shares (50 bytes at capacity 4/arena 200; 8 bytes at capacity
  8/arena defaulted to the 64-byte `MaxMessageSize` floor) rather than relying on the profile's
  default arena sizing, so each test's numbers are load-bearing and legible rather than incidental
  to whatever the default happens to be.

## For later sub-issues

- **#82 (delivery counter and its increment point).** Still untouched — no counter was added or
  incremented here. The natural hook remains `DeliverCompleteMessages`'s `Ready` arm, on
  `ArenaReservationOutcome.Committed`, exactly as #79's and #80's reports said. This issue's
  `RetryPeersWithBufferedMessages` calls back into the same `DeliverCompleteMessages` method, so
  whatever counter #82 adds there is automatically exercised on the redelivery path too — no
  separate wiring needed for the retry to be counted correctly once #82 lands.
- **Slice review.** #81's own acceptance criteria are now all satisfied except the counter-visible
  one, which is explicitly #82's — flagging this for whoever reviews slice-3 3.4 as a whole, so it
  isn't mistaken for a gap in this issue rather than a sequencing point between the two.
