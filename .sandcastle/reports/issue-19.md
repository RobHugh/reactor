# Issue #19 (3.3) — The stream handshake, announced parameters, and refusal

## Done

- `src/ReactorSockets/DiagnosticSubjectKind.cs` — added `Peer`, for `LingerNegotiated`'s subject.
- `src/ReactorSockets/DiagnosticId.cs` — added `PeerRefused = 300` (Source subject, version or
  capacity) and `LingerNegotiated = 301` (Peer subject), both `Info`.
- `src/ReactorSockets/DiagnosticRecord.cs` — added a `Peer` field (new constructor parameter, every
  existing builder updated to pass `default`), plus `ForPeerRefusedCapacity`, `ForPeerRefusedVersion`
  and `ForLingerNegotiated`.
- `src/ReactorSockets/Session/StreamHandshakeCodec.cs` — reworked to a one-byte
  `StreamHandshakeMessageType` tag (`Hello` / `Refusal`) prefixing every 41-byte record, so a Refusal
  occupies exactly the same span a Hello does and a Peer waiting on a fixed-size record never needs to
  know which is coming before it has read the whole thing. Added `TryReadMessageType`, `WriteRefusal`,
  `TryReadRefusal`; `Write`/`TryRead` (the Hello codec) now read/write the type tag too. Reuses the
  datagram profile's existing `RefusalReason`/`RefusalReasonMap` (already profile-agnostic) rather than
  building a stream-only twin.
- `src/ReactorSockets/StreamSessionReactor.cs` — the handshake guard:
  - **Version.** `OnHelloRecordReceived` compares the offered version exactly; a mismatch calls
    `RefuseVersionAndClose`, which sends a Refusal naming both versions and closes with
    `DisconnectReason.VersionMismatch` once it is fully on the wire (immediately, or from
    `OnPeerWritable` after a short send — generalised via a new `PeerSlot.PendingCloseReason`).
  - **Capacity.** `AdmitOrRefuse` checks `ShareHolderTable.HasFreeSlot` at accept time; a connection
    with nowhere to go never gets a `PeerSlot` at all. `RefuseForCapacity` drains whatever the dialler
    already sent (avoids a reset from unread inbound data at close), sends a Refusal record, and closes
    — tracked in a new `RefusalSlot`/`_refusals` list entirely outside `_peers`, since refusing is
    exactly what happens because there is no Slot to give it.
  - **Linger.** `OnHelloRecordReceived` computes `effectiveLinger = min(ours, remote)`, stores it on
    `PeerSlot.EffectiveLinger`, and logs `LingerNegotiated` naming both figures.
  - **Admission cap.** Unchanged in shape from 3.2 (bounds accepts per Frame,
    `_capDeferralsThisFrame` deferred to backlog, `AdmissionCapReached` logged once per Frame) but now
    also bounds the accept-refuse-close cycle: a capacity refusal counts toward the same
    `_admissionsThisFrame` an ordinary accept does, matching the spec's "a thousand accept-refuse-close
    cycles... the cap bounds it."
  - Removed `_capacityDeferralsThisFrame` and `CapacityExhausted` from this profile's admission
    reporting — see Decisions.
- Tests:
  - `tests/ReactorSockets.Tests/StreamSessionHandshakeGuardTests.cs` (new) — 13 tests: version refusal
    (naming both versions, framed not bare-closed, `PeerClosed(VersionMismatch)`, `PeerRefused` at
    Info/Source), capacity refusal (reason, clean close not reset, touches neither admission counter,
    no event and existing Session unaffected), Linger negotiation (two hand-built Reactors with
    different Linger, both emit `LingerNegotiated` naming their own and the same effective figure),
    admission cap (exactly N handshakes processed in one Pump, measured as N genuine Hello replies
    received — not elapsed time; exactly one `AdmissionCapReached` record per Frame), and three
    hostile-peer cases via `HostilePeer` (silence, a truncated handshake, a garbage version) — none
    throws and each frees its Slot, verified by a subsequent normal `Connect` succeeding at capacity 1.
  - `tests/ReactorSockets.Tests/DiagnosticIdTests.cs` — added the four new/previously-untested member
    values (200-202, 300-301) to the reserved-block Theory.
  - `tests/ReactorSockets.Tests/DiagnosticRecordTests.cs` — added builder tests for
    `ForPeerRefusedCapacity`, `ForPeerRefusedVersion`, `ForLingerNegotiated`.

## Acceptance criteria

- ✅ A dialler announcing a different version is refused with a reason naming both versions, framed
  rather than a bare close —
  `Connect_OfferingAVersionTheReactorDoesNotSpeak_IsRefusedNamingBothVersions`,
  `..._IsAFramedRefusalNotABareClose`.
- ✅ At capacity, a further dialler is refused with a capacity reason, `PeerRefused` fires, and the
  connection is closed cleanly rather than reset —
  `Connect_EveryCapacitySlotOccupied_IsRefusedWithReasonCapacityAndClosedCleanlyRatherThanReset`.
- ❌ **A `Send` of an oversized Message failing at `Send` — not implemented; out of scope for this
  run.** `Send` and framing are subtask 3.4's (#20), explicitly excluded by the run's instructions
  ("do not build a `Send`... reach forward"). `PeerSlot.RemoteMaxMessageSize` is already captured
  (from 3.2) and ready for 3.4 to check against.
- ✅ Two Reactors with different Linger values both compute the same effective Linger and both emit
  `LingerNegotiated` naming their own and the effective figure —
  `Connect_WithDifferentLingerValues_BothEndsComputeTheSameEffectiveLingerAndEmitLingerNegotiated`.
- ✅ With the admission cap set to N and more than N connections pending, exactly N handshakes are
  processed in one Pump — `Pump_MoreConnectionsPendingThanTheAdmissionCap_ProcessesExactlyTheCapInOnePump`
  (measured as N genuine Hello replies actually received, not as elapsed time).
- ✅ A hostile `RawReactor`-based peer that opens and sends nothing, sends a truncated handshake, or
  sends a garbage version is handled without throwing and without leaking a slot — three tests in
  `StreamSessionHandshakeGuardTests`, each followed by a normal `Connect` at capacity 1 succeeding.

## TDD cycles

- `Connect_OfferingAVersionTheReactorDoesNotSpeak_IsRefusedNamingBothVersions` — red: `reply[0]`
  expected `0x02` (Refusal) but was `0x01` (Hello). Cause: the accepter sends its own unconditional
  Hello the instant it accepts (before it has read enough of the peer's record to know the version is
  wrong), so the Refusal is the *second* 41-byte record on the wire, not the first — a direct
  consequence of the handshake's symmetric, no-request/response design. Green: added
  `RawTcpPeer.ReceiveNextNonHelloRecord()`, which skips leading Hello records.
- `StreamHandshakeCodec` (Write/TryRead/WriteRefusal/TryReadRefusal/TryReadMessageType) and
  `StreamSessionReactor`'s handshake-completion split (`OnHandshakeRecordFilled` →
  `OnHelloRecordReceived`/`OnRefusalRecordReceived`) were built together with the guard logic they
  exist to serve, then driven red-green against the full `StreamSessionHandshakeGuardTests` file in one
  pass (12 of 13 red on first compile-and-run for the reason above, 13/13 green after the fix) — the
  same departure from single-assertion TDD 3.1 and 3.2 recorded, for the same reason: an interdependent
  wire-format-plus-state-machine change has one design, not thirteen.
- The `RefusalSlot`/`RefuseForCapacity` path (drain-then-send-then-close, entirely outside `_peers`)
  and the `PendingCloseReason` generalisation of the existing handshake-send machinery were each first
  driven red by the capacity and version tests failing with a hang or a `SocketException` from a reset
  during manual local exploration before being written into the test file in final form; the committed
  suite reflects the green state.

## Verification

- `dotnet format` — no changes on the final pass (one earlier pass removed an unused local constant).
- `dotnet build` — 0 Warnings, 0 Errors.
- `dotnet test` — 347 passed, 0 failed (326 pre-existing + 13 new handshake-guard tests + 8 new
  Diagnostic builder/id tests).

## Decisions

- **Capacity refusal is not counted as an Admission Deferral or Discard.** The parent issue's Amended
  section is genuinely ambiguous here — one paragraph describes a "post-accept, before completing"
  case counted as a Deferral that "waits for a later Pump," which reads as if it might be this
  subtask's capacity-refusal path, while the Scope section and the acceptance criteria are unambiguous
  that a capacity refusal is an immediate, same-Pump, in-protocol close and "must not touch either
  admission counter." I took the acceptance criteria as authoritative (they are concrete and testable)
  and implemented the refusal as counter-neutral, with an explicit negative-assertion test
  (`Connect_EveryCapacitySlotOccupied_TouchesNeitherAdmissionCounter`). I could not identify a scenario
  this profile's design actually produces that matches the "post-accept, pre-complete, deferred"
  description literally — every accepted connection either becomes a `PeerSlot` (capacity available) or
  is refused immediately (capacity exhausted); nothing sits accepted-but-undecided across a Pump
  boundary. Flagging this rather than silently picking one reading.
- **`_capacityDeferralsThisFrame` and the `CapacityExhausted` diagnostic are no longer reachable from
  this profile.** 3.2 built a pre-accept capacity check that left a connection in the backlog
  (deferred) when the Peer table was full; this subtask's Scope explicitly converts that same scenario
  into an in-protocol refusal instead ("a refused connection is still accepted, read, written to and
  closed"). I removed the now-dead field and its `ReportDeclinedAdmissions` branch rather than leaving
  unreachable code; `DiagnosticId.CapacityExhausted` itself is untouched (still reachable from the
  datagram profile, which discards rather than refuses).
- **The admission cap now also bounds the accept-refuse-close cycle**, not just genuine admissions:
  `_admissionsThisFrame` increments on every `TcpAcceptOutcome.Accepted`, whether the connection goes on
  to become a `PeerSlot` or gets refused for capacity. This is what makes the Scope's "a thousand
  accept-refuse-close cycles... the cap bounds it" literally true.
- **A capacity refusal drains once, opportunistically, before sending.** The dialler's own Hello may
  already be sitting in the kernel receive buffer by the time this Reactor accepts the connection
  (it's usually sent immediately after `Connect` resolves), and closing with that unread means TCP
  sends a reset instead of a clean FIN on some platforms. A single non-blocking drain pass right after
  registering, before the Refusal is sent, is what the "closed cleanly rather than reset" acceptance
  criterion needed; it is best-effort (a byte arriving in the narrow window between the drain and the
  close is not covered) but reliably correct for the ordinary case the tests exercise, where the
  dialler's Hello is already sent before this Reactor gets around to accepting.
- **Version mismatch is detected, and refused, independently by whichever end reads a bad version** —
  there is no listener/dialler asymmetry in this profile's handshake (Scope: "nothing here assumes a
  role"), so both ends may send a Refusal to each other in the mismatched case; only one is needed for
  either side to reach `PeerClosed(VersionMismatch)`, and `OnRefusalRecordReceived` exists specifically
  for the dialler's side of a *capacity* refusal, where only the accepter has the information to refuse
  and the dialler must be told.
- **`RefusalReason`/`RefusalReasonMap` reused verbatim from the datagram profile** (`Session/RefusalReason.cs`)
  rather than duplicated — already `internal`, already profile-agnostic (two members, a map to
  `DisconnectReason`), and the run's own instructions call out "adding a stream-only twin of a shared
  type" as the failure mode to avoid.

## For later sub-issues

- **3.4** (framing, the delivery counter) needs to start reading a Peer's socket past the handshake —
  `OnPeerReadable` currently stops reading once `HandshakeState.Established` is reached, deliberately
  (this slice carries no application data). `PeerSlot.RemoteMaxMessageSize` is what 3.4's `Send` should
  check a Message's size against, per the AC this run could not implement.
- **3.5** (retention ring) and **3.6** (write batching) have `PeerSlot.EffectiveLinger` now available
  (computed and stored, unused until Linger expiry lands) and the immutability of every negotiated
  parameter — version, `MaxMessageSize`, send-idle interval, Linger — already holds from the moment
  `HandshakeState.Established` is reached, since nothing in this slice or the ones before it ever
  reassigns those fields afterwards.
- **The refusal wire format (`StreamHandshakeMessageType`) is fixed-size and padded** to match the
  Hello record's size (41 bytes) specifically so a Peer never needs to know in advance which is coming.
  If a later slice needs a third handshake-phase message (e.g. for Resume, per the parent spec's "a
  resume secret is established during the handshake... its use is slice 4's"), it will need either a
  new fixed size shared by all three, or a length-prefixed handshake frame — worth deciding deliberately
  rather than bolting on a fourth ad hoc shape.
- **No per-Pump batching exists yet for the Refusal or Hello sends** — each is its own `Socket.Send`
  call, exactly as 3.2 left it. Slice 3.6's write-batching change should look at `SendRecordOrArm` and
  `SendRefusalRecord` alongside whatever it adds for data Messages.
