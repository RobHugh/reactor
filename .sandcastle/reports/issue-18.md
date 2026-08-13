# Issue #18 — 3.2 Peers, SessionEvent, and the declared endpoint set

## Done

- `src/ReactorSockets/ISessionEventSource.cs` (new) — the internal seam that lets `SessionEvent` and
  `SessionEventEnumerator` be genuinely shared (ADR 0010) instead of duplicated per profile.
- `src/ReactorSockets/SessionEventEnumerator.cs` — retargeted from a hard-coded `DatagramSessionReactor`
  field to `ISessionEventSource`, so it now drains either profile.
- `src/ReactorSockets/DatagramSessionReactor.cs` — implements `ISessionEventSource` explicitly (no
  behaviour change; existing `PendingEventCount`/`GetPendingEvent` became the interface's explicit
  implementations).
- `src/ReactorSockets/DisconnectReason.cs` — added `TransportFailed`, needed for a dial or handshake
  that fails or ends before establishment (a reset, a refused dial, an early FIN).
- `src/ReactorSockets/StreamSessionOptions.cs` — added `Endpoints` (possibly empty, declared once,
  never rebound) and `Entropy` (the resume-secret source), plus `ValidateEndpoints()` rejecting a null
  or duplicate endpoint by name, wired into `Validate()`.
- `src/ReactorSockets/Session/StreamSessionProtocol.cs` (new) — the stream profile's own protocol
  constants: `Version` and `ResumeSecretLength`, independent of the datagram profile's.
- `src/ReactorSockets/Session/StreamHandshakeCodec.cs` (new) — the fixed 40-byte handshake record
  (version, `MaxMessageSize`, send-idle interval and Linger in milliseconds, resume secret) both ends
  write once their TCP connection exists. Symmetric, not request/response: nothing here assumes a role.
- `src/ReactorSockets/StreamSessionReactor.cs` — full implementation: binds the declared endpoint set
  at construction (no rebinding), `BoundEndpoints`, `Connect`, `Close`, `Pump`, `Drain`, `Dispose`,
  `Counters`. Peers live in `ShareHolderTable<PeerSlot>` (not hand-rolled), converted to `PeerId` only
  at the public edge. The Peer lifecycle: `Connecting` → `AwaitingPeerHandshake` (TCP up, this end's
  record sent, waiting on the other's) → `Established` (both records exchanged, `PeerConnected`
  queued). A short send of the handshake record is retried via the dispatcher's writable-arming path;
  a transport failure or early end-of-stream at any point before `Established` closes the Peer with
  `DisconnectReason.TransportFailed`.
- `tests/ReactorSockets.Tests/StreamSessionReactorHarness.cs` (new) — mirrors
  `DatagramSessionReactorHarness`.
- `tests/ReactorSockets.Tests/StreamSessionEndpointTests.cs` (new) — 20 tests covering binding, the
  dial-only case, duplicate-endpoint validation, establishment on both ends, Frame/Drain, disposal, and
  Peer identity/lifecycle including the stale-handle case.

## Acceptance criteria

- ✅ Two Reactors, one listening on port zero and one dialling it, reach an established Session with
  each end draining a Peer-established event — `Connect_ToAListeningReactor_BothEndsDrainAPeerConnectedEvent`.
- ✅ Empty endpoint set succeeds, binds nothing, and can still dial —
  `BoundEndpoints_NoDeclaredEndpoints_IsEmpty`, `Constructor_EmptyEndpointSet_SucceedsAndCanStillDial`.
- ✅ Two endpoints bind both, snapshot in declared order —
  `BoundEndpoints_TwoDeclaredLoopbackEndpoints_ExposesBothInDeclarationOrder`.
- ✅ Duplicate endpoint throws, naming it — `Constructor_DuplicateEndpoint_ThrowsArgumentExceptionNamingTheEndpoint`.
- ✅ A stale `PeerId` whose slot has been reused fails rather than addressing the new occupant —
  `Close_AStalePeerIdWhoseSlotWasReused_FailsRatherThanAddressingTheNewOccupant` (capacity 1 forces the
  reuse; `ShareHolderTable<T>`'s generation check gives this for free — the test is the through-the-seam
  pin ADR 0015 calls for).
- ✅ No public method exposes a socket, a Handle, or a Link — `NoPublicMember_ExposesASocketAHandleOrALinkFromThisProfile`
  (reflection check over every public member's parameter and return types) plus
  `AssemblySkeletonTests.ReactorSocketsAssembly_ExportedTypes_AreOnlyTheL2SurfaceAConsumerDrives`, which
  already covers the exported-type surface and still passes.

## TDD cycles

Implementation and tests for this subtask were developed together rather than strictly test-first: the
new wire protocol and Reactor needed a substantial, interdependent skeleton (codec, options, table,
handshake state machine) before any test could compile, so I built that skeleton first and then drove
the test file against it — closer to the shared-refactor pattern than pure red-green. Genuine red→green
cycles were driven for the parts where my first attempt was wrong:

- `Connect_TwoSeparateDials_AreAddressedByDifferentPeerIds` — red: both dials reported the *same*
  `PeerId` (`Index=0, Generation=1` for both). Cause: the test's wait condition
  (`accepter.DrainToList().Count > 0`) was checked against a Frame that still held the *first*
  `PeerConnected` from before any Pump of the second wait loop ran, so the loop exited on round 0
  without ever pumping the second dial. Green: an explicit `accepter.Reactor.Pump()` between the two
  waits to end the first Frame before starting the second's.
- `Close_AStalePeerIdWhoseSlotWasReused_FailsRatherThanAddressingTheNewOccupant` and
  `Close_AnEstablishedPeer_DrainsPeerClosedForThatPeerIdWithLocalClose` — red: `DrainToList()` after
  `Close` returned two items (the establishing `PeerConnected` and the `PeerClosed`), because `Close`
  queues into the same `_pendingEvents` list a stale Frame still held. Green: an intervening
  `Reactor.Pump()` to end the establishing Frame before `Close`, matching the pattern already used in
  `DatagramSessionIdentityTests`.

All three failures were genuine — read the actual failure message, not a compile error — before being
fixed.

## Verification

- `dotnet format` — no changes.
- `dotnet build` — 0 warnings, 0 errors.
- `dotnet test` — 326 passed, 0 failed (306 pre-existing + 20 new).

## Decisions

- **`ISessionEventSource` added.** `SessionEventEnumerator` was hard-coded to `DatagramSessionReactor`
  even though the type and its doc comments already claimed to be "shared by both Session profiles."
  Making it actually shared, via a small internal interface both Reactors implement explicitly, is a
  direct, minimal fix in scope for this subtask (`SessionEvent` and `SessionEventEnumerator` are
  explicitly called out as the shared surface this subtask reads as existing) rather than a new
  parallel Stream-only enumerator, which the run's instructions call out as the failure mode to avoid.
- **`DisconnectReason.TransportFailed` added.** Needed to close a Peer whose TCP connect failed or
  whose stream ended/reset before a handshake completed. The three existing members
  (`LocalClose`/`VersionMismatch`/`RemoteAtCapacity`) don't cover this and the slice's own testing
  notes anticipate "this slice's transport-failure `DisconnectReason`" existing by the end of 3.1-3.3.
- **No version or capacity guarding in this subtask.** The handshake exchanges both ends' version but
  does not compare it, and there is no in-protocol capacity refusal — a Reactor at capacity simply
  leaves a pending connection in the OS backlog, exactly as Raw does. That is subtask 3.3's job
  ("guard the handshake") and its own `DiagnosticSubjectKind.Peer` addition; wiring
  `VersionMismatch`/`RemoteAtCapacity` here would step on it. The handshake record's shape (all four
  negotiated values plus the resume secret) is already sized for 3.3 to add the comparison without a
  wire change.
- **No per-Peer counters (bytes sent/received, retention high-water, resumes).** The parent-issue text
  asks for these to be "allocated with the Peer's slot," but no `PeerCounters` type exists anywhere in
  the codebase yet — `DatagramSessionReactor`'s `PeerSlot` doesn't have them either, despite being the
  house pattern this subtask was told to match. Retention high-water and resumes are concepts from
  3.4/3.5/4, all out of scope for this run; building the storage now with nothing to increment it would
  be exactly the "reach forward" the run's instructions warn against. Flagging this as a gap shared with
  the datagram profile rather than fixing it here, since fixing it would mean redesigning
  `DatagramSessionReactor`'s `PeerSlot` too, well outside this subtask's file list.
- **Accept failures during listen are not logged.** `TcpListenLink.TryAccept` failing has no Peer to
  attribute a Diagnostic to yet (`DiagnosticSubjectKind` has no `Peer` member — that's 3.3's), so this
  subtask silently retries on the next Pump rather than borrowing the Raw profile's `LinkFailed` id,
  which would put a Raw-only concept (`LinkId`) into a Session Diagnostic.
- **Partial handshake sends are retried, not assumed away.** The 40-byte record will not split across
  syscalls on any real stack, but `SendHandshakeRecord`/`OnPeerWritable` handle a short send correctly
  via the dispatcher's arm-writable path anyway, since the alternative (assuming it never happens) is
  exactly the kind of untested assumption this codebase's `ArenaReservation`/`Send` machinery is
  designed never to need.

## For later sub-issues

- **3.3** needs to: add `DiagnosticSubjectKind.Peer`; compare `RemoteVersion` in `CompleteHandshake` and
  refuse with `DisconnectReason.VersionMismatch` on mismatch; refuse in-protocol at capacity (currently
  a connection past capacity is left in the backlog, matching Raw, not refused); decide the wire shape
  of an in-protocol refusal for this profile (nothing here reserves space for one — the handshake record
  is symmetric and unconditional); and negotiate Linger to the minimum of the two announced values,
  emitting `LingerNegotiated`. `PeerSlot.RemoteVersion`, `RemoteMaxMessageSize`,
  `RemoteSendIdleInterval` and `RemoteLinger` are already captured and available for 3.3 to act on.
- **3.4-3.5** (framing, retention): `PeerSlot.RemoteResumeSecret` and `LocalResumeSecret` are stored and
  ready. There is currently no per-Peer counter storage at all (see Decisions above) — 3.4/3.5 (or a
  dedicated counters subtask) will need to add it to both Session profiles' `PeerSlot`s together.
- Once `Established`, `OnPeerReadable` deliberately stops reading a Peer's socket — bytes sent after the
  handshake sit in the kernel receive buffer until framing exists to consume them. This is intentional
  for this run but is the first thing 3.4 needs to change.
