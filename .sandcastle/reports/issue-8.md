# Issue 8: 1.7 L1 UDP transport and the Raw datagram surface

## Done

L0/L1/L2 datagram machinery was already present on disk (untracked) from prior work: `UdpLink` +
its result types (`Transports/`), `RawReactor`, `RawEvent`/`RawEventKind`/`RawEventEnumerator`,
`CloseReason`, `Result`, and the test harness (`RawReactorHarness`, `RecordingDiagnosticsSink`). It
built cleanly but had no tests exercising the L2 datagram surface and one pre-existing test was red.
This session:

- **Fixed `UdpLink.Receive` truncation detection** (`src/ReactorSockets/Transports/UdpLink.cs`).
  The existing implementation used `Socket.ReceiveMessageFrom` and checked `SocketFlags.Truncated`.
  On this environment (.NET 8 / Windows 11), an oversized UDP receive neither throws
  `SocketError.MessageSize` nor sets the `Truncated` flag — it silently succeeds with exactly
  `scratch.Length` bytes and no signal, which the acceptance criteria's "truncation is unreachable
  at default / reachable when lowered" test caught immediately. Replaced with a
  `Socket.Available`-based pre-check (the OS-reported size of the *next* datagram, not the queue
  total) combined with the existing `MessageSize` exception catch, so both platform behaviors
  (Windows silent-truncate-to-buffer and the documented Windows-throws / Unix-silently-truncates
  split) are covered by one code path.
- **Updated `AssemblySkeletonTests`** (`tests/ReactorSockets.Tests/AssemblySkeletonTests.cs`) to
  include the new public L2 surface (`RawReactor`, `RawEvent`, `RawEventKind`,
  `RawEventEnumerator`, `CloseReason`, `Result`) in the expected exported-types list — it was
  failing because those types exist now and weren't in scope.
- **Added `tests/ReactorSockets.Tests/RawReactorTests.cs`**, the first test file driving the public
  L2 datagram surface end to end via `RawReactorHarness`, covering every acceptance criterion below.

## Acceptance criteria

- ✅ Bind port zero, read back a non-zero port from `LocalOf`, send from a BCL socket, Pump once,
  drain one `DatagramReceived` with the correct Source. —
  `Drain_AfterCounterpartySendsAndOnePump_YieldsOneDatagramReceivedWithCorrectSource` +
  `Bind_PortZero_ReturnsNonZeroLocalPort`.
- ✅ At the default scratch size truncation is unreachable; lowering it deliberately makes
  `DatagramTruncated` fire. —
  `Drain_AtDefaultScratchSize_LargeDatagramArrivesWholeWithoutTruncation` (full 65 507-byte
  datagram delivered whole) and `Drain_WithLoweredScratchSize_OversizedDatagramIsTruncatedAndReported`
  (16-byte scratch, 100-byte datagram → zero events, one `DatagramTruncated` record).
- ✅ Sending to a closed port and pumping produces no `Closed` Event and one
  `DestinationUnreachable` record. — `Pump_SendToClosedPort_ProducesNoClosedEventAndOneDestinationUnreachableRecord`.
- ✅ `Send` on a datagram Link returns a failed `Result` naming the illegality rather than throwing.
  — `Send_OnDatagramLink_ReturnsFailedResultNamingTheIllegality` (plus
  `Shutdown_OnDatagramLink_ReturnsFailedResultNamingTheIllegality` for the sibling call).
- ✅ Closing a Link produces exactly one `Closed` Event with `LocalClose`, and the `LinkId`'s
  generation prevents a stale id from addressing the slot's next occupant. —
  `Close_OpenLink_QueuesExactlyOneClosedEventWithLocalClose` +
  `Bind_AfterClosingPreviousLinkInSameSlot_StaleLinkIdNoLongerAddressesTheSlot`.

## Verification

- `dotnet format` — clean, no diffs beyond the source edits above.
- `dotnet build` — succeeded, 0 warnings, 0 errors.
- `dotnet test --no-build --verbosity normal` — **84/84 passed** (75 pre-existing + 9 new in
  `RawReactorTests`), re-run 3x with no flakes.

## Decisions

- **Truncation detection strategy.** The issue's scope text assumes Windows always throws
  `WSAEMSGSIZE` on an oversized `recvfrom`. That did not hold in this environment/runtime
  combination — the receive silently truncated instead. Rather than rely on a single platform
  signal, `Receive` now checks `Socket.Available` (OS-reported next-datagram size) before reading
  and still catches `SocketError.MessageSize` as a belt-and-suspenders path for whichever platform
  does throw. Behavior at the L2 surface (`DatagramTruncated`, no partial payload ever surfaced) is
  unchanged from what the issue specifies.
- Did not touch `ReadinessDispatcher`, `FrameArena`, or the diagnostics facade beyond what was
  already merged in earlier sub-issues — only `UdpLink.Receive`'s detection logic and the two test
  files were in scope for this issue.

## For later sub-issues

- The `Socket.Available` pre-check assumes UDP semantics where `Available` reports the *next*
  datagram's size, not a cumulative byte count — true on both Windows and Linux for `SocketType.Dgram`,
  but worth remembering if this code is ever reused for a stream transport in slice 2 (it must not
  be).
- `RawReactorTests` is the first test file exercising `RawReactorHarness` against real sockets;
  later slices' datagram/session tests can follow its shape directly.
