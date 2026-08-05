# Issue 6: 1.5 L0: readiness dispatch on Socket.Select, Pump, and thread affinity

## Done

- `src/ReactorSockets/ReadinessDispatcher.cs` — `ReadinessDispatcher(TimeProvider? timeProvider)`:
  captures the constructing thread's managed thread id; `Register(socket, onReadable)`/
  `Unregister(socket)` maintain a persistent Handle list plus a `Dictionary<Socket, Action>`
  callback lookup; `Pump()` checks thread affinity, records the observed monotonic interval since
  the previous Pump via the injected `TimeProvider`, builds a reused scan buffer starting at a
  rotating offset, calls `Socket.Select(scanBuffer, null, null, 0)` (zero-microsecond timeout — never
  blocks), and invokes each ready Handle's callback in scan order. The rotation offset advances by
  one Handle every Pump so the same Handle is not always scanned first.
- `tests/ReactorSockets.Tests/ReadinessDispatcherTests.cs` — 7 tests covering every acceptance
  criterion (see below).
- `tests/ReactorSockets.Tests/AssemblySkeletonTests.cs` — extended the exported-types allow-list with
  `ReadinessDispatcher`.

## Acceptance criteria

- ✅ Steady-state Pump loop allocates nothing once warm —
  `Pump_SteadyStateAfterWarmup_AllocatesNothingOnTheManagedHeap`: 10 warmup Pumps, then
  `GC.GetAllocatedBytesForCurrentThread()` delta across 1000 further Pumps (two registered sockets,
  one ready, one idle) asserted `== 0`. `Socket.Select` itself turned out allocation-free at steady
  state in .NET 8, not just the dispatcher's own scan-buffer reuse.
- ✅ Pumping from a second thread throws, exception type asserted —
  `Pump_CalledFromAnotherThread_ThrowsInvalidOperationException` runs `Pump` on a spawned `Thread`
  and asserts the caught exception `.Should().BeOfType<InvalidOperationException>()`.
- ✅ Over many Pumps with several ready Handles, no Handle is served first every time —
  `Pump_SeveralReadyHandlesOverManyPumps_DoesNotServeTheSameHandleFirstEveryTime`: 3 Handles, all
  kept ready (each has an unread datagram sitting in its receive buffer), 3 Pumps; the set of
  first-fired Handle ids across the 3 Pumps has count 3 — every Handle was first exactly once, the
  deterministic consequence of a capacity-3 rotation over exactly 3 Pumps.
- ✅ Nothing advances without a Pump — `Pump_NeverCalled_NoCallbackFiresAndNoIntervalObserved`:
  a Handle with data already waiting is registered and `Pump` is never called; callback count stays
  0 and `ObservedPumpInterval` stays `null`.
- ✅ Long-Frame / `SO_RCVBUF` consequence captured in XML docs, not defended against — see the
  `<remarks>` block on `ReadinessDispatcher`; no test, matching the issue's own wording.

## Verification

- `dotnet format --verify-no-changes` — clean, no diff.
- `dotnet build` — Build succeeded, 0 warnings, 0 errors.
- `dotnet test --no-build --verbosity normal` — Total tests: 48, Passed: 48 (7 new
  `ReadinessDispatcherTests`; 41 carried over from earlier sub-issues, unchanged).

## Decisions

- **`ReadinessDispatcher` is `public`, not `internal`, following issue 1.2/1.3/1.4's precedent.**
  No `RawReactor` exists yet on this branch to wrap L0 behind `Pump`/Drain, and the rules forbid
  `InternalsVisibleTo`, so this issue's own tests need direct access. It belongs in `Core/` once a
  later sub-issue's `RawReactor` constructs one internally and delegates `Pump` to it — at that
  point it should move to `Core/`, become `internal`, and drop out of the assembly allow-list.
- **A Handle is a bare `(Socket, Action)` pair, not a formal `IReadinessHandler` interface.** The
  issue's "Handlers are non-preemptive and internal" is satisfied by callbacks running to completion
  synchronously in scan order; a real Handler abstraction (carrying a `LinkId`, dispatching to
  `DatagramReceived` vs. TCP-specific readiness) belongs to whichever later sub-issue builds
  `RawReactor`/`Link`, which will very likely replace `Register(Socket, Action)` with something that
  registers a Link's read/accept logic instead. Introducing an interface now with only one
  implementation would be exactly the premature abstraction the project guidelines warn against.
- **`Socket.Select` is called with a `0` microsecond timeout**, not `-1` (infinite) or a positive
  value. `-1` would block the calling thread until something became ready, which contradicts "Pump
  never blocks" / "nothing advances without a Pump call" — a Pump must report readiness *at the
  instant it's called*, not wait for it.
- **Thread-affinity violation throws the BCL `InvalidOperationException`**, not a custom exception
  type. The acceptance criterion only requires the concrete type be assertable, and a bespoke
  exception class would be an abstraction this issue doesn't need.
- **Register/Unregister throw on duplicate registration** (via `Dictionary.Add`) rather than
  silently overwriting, since registering the same `Socket` twice is a programmer error the issue's
  "Handlers... internal" framing implies should never happen from outside the library.
- **Rotation advances by exactly one Handle per Pump**, not by the number served last Pump. This is
  the simplest rule that satisfies "unfairness does not accumulate on the same Handle across Pumps"
  and is what makes the fairness test's 3-Handles/3-Pumps case exactly deterministic.

## For later sub-issues

- `RawReactor`'s constructor should construct one `ReadinessDispatcher` (passing
  `ReactorOptions.TimeProvider`) and its `Pump()` should call `ReadinessDispatcher.Pump()` after
  `FrameArena.Reset()` (per issue 5's note on Frame lifetime ordering).
- The eventual UDP transport (L1) should register each bound socket's `Register(socket, onReadable)`
  callback to read off the wire into the scratch buffer and call `FrameArena.Reserve`, translating
  the result into a `DatagramReceived` Event for Drain — this issue deliberately does no I/O beyond
  readiness detection.
- `ObservedPumpInterval` is public on `ReadinessDispatcher` now; once wrapped by `RawReactor` it
  should likely be re-exposed as a `RawReactor` property (or folded into Counters) rather than
  requiring consumers to reach into L0.
- No issues found with earlier sub-issues' work; nothing outside this issue's scope was touched.
