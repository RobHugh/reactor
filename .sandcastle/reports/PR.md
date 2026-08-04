Closes #2
Closes #3
Closes #4
Closes #5
Closes #6
Closes #7
Closes #8
Closes #9

Part of #1

## What landed

The library exists. A consumer constructs `RawReactor(ReactorOptions, scratchBufferSize?)` on their
own thread, calls `Bind(IPEndPoint)` to register a UDP socket and get a `LinkId` back, and advances
everything with one synchronous `Pump()` from their own loop. A Pump wipes the previous Frame,
resets the arena, runs one `Socket.Select` scan over every registered Handle starting at a rotating
offset, and reads whatever is ready — all on the calling thread, all before `Pump()` returns.
`Drain()` then yields `RawEvent`s (`DatagramReceived { Link, Source, Payload }` and
`Closed { Link, Reason }`) as a `ref struct` enumerator whose payloads alias the frame arena
directly and are valid only until the next Pump. `SendTo(LinkId, Source, ReadOnlySpan<byte>)` is
all-or-nothing and returns a `Result`; `Send`/`Shutdown` return a failed `Result` naming the
illegality rather than throwing. Pumping from a foreign thread throws.

Alongside it, the machinery every later profile inherits: `ReactorOptions` (required uncapped
`Capacity`, `AdmissionCap` defaulted to capacity, `ArenaSize` defaulted to the profile floor,
`TimeProvider`, and `Sink` + `MinimumLevel` as one argument), with `Validate()` and
`ResolveArenaSize(floor, description)` both called from `RawReactor`'s constructor so a combination
that cannot work throws with the arithmetic in the message. `FrameArena` preallocates once and hands
out per-share-holder byte fair-shares of `arena ÷ capacity`, with the one-whole-unit escape.
`Source` is a 48-byte `readonly record struct` carrying an explicit family tag, 16 address bytes, a
scope id that participates in equality, and a port — no surface anywhere takes a string host.
Diagnostics are the two mechanisms ADR 0005 names: `DiagnosticGateway` gates on `MinimumLevel`
before a record is built, catches a throwing Sink, counts `SinkFaulted` and disables itself for the
Reactor's life; `ReactorCounters`/`LinkCounters` are pull-only monotonic snapshots with no reset.

Binds are exclusive (`ExclusiveAddressUse` set explicitly on Windows) and `DualMode` is never set,
so `IPAddress.Any` binds IPv4 and `IPv6Any` binds IPv6 and a consumer wanting both binds twice. A
duplicate bind fails at registration with an `InvalidOperationException` naming the endpoint, the
platform `SocketError` preserved as the inner exception. An oversized datagram is consumed and
reported as `DatagramTruncated` rather than left at the head of the queue; a `WSAECONNRESET` from an
ICMP port-unreachable is absorbed as a `DestinationUnreachable` record and never closes the Link.

`dotnet build`: 0 warnings, 0 errors. `dotnet test`: 88/88. `dotnet format --verify-no-changes`:
clean.

## Sub-issue status

| Issue | Title | AC met | Notes |
|---|---|---|---|
| #2 | Solution skeleton — assembly, test project, package identity | 4/4 | "Core/Transports types are internal" was vacuously true — both folders were empty. See the first review item below for what happened next. |
| #3 | Shared options value, default provenance, construction failure | 3/3 | `Validate()`/`ResolveArenaSize` are public and separately callable; `RawReactor`'s ctor calls both. |
| #4 | The `Source` value type and the two-type address vocabulary | 5/5 | Size pinned at 48 bytes, not the spec's "roughly 56" — under budget, deliberately not padded. |
| #5 | Frame arena, Frame lifetime, the derived read share | 4/4 | All four asserted against `FrameArena` directly, not through `Drain()`. |
| #6 | L0 readiness dispatch, Pump, thread affinity | 5/5 | Zero-allocation steady state asserted with `GC.GetAllocatedBytesForCurrentThread()`; rotation advances one Handle per Pump. |
| #7 | Diagnostics facade, `DiagnosticRecord`, pull-only Counters | 4/5 | "A Sink calling back into the Reactor throws" was left open — no `RawReactor` existed in that sub-issue. It is **still open**: nothing in `RawReactor.cs` guards re-entrancy. |
| #8 | L1 UDP transport and the Raw datagram surface | 5/5 | Truncation detection was rewritten mid-issue; see the review list. |
| #9 | Binding, address family policy, admission cap | 4/4 | The admission cap has no consuming path in this slice (no accepts or handshakes exist yet), as #7 also noted. |

## Update — review items 1 and 2 have been fixed

Items 1 and 2 below described the slice as it stood when this PR was opened. They have since been
acted on in a follow-up commit; the text is left as written so the finding and the fix can be read
together.

L0 now lives in `src/ReactorSockets/Core/` under the `ReactorSockets.Core` namespace, and
`FrameArena`, `ArenaReservation`, `ArenaReservationOutcome`, `ReadinessDispatcher`,
`DiagnosticGateway`, `ReactorCounterSet` and `LinkCounterSet` are all `internal`.
`ReactorOptions.Validate()` and `ResolveArenaSize()` are internal too — they were public for the
same reason and had the same fix. The exported surface is now seventeen types, all in the root
namespace: `RawReactor`, `RawEvent`, `RawEventKind`, `RawEventEnumerator`, `CloseReason`, `Result`,
`LinkId`, `Source`, `SourceFamily`, `ReactorOptions`, `IReactorDiagnostics`, `DiagnosticRecord`,
`DiagnosticLevel`, `DiagnosticId`, `DiagnosticSubjectKind`, `ReactorCounters`, `LinkCounters`.

The allow-list test that item 2 called a rubber stamp is replaced by a namespace guard —
`ReactorSocketsAssembly_ExportedTypes_ContainNothingFromL0OrL1` fails if any type in `Core/` or
`Transports/` is public, so a future sub-issue cannot re-open the leak by extending a list. The five
direct L0 unit-test files are deleted and re-expressed against `RawReactor`: `RawReactorPumpTests`,
`RawReactorFrameTests`, `RawReactorDiagnosticsTests` and `RawReactorCountersTests`, and
`ReactorOptionsTests` now drives every options failure through `new RawReactor(...)`.

**Coverage genuinely lost, since no test can reach an internal type without `InternalsVisibleTo`:**
the arena's `ReadShare` arithmetic across several (arena, capacity) pairs — only one configuration
is now pinned, through the escape; `ShareExhausted`, which item 4 already established is unreachable
through Raw; the `FrameArena` constructor and `shareHolderId` range guards, unreachable because
`ReactorOptions` validates first; and `ReadinessDispatcher.ObservedPumpInterval`, which nothing
consumes (item 11) and which now has no test — it should be consumed by slice 3's RTO work or
deleted. The suite is 73 tests, down from 88.

**Item 3 is now partly closed.** `Drain_DatagramLargerThanTheReadShare_IsDeliveredWholeViaTheEscape`
pins the escape at the observable layer, and
`Drain_SeveralLinksEachSentTwoOversizedDatagrams_YieldsExactlyOnePerLinkPerPump` pins the
one-per-Link-per-Pump behaviour a consumer sees. **Item 4 stands unchanged** — that second test
passes because a ready Link is read once per Pump, not because of the share cap, and its comment
says so in the test file rather than leaving the next reader to rediscover it. Items 5 to 11 are
untouched.

## Review this carefully

**1. L0 is public and at the root, not internal in `Core/`.** `src/ReactorSockets/Core/` contains
nothing but `.gitkeep`. `FrameArena`, `ArenaReservation`, `ArenaReservationOutcome`,
`ReadinessDispatcher`, `DiagnosticGateway`, `ReactorCounterSet` and `LinkCounterSet` all sit at
`src/ReactorSockets/*.cs` as `public`. This contradicts #1's layout decision and user story 28
("L0 and L1 to be `internal` within one assembly, so that the dispatcher and the transports can be
replaced wholesale without a major version"). `Transports/` complies — every `UdpLink*` type is
`internal`. Each sub-agent from #5 onward independently justified this the same way: no `RawReactor`
existed on the branch yet, `InternalsVisibleTo` is forbidden, so the tests needed direct access —
and each said "make it internal once `RawReactor` wraps it." `RawReactor` now exists and wraps all
of them, so the condition they named has been met and nobody went back. Files:
`src/ReactorSockets/FrameArena.cs:1`, `ReadinessDispatcher.cs:41`, `DiagnosticGateway.cs:1`,
`ReactorCounterSet.cs:1`, `LinkCounterSet.cs:1`.

**2. The single-seam testing decision is broken, and a test now codifies the breakage.**
`FrameArenaTests`, `ReadinessDispatcherTests`, `DiagnosticGatewayTests`, `ReactorCounterSetTests` and
`LinkCounterSetTests` are direct L0 unit tests — "No test reaches into L0 or L1." Worse,
`AssemblySkeletonTests.cs` maintains an allow-list of expected exported type names that was widened
by each sub-issue in turn; it records the leak rather than catching it. Deciding item 1 decides this
one: making L0 internal deletes or rewrites roughly half the test suite. That is a real cost and a
human should price it, which is why I did not do it. File:
`tests/ReactorSockets.Tests/AssemblySkeletonTests.cs:1`.

**3. THE GAP #1 FLAGS ABOUT ITSELF IS NOT CLOSED.** #1 says the read cap's escape "firing every
time, yielding one unit per share-holder per Pump — was not designed and is worth a test that pins
it, because it is the behaviour a consumer will actually observe at the defaults." No such test
exists at the observable layer. The closest is
`FrameArenaTests.Reserve_EachShareHolderExceedingItsShareOnce_YieldsExactlyOneUnitPerShareHolderThisFrame`,
which builds a `FrameArena(65_507, 4)` by hand and calls `Reserve` twice per share-holder. It never
goes through `Bind`/`Pump`/`Drain`, and it does not assert that those numbers are what a
default-constructed `RawReactor` actually produces. A consumer's observable "one datagram per Link
per Pump" is unpinned.

**4. And the one-datagram-per-Pump behaviour does not currently come from the read cap at all.**
`RawReactor.OnLinkReadable` calls `slot.Link.Receive` exactly once per readiness callback
(`src/ReactorSockets/RawReactor.cs:212`), so a Link can never reach a second `Reserve` in one Frame
and `ArenaReservationOutcome.ShareExhausted` is unreachable through the public surface. One datagram
per Link per Pump is an artefact of the single read, not of the escape. Both are defensible designs;
what matters is that the test in item 3, when written, would pass today for the wrong reason — and
that stories 20/21's per-Pump bound is currently enforced by the read loop's absence rather than by
the arena. Issue #5's own report predicted this ("`ShareExhausted` may be effectively unreachable
through Raw as actually wired") and shipped the mechanism anyway per ADR 0014. I judged this a
decision, not a defect, but it is the single most consequential thing in the slice to get a second
opinion on.

**5. Truncation detection is a pre-read probe on `Socket.Available`, not a platform signal.**
`src/ReactorSockets/Transports/UdpLink.cs:90` reads `Socket.Available > scratch.Length` before
`ReceiveFrom` and reports `Truncated` if so. Issue #8 changed to this because on this machine
(.NET 8 / Windows 11) an oversized UDP receive neither threw `SocketError.MessageSize` nor set
`SocketFlags.Truncated` — it silently returned exactly `scratch.Length` bytes. I reproduced the
green test but I have **not** verified the premise on Unix, and I cannot from here. Two specific
things to check: Winsock's `FIONREAD` documentation says it returns "the amount of data that can be
read in a single call to `recv`, which may not be the same as the total amount of data queued" —
if any platform or socket state makes `Available` report the *cumulative* queue instead of the next
datagram, several small queued datagrams would be misreported as one truncation and the payload
silently dropped. User story 13 asks for "identical truncation behaviour on Windows and Unix," and
this slice can only claim to have tested one of them.

**6. `RawReactor` is not `IDisposable`.** It owns every bound `Socket` through its `LinkSlot`s and
offers no way to release them but closing each Link individually; there is no finalizer.
`RawReactorHarness` (`tests/ReactorSockets.Tests/RawReactorHarness.cs:54`) disposes its counterparty
sockets and not the Reactor, so every test leaks its bound sockets until GC. User story 7 explicitly
names "an undisposed Reactor" as a thing consumers will hit. Nothing in #1's Implementation
Decisions mentions disposal, so this may be a genuine spec gap rather than an implementation miss.

**7. Sink re-entrancy is unguarded.** #7's fifth acceptance criterion — a Sink calling back into the
Reactor throws — was deferred to whoever wired the gateway into `RawReactor`. #8 wired it and did
not implement the guard. There is no call-depth or in-Pump flag anywhere in
`src/ReactorSockets/RawReactor.cs`. A Sink that calls `Pump()` from inside `Log` currently re-enters
and clears the pending-event list mid-Frame.

**8. Thread affinity is enforced unevenly and story 3 is untested at L2.**
`EnsureCallingThreadIsConstructingThread` is called from `Pump`, `Bind`, `SendTo` and `Close`, but
not from `Send`, `Shutdown`, `LocalOf`, `CountersOf` or `Counters` (`RawReactor.cs:151`, `158`,
`166`, `193`, `78`). The reads are arguably fine; `Send`/`Shutdown` are less obviously so. Separately,
user story 3 is only pinned at `ReadinessDispatcherTests.Pump_CalledFromAnotherThread_...` — no test
drives a foreign thread at a `RawReactor`, which is the surface the story is about.

**9. `Send`/`Shutdown` ignore their arguments entirely.** Both return a constant `Result.Failure`
without resolving the `LinkId` (`RawReactor.cs:151`, `158`). A stale or garbage `LinkId` gets "Send
is illegal on a datagram Link" rather than "not open." Correct for this slice, since every Link is
datagram; it becomes wrong the moment slice 2 adds stream Links, and the tests currently passing
(`Send_OnDatagramLink_...`) would keep passing through that change.

**10. Smaller standards items** the Standards axis raised and I did not act on: `!` used without the
documented-limitation comment `src/CLAUDE.md` requires (`UdpLink.cs:67` `socket.LocalEndPoint!`, and
several test sites); `EnsureCallingThreadIsConstructingThread` plus its captured thread-id field
duplicated verbatim between `RawReactor.cs:336` and `ReadinessDispatcher.cs:137`; three separate
recording-Sink implementations across the test project (`RecordingDiagnosticsSink.cs`, a nested one
in `DiagnosticGatewayTests`, another in `ReactorOptionsTests`); several non-atomic tests whose names
carry an "And" (`Log_ThrowingSink_IsInvokedExactlyOnceAndSinkFaultedReadsOne`); `NSubstitute`
referenced by the test project and never used; and an inconsistent error policy where "Link is not
open" is a `Result.Failure` in `SendTo` but an `ArgumentException` in `LocalOf`/`CountersOf`.

**11. Possible scope creep**, both minor and both arguably load-bearing later:
`ReadinessDispatcher.ObservedPumpInterval` is public API that nothing in this slice consumes (it is
RTO groundwork for slice 3+), and `RawReactor.Shutdown` exists at all despite `Shutdown` appearing
by name in #1's Out of Scope list — though returning a failed `Result` for it is exactly what user
story 9 asks for.

**Limits of this review.** I read the same #1 that the eight implementing agents read, so anywhere
they collectively misread it I am likely to have misread it the same way — item 4 is the clearest
candidate. I verified the standing test cases mechanically (grep for `InternalsVisibleTo`,
`DualMode`, `MaxMessageSize`, a type named `Reactor`, `PackageReference` in the src csproj: all
clean) and read `RawReactor.cs`, `ReadinessDispatcher.cs`, `UdpLink.cs`, `ReactorOptions.cs` and
`RawReactorTests.cs` in full. I did **not** independently re-derive the zero-allocation claim in
`ReadinessDispatcherTests`, run anything on Linux, or run the suite more than once, so nothing here
speaks to flakiness under load or on another platform.

## Deferred

- **Admission cap has no consumer.** `ReactorOptions.ResolvedAdmissionCap`,
  `ReactorCounterSet.RecordAdmissionDeferred` and `DiagnosticRecord.ForAdmissionCapReached` are
  reachable only from tests. #1 establishes the cap here on purpose ("in this slice it has no
  datagram work to bound"); slice 2's accepts are its first caller.
- **`DiagnosticSubjectKind` ships without a `Peer` case** (#7's decision) — no `Peer`/`PeerId` type
  exists in this slice. The enum is additive-safe; a Session-profile slice adds the case alongside
  whatever `PeerId` shape it defines.
- **No per-`LinkId` counters registry.** `LinkCounterSet` is held per slot inside `RawReactor` and
  reached through `CountersOf(LinkId)`; #7 deliberately built no `Dictionary<LinkId, …>` because
  Link lifecycle belongs elsewhere.
- **`CloseReason.ResetByPeer`, `Refused` and `Unreachable` are never produced** in this slice — only
  `LocalClose` and `Failed` have paths. The stream half of ADR 0008 supplies the rest in slice 2.
- **`ObservedPumpInterval` should probably move** off `ReadinessDispatcher`'s public surface and
  onto `RawReactor` (or into Counters) once L0 goes internal — #6's own note.
- **`Socket.Available` as a next-datagram size** must not be carried into slice 2's stream
  transport; #8 flagged this explicitly, and item 5 above is the same code from the other direction.
- **`DiagnosticFormatter` and the `ReactorSockets.Logging` adapter** are slice 9, per #1's Out of
  Scope.
