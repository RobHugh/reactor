# Issue 9 — 1.8 Binding, address family policy, and the admission cap

## Done

Most of this sub-issue's scope was already established by issues 3 (options/admission cap), 7
(diagnostics), and 8 (`UdpLink`, `DualMode` never set, `ExclusiveAddressUse` on Windows). What was
missing was the observable failure mode when exclusivity is violated, and test coverage pinning the
acceptance criteria.

- **`src/ReactorSockets/Transports/UdpLink.cs`** — `UdpLink.Bind` now catches `SocketException`
  specifically, disposes the socket, and throws `InvalidOperationException` naming the endpoint (which
  includes the port) and the platform `SocketError`, with the original `SocketException` preserved as
  `InnerException`. Previously the raw, platform-specific `SocketException` propagated with no
  endpoint in its message.
- **`tests/ReactorSockets.Tests/RawReactorTests.cs`** — four new tests:
  - `Bind_SamePinnedPortTwice_ThrowsNamingThePort` — pins a port via `LocalOf`, rebinds it, asserts
    `InvalidOperationException` whose message contains the port number.
  - `Bind_IPv6AnyAndIPv4AnyOnSamePort_ReturnsTwoIndependentLinks` — binds `IPv6Any` then `IPv4Any` on
    the same pinned port; both succeed as distinct Links.
  - `Drain_CounterpartySendsIPv4ToIPv6AnyLink_NeverYieldsDatagramReceived` — an IPv4 BCL socket sends
    to the port an `IPv6Any` Link is bound to; several Pumps never yield `DatagramReceived`.
- **`tests/ReactorSockets.Tests/AssemblySkeletonTests.cs`** —
  `ReactorSocketsAssembly_PublicMembers_NoneAcceptAStringHostOrAddress` reflects over every public
  type's public methods and fails if any parameter named like `host`/`hostname`/`address`/`endpoint`
  is typed `string`. (A blanket "no string parameter anywhere" was tried first and false-positived on
  `ReactorOptions.ResolveArenaSize(string floorDescription)`, which is an error-message fragment, not
  an address — narrowed to address-like parameter names.)

## Acceptance criteria

- ✅ Binding the same pinned port twice fails at registration, naming the port, on the host platform —
  `Bind_SamePinnedPortTwice_ThrowsNamingThePort`; verified on Windows (this branch's dev host).
- ✅ Binding `IPv6Any` accepts no IPv4 traffic; a consumer binding both families gets two Links and two
  `LocalOf` results — `Bind_IPv6AnyAndIPv4AnyOnSamePort_ReturnsTwoIndependentLinks` and
  `Drain_CounterpartySendsIPv4ToIPv6AnyLink_NeverYieldsDatagramReceived`.
- ✅ No public method anywhere accepts a `string` host —
  `ReactorSocketsAssembly_PublicMembers_NoneAcceptAStringHostOrAddress`; also true by inspection, every
  public surface (`RawReactor.Bind`, `LocalOf`, `SendTo`) takes `IPEndPoint`/`Source`/`LinkId`.
- ✅ Test-suite convention "bind port zero" is documented — already stated in
  `docs/adr/0014-binding-dual-stack-and-what-capacity-counts.md` ("A test suite binds port zero...");
  every test in this branch (issues 7-9) follows it, including the new ones here.

## Verification

- `dotnet format` — clean, no changes needed after final edit.
- `dotnet build` — 0 Warnings, 0 Errors.
- `dotnet test --no-build --verbosity normal` — **88 passed, 0 failed** (4 new tests added on top of
  the prior 84).

## Decisions

- **Exception type for a failed `Bind`.** ADR 0014 says a duplicate bind "fails at registration and
  names the port" but doesn't mandate a type. Used `InvalidOperationException` to match the existing
  pattern in `RawReactor.FindFreeSlot` (capacity exhaustion) rather than inventing a new exception
  type or trying to subclass `SocketException` (which has no constructor accepting a custom message
  alongside an error code). The original `SocketException`/`SocketErrorCode` is preserved as
  `InnerException` so platform diagnosis is still possible, matching the "exact `SocketError` still
  available" principle used elsewhere (e.g. `DiagnosticRecord.ForLinkFailed`).
- **Scope of the no-string-host reflection test.** A literal "no public method has any `string`
  parameter" is too broad — `ReactorOptions.ResolveArenaSize` legitimately takes a `string
  floorDescription` for its exception message, not an address. Narrowed the check to parameters whose
  name suggests addressing (`host`, `hostname`, `address`, `endpoint`), which is what the acceptance
  criterion is actually protecting against.
- **No IPv4-cannot-connect-to-IPv6Any test.** `Socket.Available`/OS routing makes it awkward to prove
  the negative from the IPv6 side without a working IPv6 stack on the CI host; the criterion is instead
  covered from the IPv4 sender's side (send succeeds locally, but the `IPv6Any` Reactor never sees it)
  plus the two-Links test, which is the behaviour a consumer actually observes.

## For later sub-issues

- Nothing new is owed to later slices beyond what issues 3/7/8 already wired (`AdmissionCap`,
  `ResolvedAdmissionCap`, `DiagnosticRecord.ForAdmissionCapReached`) — this slice still has no
  admission-work path that consumes it (no accepts/handshakes exist yet), same as issue 7 left it.
- The `InvalidOperationException` wrapping in `UdpLink.Bind` is the pattern to follow if a future
  slice's Session Reactor construction needs to "throw naming the endpoint" for its own bind set (ADR
  0014's "Construction validates the set... and a failure throws naming the endpoint").
