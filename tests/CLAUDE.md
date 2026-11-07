# Testing (xUnit + FluentAssertions + NSubstitute)

## The seam — read this before adding a test file

**The public L2 surface is the only seam.** Tests construct a real `RawReactor` on loopback, bind
port zero, and drive it with explicit `Pump` calls. The four things a test may observe are the Events
drained from a Pump, the `Result` returned by a call, the records a Sink received, and a Counter
snapshot. Nothing else.

- **Never add `InternalsVisibleTo`.** L0 (`Core/`) and L1 (`Transports/`) are internal and
  untested-through. `AssemblySkeletonTests` fails if the attribute appears.
- **Never insert an abstraction for testability** — no fake transport, no interface extracted so a
  test can substitute it, no member widened to `internal` to be reached, and no type made `public` so
  a test can reach it (ADR 0022).
- **An L0 invariant you cannot reach through the seam is a design signal, not a testing problem.**
  Change the code until the invariant is trivially true rather than reaching past the seam to observe
  it. Generation wraparound in `ShareHolderTable<T>` is the worked example.

A test that would fail if the dispatcher were rewritten but the behaviour preserved is testing the
wrong thing. The reasoning, and the conditions that would reopen any of this, are in
[ADR 0015](../docs/adr/0015-l0-is-untested-through-and-an-unreachable-invariant-is-designed-away.md).
Do not restate them here — one of these two places drifting from the other is what ADR 0015 exists to
fix.

## Structure
- One test file per production file: `OrderService.cs` -> `OrderServiceTests.cs`.
- Method names: `[UnitOfWork]_[StateUnderTest]_[ExpectedBehavior]` — e.g. `CreateOrderAsync_WhenInventoryIsInsufficient_ReturnsFailureResult`.
- AAA layout with `// Arrange` / `// Act` / `// Assert` headers.
- Atomic tests — no unrelated assertions bundled together.

## Mocking
- `Substitute.For<T>()` on external boundary interfaces only (`IPaymentGateway`, `IEmailSender`). Use real instances for domain entities, DTOs, records.
- Stub and verify with `Arg.Any<T>()` / `Arg.Is<T>(...)`.
- Verify side effects explicitly: `await gateway.Received(1).ChargeAsync(100.00m, Arg.Any<CancellationToken>());` or `.DidNotReceive()`.

## Time
- Production code takes `TimeProvider`; never `DateTime.Now`.
- Tests inject `FakeTimeProvider` (Microsoft.Extensions.Time.Testing) and advance it to exercise timeouts and timestamps.

## Assertions
- FluentAssertions, never raw `Assert.*`: `result.Should().NotBeNull()`, `await action.Should().ThrowAsync<InvalidOperationException>()`, `items.Should().HaveCount(3).And.ContainInOrder(expected)`.

Production-code rules (naming, async, nullability) live in `src/CLAUDE.md`.
