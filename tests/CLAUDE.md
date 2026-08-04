# Testing (xUnit + FluentAssertions + NSubstitute)

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
