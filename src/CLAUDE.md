# .NET 8+ Architecture & C# Idioms

## Language
- File-scoped namespaces only.
- Primary constructors for injected dependencies: `class OrderService(IRepo repo, ILogger<OrderService> log) : IOrderService`.
- `<Nullable>enable</Nullable>`. No `!` unless working around a documented framework limitation — add an inline comment when used.
- One top-level type per file, named after it. A small type that exists only to serve that type (an outcome enum, a shape record, a lookup map) may share its file, e.g. `ReceiveWindowOutcome` in `ReceiveWindow.cs`.
- Collection expressions: `List<string> items = [];`, `ReadOnlySpan<byte> b = [1, 2, 3];`.
- Pattern matching and switch expressions over if/else chains for type checks and state transitions.

## DI
- Transient: stateless / short-lived. Scoped: `DbContext`, unit of work, per-request. Singleton: caches, thread-safe factories.
- Register via per-layer extension methods (`services.AddInfrastructure(configuration)`), not inline in `Program.cs`.
- Never inject `IServiceProvider` to resolve dynamically. Inject contracts explicitly.

## Async
- `Async` suffix on every async method.
- `CancellationToken cancellationToken = default` as the last parameter; forward it down the stack.
- No `async void` outside UI event handlers.
- `ValueTask<T>` only on hot paths that usually complete synchronously.

## EF Core
- No generic repositories — use `DbContext`/`DbSet<T>` directly in application layers unless abstracting heterogeneous sources.
- Async EF methods always (`ToListAsync`, `FirstOrDefaultAsync`, `SaveChangesAsync`).
- `.AsNoTracking()` on read-only queries; `.Select()` early so SQL pulls only needed columns.
- Strongly typed IDs (`readonly record struct` or Vogen) over primitives.

## Errors & Events
- No exceptions for expected business-rule violations. Use `Result<T>`:
  ```csharp
  public record Error(string Code, string Message);
  public record Result<T>(T? Value, Error? Error, bool IsSuccess);
  ```
- Dispatch domain events after state is persisted, never before.
