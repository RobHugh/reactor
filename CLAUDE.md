# C# / .NET 8+ Project Guidelines

Scoped rules load on demand when files in those trees are touched:
`src/CLAUDE.md` (architecture, C# idioms, DI, async, EF Core) and
`tests/CLAUDE.md` (xUnit, FluentAssertions, NSubstitute, TimeProvider).
Read the relevant one before planning work that lands in either tree.

## Commands
- Build: `dotnet build`
- Test: `dotnet test --no-build --verbosity normal`
- Single test: `dotnet test --filter "FullyQualifiedName~MyTestMethod"`
- Format: `dotnet format`
- Package: `dotnet add package <Name>`

## Rules
- `.editorconfig` is the sole authority on formatting, indentation, braces, spacing.
- Run `dotnet format` after modifying files.
- Never leave `// TODO` or placeholder implementations unless asked.
- Verification gate: `dotnet build` and `dotnet test` must pass before reporting a task complete.
- Prefer code changes plus brief explanation over long prose.
