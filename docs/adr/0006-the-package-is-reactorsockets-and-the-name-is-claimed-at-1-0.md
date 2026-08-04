# The package is ReactorSockets, and the name is claimed at 1.0

`Reactor` is unavailable on NuGet — it is a mod API for a game, at version 2.3.1 — and the `Reactor.*`
prefix is occupied by three unrelated authors (`Reactor.Maui`, `Naos.Reactor.*`, `Eziriz.Reactor.*`).
That prefix can therefore never be reserved by us, and shipping an adapter into it would place our
package among strangers' packages under a root ID we do not own.

The library ships as **`ReactorSockets`**. Ruled out along the way: `NetReactor`, which collides with
Eziriz .NET Reactor, a widely used obfuscator; `ReactorNet`, which is one letter from `reactor-netty`,
the Java reactive-streams networking library and precisely the push-model project this library is
most likely to be mistaken for; and any `-Sharp` form, whose convention signals a C# port of a
non-C# original that does not exist. `ReactorSockets` uses a word the L2 contract deliberately
hides — Handle and Peer both list "socket" on their avoid-lists — but a package named for the OS
resource category it abstracts is the same choice `System.Net.Sockets` makes, and it is what people
actually type into NuGet search.

**Package ID, assembly name, root namespace and source folder all agree.** The reason is a genuine
collision rather than convention: the existing `Reactor` package emits `Reactor.dll`, and two
assemblies with the same simple name cannot coexist in one output folder. Layers stay folders within
the one assembly, as ADR 0003 requires:

```
src/ReactorSockets/
  Core/            (L0)  internal — ReactorSockets.Core
  Transports/      (L1)  internal — ReactorSockets.Transports
  Reactor.cs       (L2)  public   — ReactorSockets
tests/ReactorSockets.Tests/
```

The primary type stays `Reactor`. `ReactorSockets.Reactor` presents no resolution problem, because
the namespace segment and the type name differ, and the type is the one place Schmidt's term is
literally correct.

> **Superseded by ADR 0007.** There is no type named `Reactor`. The dispatcher is split into
> `SessionReactor` and `RawReactor`, so the resolution argument above applies to nothing. The package
> name is unaffected — nothing about `ReactorSockets` depended on the type — and the rest of this ADR
> stands.

**`ReactorSockets.Logging` ships separately**, taking the `Microsoft.Extensions.Logging.Abstractions`
dependency so the core keeps the zero-dependency claim ADR 0005 rests on. It is named `.Logging` and
not `.Extensions.Logging`: the Microsoft-shaped form asserts a provenance we do not have. It versions
in lockstep with the core's major.

**Nothing is published before 1.0.** No placeholder, no reserved shell. A package that resolves to an
empty or half-built assembly appears in search results, gets installed once by mistake, and can be
unlisted but never deleted — a permanent artefact bought to defend a name specific enough that nobody
is racing us for it. 1.0 waits for the wire format to freeze, because that is the point at which
ADR 0003's promise that every public type is permanent starts costing something real.

## Consequences

- **The ID prefix is unreserved until 1.0.** NuGet grants prefix reservation only against a published
  package, so until then anyone may publish `ReactorSockets.Anything`. Accepted: a squatted sibling is
  an embarrassment, not a blocker, and the alternative buys protection with a permanent hollow
  package.
- **The name itself is unprotected until 1.0.** The GitHub org and repository are claimed now, which
  is the only free part of the claim.
- **`Reactor` now names three things at three scopes** — the pattern, the type, and half the package
  name. This is coherent but erodes on contact with prose: the first time someone writes "the
  Reactor" meaning the package, the glossary entry stops being true. CONTEXT.md pins the term to the
  instance and requires the product to be written `ReactorSockets` in full, always.
- Every path, namespace, and `InternalsVisibleTo` reference is renamed before the first line of L0 is
  written, which is the cheapest this will ever be.
- `ReactorSockets.Logging` tracks `Microsoft.Extensions.Logging.Abstractions` major versions for the
  life of the project — forty lines of adapter with an unbounded maintenance tail. This is the cost
  ADR 0005 accepted in exchange for the core's dependency count.
- Nothing published before 1.0 means no external pressure and no users to break while the open gaps
  close, and equally no early feedback from anyone outside this repository.
