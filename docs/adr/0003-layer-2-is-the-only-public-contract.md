# Layer 2 is the only public contract

The dispatcher (L0) and the TCP/UDP transports (L1) are `internal`. Consumers name no type from
either, construct the Reactor through L2, and reach every capability through L2. The contractual
agreement is with L2 alone, so L0 and L1 can be refactored or replaced wholesale without affecting a
consumer or requiring a major version.

Because `internal` stops at the assembly boundary, the layers are folders and namespaces within a
single assembly rather than separate projects — a three-assembly layout would need
`InternalsVisibleTo` between production assemblies just to function, which is a weaker boundary than
one assembly gives for free, and would publish the layering in metadata we want freedom to change.
Tests reach in via a single `InternalsVisibleTo`.

> **Superseded by ADR 0015.** There is no `InternalsVisibleTo`, and there has never been one. Issue #1
> decided during slice 1 that the single seam is the public L2 surface — L0 and L1 stay internal and
> untested-through — and commit `1aeffe6` applied it, deleting five direct L0 unit-test files. The
> sentence above was left standing for two slices, so anyone reading the ADR log to learn the rule
> got the opposite of the enforced one. It is kept here rather than removed because what this project
> once intended, and why it changed its mind under contact with the code, is the record's job. ADR
> 0015 states the policy, what would reopen it, and the test that now enforces it.

## Consequences

- **No extension seam.** A consumer wanting a transport we did not ship cannot add one; they must
  open an issue or fork. This is accepted deliberately in exchange for the freedom to change the
  lower layers.
- The Raw profile absorbs the main reason someone would want to reach past L2 — a consumer wanting
  unadorned sockets uses Raw, not L1.
- The layer boundary is enforced by convention and review, not by the compiler.
- Every public L2 type is a permanent commitment, so the L2 surface must stay deliberately small.
