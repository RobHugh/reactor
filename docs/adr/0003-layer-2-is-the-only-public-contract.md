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

## Consequences

- **No extension seam.** A consumer wanting a transport we did not ship cannot add one; they must
  open an issue or fork. This is accepted deliberately in exchange for the freedom to change the
  lower layers.
- The Raw profile absorbs the main reason someone would want to reach past L2 — a consumer wanting
  unadorned sockets uses Raw, not L1.
- The layer boundary is enforced by convention and review, not by the compiler.
- Every public L2 type is a permanent commitment, so the L2 surface must stay deliberately small.
