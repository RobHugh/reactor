# A Share-holder identity names the Reactor that issued it

`LinkId` and `PeerId` were `(int Index, ulong Generation)`, and ADR 0015 called that shape a
permanent public commitment. It was one field short. Index and generation are unique only *within*
one `ShareHolderTable<T>`, and every table starts life identically — the first Share-holder any
Reactor admits sits in Slot 0 on generation 1. Two Reactors in one process therefore issued equal
identities for unrelated Share-holders, and a consumer that handed one Reactor's `PeerId` to
another's `Close` closed a live Peer of the second. Nothing threw, nothing was logged, and the Peer
that vanished belonged to a Session the consumer never named.

Both identities, and L0's `SlotHandle` beneath them, now carry a third component: the Origin of the
table that issued the handle. `ShareHolderTable<T>` takes its Origin from a process-wide counter at
construction, stamps it into every handle it issues, and `TryResolve` compares it first. A foreign
handle resolves to nothing, which is what every other unresolvable identity here already does.

## Why this is a change to the identity rather than a check at the edge

A Reactor could have rejected foreign ids at its public boundary instead — but it has nothing to
reject them *with*. `Close(PeerId)` is handed two integers that are indistinguishable from ones it
issued itself; the information that would let it tell is precisely the information the type does not
carry. Any edge check reduces to putting the Origin somewhere, and putting it in the identity is the
version where L0's existing resolve does the work and no new guard exists to be forgotten at the next
public member that takes a `PeerId`. This is ADR 0015's own move — make the invariant trivially true
rather than guard the case — applied to the thing that ADR got wrong.

## What the Origin is, and what it is not

A counter, not entropy. This defends against a consumer mixing up two of its own Reactors, which is a
mistake and not an attack, and a counter answers it exactly where random values answer it only with
high probability. It is `Interlocked.Increment`ed because two Reactors need not be constructed on the
same thread, even though each is owned by one thereafter (ADR 0002).

It is opaque, process-local, compared only for equality, and orders nothing. It never appears on the
wire and cannot: ADR 0009 rejected slot-index-plus-generation as a wire identity for being guessable,
and this widening does not make it less so — a datagram Session's wire identity remains the
receiver-assigned random 32-bit token, and no part of a `PeerId` is ever sent. It is not a `Source`,
which names a remote address.

The counter starting at 1 means no table holds Origin 0, so a `default` identity is now unresolvable
twice over — by its Origin as well as by its generation. The generation argument still stands alone
and ADR 0015's reasoning for the `ulong` width is untouched.

## Consequences

- **ADR 0015's "permanent public commitment" bullet is amended, before 1.0 and not after.** The
  shape is `(ulong Origin, int Index, ulong Generation)`. Eight more bytes on a value that is never
  allocated and never sent.
- **Both profiles are fixed by one change**, because both convert from the same `SlotHandle` at their
  own public edge (ADR 0003). Raw had the identical defect and was never reported.
- **The mistake is now silent in the way the library already is about unknown ids** — a foreign
  `PeerId` addresses nothing, so `Close` does nothing. It is not raised as a Diagnostic: a consumer
  cannot provoke this from the network, and the roster is for what the network does (ADR 0013).
- **Positional construction of `LinkId` and `PeerId` no longer compiles**, which is the point. Code
  passing two values gets a compile error rather than a silently reinterpreted identity.
- **Two tests pin it through the public seam** — two Reactors' first Peers compare unequal, and a
  `PeerId` from one Reactor passed to another's `Close` leaves that Reactor's Peer established.
