# A Share-holder identity names the Reactor that issued it

`LinkId` and `PeerId` were `(int Index, ulong Generation)`, and ADR 0015 called that shape a
permanent public commitment. It was one field short. Index and generation are unique only *within*
one `ShareHolderTable<T>`, and every table starts life identically — the first Share-holder any
Reactor admits sits in Slot 0 on generation 1. Two Reactors in one process therefore issued equal
identities for unrelated Share-holders, and a consumer that handed one Reactor's `PeerId` to
another's `Close` closed a live Peer of the second. Nothing threw, nothing was logged, and the Peer
that vanished belonged to a Session the consumer never named.

Both identities, and L0's `SlotHandle` beneath them, now carry a third component: the Origin of the
Reactor that issued the handle. A Reactor draws one from `Core.ReactorOrigin` at construction and
hands it to its `ShareHolderTable<T>`, which stamps it into every handle it issues, and `TryResolve`
compares it first. A foreign handle resolves to nothing, which is what every other unresolvable
identity here already does.

## The Origin belongs to the Reactor, and one counter serves the process

**This ADR first said the table owns its Origin, and the code drew it from a static field on
`ShareHolderTable<T>`. Both were wrong, and the review in issue #76 found it.** A static field of a
generic type exists once per *closed* instantiation, and each profile's occupant type is its own:
`RawReactor.LinkSlot`, `DatagramSessionReactor.PeerSlot`, `StreamSessionReactor.PeerSlot`. That was
three independent counters, each starting at 1. Both Session profiles share the `PeerId` type
(ADR 0010), so a datagram Session's first Peer and a stream Session's first Peer were both
`PeerId(1, 0, 1)` — the exact defect this ADR was written to fix, surviving it one profile over. The
two tests that pinned the fix used two Reactors of the same profile and so never saw it.

The counter therefore lives on a non-generic `internal static class ReactorOrigin`, and the Reactor
rather than the table draws from it. A Reactor holds exactly one table today, so the two framings
have the same behaviour — but the Origin's meaning is "which Reactor issued this identity", and only
a Reactor can say that. Sited on the table, the property was one refactor away from being wrong
again, and the original bug was precisely a sequence that was a property of instantiation rather than
of anything a reader would think to check.

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

The counter starting at 1 means **0 is reserved and issued to no Reactor**, so a `default` identity is
unresolvable twice over — by its Origin as well as by its generation. That reservation is a stated
property rather than an implication, because it is what a test means when it builds an id that must
address nothing: two tests used `Origin: 1` for that, which is the first Reactor constructed in the
process, and they now use 0. The generation argument still stands alone and ADR 0015's reasoning for
the `ulong` width is untouched.

## `uint`, and why its bound needs a different argument from the generation's

The Origin is a `uint`. The two unreachable bounds in this identity are unreachable for different
reasons, and reading the generation's argument across to the Origin gets the wrong answer.

The generation climbs per Slot *reuse*, which connection churn drives on the hot path, and an `int`
there wraps in days — ADR 0015 widened it to `ulong` so the zero sentinel is safe by unreachability
rather than by a check. The Origin climbs once per Reactor *construction*, and a Reactor binds a
socket and allocates its arenas up front. Exhausting 32 bits means constructing 4,294,967,296 of
them in one process, which fails on those resources long before the counter is interesting. The bound
is therefore already unreachable at 32 bits, and `uint` is not a width chosen under pressure.

**The wrap is not guarded**, for the same reason ADR 0015 declined a skip-zero branch on the
generation: an invariant that cannot be reached is better made trivially true than defended by a
check no test can execute. A throw would also be the least useful response available, failing the
constructor of a Reactor whose only fault is being the four-billionth.

`ulong` was the shape this ADR originally shipped, and narrowing it costs nothing:
`(uint, int, ulong)` is 16 bytes — `Unsafe.SizeOf<PeerId>()` confirms it — because a 4-byte field
lands in the padding the `int`/`ulong` pair already carried for the `ulong`'s alignment.

## The mistake is unreportable, not merely unreported

No Diagnostic is raised when an id's Origin does not match. The original reasoning was consistency: a
foreign `PeerId` addresses nothing, so `Close` does nothing, and a consumer cannot provoke this from
the network, so the roster — which is for what the network does (ADR 0013) — has no place for it.

That is true but not the deciding argument. **A foreign id and a stale id are the same value.** A
consumer holding a `PeerId` past its Peer's close is correct, routine usage, and once the Origin
differs there is nothing to distinguish "another Reactor's live Peer" from "this Reactor's closed
one". Any signal on Origin mismatch therefore fires on correct code, and a throw would turn correct
code into an exception. There is also no cheap level to put it at: `DiagnosticLevel` has four members
and ADR 0013 rejected a fifth, and `Trace` means "protocol machinery working correctly", which a
consumer bug is not.

So the mistake stays silent because it cannot be reported, not because reporting it was judged
unnecessary.

## Consequences

- **ADR 0015's "permanent public commitment" bullet is amended, before 1.0 and not after.** The
  shape is `(uint Origin, int Index, ulong Generation)`, 16 bytes — the same size as the
  `(int Index, ulong Generation)` it replaces, so the Origin costs no memory at all.
- **Both profiles are fixed by one change**, because all three Reactors convert from the same
  `SlotHandle` at their own public edge (ADR 0003). Raw had the identical defect and was never
  reported.
- **`ReactorOrigin` is the only process-wide mutable state in the library, and a monotonic counter is
  the only thing that may ever live there.** Naming the precedent is the point: a Reactor owns no
  threads and shares nothing else across instances (ADR 0002), and the sole reason this exists is
  that an Origin means nothing unless every Reactor draws from one sequence. An injected origin
  source was declined — it would put a seam in the construction surface (ADRs 0011, 0012) to solve a
  problem no test has, and it would make duplicate Origins configurable, which is worse than a
  static.
- **Positional construction of `LinkId` and `PeerId` no longer compiles**, which is the point. Code
  passing two values gets a compile error rather than a silently reinterpreted identity.
- **Both remain publicly constructible**, which is a 1.0 commitment and not an accident of
  `record struct`. A consumer can fabricate `new PeerId(7, 0, 1)`, and it is harmless: a fabricated
  id is indistinguishable from a stale one and resolves to nothing, and it can only address the
  consumer's own Reactors, which it can already address anyway.
- **Four tests pin it through the public seam** — within one profile, two Reactors' first Peers
  compare unequal and a `PeerId` from one passed to another's `Close` leaves that Reactor's Peer
  established; `PeerIdOriginTests` asserts both across a datagram Reactor and a stream Reactor, which
  is the pair the per-profile tests cannot see. The cross-profile `Close` test fails against the
  pre-#76 code. Its distinctness sibling only fails there when it runs first, because the counters it
  catches are process-wide — an artefact of the bug, not a weakness in the invariant asserted.
