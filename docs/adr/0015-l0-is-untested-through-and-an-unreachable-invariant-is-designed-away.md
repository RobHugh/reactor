# L0 is untested-through, and an unreachable invariant is designed away

ADR 0003 made L0 and L1 internal so they could be replaced wholesale without a major version, and
closed by anticipating that tests would reach in via a single `InternalsVisibleTo`. Issue #1 decided
otherwise when the first slice was planned: the single seam is the public L2 surface, and L0 and L1
stay internal and untested-through. Commit `1aeffe6` applied that, deleting five direct L0 unit-test
files and re-expressing what they asserted through the public surface — 88 tests to 73, with the lost
coverage recorded rather than glossed. Nothing amended ADR 0003, so for two slices the ADR log stated
the opposite of the enforced policy. This ADR settles both halves: the policy stands, and the record
now says so.

**A good test here asserts what a consumer can observe and nothing else.** The four observation
surfaces are the Events drained from a Pump, the `Result` returned by a call, the records a Sink
received, and a Counter snapshot. No test reaches into L0 or L1, reads a private field, or asserts
that a particular internal method was called. There is no `InternalsVisibleTo`, no fake transport,
and no abstraction inserted for testability. The reason is the one issue #1 gave: a test that would
fail if the dispatcher were rewritten but the behaviour preserved is testing the wrong thing.

## What the review found, and why the policy survived it

The policy was reopened after the August 2026 architecture review lifted the generational slot table
out of `RawReactor` into `ShareHolderTable<T>` in `Core/`. Landing in `Core/` makes it internal, so
its invariants are verifiable only through live sockets, and three were nominated as awkward or
impossible to reach that way. Two of them turned out to be neither.

**Stale-handle rejection across reuse was already covered**, through both channels a bad `LinkId` is
reported on — `RemoteOf`'s throw and `Send`'s failed `Result` — plus a third test driving bind,
close and rebind into the same Slot. **Capacity refusal was already covered** by
`Listen_ConsumesACapacitySlot_SoBindFailsOnceCapacityIsSpent` and the `Pump_MorePendingThanFreeSlots`
family. Both were argued to be expensive — a real socket per share-holder — and both were expensive
exactly once, in tests that have shipped and pass. **A policy is not too costly when the bill is
already settled.**

**Generation wraparound was the real case, and the answer was to remove it.** `_generations[index]++`
on an `int` wraps after 2^31 removals on one Slot — roughly two and a half days at ten thousand
short-lived connections per second — and counts back up *through zero*. Zero is not an arbitrary
value there: generations begin at 1 precisely so that a `default` handle, which carries generation 0,
matches no live Slot, which is what stops `default(LinkId)` from addressing the first thing admitted
to Slot 0. A wrapped Slot therefore does not merely produce a surprising number; it silently turns
the sentinel into an alias. That is a design defect, and it was masquerading as a coverage gap.

**The generation is now `ulong`.** 2^64 removals on one Slot is unreachable by any process that will
ever run, so the counter cannot come back round to 0 and the sentinel is safe by unreachability. The
index stays `int`, because .NET arrays are `int`-indexed and widening it would buy one bounds check
at the cost of a cast on every resolve. A skip-zero branch on the increment was considered and
declined: it would run on every removal, and no test could ever reach it, so it would trade an
unreachable failure for a permanently dead line whose correctness is asserted only by comment.
Widening removes the failure *and* the branch.

**The generalisable move is the one this is an instance of.** When an L0 invariant cannot be reached
through the seam, the first response is to change the code until the invariant is trivially true —
not to change the policy until the invariant is observable. The category the review hypothesised, an
L0 that had grown pure invariants the original decision did not anticipate, had exactly one member,
and that member did not survive being looked at properly.

## What would reopen this

The policy is conditional, not absolute, and the condition is written here so that reopening it is a
matter of evidence rather than of appetite. **All three of the following must hold**, and the third
must have been attempted and failed rather than judged unpromising:

1. The invariant is **pure** — arithmetic or identity, with no I/O in it.
2. It is **unreachable through the public seam at any cost** — not expensive, not awkward,
   unreachable. Both surviving candidates above fail this test, and their tests are green.
3. The code **cannot be changed to make the invariant trivially true**. Generation wraparound failed
   this test, and it is the test that dissolved it.

**Independently of those, one observation reopens this on its own:** a bug ships in L0 that a direct
unit test would plainly have caught. That is the falsifiable trigger, and it is here deliberately — a
policy that no observation could ever count against is a preference, not a decision.

**Insufficient, individually or together:** convenience, test-writing speed, coverage percentage, and
"this module is pure, so it is the kind of thing one unit-tests." The last is the exact shape of the
argument that reopened this, and criterion 3 is the answer to it.

## Consequences

- **ADR 0003's closing sentence is superseded.** "Tests reach in via a single `InternalsVisibleTo`"
  is struck in place with a note pointing here, not deleted — the log records what was believed and
  when, and that this project intended to test through internals and changed its mind under contact
  with the code is the most useful thing on that page for whoever proposes reopening it next.
- **The policy is now enforced rather than remembered.**
  `ReactorSocketsAssembly_Attributes_GrantNoTestAssemblyAccessToInternals` fails if the attribute
  ever appears. `AssemblySkeletonTests`' namespace guard never covered this: `InternalsVisibleTo`
  exports no type, so every other test in that file stays green while the policy is breached. Two
  slices of ADR 0003 saying the wrong thing is what an unenforced rule looks like.
- **`tests/CLAUDE.md` carries the rule, and this ADR carries the reasoning.** `CLAUDE.md` sends
  anyone working in `tests/` to that file and not to `docs/adr/`, so a rule recorded only here is a
  rule the next author never reads. It states the seam and links here rather than restating the
  argument, so the two cannot drift the way this ADR and issue #1 did.
- **`LinkId` is `(int Index, ulong Generation)`** — *amended by ADR 0016, which adds an Origin
  naming the issuing Reactor; two fields were unique only within one table* — and that is a
  permanent public commitment on
  ADR 0003's terms. It costs eight bytes on a value that rides in a `ref struct` and is never
  allocated, and it costs nothing on the wire: ADR 0009 already rejected slot-index-plus-generation
  as a wire identity for being guessable, so this identity is in-process only.
- **A Session's `PeerId` inherits the widening.** ADR 0007 records that `PeerId` is
  index-plus-generation "like `LinkId`", and it will be built on the same `ShareHolderTable<T>`, so
  its generation is `ulong` for the reason given here rather than by imitation.
- **The sentinel argument's basis has changed and the doc comments say so.** `SlotHandle`,
  `LinkId` and `ShareHolderTable<T>` previously asserted that nothing has to check for generation 0.
  That was true by accident; it is now true because the counter cannot wrap, and each states which.
- **Coverage lost in `1aeffe6` stays lost, knowingly** — the `ReadShare` arithmetic beyond the one
  configuration the escape test pins, `ShareExhausted`, the `FrameArena` guards `ReactorOptions`
  makes unreachable, and `ObservedPumpInterval`. Nothing above changes that ledger; this ADR only
  refuses to reopen the seam to recover it.

> **Amended by ADR 0022.** The rule above covers L2 helpers too. A type is never made public so a test
> can reach it, and the eleven types that were are internal again.
