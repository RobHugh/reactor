# The partial-set flood is measured against a Reactor holding nothing

ADR 0004 puts Fragment reassembly inside the receive window, so a hostile sender flooding partial
Fragment sets can only fill that Peer's own window and never a separate buffer. Issue #261 asked for
that property to be asserted by measurement: memory at construction equals memory under flood. The test
#261 delivered did not assert it (issue #279):

- It flooded "index 0 of 2" pieces at consecutive sequences, so the window's base advanced past every
  group and few sets were ever held at once.
- It compared two flood phases to each other with a `2x + 1` bound, never against construction. A real
  per-group allocation could pass that bound.

## Decision

`Pump_AFloodHoldingAWindowFullOfPartialSets_AllocatesNoMoreFromConstructionThanAReactorHoldingNothing`
replaces the consecutive-sequence test. The other three #261 tests are unchanged.

**The flood holds a full window.** Lone "index 0 of 2" pieces go to odd sequences 1 through 4093 in a
4096-slot `ReliableOrdered` window. Sequence 0 is never sent, so the base stays at 0, and all 2047 sets
are held at once. The flood then sends 2047 more pieces beyond the window, which are refused. Counter
assertions prove the state was reached: `OutOfWindowPackets` is exactly 2047, `DuplicatePackets` is 0, and
no Message is received.

**The control holds nothing.** A second Reactor receives the same records, the same count and size in the
same datagrams, at sequences that are all beyond its window. It refuses every one, so it stays in its
state at construction. Its `OutOfWindowPackets` is asserted to be 4094.

**Both are measured from construction.** Each run counts `GC.GetAllocatedBytesForCurrentThread` from
before the Reactor is built to the arrival of the last datagram, after one warm-up run of each.
Construction, the handshake and the per-datagram socket cost are identical in both, so the difference is
what holding the partial sets cost. Diagnostics are off, because the test sink records into a list.

**The bound is 8 KB.** The two runs measure exactly equal on every run so far. 8 KB sits above the 7.4 KB
of jitter seen in `DatagramSessionReceiveArenaAllocationTests`'s exact-equality tests. To show the
bound catches a leak, a 24-byte object allocated per held Fragment in `HoldFragment` was injected
temporarily. The test failed at +24,564 bytes.

## Alternatives rejected

- **Unfragmented records with the same holes as the control.** This was the first choice. Held
  unfragmented records go through `DeliverAdmittedRecord`, which allocates. The control cost about 53 KB
  more than the flood, so the injected leak still passed.
- **Two flood phases compared to each other.** This is what #261 shipped. It never measures construction,
  and its proportional bound absorbs a per-group cost.
- **Retained heap via `GC.GetTotalMemory`.** It counts the whole process, so it needs a non-parallel
  collection and a tolerance large enough to hide the runner's own allocations.
- **Asserting `ReceiveBudgetBytes` is unchanged.** The figure is computed from the options alone, so no
  flood can change it and the test could never fail.

## Cost

- The test asserts allocation, not retention. A per-set cost that reused memory allocated before the
  flood, for example a pooled object, would not show up. Such a pool would itself sit outside
  `ReceiveBudgetBytes`, which the arena allocation tests bound exactly.
- Each run builds a 4096-slot arena of about 19 MB, four runs per test.
- The injected leak measured 24,564 bytes, about half of the 49,128 that 2047 objects of 24 bytes
  should add. The cause was not found. The margin over the bound is about 3x the bound rather than 6x.

## Risk

The flood Reactor admits 2047 records and the control admits none. Admitted records produce
acknowledgements, so only the flood Reactor sends ack traffic, and both Reactors run on the measuring
thread. Today the send path allocates nothing for it, which is why the runs measure equal. If ack
sending ever allocates, that cost lands on the flood side only, and the test fails without any change
to reassembly.

The risk can only produce a false failure, never a false pass. If this test fails near 8 KB, check the
ack send path before the reassembly path.
