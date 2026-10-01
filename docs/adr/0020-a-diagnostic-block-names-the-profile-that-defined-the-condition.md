# A diagnostic block names the profile that defined the condition

ADR 0013 split `DiagnosticId` into reserved blocks: common `1–99`, Raw `100–199`, datagram session
`200–299`, stream session `300–399`. Slices 3, 4 and 8 then made the datagram session profile emit
`LingerNegotiated` (301), `ResumeMatched` (304) and `ResumeUnmatched` (305), which sit in the stream
block. Nothing decided that. Issue #59 asked for the decision to be made deliberately before 1.0, along
with an audit of the whole roster against ADR 0013's rules.

## Decision

**The datagram session profile shares the stream members. It gets no members of its own for these
three conditions.** A block names the profile that first defined a condition, not the profile that
emitted the record.

ADR 0013 makes a member name one condition. A Session establishing with a negotiated Linger is one
condition on both profiles, and so are a Resume matched and a Resume refused. Duplicating them would
give two members for one condition, so a Sink filtering on "Linger negotiated" would have to list both
and a third session profile would add a third. Every one of those members is permanent.

Sharing costs the meaning "the block tells you which profile emitted this". That meaning was never
needed:

- A Raw-only Sink's range check over `100–199` stays exact, because no other profile emits a Raw member.
- A Sink that needs the emitting profile knows which Reactor it was given to.
- Moving the three members into the common block was rejected. Common means all three profiles, and
  Raw has no Linger and no Resume. It would also renumber shipped values.

**The supported Sink style is range-checking the blocks**, or a `switch` with a default arm. A `switch`
that assumes it lists every member is not supported, because the enum grows inside its blocks. The
enum's XML docs say this.

## Audit result

The issue's table listed the sixteen members ADR 0013 first drafted. The code ships twenty-two. ADR
0013's own table already follows the code, and each of the six additions was argued against the
admission rule in the slice that added it:

| Member | What a count cannot carry |
| --- | --- |
| `CapacityExhausted` (3) | The reason. It is fixed by a different knob from `AdmissionCapReached`. |
| `OversizedFragmentSet` (205) | The Peer, and the declared count against the legal maximum. Closes the Session, so once per Peer. |
| `UnreliableFragment` (206) | The Source, and the mode it claimed. Names a sender misconfiguration, not rubbish. |
| `MalformedFraming` (302) | The Peer, and the prefix against `MaxMessageSize`. Closes the Session. |
| `DataAfterGoodbye` (303) | The Peer. Closes the Session. |
| `DeliveryCounterRefused` (308) | The Peer, and the claimed count against the appended count. Closes the Session. |

Every member was re-read against "would a count have told me the same thing". None is per-packet volume
driven by loss or reordering, which is the class ADR 0013 moved to counters. Each carries a subject
identity, a reason, or a figure specific to the occurrence. The closest call is
`RetransmissionTimerBackedOff`: it recurs on a lossy path, but at most once per Peer per Pump, and each
backoff doubles the RTO, so occurrences thin out. It carries the new RTO and the streak length, which no
counter holds, and it sits at `Trace`.

Two docs were wrong and are fixed: `ArenaExhausted` named only a Link subject, and `ResumeMatched` said
`Value0` reads 0 until issue #127, which has since shipped the rewind on the stream profile.

## How it is checked

`DiagnosticIdTests` pins every member's value and level in one table and fails if a member is added
without a row. It asserts every value sits in a block, none is zero, no member is called `None` or
`Unknown`, no `DiagnosticRecord` factory builds an `Error` record, and every member's XML summary names
`Value0` and `Value1`.

Reachability is a table naming, for each member, a test that provokes it through the public surface and
asserts the record arrived. A shared member names one producer per profile. `nameof` proves each named
test exists, and a test fails if any member has no row.

These tests observe the public type surface and its shipped documentation, not a running Reactor, in
the same way `AssemblySkeletonTests` does. They reach nothing internal, so ADR 0015's seam is unchanged.

## Cost

- The block no longer says which profile emitted a record. A datagram-only Sink range-checking
  `200–299` misses the three shared members.
- Reachability is review-backed. The table proves a named test exists and is runnable. It does not
  prove the test still asserts that member. A test edited to stop asserting it leaves the row passing.
  Running each producer and observing a shared Sink was rejected: xUnit 2 has no assembly teardown to
  check a set collected across test classes.
