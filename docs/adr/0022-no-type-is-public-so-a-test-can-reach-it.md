# No type is public so a test can reach it

ADR 0003 makes every public type a permanent commitment and asks for a small surface. ADR 0015 makes
the public surface the only seam tests use, with no `InternalsVisibleTo`. Together they left one way
to unit-test an L2 helper: make it public. Eleven types took it. `PumpIntervalObserver`,
`RoundTripEstimator`, `RetransmissionTimer`, `FragmentStallSignature`, `FragmentPlan`,
`FragmentPlanResult`, `FragmentGroup`, `FragmentSetValidator`, `ClosedSessionMemory`,
`ClosedSessionAck` and `AckBlock` were public, listed on the surface guard in `AssemblySkeletonTests`,
and each doc comment said it was public "for the same reason" as another, "(ADR 0015)".

ADR 0015 never said that. The chain started at `RedialSchedule`, and commit `58f7547` (#111) had
already reversed `RedialSchedule`: it became internal, its unit tests were deleted, and its sequence
was proved through the Reactor that re-dials on it. The other eleven kept citing a precedent that no
longer existed. Issue #443 found the conflict between the two ADRs.

## Decision

**A type is public because a consumer uses it, never because a test does.** No consumer named any of
the eleven. Each was built and owned by a Reactor, and reached only through it. All eleven are now
internal, and their eight direct test files are deleted. ADR 0015's rule now covers L2 helpers as
well as L0 and L1: what a helper does is asserted through the four observation surfaces, or not at
all.

Making a type public so a test can reach it is the same move as `InternalsVisibleTo`, made with the
consumer's surface instead of an attribute. It is worse, because the attribute can be removed and a
public type cannot be removed before the next major version.

## How each behaviour is now proved

Every new or extended seam test below was shown to fail when the mechanism it covers was broken, and
to pass when it was restored.

- **Adaptive RTO** (`RoundTripEstimator`, `RetransmissionTimer`, `PumpIntervalObserver`). The
  `RetransmissionTimerBackedOff` record carries the RTO and the backoff count, and a hand-built client
  times every ack with the fake clock. `DatagramSessionAdaptiveRtoTests` now also pins the RFC 6298
  recurrence on a second sample, the Pump-interval floor, saturation over 64 losses, and that the wall
  clock is never read. Karn's rule, the doubling, the reset and the Linger ceiling were already pinned
  there.
- **Fragment-size stall** (`FragmentStallSignature`). `DatagramSessionFragmentSizeUnviableTests` now
  also pins the fourth round as the first that closes, the latch once any Fragment is acked, and an
  unfragmented ack that came before the Fragments being ignored.
- **Closed-Session re-ack** (`ClosedSessionMemory`, `ClosedSessionAck`, `AckBlock`).
  `DatagramSessionClosedSessionReAckTests` sends a freed Session crafted datagrams. A Goodbye off the
  stored base gets the stored blocks back. An ordinary ordered Message is not acked. A datagram with no
  ordered record, or with a truncated record, gets no reply. A full ring forgets the oldest Session.
- **Cutting and refusing** (`FragmentPlan`, `FragmentPlanResult`). The record count on the wire at
  zero length, at one byte past a Fragment and at an exact multiple, plus the existing refusal tests on
  `SendOutcome`.
- **Fragment-set legality** (`FragmentSetValidator`, `FragmentGroup`). `DatagramSessionHostileFragmentDiscardTests`
  already mirrored the rejecting cases. `DatagramSessionFragmentDeliveryTests` now also pins the two
  accepting cases: touching ranges in one Space, and the same first sequence in two Spaces.

## Coverage given up

These have no test, knowingly, on the terms ADR 0015 records for the coverage `1aeffe6` gave up.

- `RoundTripEstimator`'s saturating add and quadruple. Overflowing them needs a single round-trip
  sample of roughly ten thousand years.
- `FragmentGroup.AssertContiguousSet`'s two failure shapes. It is a debug-only check on sets the send
  path builds itself.
- `FragmentPlan`'s undefined-mode arm. `Send` converts the mode first and throws there.
- `FragmentSetValidator`'s body-length guard. One UDP datagram cannot carry a body that long.
- `ClosedSessionMemory`'s refusal of session id 0, its handling of the same id remembered again after
  an overwrite, and `ForgetAll`, which runs only on `Dispose`.
- The tests that pinned each type's public member list and null-argument checks. Those exist only for
  a public type.

## Cost

- **The tests are larger.** A sample, a backoff or a re-ack now needs a handshake and real datagrams.
  The seam tests take milliseconds each, but every arithmetic case costs a live Session to set up.
- **A failure points at less.** A wrong RTO in a backoff record could come from the estimator, the
  timer or the floor. The mutation runs above show each test isolates one of them, but the test
  name, not the type, says which.
- **Some arithmetic is pinned only at the values a scenario can produce.** The direct tests could
  pick any input. The seam tests pick values a Peer can be driven to.

## Consequences

- **ADR 0015 is amended, not superseded.** Its policy stands, and this ADR applies it to L2 helpers. A
  note at the end of ADR 0015 points here.
- **`tests/CLAUDE.md` forbids it by name.** The no-abstraction-for-testability rule now also lists
  making a type public.
- **The surface guard lists only consumer types.** `AssemblySkeletonTests` lost the eleven names, so
  making any of them public again fails a test.
- **The committed DocFX snapshot in `docs/api/docfx` still lists `PumpIntervalObserver`.** It was
  generated once, in `ac0343f`, and is stale on other counts too. Regenerating it is left to whoever
  next publishes the reference.
