# The dialler seeds its round-trip estimate from the handshake

A datagram Peer's retransmission timeout is its round-trip estimate clamped into
`[observed Pump interval, Linger]`. An estimator with no sample reads `TimeSpan.MaxValue`, so the clamp
resolves to the Linger. Until the first reliable record was acked, the timeout therefore equalled the
Linger, and the first reliable record on a Session could be resent at most once per Linger.

That broke Goodbye (issue #57). A Closing sender whose Goodbye was lost before any round trip had been
sampled reached Linger expiry since Close in the same Pump its resend would have fired. Linger expiry runs
first, so the Session ended `LingerExpired` and the Goodbye was never resent.

## Decision

A dialled Peer samples the time from sending its Hello-with-cookie to receiving the Accept, and feeds it
to its `RoundTripEstimator` when it becomes Established. Every dialled Peer then has an estimate before it
sends anything.

**Why that leg.** Hello to Cookie is answered without per-sender state, so it does not exercise the
accepter's admission path. Hello-with-cookie to Accept covers the accepter's full handshake processing.

**Karn's rule applies.** A pending dial resends its Hello-with-cookie (ADR 0009, amendment of 2026-11-10),
and an Accept may answer any copy. A dial that sent its Hello-with-cookie more than once is not seeded, and
its first timeout is the Linger, as before this ADR. A dial whose Hello alone was resent is still seeded,
because the leg starts at the Hello-with-cookie.

**A zero measurement is not sampled.** A leg that spans zero clock ticks is below the clock's resolution.
It only happens on a frozen `FakeTimeProvider`. Sampling it would seed a zero timeout, which the Pump
interval floor cannot rescue when the clock has not moved, and every Pump would resend. Leaving the
estimator unseeded there keeps the earlier behaviour for tests that never advance the clock through the
handshake.

## Cost

- The seed is one sample. A handshake that happened to be unusually fast or slow sets the first timeout,
  until real acks correct it through the ordinary smoothing. A slow first leg makes the first timeout
  long, a fast one makes it short and costs at most a spurious resend.
- The handshake leg carries the accepter's admission work, which an ack of a later record does not.
  That bias is small next to a timeout equal to the Linger.
- Until a second Pump is observed the Pump interval is `MaxValue`, so the clamp still resolves to the
  Linger. A dialled Peer whose Reactor has been pumped only once has no shorter timeout.

## The accepter is not seeded

The accepter answers a Hello with a Cookie and keeps no state. The cookie carries an epoch of whole-second
granularity, which cannot time a leg that takes milliseconds. The Slot is created on Hello-with-cookie,
and the accepter's next datagram to the dialler is the Accept, whose acknowledgement is not distinguishable
from any other datagram from the dialler. So the accepter has no clean sample without a wire-format change,
and none was made.

Until the accepter's first reliable record is acked, its timeout is still the Linger. Its first reliable
record, a Goodbye included, resends only once per Linger.

Options left for a later decision:

1. Carry a fine timestamp in the cookie and echo it in the Hello-with-cookie, so the accepter can measure
   Cookie to Hello-with-cookie without keeping state. This changes the wire format of ADR 0009.
2. Leave it as it is, and let the accepter's first acked reliable record seed the estimate.
3. Have the accepter time Accept to the dialler's first datagram carrying an ack or a record. This needs
   a marker that says which datagram answers the Accept, which is also a wire change.
