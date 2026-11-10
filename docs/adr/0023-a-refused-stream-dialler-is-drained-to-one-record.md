# A refused stream dialler is drained to one record

A stream Reactor refusing a connection for capacity drains what the dialler sent before it closes,
because closing with unread inbound data forces a reset that can destroy the Refusal before the
dialler reads it. The drain had no read limit. A remote that kept sending held one Pump open for as
long as it kept the socket full, which breaks ADR 0012's rule that every per-Pump cost a remote party
can inflate is bounded at construction (issue #455).

## Decision

**The drain reads at most one handshake record past the first record.** The limit is
`StreamHandshakeCodec.RecordSize` bytes, counted across every drain of the same connection. It is a
constant, not a knob. The drain exists to absorb an unread Hello, and a conforming dialler sends
nothing behind its first record until it is answered, so no setting would change what a conforming
dialler needs.

**A dialler over the limit is told so.** Its Refusal carries a new reason, `ProtocolViolation`
(wire byte 3), with the limit in bytes as its first value, in place of the capacity Refusal. The
dialler maps it to `DisconnectReason.ProtocolViolation`. A dialler that crosses the limit only after
its capacity Refusal is already partly sent is closed at once, because that record cannot be
rewritten.

## Consequences

- **A sender over the limit may see a reset rather than its Refusal.** Bytes it sent past the limit
  can be unread at close. This is accepted because that sender has broken the handshake.
- **The reason is shared with the datagram profile's Refusal roster.** The datagram profile never
  sends it, but it now reads byte 3 as `ProtocolViolation` rather than discarding the Refusal as
  malformed.
- **A dialler built before this reason existed treats it as a malformed Refusal.** Its connection
  still ends, as `TransportFailed` rather than `ProtocolViolation`.
