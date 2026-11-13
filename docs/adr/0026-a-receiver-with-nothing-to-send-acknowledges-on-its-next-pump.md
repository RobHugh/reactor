# A receiver with nothing to send acknowledges on its next Pump

ADR 0017 decided that an ack rides a data Datagram and that a Peer with nothing queued sends none. It
deferred the one-way case to slice 8. Slice 8 added the Ping, but a Ping goes out only once per
`SendIdleInterval`. A one-way reliable flow was therefore acknowledged once per interval, and its
throughput was capped at about `RetentionBytes` per `SendIdleInterval` (issue #487). Measured at
`0bbcf62` with a 1 s interval, 64 000 retention bytes and one 100-byte `ReliableOrdered` Message
every 10 ms for 5 s: 235 of 500 Sends were refused, and 37 records were retransmitted with no loss.
This ADR supersedes ADR 0017's note deferring the case.

## Decision

**A Peer owes an ack once it classifies a reliable record.** That covers any `ReliableOrdered` or
`ReliableUnordered` record that reaches the receive window's classification, duplicates included,
because a repeat means the ack that covered it was lost. A record classified outside the window owes
one too. Its ack changes nothing, but telling it from a repeat would cost a second check, and the
cost is bounded at one Datagram per Pump. A record refused before classification (hold budget, arena
room, a deliverable Fragment's slot) owes nothing. Unreliable records owe nothing.

**Every data Datagram sent to the Peer pays the debt.** Every data Datagram carries both ack blocks,
so the first one that leaves for the Peer clears it, whatever it carries.

**A debt owed since an earlier Pump is paid by a header-only Datagram.** If a Pump's flush finds the
Peer with nothing queued and a debt from an earlier Pump, it sends the 20-byte header and both ack
blocks with no records. This shape already exists for the Goodbye final ack, so the wire format does
not change. The Pump that classified the record does not send the ack on its own, so a reply the
consumer queues after Drain carries it on the next Pump instead.

**At most one per Peer per Pump, and none when a data Datagram is going out.** The header-only
Datagram is sent only by a flush with nothing queued, so it is that flush's only Datagram. It carries
no records, so it owes nothing on the other end and two silent ends never answer each other.

**It counts as a sent Datagram.** It restarts the send-idle interval, like a Ping. An idle Session
then sends at most a Ping and an ack of the other end's Ping per end per interval. Before this
change, an unacknowledged Ping was retransmitted until the other end's own Ping carried its ack. In
the idle test, ten 1 s intervals cost 59 Datagrams before and at most 20 per end after.

## Rejected

- **Acknowledging in the same Pump.** It never waits a Pump, so it can never lose a race with the
  sender's RTO, whose floor is the observed Pump interval. But a reply is queued only after Drain, so
  every request in a request and reply exchange would cost an extra Datagram. The next-Pump rule
  costs one receiver Pump of ack latency instead, which the sender's RTT samples absorb.
- **A time-based delay, a fraction of the RTT with a floor and a ceiling.** The library owns no threads
  (ADR 0002), so any delay is rounded up to a Pump anyway. A timer would add an option or a constant
  with no evidence for its value.
- **An early Ping.** A Ping is a reliable record. It spends a sequence and retention, it is retransmitted
  itself, and it owes an ack on the other end.
- **A count threshold of N unacknowledged records.** Pump batching already bounds the rate at one
  Datagram per Peer per Pump.

## Consequences

- **The issue #487 probe delivers all 500 Messages**, with every Send returning `Sent` and
  `PacketsRetransmitted = 0` (`DatagramSessionDelayedAckTests`).
- **An ack waits up to one receiver Pump.** The sender's RTT samples include that wait. A sender whose
  RTO was seeded from a handshake that took no time can retransmit before the first sample arrives.
- **A silent receiver is no longer silent.** Tests that count the reverse direction, or rely on acks
  arriving only with a reply, see one header-only Datagram per Pump that classified a reliable record.
  Tests on a frozen fake clock that must not take RTT samples mid-transfer hold the reverse direction
  back with a `LossyProxy` drop rule.
- **`DatagramsSent`, `SocketWrites` and `BytesSent` count the header-only Datagrams** like any other.
