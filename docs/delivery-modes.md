# Choosing a delivery mode

A datagram Session offers four delivery modes. This page will not guess what you are building; it asks
four questions about your data and each answer removes options.

## The four questions

**1. Must every Message arrive?**

If losing one is unacceptable, you need a reliable mode — `ReliableOrdered` or `ReliableUnordered`.
If a lost Message can simply be missing, continue to question 3.

**2. Must Messages be acted on in the order you sent them?**

Only if the meaning of one depends on an earlier one having been applied first.

- Yes → `ReliableOrdered`
- No → `ReliableUnordered`

Order costs latency, and it costs it to Messages that are unrelated to the delay. A Message held up in
transit blocks every later Message in the same Space until it arrives, however long that takes and
whatever those later Messages contain. If you do not need order, taking it anyway makes your worst-case
latency the worst case of your slowest Message.

**3. Once a newer Message exists, is the older one worthless?**

- Yes → `UnreliableSequenced`
- No → `Unreliable`

`UnreliableSequenced` delivers a Message only if it is strictly newer than the last one it delivered.
The guarantee has no exceptions: you never receive an older Message after a newer one. The cost is that
a Message which merely arrives late is discarded, even though it arrived — which is correct if it was
superseded and wrong if you needed it.

`Unreliable` delivers whatever arrives, in whatever order. It suppresses duplicates within a bounded
window of 64 Messages; a duplicate delayed longer than that is delivered twice.

**4. Is a single Message larger than one Datagram?**

Only the reliable modes fragment. `Unreliable` and `UnreliableSequenced` are capped at a single
Datagram, and `Send` returns `MessageTooLarge` above it.

This is not an oversight. Fragmenting what cannot be retransmitted multiplies the failure rate by the
number of pieces: a ten-Fragment Message on a path losing 1% of Datagrams arrives about 90% of the time,
having spent ten Datagrams to do it. You may only fragment what you can repair.

## What each mode promises

| | Arrives | Ordered | Duplicates | Late arrivals | Fragments |
|---|---|---|---|---|---|
| `ReliableOrdered` | always | in send order | suppressed | delivered | yes |
| `ReliableUnordered` | always | no | suppressed | delivered | yes |
| `UnreliableSequenced` | maybe | newest only | suppressed | discarded if superseded | no |
| `Unreliable` | maybe | no | suppressed within 64 | delivered | no |

Fragmentation amplifies the head-of-line blocking from question 2. A 57-Fragment `ReliableOrdered`
Message blocks every later Message in that ordering domain until all its Fragments arrive. This is
inherent to ordered delivery, not to fragmentation, but fragmentation makes the stall proportionally
longer: the wait is now the time to deliver the whole Message, not one Datagram. It is the strongest
practical argument for `ReliableUnordered`, which is the answer for bulk transfer.

## Modes do not interfere with each other

Each mode has its own Space, so a hole in one cannot stall another. A large `ReliableOrdered` transfer
and a stream of `UnreliableSequenced` Messages to the same Peer are independent: the transfer's
head-of-line blocking is confined to its own mode.

This is the reason the answer to question 2 matters. If you need both bulk reliable transfer and
low-latency reliable Messages, send them under different modes rather than accepting the ordering
guarantee for both.

## A note on the stream Session

A `StreamSessionReactor` has no delivery modes. TCP delivers every Message, in order, or the Session is
interrupted — which is `ReliableOrdered` and nothing else. If your data wanted one of the other three,
that is a reason to choose the datagram Session, and the clearest one there is.
