# Counters and diagnostics

Two separate surfaces. Counters are pulled by you; diagnostics are pushed to a Sink you install.

## Counters

Counters are monotonic from construction, never reset, and fetched by value. Diffing two snapshots
gives you everything a reset would, from state you already own.

```csharp
ReactorCounters reactor = myReactor.Counters;
PeerCounters    peer    = myReactor.CountersOf(peerId);
```

`CountersOf` throws `ArgumentException` if the `PeerId` names no open Peer. It keeps answering for an
Interrupted or Closing Peer, right up to the close.

### Reactor counters on this profile

| Member | Meaning |
|---|---|
| `DatagramsSent` | Datagrams the transport accepted for sending, handshake control Datagrams included. |
| `DatagramsReceived` | Datagrams taken from the transport. The denominator for the two discard figures below. |
| `ArenaHighWaterBytes` | The most arena bytes any single Frame used. Approaching `ArenaSize` means Messages are being refused. |
| `AdmissionDiscards` | Hellos turned away, by the admission cap, by a full Reactor, or by `MaxPeersPerAddress`. Exact: a discarded Hello never comes back. |
| `UnknownSourceDatagrams` | Datagrams that named no Peer: malformed control Datagrams, unknown session ids, forged cookies, stray replies, truncated Datagrams. |
| `MalformedSessionDatagrams` | Data Datagrams that named a live Session and then failed to parse. Not charged to that Peer, because a body that did not parse proves nothing about who sent it. |
| `SinkFaulted` | Times your Sink threw. Non-zero means diagnostics are off for the rest of this Reactor's life. |

`AdmissionDeferrals` stays at zero here. UDP gives a declined Hello nowhere to wait, so this profile
discards where the others defer.

A rising `UnknownSourceDatagrams` on an otherwise healthy Reactor is a scan or a flood, not a fault.

### Peer counters

| Member | Meaning |
|---|---|
| `MessagesReceived` | Whole Messages handed to the arena for delivery. A Message the arena refused is not counted. |
| `RetentionHighWaterBytes` | The most bytes this Peer's retention ever held at once, both reliable Spaces summed. Compare against `2 × RetentionBytes`. |
| `Resumes` | Times this end raised `PeerResumed` for this Peer. Never reset, so a flapping path shows itself here. |
| `PacketsRetransmitted` | Packet records this end resent. Records, not Datagrams and not Messages. |
| `DuplicatePackets` | Records the receive windows refused as already seen. Usually the far end retransmitting because an ack was lost. |
| `OutOfWindowPackets` | Records too far from a Space's window base to be tracked, or past what `ReliableOrdered` will hold behind a gap. Sustained reordering or loss. |
| `UnreliableLossPackets` | Sequences the two unreliable Spaces slid past without ever receiving. Your unreliable loss rate. |
| `HorizonDeferrals` | Reliable sequences whose retransmit timer was withheld because they sat beyond the 32-sequence ack horizon. A deferral, not a loss. Zero in ordinary operation. |
| `SocketWrites` | Datagrams written for this Peer: one per data Datagram, plus each Resume. Handshake Datagrams are not counted. |
| `BytesSent` | Datagram bytes written for this Peer, headers included. |
| `BytesReceived` | Datagram bytes attributed to this Peer on arrival, headers included. |
| `ReadShareExhaustions` | Times this Peer had a Message ready but had used its read share for the Frame. An unreliable Message is lost. A reliable one is delivered on a later Pump (see [The loop](02-the-loop.md#delivery-is-bounded-per-frame)). |

## Installing a Sink

`Sink` and `MinimumLevel` are one decision. Supply both or neither.

```csharp
sealed class ConsoleSink : IReactorDiagnostics
{
    public bool IsEnabled(DiagnosticLevel level) => true;

    public void Log(in DiagnosticRecord record) =>
        Console.WriteLine($"{record.Level} {record.Id} {record.Value0} {record.Value1}");
}

var options = new ReactorOptions
{
    Capacity = 64,
    Sink = new ConsoleSink(),
    MinimumLevel = DiagnosticLevel.Info,
};
```

Rules for a Sink:

- It is called synchronously on your thread, inside `Pump`. A Sink that stalls stalls the Frame.
- A Sink that throws is caught, counted as `SinkFaulted`, and **disabled for the life of the
  Reactor**. Never let one throw.
- Calling back into the Reactor from a Sink is a programmer error and throws.
- `IsEnabled` gates record construction, so a disabled level costs one comparison.

To write diagnostics to `Microsoft.Extensions.Logging`, use `LoggerSink` from the
`ReactorSockets.Logging` package instead of writing your own. To render a record as text in your
own Sink, call `DiagnosticFormatter.Format(record)`.

A record carries no strings, no boxing and no closures: an id, a level, a tagged subject (Peer,
Source or none) and two numeric slots whose meaning is documented per id.

## The datagram profile's diagnostics

| Id | Level | Subject | What it tells you |
|---|---|---|---|
| `AdmissionCapReached` (1) | Warn | — | A Pump hit its admission cap and discarded handshake Datagrams. `Value0` the cap, `Value1` the discards. |
| `ArenaExhausted` (2) | Warn | Peer | The arena had no room for a Message. `Value0` bytes requested, `Value1` bytes free. |
| `CapacityExhausted` (3) | Warn | — | Every Slot was occupied and verified Hellos were refused. `Value0` the capacity, `Value1` the refusals. |
| `DatagramTruncated` (102) | Warn | Source | A Datagram exceeded the receive buffer. `Value0` the buffer size. |
| `DatagramMalformed` (200) | Warn | Source | A Datagram was discarded because it did not parse or named nobody. `Value0` bytes received. |
| `CookieRejected` (201) | Warn | Source | A Hello carried a cookie this Reactor did not mint, or minted too long ago. `Value0` the cookie's epoch, `Value1` the current one. |
| `VersionRefused` (202) | Warn | Source | A verified Hello offered the wrong version and was refused. `Value0` offered, `Value1` required. |
| `FragmentSetAbandoned` (203) | Warn | Peer | A Fragment set was dropped before it completed. `Value0` Fragments held, `Value1` Fragments declared. |
| `RetransmissionTimerBackedOff` (204) | Trace | Peer | Records expired and were resent. `Value0` the new timeout in ms, `Value1` consecutive backoffs. |
| `OversizedFragmentSet` (205) | Warn | Peer | A Fragment set claimed more Fragments than your window holds, closing the Session. `Value0` declared, `Value1` the largest legal count. |
| `UnreliableFragment` (206) | Warn | Source | A Fragment arrived in an unreliable mode. `Value0` the mode (0 `Unreliable`, 1 `UnreliableSequenced`). |
| `OversizedMessage` (207) | Warn | Peer | A completed Message was larger than your `MaxMessageSize` and was discarded. `Value0` its length, `Value1` your limit. |
| `AddressPeerLimitReached` (208) | Warn | Source | A verified handshake was refused because its address holds `MaxPeersPerAddress` accepted Peers. `Value0` the limit. |
| `DatagramBudgetReached` (209) | Warn | — | A Pump read `MaxDatagramsPerPump` datagrams from one bound socket and stopped, whether or not more were queued. At most one per bound socket per Pump. `Value0` the limit. |
| `LingerNegotiated` (301) | Info | Peer | A handshake completed. `Value0` your Linger in ms, `Value1` the effective one. |
| `ResumeMatched` (304) | Info | Peer | A Resume matched, or the dialling end heard its Session again. `Value0` always 0 here. |
| `ResumeUnmatched` (305) | Warn | Source | A Resume was refused. `Value0` 0 no such Peer, 1 wrong secret or closing. `Value1` 6, the Resume type. |

`LingerNegotiated`, `ResumeMatched` and `ResumeUnmatched` sit in the stream block (300-399) because
each names one condition shared by both profiles. A Sink that range-checks 200-299 for this profile
will miss them.

`FragmentSetAbandoned` fires on ordinary loss and never closes anything. A low held count against a
high declared count, repeated, is the early sign of a fragment size the path will not carry.

## What to watch in production

- `RetentionHighWaterBytes` near `2 × RetentionBytes` — you are about to start seeing
  `RetentionExhausted`.
- `ArenaHighWaterBytes` near `ArenaSize`, any `ArenaExhausted`, or rising `ReadShareExhaustions` —
  Messages are being refused at delivery. Raise `ArenaSize`.
- `PacketsRetransmitted` rising faster than your send rate — a lossy path.
- `UnreliableLossPackets` — the loss your unreliable traffic is actually seeing.
- Rising `Resumes` on one Peer — a flapping path, not a healthy connection.
- `CapacityExhausted` — you are turning peers away; check for Slots held by interrupted, closing or
  stuck dialling Peers.
- Any `SinkFaulted` at all — you are blind from that point on.
