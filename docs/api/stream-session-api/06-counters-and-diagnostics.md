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
Interrupted Peer, right up to the close.

### Reactor counters on this profile

| Member | Meaning |
|---|---|
| `ArenaHighWaterBytes` | The most arena bytes any single Frame used. Approaching `ArenaSize` means Messages are being held to later Pumps. |
| `AdmissionDeferrals` | Times a listening endpoint turned its backlog away because the admission cap was reached. Coarse — one declining per endpoint, not per connection. |
| `SinkFaulted` | Times your Sink threw. Non-zero means diagnostics are off for the rest of this Reactor's life. |

`DatagramsSent`, `DatagramsReceived`, `AdmissionDiscards`, `UnknownSourceDatagrams` and
`MalformedSessionDatagrams` stay at zero here. They are true zeros, not gaps — this profile carries no datagram transport, and it answers a
refused connection in protocol rather than discarding it.

A capacity or per-address refusal touches neither admission counter. Read `PeerRefused` (300) and
`AddressPeerLimitReached` (208) for those. They name the remote, which a counter cannot.

### Peer counters

| Member | Meaning |
|---|---|
| `MessagesReceived` | Whole Messages handed to the arena for delivery. Counted at delivery only, so bytes arriving without this moving means a Message is buffered, not lost. |
| `RetentionHighWaterBytes` | The most bytes this Peer's retention ever held at once. Compare against `RetentionBytes` to size it. |
| `Resumes` | Successful resumes for this Peer. Never reset by a resume, so a flapping path shows itself here. |
| `SocketWrites` | Socket send calls made on this Peer's connection, handshake records included. Well above your Pump rate means writes are not batching. |
| `BytesSent` | Bytes the socket accepted on this Peer's connection, framing and handshake records included. |
| `BytesReceived` | Bytes read from this Peer's connection, framing and handshake records included. Once nothing is in flight, it equals the far end's `BytesSent`. |
| `ReadShareExhaustions` | Times this Peer had a whole Message ready but had used its read share for the Frame. The Message waits for a later Pump. A steady rise means one Peer is outpacing its share of `ArenaSize`. |

`OutOfWindowPackets`, `DuplicatePackets`, `UnreliableLossPackets`, `HorizonDeferrals` and
`PacketsRetransmitted` stay at zero here. This profile has no packets.

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

## The stream profile's diagnostics

| Id | Level | Subject | What it tells you |
|---|---|---|---|
| `AdmissionCapReached` (1) | Warn | — | A Pump hit its admission cap with work still waiting. `Value0` the cap, `Value1` the declinings. |
| `ArenaExhausted` (2) | Warn | Peer | The arena had no room for a Message, which is held for a later Pump. `Value0` bytes requested, `Value1` bytes free. |
| `AddressPeerLimitReached` (208) | Warn | Source | A Hello was refused because its address holds `MaxPeersPerAddress` Peers while Slots are free. `Value0` the limit. |
| `PeerRefused` (300) | Info | Source | A dialler was refused: version (`Value0` offered, `Value1` required) or capacity (`Value0` = capacity). |
| `LingerNegotiated` (301) | Info | Peer | A handshake completed. `Value0` your Linger in ms, `Value1` the effective one. |
| `MalformedFraming` (302) | Warn | Peer | A framing violation closed a Session. `Value0` the offending prefix, `Value1` your `MaxMessageSize`. |
| `DataAfterGoodbye` (303) | Warn | Peer | A peer kept sending after its own Goodbye. |
| `ResumeMatched` (304) | Info | Peer | A Session resumed. `Value0` retained bytes rewound. |
| `ResumeUnmatched` (305) | Warn | Source | A resume claim was refused. `Value0` reservations checked, `Value1` the record type (3 Resume, 1 Hello). |
| `RedialAttempted` (306) | Info | Peer | A re-dial began. `Value0` attempt number, `Value1` ms actually waited. |
| `RedialFailed` (307) | Info | Peer | A re-dial could not connect. `Value0` the `SocketError`, `Value1` the attempt number. |
| `DeliveryCounterRefused` (308) | Warn | Peer | A peer claimed more Messages than you ever sent it. `Value0` claimed, `Value1` actually appended. |

Neither re-dial record closes a Peer. Only Linger does.

## What to watch in production

- `RetentionHighWaterBytes` near `RetentionBytes` — you are about to start seeing
  `RetentionExhausted`.
- `ArenaHighWaterBytes` near `ArenaSize`, or repeated `ArenaExhausted` — delivery is being delayed.
- Rising `Resumes` on one Peer — a flapping path, not a healthy connection.
- `PeerRefused` with a capacity value — you are turning peers away; check for Slots held by
  interrupted Peers.
- `AddressPeerLimitReached` — one address is asking for more than its share. Raise the limit if it is a
  NAT or proxy, or block it.
- Any `SinkFaulted` at all — you are blind from that point on.
