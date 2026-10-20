# Counters and diagnostics

Two separate surfaces. Counters are pulled by you. Diagnostics are pushed to a Sink you install.

## Counters

Counters are monotonic from construction or registration, never reset, and fetched by value. Diff
two snapshots for a rate.

```csharp
ReactorCounters reactor = myReactor.Counters;
LinkCounters    link    = myReactor.CountersOf(linkId);
```

`CountersOf` throws `ArgumentException` if the `LinkId` names no open Link. A Link's counters end
with the Link.

### Reactor counters on this profile

| Member | Meaning |
|---|---|
| `DatagramsSent` | Datagrams `SendTo` handed to the socket, across every Link. |
| `DatagramsReceived` | Datagrams delivered as `DatagramReceived`. Truncated datagrams are not counted. |
| `ArenaHighWaterBytes` | The most arena bytes any single Frame used. Near `ArenaSize` means reads are being refused. |
| `AdmissionDeferrals` | Times a listening Link left connections in the backlog, because of the admission cap or because every Slot was taken. One per listening Link per Pump, not one per connection. |
| `SinkFaulted` | Times your Sink threw. Non-zero means diagnostics are off for the rest of this Reactor's life. |

`AdmissionDiscards`, `UnknownSourceDatagrams` and `MalformedSessionDatagrams` stay at zero here.
They are true zeros: Raw has no Sessions and never discards a connection it could defer.

### Link counters

| Member | Meaning |
|---|---|
| `BytesSent` | Bytes the socket accepted on this Link. A short `Send` counts only what was taken. |
| `BytesReceived` | Bytes delivered from this Link into the arena. Discarded reads are not counted. |

There is no Segment counter, because a Segment's size is an accident of timing, and no read-share
counter, because a Link is read once per Pump and never exhausts its share.

## Installing a Sink

```csharp
sealed class ConsoleSink : IReactorDiagnostics
{
    public bool IsEnabled(DiagnosticLevel level) => true;

    public void Log(in DiagnosticRecord record) =>
        Console.WriteLine(DiagnosticFormatter.Format(record));
}

var reactor = new RawReactor(new ReactorOptions
{
    Capacity = 16,
    Sink = new ConsoleSink(),
    MinimumLevel = DiagnosticLevel.Info,
});
```

Rules for a Sink:

- It is called synchronously on your thread, inside `Pump`. A Sink that stalls stalls the Frame.
- A Sink that throws is caught, counted as `SinkFaulted`, and **disabled for the life of the
  Reactor**. Never let one throw.
- Calling back into the Reactor from a Sink throws.
- `IsEnabled` gates record construction, so a disabled level costs one comparison.

To write to `Microsoft.Extensions.Logging`, use `LoggerSink` from the `ReactorSockets.Logging`
package.

A record carries no strings and no allocations: an id, a level, a subject and two numeric slots. On
this profile the subject is a `Link`, a `Source`, or none.

## The Raw profile's diagnostics

Every Raw diagnostic is `Warn`.

| Id | Subject | What it tells you |
|---|---|---|
| `AdmissionCapReached` (1) | — | A Pump hit `AdmissionCap` with connections still waiting. `Value0` the cap, `Value1` listening Links that deferred. |
| `ArenaExhausted` (2) | Link | The arena had no room for a read, which was not delivered. `Value0` bytes requested, `Value1` bytes free. |
| `CapacityExhausted` (3) | — | Every Slot was taken with connections still waiting. `Value0` the capacity, `Value1` listening Links that deferred. |
| `LinkFailed` (100) | Link | A dial, read or accept failed. `Value0` the `SocketError`. A failed accept leaves the listening Link open; anything else closes the Link. |
| `DestinationUnreachable` (101) | Link | An ICMP unreachable was reported for an earlier send. The Link stays open. `Value0` the `SocketError`. |
| `DatagramTruncated` (102) | Source | A datagram exceeded the scratch buffer and was discarded. `Value0` the buffer size. |

`AdmissionCapReached` and `CapacityExhausted` want different fixes. The first wants `AdmissionCap`
raised. The second wants `Capacity` raised or Links closed sooner.

## What to watch in production

- `ArenaExhausted`, or `ArenaHighWaterBytes` near `ArenaSize`. Reads are being lost. Raise
  `ArenaSize`.
- `DatagramTruncated`. Senders exceed `scratchBufferSize`.
- `CapacityExhausted`. Connections are waiting for Slots. Look for stream Links left open after
  `EndOfStream`.
- A steady `DestinationUnreachable`. You are sending to something that is not listening.
- Any `SinkFaulted` at all. You are blind from that point on.
