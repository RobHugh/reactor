# <a id="ReactorSockets_PeerCounters_HorizonDeferrals"></a> Property HorizonDeferrals

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_PeerCounters_HorizonDeferrals"></a> HorizonDeferrals

How many sequences arrived beyond a reliable Space's ack horizon and so had their retransmit timer
withheld rather than started (ADR 0013). It measures a deferral, not a loss: the sequence is not
missing and nothing was discarded, only judged too far ahead of <code>[base, base + 32]</code> for this
Pump's ack blocks to describe yet, so the timer that would otherwise start ticking toward a spurious
retransmission is withheld until the horizon slides over it. This is why it is a Counter and not a
Diagnostic (ADR 0013): sustained load with many sequences in flight defers on every Pump, which
fails the record test, and it keeps its own identity — rather than folding into
[`PeerCounters.OutOfWindowPackets`](ReactorSockets.PeerCounters.OutOfWindowPackets.md#ReactorSockets_PeerCounters_OutOfWindowPackets) — because counters have a per-Peer scope. Only the two reliable
Spaces contribute; the unreliable Spaces have no ack horizon to sit beyond. Rises only under the
horizon scenario and stays zero in ordinary operation, where every sequence lands inside the horizon
as it arrives. Monotonic and never reset, exactly like [`PeerCounters.OutOfWindowPackets`](ReactorSockets.PeerCounters.OutOfWindowPackets.md#ReactorSockets_PeerCounters_OutOfWindowPackets): a Resume
carries no counter's history away with it, and this one's lifetime is the Peer's, not the
transport's. A freshly connected Peer reads zero. Moves only on the datagram profile. Not derivable:
these sequences were admitted, so no refusal figure counts them, and only this one says the ack
horizon was outrun.

```csharp
public ulong HorizonDeferrals { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

