# <a id="ReactorSockets_PeerCounters_PacketsRetransmitted"></a> Property PacketsRetransmitted

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_PeerCounters_PacketsRetransmitted"></a> PacketsRetransmitted

How many packet records this Peer has resent (ADR 0013). Counts packet RECORDS resent, not datagrams
sent and not Messages: retransmission is byte-identical at packet- record granularity, because a
datagram is repacked on every send and is not itself reproducible — several records can ride one
datagram, and the same record can ride a different datagram on retry. This is why it is a Counter
and not a Diagnostic (ADR 0013): a retransmit recurs continuously on a lossy path, which fails the
record test, and it keeps its own identity — rather than folding into
[`PeerCounters.OutOfWindowPackets`](ReactorSockets.PeerCounters.OutOfWindowPackets.md#ReactorSockets_PeerCounters_OutOfWindowPackets) or [`PeerCounters.HorizonDeferrals`](ReactorSockets.PeerCounters.HorizonDeferrals.md#ReactorSockets_PeerCounters_HorizonDeferrals) — because counters have a
per-Peer scope. Monotonic and never reset, exactly like [`PeerCounters.HorizonDeferrals`](ReactorSockets.PeerCounters.HorizonDeferrals.md#ReactorSockets_PeerCounters_HorizonDeferrals): its lifetime
is the Peer's, not the transport's, so a Resume does not zero it. Raised by the retransmit expiry
sweep; a freshly connected Peer reads zero. Moves only on the datagram profile. Not derivable: bytes
sent mixes first sends with resends, and only this figure separates them.

```csharp
public ulong PacketsRetransmitted { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

