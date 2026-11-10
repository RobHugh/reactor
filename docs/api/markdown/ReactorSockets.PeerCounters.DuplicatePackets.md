# <a id="ReactorSockets_PeerCounters_DuplicatePackets"></a> Property DuplicatePackets

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_PeerCounters_DuplicatePackets"></a> DuplicatePackets

How many packet records this Peer's per-Space receive window has classified as duplicates — a
sequence number already accounted for, most often a retransmission the far end sent because it
never saw this end's acknowledgement rather than a protocol violation (ADR 0013). Recurs every Pump
under sustained loss exactly like [`PeerCounters.OutOfWindowPackets`](ReactorSockets.PeerCounters.OutOfWindowPackets.md#ReactorSockets_PeerCounters_OutOfWindowPackets), and for the same reason is a
Counter. Incremented by the parse loop's classification of each arriving record; a freshly connected
Peer reads zero. The plain <code>Unreliable</code> Space contributes from a second place as well (ADR
0017): that Space suppresses repeats within its own bounded 64-sequence window, a protocol constant
rather than the receive window, and a repeat that window catches after the receive window
<em>admitted</em> it raises this figure. A repeat the receive window itself refused is counted there
instead — most often on [`PeerCounters.OutOfWindowPackets`](ReactorSockets.PeerCounters.OutOfWindowPackets.md#ReactorSockets_PeerCounters_OutOfWindowPackets), since a repeat usually arrives below a
base that has already passed it — so one record raises one figure and never both. A repeat delayed
past those 64 sequences is delivered and raises neither: suppression there is bounded by design, and
these figures count what was refused rather than what was repeated. Moves only on the datagram
profile. Not derivable: this end's retransmissions are [`PeerCounters.PacketsRetransmitted`](ReactorSockets.PeerCounters.PacketsRetransmitted.md#ReactorSockets_PeerCounters_PacketsRetransmitted), and only
this figure sees the far end's, from the receiving side.

```csharp
public ulong DuplicatePackets { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

