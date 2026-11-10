# <a id="ReactorSockets_PeerCounters_UnreliableLossPackets"></a> Property UnreliableLossPackets

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_PeerCounters_UnreliableLossPackets"></a> UnreliableLossPackets

How many sequences an unreliable Space's receive window has lost — retired by sliding its base
forward before that sequence ever arrived (ADR 0013). A gap is provisional until the window passes
it by: a sequence still inside the window may yet arrive late and be admitted without ever touching
this figure, so it reflects only sequences a window has moved past, never every sequence not yet
seen. This is one figure covering both unreliable Spaces — <code>Unreliable</code> and
<code>UnreliableSequenced</code> — summed, exactly as [`PeerCounters.OutOfWindowPackets`](ReactorSockets.PeerCounters.OutOfWindowPackets.md#ReactorSockets_PeerCounters_OutOfWindowPackets) and
[`PeerCounters.DuplicatePackets`](ReactorSockets.PeerCounters.DuplicatePackets.md#ReactorSockets_PeerCounters_DuplicatePackets) already cover all four Spaces summed, so per-Space loss is not
separable from it. The two reliable Spaces never contribute, because their windows do not slide:
retransmission is what fills their holes, and what they refuse lands on
[`PeerCounters.OutOfWindowPackets`](ReactorSockets.PeerCounters.OutOfWindowPackets.md#ReactorSockets_PeerCounters_OutOfWindowPackets) instead. No Diagnostic accompanies a gap in either Space; a lossy
path must not fill the Sink with volume. Monotonic and never reset, exactly like
[`PeerCounters.OutOfWindowPackets`](ReactorSockets.PeerCounters.OutOfWindowPackets.md#ReactorSockets_PeerCounters_OutOfWindowPackets); a freshly connected Peer reads zero. Moves only on the datagram
profile. Not derivable: a sequence that never arrives is never classified, so no other figure can
see it.

```csharp
public ulong UnreliableLossPackets { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

