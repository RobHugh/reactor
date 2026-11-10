# <a id="ReactorSockets_StreamSessionReactor_CountersOf_ReactorSockets_PeerId_"></a> Method CountersOf

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionReactor_CountersOf_ReactorSockets_PeerId_"></a> CountersOf\(PeerId\)

A point-in-time snapshot of one Peer's counters (ADR 0013). Throws if the Peer is not open.
Mirrors [`RawReactor.CountersOf(LinkId)`](ReactorSockets.RawReactor.CountersOf.md#ReactorSockets_RawReactor_CountersOf_ReactorSockets_LinkId_)'s shape for the Raw profile.

```csharp
public PeerCounters CountersOf(PeerId peer)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

### Returns

 [PeerCounters](ReactorSockets.PeerCounters.md)

