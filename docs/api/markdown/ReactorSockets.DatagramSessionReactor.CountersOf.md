# <a id="ReactorSockets_DatagramSessionReactor_CountersOf_ReactorSockets_PeerId_"></a> Method CountersOf

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionReactor_CountersOf_ReactorSockets_PeerId_"></a> CountersOf\(PeerId\)

A point-in-time snapshot of one Peer's counters (ADR 0013). Throws if the Peer is not open,
matching [`StreamSessionReactor.CountersOf(PeerId)`](ReactorSockets.StreamSessionReactor.CountersOf.md#ReactorSockets_StreamSessionReactor_CountersOf_ReactorSockets_PeerId_)'s shape for the stream profile —
the datagram profile had no per-Peer counter reader until this member.
[`PeerCounters.OutOfWindowPackets`](ReactorSockets.PeerCounters.OutOfWindowPackets.md#ReactorSockets_PeerCounters_OutOfWindowPackets) and [`PeerCounters.DuplicatePackets`](ReactorSockets.PeerCounters.DuplicatePackets.md#ReactorSockets_PeerCounters_DuplicatePackets)
now reflect what the four per-Space receive windows have refused;
[`PeerCounters.UnreliableLossPackets`](ReactorSockets.PeerCounters.UnreliableLossPackets.md#ReactorSockets_PeerCounters_UnreliableLossPackets) reflects gaps either unreliable Space's window
has slid past. [`PeerCounters.MessagesReceived`](ReactorSockets.PeerCounters.MessagesReceived.md#ReactorSockets_PeerCounters_MessagesReceived) counts whole Messages handed to the
frame arena for delivery, and not a Message the arena refused.
[`PeerCounters.Resumes`](ReactorSockets.PeerCounters.Resumes.md#ReactorSockets_PeerCounters_Resumes) counts the Peer's resumes: once per
[`SessionEventKind.PeerResumed`](ReactorSockets.SessionEventKind.md) raised for it, on the accepting end when a matching
Resume moves it out of Interrupted and on the dialling end when it hears its session again after
sending a Resume, and never reset by one (ADR 0013).
[`PeerCounters.RetentionHighWaterBytes`](ReactorSockets.PeerCounters.RetentionHighWaterBytes.md#ReactorSockets_PeerCounters_RetentionHighWaterBytes) reads the true per-Peer figure — summed
across both reliable Spaces — from `_retention`.

```csharp
public PeerCounters CountersOf(PeerId peer)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

### Returns

 [PeerCounters](ReactorSockets.PeerCounters.md)

