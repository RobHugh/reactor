# <a id="ReactorSockets_PeerCounters_RetentionHighWaterBytes"></a> Property RetentionHighWaterBytes

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_PeerCounters_RetentionHighWaterBytes"></a> RetentionHighWaterBytes

The most bytes this Peer's retention ring has ever held at once (written minus released) since
the Peer was established — `HighWaterBytes` read fresh at
snapshot time, never pushed. Monotonic for the Peer's lifetime: draining the ring after a burst
lowers what is currently retained but never lowers this figure. Not derivable: bytes sent is a
running total, and how much of it was unacknowledged at once is what sizes retention.

```csharp
public ulong RetentionHighWaterBytes { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

