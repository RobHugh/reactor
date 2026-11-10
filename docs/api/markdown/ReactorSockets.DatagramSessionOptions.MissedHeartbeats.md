# <a id="ReactorSockets_DatagramSessionOptions_MissedHeartbeats"></a> Property MissedHeartbeats

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_MissedHeartbeats"></a> MissedHeartbeats

How many consecutive heartbeat intervals may pass in silence before a Peer is declared gone.
Defaults to 3. Provenance: RFC 4271's hold time, three times the keepalive interval. Must be at
least 2, because a count of 1 makes a single lost datagram fatal and no network is that reliable.

```csharp
public int? MissedHeartbeats { get; init; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)?

