# <a id="ReactorSockets_ReactorCounters_DatagramsSent"></a> Property DatagramsSent

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_ReactorCounters_DatagramsSent"></a> DatagramsSent

Datagrams handed to the transport for sending. Not derivable: per-Peer byte figures cover only
traffic a Peer exists to charge and end with the Peer, while this counts every datagram for the
Reactor's life.

```csharp
public ulong DatagramsSent { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

