# <a id="ReactorSockets_ReactorCounters_DatagramsReceived"></a> Property DatagramsReceived

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_ReactorCounters_DatagramsReceived"></a> DatagramsReceived

Datagrams taken from the transport. Not derivable: it is the denominator for
<code class="paramref">UnknownSourceDatagrams</code> and <code class="paramref">MalformedSessionDatagrams</code>, and
per-Peer figures lose closed Peers and never see unattributed traffic.

```csharp
public ulong DatagramsReceived { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

