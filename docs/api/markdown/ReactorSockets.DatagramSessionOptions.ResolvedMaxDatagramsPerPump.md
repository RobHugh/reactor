# <a id="ReactorSockets_DatagramSessionOptions_ResolvedMaxDatagramsPerPump"></a> Property ResolvedMaxDatagramsPerPump

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_ResolvedMaxDatagramsPerPump"></a> ResolvedMaxDatagramsPerPump

The resolved per-socket read limit: [`DatagramSessionOptions.MaxDatagramsPerPump`](ReactorSockets.DatagramSessionOptions.MaxDatagramsPerPump.md#ReactorSockets_DatagramSessionOptions_MaxDatagramsPerPump) if set, otherwise one receive
window for every Space of every Peer, plus every handshake the admission cap allows in one Pump,
clamped to [`MaxValue`](https://learn.microsoft.com/dotnet/api/system.int32.maxvalue).

```csharp
public int ResolvedMaxDatagramsPerPump { get; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)

