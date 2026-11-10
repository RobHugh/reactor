# <a id="ReactorSockets_StreamSessionOptions_ResolvedMaxPeersPerAddress"></a> Property ResolvedMaxPeersPerAddress

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionOptions_ResolvedMaxPeersPerAddress"></a> ResolvedMaxPeersPerAddress

The resolved per-address limit: [`StreamSessionOptions.MaxPeersPerAddress`](ReactorSockets.StreamSessionOptions.MaxPeersPerAddress.md#ReactorSockets_StreamSessionOptions_MaxPeersPerAddress) if set, otherwise one fewer than
[`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity), and at least 1.

```csharp
public int ResolvedMaxPeersPerAddress { get; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)

