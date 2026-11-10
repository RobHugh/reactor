# <a id="ReactorSockets_DatagramSessionReactor_BoundEndpoints"></a> Property BoundEndpoints

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionReactor_BoundEndpoints"></a> BoundEndpoints

The endpoints this Reactor is bound to right now, in the order they were declared, with any
socket bound by a dial appended. A snapshot: it answers what is bound, and there is no member
that binds another.

```csharp
public IReadOnlyList<IPEndPoint> BoundEndpoints { get; }
```

### Property Value

 [IReadOnlyList](https://learn.microsoft.com/dotnet/api/system.collections.generic.ireadonlylist\-1)<[IPEndPoint](https://learn.microsoft.com/dotnet/api/system.net.ipendpoint)\>

