# <a id="ReactorSockets_StreamSessionReactor_BoundEndpoints"></a> Property BoundEndpoints

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionReactor_BoundEndpoints"></a> BoundEndpoints

The endpoints this Reactor is bound to right now, in the order they were declared. A snapshot:
it answers what is bound, and there is no member that binds another — there is no rebinding
(ADR 0014), and unlike the datagram profile a stream dial never binds a wildcard socket of its
own, since TCP's own connect already binds an ephemeral local port implicitly.

```csharp
public IReadOnlyList<IPEndPoint> BoundEndpoints { get; }
```

### Property Value

 [IReadOnlyList](https://learn.microsoft.com/dotnet/api/system.collections.generic.ireadonlylist\-1)<[IPEndPoint](https://learn.microsoft.com/dotnet/api/system.net.ipendpoint)\>

