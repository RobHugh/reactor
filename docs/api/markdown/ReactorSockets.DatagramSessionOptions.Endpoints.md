# <a id="ReactorSockets_DatagramSessionOptions_Endpoints"></a> Property Endpoints

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_Endpoints"></a> Endpoints

The endpoints to bind at construction, in the order supplied; possibly empty. Declared once
and never rebound (ADR 0014) — a Session's addresses are part of what it is, not something it
acquires later. An empty set binds nothing until the first dial, which binds a single
wildcard, port-zero socket in the destination's address family. Provenance: empty is the
absence of a declaration, not a chosen address — the dial-only socket's family and port are
derived from the destination and the OS, never configured.

```csharp
public IReadOnlyList<IPEndPoint> Endpoints { get; init; }
```

### Property Value

 [IReadOnlyList](https://learn.microsoft.com/dotnet/api/system.collections.generic.ireadonlylist\-1)<[IPEndPoint](https://learn.microsoft.com/dotnet/api/system.net.ipendpoint)\>

