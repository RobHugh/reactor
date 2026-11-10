# <a id="ReactorSockets_StreamSessionOptions_Endpoints"></a> Property Endpoints

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionOptions_Endpoints"></a> Endpoints

The endpoints to bind at construction, in the order supplied; possibly empty. Declared once
and never rebound (ADR 0014) — a Session's addresses are part of what it is, not something it
acquires later. A consumer addresses Peers, never the sockets this profile binds, so binding
returns no identity: an empty set means dial-only, a meaningful configuration rather than a
hole, and there is no rebind — a Reactor that must listen elsewhere is a new Reactor.
Provenance: empty is the absence of a declaration, not a chosen address — a dial's local port
is the one TCP's connect binds implicitly.

```csharp
public IReadOnlyList<IPEndPoint> Endpoints { get; init; }
```

### Property Value

 [IReadOnlyList](https://learn.microsoft.com/dotnet/api/system.collections.generic.ireadonlylist\-1)<[IPEndPoint](https://learn.microsoft.com/dotnet/api/system.net.ipendpoint)\>

