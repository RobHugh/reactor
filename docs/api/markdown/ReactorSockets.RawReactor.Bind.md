# <a id="ReactorSockets_RawReactor_Bind_System_Net_IPEndPoint_"></a> Method Bind

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawReactor_Bind_System_Net_IPEndPoint_"></a> Bind\(IPEndPoint\)

Binds a new UDP socket to <code class="paramref">endpoint</code> and returns the [`LinkId`](ReactorSockets.LinkId.md) that
addresses it. Raw registers its transports at runtime, unlike a Session Reactor which declares
its endpoints at construction (ADR 0014).

```csharp
public LinkId Bind(IPEndPoint endpoint)
```

### Parameters

`endpoint` [IPEndPoint](https://learn.microsoft.com/dotnet/api/system.net.ipendpoint)

### Returns

 [LinkId](ReactorSockets.LinkId.md)

