# <a id="ReactorSockets_RawReactor_LocalOf_ReactorSockets_LinkId_"></a> Method LocalOf

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawReactor_LocalOf_ReactorSockets_LinkId_"></a> LocalOf\(LinkId\)

Returns the bound address of <code class="paramref">link</code>. Link state, constant for the Link's
lifetime, and therefore not event data (ADR 0014). Throws [`ArgumentException`](https://learn.microsoft.com/dotnet/api/system.argumentexception) if
<code class="paramref">link</code> does not address an open Link.

```csharp
public IPEndPoint LocalOf(LinkId link)
```

### Parameters

`link` [LinkId](ReactorSockets.LinkId.md)

### Returns

 [IPEndPoint](https://learn.microsoft.com/dotnet/api/system.net.ipendpoint)

