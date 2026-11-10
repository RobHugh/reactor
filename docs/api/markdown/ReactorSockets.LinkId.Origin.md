# <a id="ReactorSockets_LinkId_Origin"></a> Property Origin

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_LinkId_Origin"></a> Origin

Which Reactor issued this [`LinkId`](ReactorSockets.LinkId.md). Opaque and process-local — compared for equality
and nothing else, ordering nothing and meaning nothing outside this process. Never 0: that value
is reserved and no Reactor is ever issued it, so a <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/default">default</a>
[`LinkId`](ReactorSockets.LinkId.md) addresses nothing twice over.

```csharp
public uint Origin { get; init; }
```

### Property Value

 [uint](https://learn.microsoft.com/dotnet/api/system.uint32)

