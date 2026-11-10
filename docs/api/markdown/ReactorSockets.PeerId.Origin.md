# <a id="ReactorSockets_PeerId_Origin"></a> Property Origin

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_PeerId_Origin"></a> Origin

Which Reactor issued this [`PeerId`](ReactorSockets.PeerId.md). Opaque and process-local — it is compared for
equality and nothing else, orders nothing, and never appears on the wire. Never 0: that value is
reserved and no Reactor is ever issued it, so a <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/default">default</a> [`PeerId`](ReactorSockets.PeerId.md)
addresses nothing twice over. Not a [`Source`](ReactorSockets.Source.md), which names a remote address; this
names a Reactor of this process.

```csharp
public uint Origin { get; init; }
```

### Property Value

 [uint](https://learn.microsoft.com/dotnet/api/system.uint32)

