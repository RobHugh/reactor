# <a id="ReactorSockets_PeerId_Generation"></a> Property Generation

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_PeerId_Generation"></a> Generation

The generation that Slot was on when this Peer was admitted. Live Slots begin at 1, so a
<a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/default">default</a> [`PeerId`](ReactorSockets.PeerId.md) — generation 0 — addresses nothing.

```csharp
public ulong Generation { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

