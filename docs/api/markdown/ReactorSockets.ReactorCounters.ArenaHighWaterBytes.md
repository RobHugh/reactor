# <a id="ReactorSockets_ReactorCounters_ArenaHighWaterBytes"></a> Property ArenaHighWaterBytes

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_ReactorCounters_ArenaHighWaterBytes"></a> ArenaHighWaterBytes

The most Arena bytes any single Frame has used. Moves on every profile. Not derivable: byte totals
say how much arrived over time, never how much of it landed in one Frame, which is the figure that
sizes the Arena.

```csharp
public ulong ArenaHighWaterBytes { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

