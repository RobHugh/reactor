# <a id="ReactorSockets_ReactorOptions_ArenaSize"></a> Property ArenaSize

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_ReactorOptions_ArenaSize"></a> ArenaSize

The frame arena's size in bytes. Left unset, a Reactor defaults it to its own profile floor.
Provenance: derived from the profile — one maximum deliverable unit, since below it that unit
could never be delivered, and only the profile's constructor knows what it must exceed
(ADR 0012). A value below that floor throws at construction, naming the arithmetic.

```csharp
public int? ArenaSize { get; init; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)?

