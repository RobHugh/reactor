# <a id="ReactorSockets_ReactorOptions_Capacity"></a> Property Capacity

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_ReactorOptions_Capacity"></a> Capacity

The number of share-holders this Reactor admits — Links in Raw, Peers in a Session
(ADR 0014) — and the multiplier on every preallocation. Required; there is no default
derivable from anything the library owns (ADR 0011, superseding ADR 0001's "default 64").
Must be at least 1.

```csharp
public required int Capacity { get; init; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)

### Remarks

The library is designed for capacity in the tens and is unsuitable above the low hundreds; 64
survives as that statement of intent (ADR 0011), not as a default. It is not capped: a cap would
be permanent, would defend a number with no derivation behind it, and would refuse a consumer who
has measured their own case. A capacity too large to allocate fails at construction instead.

