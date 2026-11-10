# <a id="ReactorSockets_RawReactor_MaxUdpDatagramSize"></a> Field MaxUdpDatagramSize

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawReactor_MaxUdpDatagramSize"></a> MaxUdpDatagramSize

The largest a UDP datagram can be, and this Reactor's default scratch buffer size and arena
floor: a Segment is as large as one read, so the arena must hold at least one (ADR 0012).

```csharp
public const int MaxUdpDatagramSize = 65507
```

### Field Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)

