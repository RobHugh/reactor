# <a id="ReactorSockets_RawReactor_MaxUdpDatagramSize"></a> Field MaxUdpDatagramSize

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawReactor_MaxUdpDatagramSize"></a> MaxUdpDatagramSize

The largest payload a UDP datagram can carry over IPv4, and this Reactor's default scratch
buffer size and arena floor: a Segment is as large as one read, so the arena must hold at least
one (ADR 0012). An IPv6 datagram can carry up to 65 527 bytes, more than this bound.

```csharp
public const int MaxUdpDatagramSize = 65507
```

### Field Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)

