# <a id="ReactorSockets_RawEvent_Payload"></a> Property Payload

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawEvent_Payload"></a> Payload

The received bytes, valid only until the next [`RawReactor.Pump()`](ReactorSockets.RawReactor.Pump.md#ReactorSockets_RawReactor_Pump). Populated when
[`RawEvent.Kind`](ReactorSockets.RawEvent.Kind.md#ReactorSockets_RawEvent_Kind) is [`RawEventKind.DatagramReceived`](ReactorSockets.RawEventKind.md) or
[`RawEventKind.SegmentReceived`](ReactorSockets.RawEventKind.md). For a Segment this is exactly the run of bytes that
arrived on this Link in this Frame — no claim of being a whole anything, and nothing is held
across a Pump to make one (ADR 0008).

```csharp
public ReadOnlySpan<byte> Payload { get; }
```

### Property Value

 [ReadOnlySpan](https://learn.microsoft.com/dotnet/api/system.readonlyspan\-1)<[byte](https://learn.microsoft.com/dotnet/api/system.byte)\>

