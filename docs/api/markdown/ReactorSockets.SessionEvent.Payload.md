# <a id="ReactorSockets_SessionEvent_Payload"></a> Property Payload

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_SessionEvent_Payload"></a> Payload

The whole Message this event carries, aliasing the frame arena. Populated only when
[`SessionEvent.Kind`](ReactorSockets.SessionEvent.Kind.md#ReactorSockets_SessionEvent_Kind) is [`SessionEventKind.MessageReceived`](ReactorSockets.SessionEventKind.md), and valid only until the
next Pump (ADR 0008) — copy it out before draining or pumping again.

```csharp
public ReadOnlySpan<byte> Payload { get; }
```

### Property Value

 [ReadOnlySpan](https://learn.microsoft.com/dotnet/api/system.readonlyspan\-1)<[byte](https://learn.microsoft.com/dotnet/api/system.byte)\>

