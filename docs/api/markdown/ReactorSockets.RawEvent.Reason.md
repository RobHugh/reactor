# <a id="ReactorSockets_RawEvent_Reason"></a> Property Reason

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawEvent_Reason"></a> Reason

Why the Link closed. Populated only when [`RawEvent.Kind`](ReactorSockets.RawEvent.Kind.md#ReactorSockets_RawEvent_Kind) is [`RawEventKind.Closed`](ReactorSockets.RawEventKind.md).

```csharp
public CloseReason Reason { get; }
```

### Property Value

 [CloseReason](ReactorSockets.CloseReason.md)

