# <a id="ReactorSockets_SessionEvent_Reason"></a> Property Reason

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_SessionEvent_Reason"></a> Reason

Why the Session ended. Populated only when [`SessionEvent.Kind`](ReactorSockets.SessionEvent.Kind.md#ReactorSockets_SessionEvent_Kind) is
[`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md).

```csharp
public DisconnectReason Reason { get; }
```

### Property Value

 [DisconnectReason](ReactorSockets.DisconnectReason.md)

