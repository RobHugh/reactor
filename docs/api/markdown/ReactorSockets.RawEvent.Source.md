# <a id="ReactorSockets_RawEvent_Source"></a> Property Source

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawEvent_Source"></a> Source

Who claims to have sent this datagram — an unauthenticated hint, never an identity. Populated
only when [`RawEvent.Kind`](ReactorSockets.RawEvent.Kind.md#ReactorSockets_RawEvent_Kind) is [`RawEventKind.DatagramReceived`](ReactorSockets.RawEventKind.md).

```csharp
public Source Source { get; }
```

### Property Value

 [Source](ReactorSockets.Source.md)

