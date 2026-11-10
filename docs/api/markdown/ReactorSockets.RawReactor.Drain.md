# <a id="ReactorSockets_RawReactor_Drain"></a> Method Drain

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawReactor_Drain"></a> Drain\(\)

Drains every event produced this Frame — since the last [`RawReactor.Pump()`](ReactorSockets.RawReactor.Pump.md#ReactorSockets_RawReactor_Pump) — in the order it was
produced. Every drained [`RawEvent`](ReactorSockets.RawEvent.md)'s [`RawEvent.Payload`](ReactorSockets.RawEvent.Payload.md#ReactorSockets_RawEvent_Payload) is valid only
until the next [`RawReactor.Pump()`](ReactorSockets.RawReactor.Pump.md#ReactorSockets_RawReactor_Pump).

```csharp
public RawEventEnumerator Drain()
```

### Returns

 [RawEventEnumerator](ReactorSockets.RawEventEnumerator.md)

