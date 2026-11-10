# <a id="ReactorSockets_DatagramSessionReactor_Drain"></a> Method Drain

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionReactor_Drain"></a> Drain\(\)

Drains every event produced this Frame — since the last [`DatagramSessionReactor.Pump()`](ReactorSockets.DatagramSessionReactor.Pump.md#ReactorSockets_DatagramSessionReactor_Pump) — in the order it was
produced.

```csharp
public SessionEventEnumerator Drain()
```

### Returns

 [SessionEventEnumerator](ReactorSockets.SessionEventEnumerator.md)

