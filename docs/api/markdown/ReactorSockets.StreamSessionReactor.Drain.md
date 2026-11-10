# <a id="ReactorSockets_StreamSessionReactor_Drain"></a> Method Drain

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionReactor_Drain"></a> Drain\(\)

Drains every event produced this Frame — since the last [`StreamSessionReactor.Pump()`](ReactorSockets.StreamSessionReactor.Pump.md#ReactorSockets_StreamSessionReactor_Pump) — in the order it was
produced.

```csharp
public SessionEventEnumerator Drain()
```

### Returns

 [SessionEventEnumerator](ReactorSockets.SessionEventEnumerator.md)

