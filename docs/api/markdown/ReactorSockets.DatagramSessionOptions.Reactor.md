# <a id="ReactorSockets_DatagramSessionOptions_Reactor"></a> Property Reactor

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_Reactor"></a> Reactor

The shared L0 budget: capacity, admission cap, arena size, time source and Diagnostic Sink.
Required — a Reactor cannot be built without one, and its [`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity)
has no default of its own.

```csharp
public required ReactorOptions Reactor { get; init; }
```

### Property Value

 [ReactorOptions](ReactorSockets.ReactorOptions.md)

