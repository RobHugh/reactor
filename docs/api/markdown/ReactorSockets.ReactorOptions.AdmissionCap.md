# <a id="ReactorSockets_ReactorOptions_AdmissionCap"></a> Property AdmissionCap

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_ReactorOptions_AdmissionCap"></a> AdmissionCap

The per-Pump bound on accepts and handshakes alike. Defaults to [`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity).
Provenance: derived from [`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity) — one Pump never spends more admission work
than a full Reactor's worth, and the remainder waits in the OS backlog or is dropped (ADR 0012).

```csharp
public int? AdmissionCap { get; init; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)?

