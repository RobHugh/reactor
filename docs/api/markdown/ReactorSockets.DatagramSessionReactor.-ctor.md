# <a id="ReactorSockets_DatagramSessionReactor__ctor_ReactorSockets_DatagramSessionOptions_"></a> Constructor DatagramSessionReactor

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionReactor__ctor_ReactorSockets_DatagramSessionOptions_"></a> DatagramSessionReactor\(DatagramSessionOptions\)

Constructs a Reactor bound to the calling thread (ADR 0002) — every future mutating call must
come from this same thread or it throws — and binds every declared endpoint in the order
supplied. A bind that fails throws here, naming the endpoint: binding is part of what this
Reactor is, so there is no half-bound Reactor to hand back and no
[`DiagnosticId`](ReactorSockets.DiagnosticId.md) member for the failure.

```csharp
public DatagramSessionReactor(DatagramSessionOptions options)
```

### Parameters

`options` [DatagramSessionOptions](ReactorSockets.DatagramSessionOptions.md)

Every knob is validated before anything is allocated or bound, so a combination that cannot
work throws at construction and nowhere else.

