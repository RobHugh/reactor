# <a id="ReactorSockets_StreamSessionReactor__ctor_ReactorSockets_StreamSessionOptions_"></a> Constructor StreamSessionReactor

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionReactor__ctor_ReactorSockets_StreamSessionOptions_"></a> StreamSessionReactor\(StreamSessionOptions\)

Validates <code class="paramref">options</code> — every knob and cross-check, then the frame arena's
floor — before anything is allocated or bound, then binds every declared endpoint in the order
supplied. A bind that fails throws here, naming the endpoint: binding is part of what this
Reactor is, so there is no half-built Reactor to hand back.

```csharp
public StreamSessionReactor(StreamSessionOptions options)
```

### Parameters

`options` [StreamSessionOptions](ReactorSockets.StreamSessionOptions.md)

Composed rather than inherited (ADR 0011): the shared L0 budget plus this profile's own
required and defaulted knobs, with no shared base between profiles.

