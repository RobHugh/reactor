# <a id="ReactorSockets_RawReactor_Close_ReactorSockets_LinkId_"></a> Method Close

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawReactor_Close_ReactorSockets_LinkId_"></a> Close\(LinkId\)

Closes <code class="paramref">link</code> and queues exactly one [`RawEventKind.Closed`](ReactorSockets.RawEventKind.md) event
carrying [`CloseReason.LocalClose`](ReactorSockets.CloseReason.md). A no-op if <code class="paramref">link</code> does not address
an open Link. The slot's generation advances, so a stale copy of <code class="paramref">link</code> can never
address whatever binds into this slot next (ADR 0014).

```csharp
public void Close(LinkId link)
```

### Parameters

`link` [LinkId](ReactorSockets.LinkId.md)

