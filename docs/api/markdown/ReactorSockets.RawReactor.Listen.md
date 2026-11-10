# <a id="ReactorSockets_RawReactor_Listen_System_Net_IPEndPoint_"></a> Method Listen

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawReactor_Listen_System_Net_IPEndPoint_"></a> Listen\(IPEndPoint\)

Binds and listens on a new TCP socket at <code class="paramref">endpoint</code>, returning the
[`LinkId`](ReactorSockets.LinkId.md) that addresses it. A listening socket is a Link like any other — one
identity type, one lifecycle, one [`RawEventKind.Closed`](ReactorSockets.RawEventKind.md) event — because "Listener"
is a role a Link plays for a consumer, not a kind of thing this library names (ADR 0008). It
consumes a capacity slot exactly like any other Link, but holds no read share: accepting reads
nothing into the frame arena (ADR 0014).

```csharp
public LinkId Listen(IPEndPoint endpoint)
```

### Parameters

`endpoint` [IPEndPoint](https://learn.microsoft.com/dotnet/api/system.net.ipendpoint)

### Returns

 [LinkId](ReactorSockets.LinkId.md)

### Remarks

Inbound connections are admitted by whichever [`RawReactor.Pump()`](ReactorSockets.RawReactor.Pump.md#ReactorSockets_RawReactor_Pump) finds this Link ready, bounded
by the shared admission cap ([`ReactorOptions.ResolvedAdmissionCap`](ReactorSockets.ReactorOptions.ResolvedAdmissionCap.md#ReactorSockets_ReactorOptions_ResolvedAdmissionCap)) every accept and
handshake in this library draws from. Beyond the cap, a pending connection is left exactly where
it was — in the OS backlog — for a later Pump to admit; Raw invents nothing on the wire, so it
says nothing to a remote waiting past the cap (ADR 0007). Binding rules are unchanged from
[`RawReactor.Bind(IPEndPoint)`](ReactorSockets.RawReactor.Bind.md#ReactorSockets_RawReactor_Bind_System_Net_IPEndPoint_): exclusive, no dual-mode, no hostname, no configure callback.

