# <a id="ReactorSockets_RawReactor_Connect_System_Net_IPEndPoint_"></a> Method Connect

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawReactor_Connect_System_Net_IPEndPoint_"></a> Connect\(IPEndPoint\)

Begins dialling a new outbound TCP connection to <code class="paramref">endpoint</code> and returns the
[`LinkId`](ReactorSockets.LinkId.md) that addresses it immediately — connecting is advanced by the consumer's
own loop like everything else, so this call never blocks and the handshake's outcome arrives
later, from whichever [`RawReactor.Pump()`](ReactorSockets.RawReactor.Pump.md#ReactorSockets_RawReactor_Pump) first finds it resolved. A dial that succeeds produces
exactly one [`RawEventKind.Connected`](ReactorSockets.RawEventKind.md); a dial that fails produces a
[`RawEventKind.Closed`](ReactorSockets.RawEventKind.md) naming [`CloseReason.Refused`](ReactorSockets.CloseReason.md) or
[`CloseReason.Unreachable`](ReactorSockets.CloseReason.md) (or [`CloseReason.Failed`](ReactorSockets.CloseReason.md) for anything else),
alongside a [`DiagnosticId.LinkFailed`](ReactorSockets.DiagnosticId.md) carrying the platform
[`SocketError`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror). Nothing about a dial in progress is delivered except in response to a
Pump.

```csharp
public LinkId Connect(IPEndPoint endpoint)
```

### Parameters

`endpoint` [IPEndPoint](https://learn.microsoft.com/dotnet/api/system.net.ipendpoint)

### Returns

 [LinkId](ReactorSockets.LinkId.md)

### Remarks

There is no hostname overload and no connect-timeout knob: nothing ticks unless the consumer
Pumps, and a dial going nowhere is closed by the consumer or by the OS, not by a timer this
library invented. The dialled Link consumes a capacity slot exactly like a Bind or a Listen;
[`RawReactor.LocalOf(LinkId)`](ReactorSockets.RawReactor.LocalOf.md#ReactorSockets_RawReactor_LocalOf_ReactorSockets_LinkId_) answers the address TCP bound implicitly the moment the connect began,
whether or not the handshake has finished.

