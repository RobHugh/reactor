# <a id="ReactorSockets_DatagramSessionReactor_Connect_System_Net_IPEndPoint_"></a> Method Connect

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionReactor_Connect_System_Net_IPEndPoint_"></a> Connect\(IPEndPoint\)

Begins a Session with <code class="paramref">destination</code>, returning the [`PeerId`](ReactorSockets.PeerId.md) that
will address it. The Peer occupies a capacity Slot from this moment, exactly as a dialling Link
does in [`RawReactor`](ReactorSockets.RawReactor.md): the handshake is work this Reactor is holding, and pretending
otherwise would let a consumer dial past capacity and discover it only on the far side of a
round trip. [`SessionEventKind.PeerConnected`](ReactorSockets.SessionEventKind.md) or
[`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md) resolves it on a later Pump.

```csharp
public PeerId Connect(IPEndPoint destination)
```

### Parameters

`destination` [IPEndPoint](https://learn.microsoft.com/dotnet/api/system.net.ipendpoint)

### Returns

 [PeerId](ReactorSockets.PeerId.md)

### Remarks

While the dial is pending, a Pump resends its current flight: the Hello while it awaits a Cookie,
and the Hello-with-cookie while it awaits an Accept. The delay starts at the observed Pump
interval, doubles, stops at this Reactor's [`DatagramSessionOptions.Linger`](ReactorSockets.DatagramSessionOptions.Linger.md#ReactorSockets_DatagramSessionOptions_Linger) and is
drawn with full jitter, the same schedule the stream profile re-dials on. A dial whose cookie is
one epoch old restarts from a fresh Hello. A dial still pending one
[`DatagramSessionOptions.Linger`](ReactorSockets.DatagramSessionOptions.Linger.md#ReactorSockets_DatagramSessionOptions_Linger) after this call ends with
[`DisconnectReason.TransportFailed`](ReactorSockets.DisconnectReason.md) and frees its Slot. [`DatagramSessionReactor.Close(PeerId)`](ReactorSockets.DatagramSessionReactor.Close.md#ReactorSockets_DatagramSessionReactor_Close_ReactorSockets_PeerId_) ends it
sooner and stops the resends. Nothing is resent while the clock has not moved since the last send.

