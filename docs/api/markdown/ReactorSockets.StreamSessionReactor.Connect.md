# <a id="ReactorSockets_StreamSessionReactor_Connect_System_Net_IPEndPoint_"></a> Method Connect

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionReactor_Connect_System_Net_IPEndPoint_"></a> Connect\(IPEndPoint\)

Begins a Session with <code class="paramref">destination</code>, returning the [`PeerId`](ReactorSockets.PeerId.md) that
will address it. The Peer occupies a capacity Slot from this moment, exactly as a dialling Link
does in [`RawReactor`](ReactorSockets.RawReactor.md): the handshake is work this Reactor is holding, and pretending
otherwise would let a consumer dial past capacity and discover it only on the far side of a
round trip. [`SessionEventKind.PeerConnected`](ReactorSockets.SessionEventKind.md) or [`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md)
resolves it on a later Pump. A dial that has not established one
[`StreamSessionOptions.Linger`](ReactorSockets.StreamSessionOptions.Linger.md#ReactorSockets_StreamSessionOptions_Linger) after this call closes with
[`DisconnectReason.TransportFailed`](ReactorSockets.DisconnectReason.md), whether its TCP connect is still pending or the far
end connected and sent no handshake record.

```csharp
public PeerId Connect(IPEndPoint destination)
```

### Parameters

`destination` [IPEndPoint](https://learn.microsoft.com/dotnet/api/system.net.ipendpoint)

### Returns

 [PeerId](ReactorSockets.PeerId.md)

