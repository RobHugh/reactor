# <a id="ReactorSockets_StreamSessionOptions_Linger"></a> Property Linger

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionOptions_Linger"></a> Linger

How long a closing Session stays alive to retransmit its Goodbye and absorb what is still in
flight. Required, for the same reason as [`StreamSessionOptions.SendIdleInterval`](ReactorSockets.StreamSessionOptions.SendIdleInterval.md#ReactorSockets_StreamSessionOptions_SendIdleInterval): it trades a prompt
teardown against a clean one. It also bounds a dial: a Peer this end dialled that has not
established one Linger after [`StreamSessionReactor.Connect(IPEndPoint)`](ReactorSockets.StreamSessionReactor.Connect.md#ReactorSockets_StreamSessionReactor_Connect_System_Net_IPEndPoint_) closes with
[`DisconnectReason.TransportFailed`](ReactorSockets.DisconnectReason.md), so a Linger shorter than the round trip plus the
far end's Pump interval closes a dial before its answer arrives. At most 2^32 - 1 milliseconds
(about 49.7 days), the most a handshake may announce.

```csharp
public required TimeSpan Linger { get; init; }
```

### Property Value

 [TimeSpan](https://learn.microsoft.com/dotnet/api/system.timespan)

