# <a id="ReactorSockets_DatagramSessionOptions_SendIdleInterval"></a> Property SendIdleInterval

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_SendIdleInterval"></a> SendIdleInterval

How long a Session may go without sending before it sends a Ping instead. Required: shorter
costs bandwidth on an idle link, longer costs time to notice a dead Peer, and only the consumer
knows which they would rather spend. There is no option to disable this: every Established Peer
that has sent no data datagram for this long is always sent one seven-byte control record,
whatever else it did or did not receive. At most 2^32 - 1 milliseconds (about 49.7 days), the
most a handshake may announce.

```csharp
public required TimeSpan SendIdleInterval { get; init; }
```

### Property Value

 [TimeSpan](https://learn.microsoft.com/dotnet/api/system.timespan)

### Remarks

The Ping this produces is a Heartbeat, and a Heartbeat is not a keepalive: it proves the party
behind this Session is still there — able to receive and eventually acknowledge application data
— not merely that some NAT mapping between here and there is still open, which a bare keepalive a
middlebox itself could answer would not prove. The receive side is derived, not configured: a
Peer is declared gone after the remote's announced interval times [`DatagramSessionOptions.MissedHeartbeats`](ReactorSockets.DatagramSessionOptions.MissedHeartbeats.md#ReactorSockets_DatagramSessionOptions_MissedHeartbeats)
of silence (ADR 0012).

