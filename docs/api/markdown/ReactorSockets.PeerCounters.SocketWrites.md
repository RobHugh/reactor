# <a id="ReactorSockets_PeerCounters_SocketWrites"></a> Property SocketWrites

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_PeerCounters_SocketWrites"></a> SocketWrites

How many socket send calls this Reactor has made on this Peer's transport (ADR 0013). One per call,
whatever it carried and however it ended — a whole write, a short write and a failed call each count
once — because the figure measures what the Reactor paid in syscalls, not what the socket accepted.
On the stream profile every write on the Peer's connection counts: handshake and Resume records,
delivery-counter records sent alone, and each retention write, where a delivery counter riding that
write adds nothing. On the datagram profile it counts one per data Datagram a flush hands the
transport and one per Resume control datagram the dialling end sends, beside
[`PeerCounters.BytesSent`](ReactorSockets.PeerCounters.BytesSent.md#ReactorSockets_PeerCounters_BytesSent); a handshake's control datagrams are never counted, for the same reason they
never reach [`PeerCounters.BytesSent`](ReactorSockets.PeerCounters.BytesSent.md#ReactorSockets_PeerCounters_BytesSent). A rate well above the Pump rate for one Peer names a write path
that is not batching. Monotonic and never reset, exactly like [`PeerCounters.HorizonDeferrals`](ReactorSockets.PeerCounters.HorizonDeferrals.md#ReactorSockets_PeerCounters_HorizonDeferrals): its
lifetime is the Peer's, not the transport's, so a Resume does not zero it. Not derivable: how many
bytes went out says nothing about how many calls carried them.

```csharp
public ulong SocketWrites { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

