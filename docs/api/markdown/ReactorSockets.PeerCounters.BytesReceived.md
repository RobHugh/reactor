# <a id="ReactorSockets_PeerCounters_BytesReceived"></a> Property BytesReceived

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_PeerCounters_BytesReceived"></a> BytesReceived

On the datagram profile, the datagram bytes attributed to this Peer on arrival (ADR 0013).
Incremented once per data Datagram that both names an established Peer's session id and proves
well-formed — the same proof that gates ack application and record classification — at the whole
Datagram's length, header included, mirroring [`PeerCounters.BytesSent`](ReactorSockets.PeerCounters.BytesSent.md#ReactorSockets_PeerCounters_BytesSent). A Datagram naming no Session,
or one so malformed it cannot be attributed to the Peer it claims, charges no Peer: it lands on a
Reactor-scope figure instead. A handshake's control datagrams are never counted here either, for the
same reason as [`PeerCounters.BytesSent`](ReactorSockets.PeerCounters.BytesSent.md#ReactorSockets_PeerCounters_BytesSent): at the point one arrives, no Peer's session id has been
assigned yet for it to name. A Resume that matches this Peer is counted at its fixed 256 bytes,
because a Peer exists to charge. A Resume naming no Peer, or presenting the wrong secret, charges
none. On the stream profile, every byte read from this Peer's connection, framing and handshake
records included. The first record on an accepted connection is charged once it is admitted as this
Peer or matched as its Resume. A connection refused or never matched charges no Peer. Monotonic and
never reset, exactly like [`PeerCounters.HorizonDeferrals`](ReactorSockets.PeerCounters.HorizonDeferrals.md#ReactorSockets_PeerCounters_HorizonDeferrals): its lifetime is the Peer's, not the
transport's, so a Resume does not zero it. A freshly connected datagram Peer reads zero. Not
derivable: [`PeerCounters.MessagesReceived`](ReactorSockets.PeerCounters.MessagesReceived.md#ReactorSockets_PeerCounters_MessagesReceived) counts Messages rather than bytes, and the Reactor-scope
figures neither attribute bytes to a Peer nor see a stream connection.

```csharp
public ulong BytesReceived { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

