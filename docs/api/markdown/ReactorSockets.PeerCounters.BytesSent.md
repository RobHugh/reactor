# <a id="ReactorSockets_PeerCounters_BytesSent"></a> Property BytesSent

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_PeerCounters_BytesSent"></a> BytesSent

On the datagram profile, the datagram bytes a flush has handed the transport for this Peer, header
included (ADR 0013). Incremented once per Datagram, at the size the coalescing packer actually wrote
for it — the 20-byte session header plus every packet record's own header and payload — rather than
per record or per payload byte, since header bytes amortising across a Pump's coalesced records is
exactly what this figure exists to let a consumer see; counting payload alone would hide that
amortisation. Raised regardless of whether the underlying send call reports success, since the bytes
were handed to the transport at that point either way — distinct from the Reactor-scope send figure,
which counts only a confirmed success. A data Datagram to an Established Peer moves this, and so
does each Resume control datagram the dialling end sends it, at its fixed 256 bytes, because a Peer
exists to charge. A handshake's control datagrams carry no session id yet to attribute them to, so
they are never counted here, on either the sent or the received side (see
[`PeerCounters.BytesReceived`](ReactorSockets.PeerCounters.BytesReceived.md#ReactorSockets_PeerCounters_BytesReceived)). On the stream profile, the bytes the socket accepted on this Peer's
connection, framing included: every write a Peer is charged a [`PeerCounters.SocketWrites`](ReactorSockets.PeerCounters.SocketWrites.md#ReactorSockets_PeerCounters_SocketWrites) for,
handshake and Resume records too, at the count each call reported written. A short write counts the
part that went. Monotonic and never reset, exactly like [`PeerCounters.HorizonDeferrals`](ReactorSockets.PeerCounters.HorizonDeferrals.md#ReactorSockets_PeerCounters_HorizonDeferrals): its lifetime
is the Peer's, not the transport's, so a Resume does not zero it. Not derivable: the Reactor-scope
datagram figures count datagrams for every Peer at once and lose closed Peers, and the stream
profile has no other byte figure.

```csharp
public ulong BytesSent { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

