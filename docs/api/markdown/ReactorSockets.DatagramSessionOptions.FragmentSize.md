# <a id="ReactorSockets_DatagramSessionOptions_FragmentSize"></a> Property FragmentSize

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_FragmentSize"></a> FragmentSize

The bytes a fragment carries, and therefore the unit every buffer here is measured in.
Defaults to 1200. Provenance: QUIC's floor beneath the 1280-byte IPv6 minimum MTU (RFC 9000),
the one value with citable provenance for surviving an arbitrary path. Must lie between 576
(RFC 791's guaranteed reassembly minimum) and 65 476 (the 65 507-byte maximum UDP payload less the
20-byte Datagram header and the 11-byte Fragment record header). A data Datagram carries at most
this many payload bytes plus a 31-byte header, so at the default a Datagram is at most 1231 bytes,
which with IPv6 and UDP headers fits the 1280-byte minimum MTU.

```csharp
public int? FragmentSize { get; init; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)?

### Remarks

Announced in the handshake (ADR 0024). Each Peer cuts its reliable Messages at the smaller of
this value and the peer's announced value, fixed once when the Peer reaches Established and
unchanged across a Resume, so two Reactors configured with different values interoperate. A
reliable Fragment wider than this value closes the Peer with
[`DisconnectReason.ProtocolViolation`](ReactorSockets.DisconnectReason.md), which only a non-conforming peer causes. The
same smaller value is the single-Datagram cap of an unreliable Message, refused above it with
[`SendOutcome.MessageTooLarge`](ReactorSockets.SendOutcome.md). A later change to this options instance never reaches a Peer already open. See
[`DatagramSessionOptions.ResolvedFragmentSize`](ReactorSockets.DatagramSessionOptions.ResolvedFragmentSize.md#ReactorSockets_DatagramSessionOptions_ResolvedFragmentSize).

