# <a id="ReactorSockets_DatagramSessionOptions_FragmentSize"></a> Property FragmentSize

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_FragmentSize"></a> FragmentSize

The bytes a fragment carries, and therefore the unit every buffer here is measured in.
Defaults to 1200. Provenance: QUIC's floor beneath the 1280-byte IPv6 minimum MTU (RFC 9000),
the one value with citable provenance for surviving an arbitrary path. Must lie between 576
(RFC 791's guaranteed reassembly minimum) and 65 507 (the maximum UDP payload).

```csharp
public int? FragmentSize { get; init; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)?

### Remarks

Unilateral and per-direction (ADR 0004): each side of a Session cuts its own outbound Messages
by its own configured value, and the two directions need not agree. It is never announced in
the handshake, because a receiver reassembles a Fragment set from <code>fragmentIndex</code> and
<code>fragmentCount</code> alone and never needs to know the sender's choice — so two Sessions
configured with different values still interoperate, each cutting by its own. This value is
resolved once per Peer, at admission, onto that Peer's own slot; a later change to this options
instance never reaches a Peer already open. See [`DatagramSessionOptions.ResolvedFragmentSize`](ReactorSockets.DatagramSessionOptions.ResolvedFragmentSize.md#ReactorSockets_DatagramSessionOptions_ResolvedFragmentSize).

