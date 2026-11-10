# <a id="ReactorSockets_DatagramSessionOptions_PeerLimitIPv6PrefixLength"></a> Property PeerLimitIPv6PrefixLength

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_PeerLimitIPv6PrefixLength"></a> PeerLimitIPv6PrefixLength

How many leading bits of an IPv6 address form the key for [`DatagramSessionOptions.MaxPeersPerAddress`](ReactorSockets.DatagramSessionOptions.MaxPeersPerAddress.md#ReactorSockets_DatagramSessionOptions_MaxPeersPerAddress), so
every address in one prefix shares one limit. Defaults to [`DatagramSessionOptions.ResolvedPeerLimitIPv6PrefixLength`](ReactorSockets.DatagramSessionOptions.ResolvedPeerLimitIPv6PrefixLength.md#ReactorSockets_DatagramSessionOptions_ResolvedPeerLimitIPv6PrefixLength),
64. Provenance: RFC 4291 §2.5.1 fixes the interface identifier of a unicast address at 64 bits, so a
/64 is one link (ADR 0014). From 1 to 128. A value of 128 counts each address on its own.

```csharp
public int? PeerLimitIPv6PrefixLength { get; init; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)?

### Remarks

Every on-link host on an IPv6 LAN shares one /64, so at the default those Peers share one limit.
Set 128 when Peers legitimately arrive from one link. A shorter length groups more of a customer's
delegated prefix, at the cost of grouping unrelated customers. The scope stays in the key, so
link-local addresses are grouped per interface. IPv4 addresses are never grouped.

