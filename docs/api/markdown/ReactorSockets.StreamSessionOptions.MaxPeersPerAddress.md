# <a id="ReactorSockets_StreamSessionOptions_MaxPeersPerAddress"></a> Property MaxPeersPerAddress

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionOptions_MaxPeersPerAddress"></a> MaxPeersPerAddress

The most accepted Peers one address may hold at once (issue #452), so one reachable address
cannot occupy every capacity Slot. Defaults to [`StreamSessionOptions.ResolvedMaxPeersPerAddress`](ReactorSockets.StreamSessionOptions.ResolvedMaxPeersPerAddress.md#ReactorSockets_StreamSessionOptions_ResolvedMaxPeersPerAddress), one fewer
than [`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity). Provenance: derived from capacity, as the largest limit
that still keeps one address from holding every Slot (ADR 0014). How many Peers legitimately share
an address depends on the NATs and proxies in front of this Reactor, so set it lower when strangers
can reach it. At least 1. A value of [`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity) or above sets no limit,
which is what the default is at a capacity of 1.

```csharp
public int? MaxPeersPerAddress { get; init; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)?

### Remarks

<p>
The count is by address, without the port: one host can open connections from as many ports as it
can bind, so a limit that counted ports would limit nothing. The address family and IPv6 scope stay
in the key, because they distinguish hosts (ADR 0014). IPv6 addresses are counted by the prefix
[`StreamSessionOptions.PeerLimitIPv6PrefixLength`](ReactorSockets.StreamSessionOptions.PeerLimitIPv6PrefixLength.md#ReactorSockets_StreamSessionOptions_PeerLimitIPv6PrefixLength) sets, so a host that can bind a whole prefix gets the limit
once. IPv4 addresses are counted whole.
</p>
<p>
A Peer is charged to the address its connection was accepted from for as long as it holds its Slot,
including while Interrupted or Closing and after a Resume moves it to another address. A Resume is
never refused by this limit, because the Peer it names already owns its Slot (ADR 0011). A dialled
Peer is not charged: this Reactor chose to make it. A connection over the limit is refused the same way as
one at a full Reactor, and reported with [`DiagnosticId.AddressPeerLimitReached`](ReactorSockets.DiagnosticId.md).
</p>

