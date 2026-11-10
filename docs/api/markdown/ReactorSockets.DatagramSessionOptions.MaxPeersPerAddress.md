# <a id="ReactorSockets_DatagramSessionOptions_MaxPeersPerAddress"></a> Property MaxPeersPerAddress

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_MaxPeersPerAddress"></a> MaxPeersPerAddress

The most accepted Peers one address may hold at once, so one reachable address cannot occupy
every capacity Slot. Defaults to [`DatagramSessionOptions.ResolvedMaxPeersPerAddress`](ReactorSockets.DatagramSessionOptions.ResolvedMaxPeersPerAddress.md#ReactorSockets_DatagramSessionOptions_ResolvedMaxPeersPerAddress), one fewer than
[`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity). Provenance: derived from capacity, as the largest limit that
still keeps one address from holding every Slot (ADR 0014). How many Peers legitimately share an
address depends on the NATs and proxies in front of this Reactor, so set it lower when strangers can
reach it. At least 1. A value of [`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity) or above sets no limit, which
is what the default is at a capacity of 1.

```csharp
public int? MaxPeersPerAddress { get; init; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)?

### Remarks

<p>
The count is by address, without the port: a host can answer a Cookie from as many ports as it can
bind, so a limit that counted ports would limit nothing. The address family and IPv6 scope stay in
the key, because they distinguish hosts (ADR 0014). IPv6 addresses are counted by the prefix
[`DatagramSessionOptions.PeerLimitIPv6PrefixLength`](ReactorSockets.DatagramSessionOptions.PeerLimitIPv6PrefixLength.md#ReactorSockets_DatagramSessionOptions_PeerLimitIPv6PrefixLength) sets, so a host that can bind a whole prefix gets the limit
once. IPv4 addresses are counted whole.
</p>
<p>
A Peer is charged to the address its handshake came from for as long as it holds its Slot, including
while Interrupted or Closing and after a Resume moves it to another address. A dialled Peer is not
charged: this Reactor chose to make it. A handshake over the limit is refused the same way as one at
a full Reactor, and reported with [`DiagnosticId.AddressPeerLimitReached`](ReactorSockets.DiagnosticId.md).
</p>

