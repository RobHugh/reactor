# <a id="ReactorSockets_SourceFamily"></a> Enum SourceFamily

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

The address family of a [`Source`](ReactorSockets.Source.md), carried as an explicit tag rather than inferred
from the address bytes (ADR 0014). With <code>DualMode</code> never set, nothing in a 16-byte field
distinguishes a zero-padded IPv4 address from an IPv6 one.

```csharp
public enum SourceFamily
```

## Fields

`IPv4 = 0` 

An IPv4 address, stored in the low-order 4 bytes of [`Source.AddressBytes`](ReactorSockets.Source.AddressBytes.md#ReactorSockets_Source_AddressBytes).



`IPv6 = 1` 

An IPv6 address, occupying all 16 bytes of [`Source.AddressBytes`](ReactorSockets.Source.AddressBytes.md#ReactorSockets_Source_AddressBytes).



