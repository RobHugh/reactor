# <a id="ReactorSockets_Source__ctor_ReactorSockets_SourceFamily_System_UInt128_System_Int64_System_Int32_"></a> Constructor Source

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_Source__ctor_ReactorSockets_SourceFamily_System_UInt128_System_Int64_System_Int32_"></a> Source\(SourceFamily, UInt128, long, int\)

Who a datagram claims to be from — an unauthenticated hint about origin, never an identity
(ADR 0014). Carried on the record path as a value type: <code>{ family, 16 address bytes, scope id,
port }</code>, roughly 56 bytes, so a Diagnostic or an Event never allocates to name a Source.

```csharp
public Source(SourceFamily Family, UInt128 AddressBytes, long ScopeId, int Port)
```

### Parameters

`Family` [SourceFamily](ReactorSockets.SourceFamily.md)

`AddressBytes` [UInt128](https://learn.microsoft.com/dotnet/api/system.uint128)

`ScopeId` [long](https://learn.microsoft.com/dotnet/api/system.int64)

`Port` [int](https://learn.microsoft.com/dotnet/api/system.int32)

### Remarks

Equality is total and exact over every field, including [`Source.ScopeId`](ReactorSockets.Source.ScopeId.md#ReactorSockets_Source_ScopeId): an IPv6
link-local address is only meaningful with its zone index, and two hosts reachable at the same
<code>fe80::/10</code> address on different interfaces must not compare equal — the datagram profile keys
a cookie HMAC over this value. [`Source.Family`](ReactorSockets.Source.Family.md#ReactorSockets_Source_Family) is an explicit tag rather than inferred from
[`Source.AddressBytes`](ReactorSockets.Source.AddressBytes.md#ReactorSockets_Source_AddressBytes), because with <code>DualMode</code> never set there is nothing in a 16-byte
field distinguishing a zero-padded IPv4 address from an IPv6 one.

