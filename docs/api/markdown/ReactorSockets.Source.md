# <a id="ReactorSockets_Source"></a> Struct Source

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

Who a datagram claims to be from — an unauthenticated hint about origin, never an identity
(ADR 0014). Carried on the record path as a value type: <code>{ family, 16 address bytes, scope id,
port }</code>, roughly 56 bytes, so a Diagnostic or an Event never allocates to name a Source.

```csharp
public readonly record struct Source : IEquatable<Source>
```

#### Implements

[IEquatable<Source\>](https://learn.microsoft.com/dotnet/api/system.iequatable\-1)

#### Inherited Members

[object.Equals\(object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\)), 
[object.Equals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\-system\-object\)), 
[object.GetHashCode\(\)](https://learn.microsoft.com/dotnet/api/system.object.gethashcode), 
[object.GetType\(\)](https://learn.microsoft.com/dotnet/api/system.object.gettype), 
[object.ReferenceEquals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.referenceequals), 
[object.ToString\(\)](https://learn.microsoft.com/dotnet/api/system.object.tostring)

## Remarks

Equality is total and exact over every field, including [`Source.ScopeId`](ReactorSockets.Source.ScopeId.md#ReactorSockets_Source_ScopeId): an IPv6
link-local address is only meaningful with its zone index, and two hosts reachable at the same
<code>fe80::/10</code> address on different interfaces must not compare equal — the datagram profile keys
a cookie HMAC over this value. [`Source.Family`](ReactorSockets.Source.Family.md#ReactorSockets_Source_Family) is an explicit tag rather than inferred from
[`Source.AddressBytes`](ReactorSockets.Source.AddressBytes.md#ReactorSockets_Source_AddressBytes), because with <code>DualMode</code> never set there is nothing in a 16-byte
field distinguishing a zero-padded IPv4 address from an IPv6 one.

## Constructors

 [Source\(SourceFamily, UInt128, long, int\)](ReactorSockets.Source.\-ctor.md\#ReactorSockets\_Source\_\_ctor\_ReactorSockets\_SourceFamily\_System\_UInt128\_System\_Int64\_System\_Int32\_)

Who a datagram claims to be from — an unauthenticated hint about origin, never an identity
(ADR 0014). Carried on the record path as a value type: <code>{ family, 16 address bytes, scope id,
port }</code>, roughly 56 bytes, so a Diagnostic or an Event never allocates to name a Source.

## Properties

 [AddressBytes](ReactorSockets.Source.AddressBytes.md\#ReactorSockets\_Source\_AddressBytes)

 [Family](ReactorSockets.Source.Family.md\#ReactorSockets\_Source\_Family)

 [Port](ReactorSockets.Source.Port.md\#ReactorSockets\_Source\_Port)

 [ScopeId](ReactorSockets.Source.ScopeId.md\#ReactorSockets\_Source\_ScopeId)

## Methods

 [FromIPEndPoint\(IPEndPoint\)](ReactorSockets.Source.FromIPEndPoint.md\#ReactorSockets\_Source\_FromIPEndPoint\_System\_Net\_IPEndPoint\_)

Converts an [`IPEndPoint`](https://learn.microsoft.com/dotnet/api/system.net.ipendpoint) — the type the consumer already holds on the cold
bind/dial surface — into the value type carried on the record path (ADR 0014). Throws
[`ArgumentException`](https://learn.microsoft.com/dotnet/api/system.argumentexception) for anything other than IPv4 or IPv6, since <code>DualMode</code>
is never set and no mapped form can appear here.

 [ToIPEndPoint\(\)](ReactorSockets.Source.ToIPEndPoint.md\#ReactorSockets\_Source\_ToIPEndPoint)

Converts this value back into an [`IPEndPoint`](https://learn.microsoft.com/dotnet/api/system.net.ipendpoint) for the cold surfaces that already
hold that type (ADR 0014).

