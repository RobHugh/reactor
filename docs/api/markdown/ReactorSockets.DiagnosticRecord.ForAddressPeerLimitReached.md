# <a id="ReactorSockets_DiagnosticRecord_ForAddressPeerLimitReached_ReactorSockets_Source_System_Int32_System_Int32_"></a> Method ForAddressPeerLimitReached

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForAddressPeerLimitReached_ReactorSockets_Source_System_Int32_System_Int32_"></a> ForAddressPeerLimitReached\(Source, int, int\)

Builds a [`DiagnosticId.AddressPeerLimitReached`](ReactorSockets.DiagnosticId.md) record for the claimed Source whose
handshake was refused, or whose held stream connection was closed, carrying the per-address limit and the prefix length its address was
counted by: 32 for IPv4, and the IPv6 grouping length for IPv6 (ADR 0014).

```csharp
public static DiagnosticRecord ForAddressPeerLimitReached(Source source, int limit, int prefixLength)
```

### Parameters

`source` [Source](ReactorSockets.Source.md)

`limit` [int](https://learn.microsoft.com/dotnet/api/system.int32)

`prefixLength` [int](https://learn.microsoft.com/dotnet/api/system.int32)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

