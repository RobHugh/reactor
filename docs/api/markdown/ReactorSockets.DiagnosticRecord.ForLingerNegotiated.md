# <a id="ReactorSockets_DiagnosticRecord_ForLingerNegotiated_ReactorSockets_PeerId_System_Int64_System_Int64_"></a> Method ForLingerNegotiated

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForLingerNegotiated_ReactorSockets_PeerId_System_Int64_System_Int64_"></a> ForLingerNegotiated\(PeerId, long, long\)

Builds a [`DiagnosticId.LingerNegotiated`](ReactorSockets.DiagnosticId.md) record for the newly established
<code class="paramref">peer</code>, on either profile — stream or datagram. <code>Value0</code> is this end's own
Linger in milliseconds; <code>Value1</code> is the effective (minimum of the two ends') Linger in
milliseconds.

```csharp
public static DiagnosticRecord ForLingerNegotiated(PeerId peer, long oursMilliseconds, long effectiveMilliseconds)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

`oursMilliseconds` [long](https://learn.microsoft.com/dotnet/api/system.int64)

`effectiveMilliseconds` [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

