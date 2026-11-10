# <a id="ReactorSockets_DiagnosticRecord_ForOversizedFragmentSet_ReactorSockets_PeerId_System_Int64_System_Int64_"></a> Method ForOversizedFragmentSet

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForOversizedFragmentSet_ReactorSockets_PeerId_System_Int64_System_Int64_"></a> ForOversizedFragmentSet\(PeerId, long, long\)

Builds a [`DiagnosticId.OversizedFragmentSet`](ReactorSockets.DiagnosticId.md) record for the Peer whose Fragment
declared a <code>fragmentCount</code> larger than this Reactor's own receive window could ever hold,
carrying the declared count and the largest legal count.

```csharp
public static DiagnosticRecord ForOversizedFragmentSet(PeerId peer, long declaredFragmentCount, long maxLegalFragmentCount)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

`declaredFragmentCount` [long](https://learn.microsoft.com/dotnet/api/system.int64)

`maxLegalFragmentCount` [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

