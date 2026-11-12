# <a id="ReactorSockets_DiagnosticRecord_ForOversizedFragment_ReactorSockets_PeerId_System_Int64_System_Int64_"></a> Method ForOversizedFragment

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForOversizedFragment_ReactorSockets_PeerId_System_Int64_System_Int64_"></a> ForOversizedFragment\(PeerId, long, long\)

Builds a [`DiagnosticId.OversizedFragment`](ReactorSockets.DiagnosticId.md) record for the Peer whose reliable Fragment
was wider than this Reactor's own Fragment size, carrying the Fragment's payload length and that
size.

```csharp
public static DiagnosticRecord ForOversizedFragment(PeerId peer, long fragmentLength, long fragmentSize)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

`fragmentLength` [long](https://learn.microsoft.com/dotnet/api/system.int64)

`fragmentSize` [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

