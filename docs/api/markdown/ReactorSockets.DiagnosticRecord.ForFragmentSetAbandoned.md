# <a id="ReactorSockets_DiagnosticRecord_ForFragmentSetAbandoned_ReactorSockets_PeerId_System_Int64_System_Int64_"></a> Method ForFragmentSetAbandoned

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForFragmentSetAbandoned_ReactorSockets_PeerId_System_Int64_System_Int64_"></a> ForFragmentSetAbandoned\(PeerId, long, long\)

Builds a [`DiagnosticId.FragmentSetAbandoned`](ReactorSockets.DiagnosticId.md) record for the Peer whose Fragment set
was abandoned before it ever completed, carrying the fragments actually held and the number the
set's own metadata declared.

```csharp
public static DiagnosticRecord ForFragmentSetAbandoned(PeerId peer, long fragmentsHeld, long fragmentsExpected)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

`fragmentsHeld` [long](https://learn.microsoft.com/dotnet/api/system.int64)

`fragmentsExpected` [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

