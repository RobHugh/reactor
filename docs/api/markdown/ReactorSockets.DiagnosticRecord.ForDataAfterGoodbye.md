# <a id="ReactorSockets_DiagnosticRecord_ForDataAfterGoodbye_ReactorSockets_PeerId_"></a> Method ForDataAfterGoodbye

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForDataAfterGoodbye_ReactorSockets_PeerId_"></a> ForDataAfterGoodbye\(PeerId\)

Builds a [`DiagnosticId.DataAfterGoodbye`](ReactorSockets.DiagnosticId.md) record for the Peer that kept sending
after its own Goodbye. No claimed length: <code>Value0</code> and <code>Value1</code> are unused.

```csharp
public static DiagnosticRecord ForDataAfterGoodbye(PeerId peer)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

