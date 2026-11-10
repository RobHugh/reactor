# <a id="ReactorSockets_DiagnosticRecord_ForPeerRefusedCapacity_ReactorSockets_Source_System_Int32_"></a> Method ForPeerRefusedCapacity

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForPeerRefusedCapacity_ReactorSockets_Source_System_Int32_"></a> ForPeerRefusedCapacity\(Source, int\)

Builds a [`DiagnosticId.PeerRefused`](ReactorSockets.DiagnosticId.md) record for the claimed Source of a stream
dialler refused because every capacity Slot was occupied. <code>Value0</code> carries this Reactor's
capacity; <code>Value1</code> is unused.

```csharp
public static DiagnosticRecord ForPeerRefusedCapacity(Source source, int capacity)
```

### Parameters

`source` [Source](ReactorSockets.Source.md)

`capacity` [int](https://learn.microsoft.com/dotnet/api/system.int32)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

