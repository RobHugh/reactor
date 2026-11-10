# <a id="ReactorSockets_DiagnosticRecord_ForDatagramTruncated_ReactorSockets_Source_System_Int32_"></a> Method ForDatagramTruncated

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForDatagramTruncated_ReactorSockets_Source_System_Int32_"></a> ForDatagramTruncated\(Source, int\)

Builds a [`DiagnosticId.DatagramTruncated`](ReactorSockets.DiagnosticId.md) record for the claimed Source, carrying
the scratch buffer size the datagram was truncated to.

```csharp
public static DiagnosticRecord ForDatagramTruncated(Source source, int bufferSize)
```

### Parameters

`source` [Source](ReactorSockets.Source.md)

`bufferSize` [int](https://learn.microsoft.com/dotnet/api/system.int32)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

