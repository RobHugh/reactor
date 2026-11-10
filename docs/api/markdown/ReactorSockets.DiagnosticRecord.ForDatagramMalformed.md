# <a id="ReactorSockets_DiagnosticRecord_ForDatagramMalformed_ReactorSockets_Source_System_Int32_"></a> Method ForDatagramMalformed

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForDatagramMalformed_ReactorSockets_Source_System_Int32_"></a> ForDatagramMalformed\(Source, int\)

Builds a [`DiagnosticId.DatagramMalformed`](ReactorSockets.DiagnosticId.md) record for the claimed Source, carrying
the number of bytes that arrived.

```csharp
public static DiagnosticRecord ForDatagramMalformed(Source source, int bytesReceived)
```

### Parameters

`source` [Source](ReactorSockets.Source.md)

`bytesReceived` [int](https://learn.microsoft.com/dotnet/api/system.int32)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

