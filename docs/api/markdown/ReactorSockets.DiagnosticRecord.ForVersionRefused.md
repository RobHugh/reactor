# <a id="ReactorSockets_DiagnosticRecord_ForVersionRefused_ReactorSockets_Source_System_Int64_System_Int64_"></a> Method ForVersionRefused

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForVersionRefused_ReactorSockets_Source_System_Int64_System_Int64_"></a> ForVersionRefused\(Source, long, long\)

Builds a [`DiagnosticId.VersionRefused`](ReactorSockets.DiagnosticId.md) record for the claimed Source, carrying the
version offered and the version this Reactor requires.

```csharp
public static DiagnosticRecord ForVersionRefused(Source source, long versionOffered, long versionRequired)
```

### Parameters

`source` [Source](ReactorSockets.Source.md)

`versionOffered` [long](https://learn.microsoft.com/dotnet/api/system.int64)

`versionRequired` [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

