# <a id="ReactorSockets_DiagnosticRecord_ForPeerRefusedVersion_ReactorSockets_Source_System_Int64_System_Int64_"></a> Method ForPeerRefusedVersion

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForPeerRefusedVersion_ReactorSockets_Source_System_Int64_System_Int64_"></a> ForPeerRefusedVersion\(Source, long, long\)

Builds a [`DiagnosticId.PeerRefused`](ReactorSockets.DiagnosticId.md) record for the claimed Source of a stream
dialler refused because it announced a version this Reactor does not speak. <code>Value0</code> is
the version offered; <code>Value1</code> is the version this Reactor requires.

```csharp
public static DiagnosticRecord ForPeerRefusedVersion(Source source, long versionOffered, long versionRequired)
```

### Parameters

`source` [Source](ReactorSockets.Source.md)

`versionOffered` [long](https://learn.microsoft.com/dotnet/api/system.int64)

`versionRequired` [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

