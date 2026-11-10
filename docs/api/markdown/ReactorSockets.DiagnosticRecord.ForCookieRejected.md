# <a id="ReactorSockets_DiagnosticRecord_ForCookieRejected_ReactorSockets_Source_System_Int64_System_Int64_"></a> Method ForCookieRejected

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForCookieRejected_ReactorSockets_Source_System_Int64_System_Int64_"></a> ForCookieRejected\(Source, long, long\)

Builds a [`DiagnosticId.CookieRejected`](ReactorSockets.DiagnosticId.md) record for the claimed Source, carrying the
epoch the cookie offered and the epoch current when it was checked.

```csharp
public static DiagnosticRecord ForCookieRejected(Source source, long epochOffered, long epochCurrent)
```

### Parameters

`source` [Source](ReactorSockets.Source.md)

`epochOffered` [long](https://learn.microsoft.com/dotnet/api/system.int64)

`epochCurrent` [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

