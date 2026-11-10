# <a id="ReactorSockets_DiagnosticRecord_ForResumeUnmatched_ReactorSockets_Source_System_Int64_System_Int64_"></a> Method ForResumeUnmatched

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForResumeUnmatched_ReactorSockets_Source_System_Int64_System_Int64_"></a> ForResumeUnmatched\(Source, long, long\)

Builds a [`DiagnosticId.ResumeUnmatched`](ReactorSockets.DiagnosticId.md) record for the claimed Source of a
connection that claimed an existing Session and was refused rather than resumed.
<code>Value0</code> is the number of Interrupted Peers the claim was checked against;
<code>Value1</code> is the wire record type the claim arrived as.

```csharp
public static DiagnosticRecord ForResumeUnmatched(Source source, long candidatesChecked, long recordType)
```

### Parameters

`source` [Source](ReactorSockets.Source.md)

`candidatesChecked` [long](https://learn.microsoft.com/dotnet/api/system.int64)

`recordType` [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

