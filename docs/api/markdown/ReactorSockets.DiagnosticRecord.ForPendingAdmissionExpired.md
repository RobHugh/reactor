# <a id="ReactorSockets_DiagnosticRecord_ForPendingAdmissionExpired_ReactorSockets_Source_System_Int32_System_Int64_"></a> Method ForPendingAdmissionExpired

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForPendingAdmissionExpired_ReactorSockets_Source_System_Int32_System_Int64_"></a> ForPendingAdmissionExpired\(Source, int, long\)

Builds a [`DiagnosticId.PendingAdmissionExpired`](ReactorSockets.DiagnosticId.md) record for the claimed Source of an
accepted stream connection closed for sending no whole first record within its deadline.
<code>Value0</code> is the bytes of that record it had sent; <code>Value1</code> is the deadline in milliseconds.

```csharp
public static DiagnosticRecord ForPendingAdmissionExpired(Source source, int bytesReceived, long deadlineMilliseconds)
```

### Parameters

`source` [Source](ReactorSockets.Source.md)

`bytesReceived` [int](https://learn.microsoft.com/dotnet/api/system.int32)

`deadlineMilliseconds` [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

