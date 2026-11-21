# <a id="ReactorSockets_DiagnosticRecord_ForPendingAdmissionLimitReached_System_Int32_System_Int32_"></a> Method ForPendingAdmissionLimitReached

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForPendingAdmissionLimitReached_System_Int32_System_Int32_"></a> ForPendingAdmissionLimitReached\(int, int\)

Builds a [`DiagnosticId.PendingAdmissionLimitReached`](ReactorSockets.DiagnosticId.md) record. No subject. Carries the
held-connection limit and the Admissions declined this Pump, counted as
[`DiagnosticRecord.ForAdmissionCapReached(int, int)`](ReactorSockets.DiagnosticRecord.ForAdmissionCapReached.md#ReactorSockets_DiagnosticRecord_ForAdmissionCapReached_System_Int32_System_Int32_)'s are.

```csharp
public static DiagnosticRecord ForPendingAdmissionLimitReached(int limit, int declined)
```

### Parameters

`limit` [int](https://learn.microsoft.com/dotnet/api/system.int32)

`declined` [int](https://learn.microsoft.com/dotnet/api/system.int32)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

