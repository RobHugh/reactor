# <a id="ReactorSockets_DiagnosticRecord_ForCapacityExhausted_System_Int32_System_Int32_"></a> Method ForCapacityExhausted

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForCapacityExhausted_System_Int32_System_Int32_"></a> ForCapacityExhausted\(int, int\)

Builds a [`DiagnosticId.CapacityExhausted`](ReactorSockets.DiagnosticId.md) record. No subject.
<code class="paramref">declined</code> is counted exactly as
[`DiagnosticRecord.ForAdmissionCapReached(int, int)`](ReactorSockets.DiagnosticRecord.ForAdmissionCapReached.md#ReactorSockets_DiagnosticRecord_ForAdmissionCapReached_System_Int32_System_Int32_)'s is.

```csharp
public static DiagnosticRecord ForCapacityExhausted(int capacity, int declined)
```

### Parameters

`capacity` [int](https://learn.microsoft.com/dotnet/api/system.int32)

`declined` [int](https://learn.microsoft.com/dotnet/api/system.int32)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

