# <a id="ReactorSockets_DiagnosticRecord_ForAdmissionCapReached_System_Int32_System_Int32_"></a> Method ForAdmissionCapReached

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForAdmissionCapReached_System_Int32_System_Int32_"></a> ForAdmissionCapReached\(int, int\)

Builds a [`DiagnosticId.AdmissionCapReached`](ReactorSockets.DiagnosticId.md) record. No subject.
<code class="paramref">declined</code> counts declinings where the work waits out of reach and pieces of
work where the Reactor held it — see the remarks on
[`DiagnosticId.AdmissionCapReached`](ReactorSockets.DiagnosticId.md).

```csharp
public static DiagnosticRecord ForAdmissionCapReached(int cap, int declined)
```

### Parameters

`cap` [int](https://learn.microsoft.com/dotnet/api/system.int32)

`declined` [int](https://learn.microsoft.com/dotnet/api/system.int32)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

