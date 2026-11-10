# <a id="ReactorSockets_IReactorDiagnostics_IsEnabled_ReactorSockets_DiagnosticLevel_"></a> Method IsEnabled

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_IReactorDiagnostics_IsEnabled_ReactorSockets_DiagnosticLevel_"></a> IsEnabled\(DiagnosticLevel\)

Gates construction of a [`DiagnosticRecord`](ReactorSockets.DiagnosticRecord.md) before it is built, so a disabled
level costs a comparison and never reaches [`IReactorDiagnostics.Log(in DiagnosticRecord)`](ReactorSockets.IReactorDiagnostics.Log.md#ReactorSockets_IReactorDiagnostics_Log_ReactorSockets_DiagnosticRecord__).

```csharp
bool IsEnabled(DiagnosticLevel level)
```

### Parameters

`level` [DiagnosticLevel](ReactorSockets.DiagnosticLevel.md)

### Returns

 [bool](https://learn.microsoft.com/dotnet/api/system.boolean)

