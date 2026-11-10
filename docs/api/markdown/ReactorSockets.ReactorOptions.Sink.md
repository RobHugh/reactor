# <a id="ReactorSockets_ReactorOptions_Sink"></a> Property Sink

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_ReactorOptions_Sink"></a> Sink

The Diagnostic Sink records are sent to, or <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/null">null</a> to disable diagnostics
entirely. Must be supplied together with [`ReactorOptions.MinimumLevel`](ReactorSockets.ReactorOptions.MinimumLevel.md#ReactorSockets_ReactorOptions_MinimumLevel) — both present or both
absent. Provenance: absent means off, which is derived rather than chosen — with no Sink there
is nowhere for a record to go, so no level can matter (ADR 0012).

```csharp
public IReactorDiagnostics? Sink { get; init; }
```

### Property Value

 [IReactorDiagnostics](ReactorSockets.IReactorDiagnostics.md)?

