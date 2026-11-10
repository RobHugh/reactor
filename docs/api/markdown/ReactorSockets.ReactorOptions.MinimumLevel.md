# <a id="ReactorSockets_ReactorOptions_MinimumLevel"></a> Property MinimumLevel

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_ReactorOptions_MinimumLevel"></a> MinimumLevel

The minimum [`DiagnosticLevel`](ReactorSockets.DiagnosticLevel.md) the installed [`ReactorOptions.Sink`](ReactorSockets.ReactorOptions.Sink.md#ReactorSockets_ReactorOptions_Sink) receives.
Must be supplied together with [`ReactorOptions.Sink`](ReactorSockets.ReactorOptions.Sink.md#ReactorSockets_ReactorOptions_Sink) — neither carries a default, since a
default level would be a guess at how much diagnostic volume the consumer wants. Provenance:
absent only alongside an absent [`ReactorOptions.Sink`](ReactorSockets.ReactorOptions.Sink.md#ReactorSockets_ReactorOptions_Sink), where no level can matter (ADR 0012).

```csharp
public DiagnosticLevel? MinimumLevel { get; init; }
```

### Property Value

 [DiagnosticLevel](ReactorSockets.DiagnosticLevel.md)?

