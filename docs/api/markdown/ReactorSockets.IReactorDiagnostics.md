# <a id="ReactorSockets_IReactorDiagnostics"></a> Interface IReactorDiagnostics

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

The Diagnostic Sink contract. Implementations are called synchronously, on the consumer's
thread, inside <code>Pump</code> — a Sink that stalls stalls the Frame, and a Sink that throws is
caught, counted as <code>SinkFaulted</code>, and disabled for the life of the Reactor (ADR 0005).
Calling back into the Reactor from a Sink is a programmer error and throws.

```csharp
public interface IReactorDiagnostics
```

## Methods

 [IsEnabled\(DiagnosticLevel\)](ReactorSockets.IReactorDiagnostics.IsEnabled.md\#ReactorSockets\_IReactorDiagnostics\_IsEnabled\_ReactorSockets\_DiagnosticLevel\_)

Gates construction of a [`DiagnosticRecord`](ReactorSockets.DiagnosticRecord.md) before it is built, so a disabled
level costs a comparison and never reaches [`IReactorDiagnostics.Log(in DiagnosticRecord)`](ReactorSockets.IReactorDiagnostics.Log.md#ReactorSockets_IReactorDiagnostics_Log_ReactorSockets_DiagnosticRecord__).

 [Log\(in DiagnosticRecord\)](ReactorSockets.IReactorDiagnostics.Log.md\#ReactorSockets\_IReactorDiagnostics\_Log\_ReactorSockets\_DiagnosticRecord\_\_)

Receives a record already known to satisfy [`IReactorDiagnostics.IsEnabled(DiagnosticLevel)`](ReactorSockets.IReactorDiagnostics.IsEnabled.md#ReactorSockets_IReactorDiagnostics_IsEnabled_ReactorSockets_DiagnosticLevel_).

