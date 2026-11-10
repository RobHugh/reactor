# <a id="ReactorSockets_ReactorOptions_TimeProvider"></a> Property TimeProvider

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_ReactorOptions_TimeProvider"></a> TimeProvider

The source of monotonic time for every duration this library measures. Defaults to
[`System`](https://learn.microsoft.com/dotnet/api/system.timeprovider.system); only <code>GetTimestamp</code>/<code>GetElapsedTime</code>
are ever consulted, never <code>GetUtcNow</code>. Provenance: the framework's own clock, and the only
requirement on it is that reads be monotonic, which <code>GetTimestamp</code> is (ADR 0012).

```csharp
public TimeProvider TimeProvider { get; init; }
```

### Property Value

 [TimeProvider](https://learn.microsoft.com/dotnet/api/system.timeprovider)

### Remarks

The Raw profile measures no durations and never reads this. It lives on the shared options
value because it is the same knob every profile takes, and the Session profiles' liveness
deadlines, retransmit timers and re-dial schedules read it.

