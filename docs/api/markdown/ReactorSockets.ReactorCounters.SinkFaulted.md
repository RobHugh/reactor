# <a id="ReactorSockets_ReactorCounters_SinkFaulted"></a> Property SinkFaulted

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_ReactorCounters_SinkFaulted"></a> SinkFaulted

Times the installed Sink threw and was disabled for the Reactor's life. Not derivable: the Sink is
the only other place this could be reported, and it is the thing that failed.

```csharp
public ulong SinkFaulted { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

