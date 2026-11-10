# <a id="ReactorSockets_RawReactor_Pump"></a> Method Pump

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawReactor_Pump"></a> Pump\(\)

One synchronous advance: wipes the previous Frame's events and arena, then runs the readiness
dispatch that reads whatever registered Link is ready right now. Throws
[`InvalidOperationException`](https://learn.microsoft.com/dotnet/api/system.invalidoperationexception) if called from any thread other than the one that
constructed this Reactor, or from inside a Pump already in progress.

```csharp
public void Pump()
```

