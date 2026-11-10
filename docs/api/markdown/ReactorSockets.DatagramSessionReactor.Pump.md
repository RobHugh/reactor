# <a id="ReactorSockets_DatagramSessionReactor_Pump"></a> Method Pump

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionReactor_Pump"></a> Pump\(\)

One synchronous advance: records this call's arrival against the last one
(`ObservedPumpInterval`) before anything else, then wipes the previous Frame's
events and arena, then reads whatever bound socket is ready right now, running the handshake for
everything that arrived. Throws [`InvalidOperationException`](https://learn.microsoft.com/dotnet/api/system.invalidoperationexception) if called from any
thread other than the one that constructed this Reactor, or from inside a Pump already in
progress.

```csharp
public void Pump()
```

