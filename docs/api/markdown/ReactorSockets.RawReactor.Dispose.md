# <a id="ReactorSockets_RawReactor_Dispose"></a> Method Dispose

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawReactor_Dispose"></a> Dispose\(\)

Closes every Link still open and releases its socket. A Reactor owns every socket it bound, so
a consumer that drops one without disposing it leaks those sockets until the finalizer each
[`Socket`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socket) carries runs. No [`RawEventKind.Closed`](ReactorSockets.RawEventKind.md) events are queued:
nothing can drain a disposed Reactor. Idempotent; every other member throws
[`ObjectDisposedException`](https://learn.microsoft.com/dotnet/api/system.objectdisposedexception) afterwards.

```csharp
public void Dispose()
```

### Remarks

There is deliberately no finalizer here. This type holds nothing unmanaged of its own — the
sockets do, and each already finalizes itself — so a finalizer would add a GC cost to every
Reactor to duplicate a guarantee the runtime already makes.

