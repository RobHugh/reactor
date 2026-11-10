# <a id="ReactorSockets_StreamSessionReactor_Dispose"></a> Method Dispose

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionReactor_Dispose"></a> Dispose\(\)

Releases every socket this Reactor bound or accepted. A Reactor owns every socket it bound, so
a consumer that drops one without disposing it leaks those sockets until each
[`Socket`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socket)'s finalizer runs. No [`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md) events are
queued: nothing can drain a disposed Reactor. Idempotent; every other member throws
[`ObjectDisposedException`](https://learn.microsoft.com/dotnet/api/system.objectdisposedexception) afterwards.

```csharp
public void Dispose()
```

### Remarks

[`StreamSessionOptions.Entropy`](ReactorSockets.StreamSessionOptions.Entropy.md#ReactorSockets_StreamSessionOptions_Entropy) is deliberately not disposed. It may be a source the
consumer holds and uses elsewhere, and a Reactor disposing a value it was merely handed would be
reaching outside what it owns.

