# <a id="ReactorSockets_PeerCounters_MessagesReceived"></a> Property MessagesReceived

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_PeerCounters_MessagesReceived"></a> MessagesReceived

Whole Messages handed to the frame arena for delivery — incremented at that point only, never
when a Message's bytes finish arriving or while it sits buffered waiting for a read share or
arena room. This is what makes a share exhaustion visible: a Frame in which bytes plainly arrived
but this figure did not move is a Message still buffered, not one dropped. Not derivable: byte
figures include framing and handshake bytes and say nothing of how many Messages they carried.

```csharp
public ulong MessagesReceived { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

