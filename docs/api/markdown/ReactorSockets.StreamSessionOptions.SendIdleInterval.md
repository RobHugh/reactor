# <a id="ReactorSockets_StreamSessionOptions_SendIdleInterval"></a> Property SendIdleInterval

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionOptions_SendIdleInterval"></a> SendIdleInterval

How long a Session may go without sending before it sends a heartbeat instead. Required:
shorter costs bandwidth on an idle link, longer costs time to notice a dead Peer, and only the
consumer knows which they would rather spend. At most 2^32 - 1 milliseconds (about 49.7 days), the
most a handshake may announce.

```csharp
public required TimeSpan SendIdleInterval { get; init; }
```

### Property Value

 [TimeSpan](https://learn.microsoft.com/dotnet/api/system.timespan)

