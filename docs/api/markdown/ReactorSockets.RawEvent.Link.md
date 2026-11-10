# <a id="ReactorSockets_RawEvent_Link"></a> Property Link

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawEvent_Link"></a> Link

The Link this event concerns. Always populated — for [`RawEventKind.Accepted`](ReactorSockets.RawEventKind.md) this
is the new connection's identity, not the listening Link's (see [`RawEvent.ListeningLink`](ReactorSockets.RawEvent.ListeningLink.md#ReactorSockets_RawEvent_ListeningLink)
for that).

```csharp
public LinkId Link { get; }
```

### Property Value

 [LinkId](ReactorSockets.LinkId.md)

