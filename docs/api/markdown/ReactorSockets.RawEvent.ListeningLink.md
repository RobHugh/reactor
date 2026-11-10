# <a id="ReactorSockets_RawEvent_ListeningLink"></a> Property ListeningLink

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawEvent_ListeningLink"></a> ListeningLink

The listening Link that produced this accepted connection. Populated only when
[`RawEvent.Kind`](ReactorSockets.RawEvent.Kind.md#ReactorSockets_RawEvent_Kind) is [`RawEventKind.Accepted`](ReactorSockets.RawEventKind.md); a consumer listening on several
Links needs this to know which one just fired. Named for the Link rather than a "Listener"
deliberately: listening is a role a Link plays, not a kind of thing this library names.

```csharp
public LinkId ListeningLink { get; }
```

### Property Value

 [LinkId](ReactorSockets.LinkId.md)

