# <a id="ReactorSockets_RawReactor_RemoteOf_ReactorSockets_LinkId_"></a> Method RemoteOf

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawReactor_RemoteOf_ReactorSockets_LinkId_"></a> RemoteOf\(LinkId\)

Returns the remote address of the stream Link <code class="paramref">link</code> — Link state, constant
for the Link's lifetime and therefore not event data, which is exactly why
[`RawEventKind.Accepted`](ReactorSockets.RawEventKind.md) carries no address (ADR 0014). Illegal on a datagram or
listening Link, since neither has a single remote for its whole lifetime the way a stream
connection does — a bound UDP socket answers many senders, and a listening socket answers
none. Throws [`ArgumentException`](https://learn.microsoft.com/dotnet/api/system.argumentexception) if <code class="paramref">link</code> does not address an
open Link, or does but is the wrong kind, for the reason given on this type's own remarks: a
member that returns data has no [`Result`](ReactorSockets.Result.md) channel to report a wrong-kind call
through instead.

```csharp
public IPEndPoint RemoteOf(LinkId link)
```

### Parameters

`link` [LinkId](ReactorSockets.LinkId.md)

### Returns

 [IPEndPoint](https://learn.microsoft.com/dotnet/api/system.net.ipendpoint)

