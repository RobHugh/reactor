# <a id="ReactorSockets_DiagnosticRecord_ForDestinationUnreachable_ReactorSockets_LinkId_System_Net_Sockets_SocketError_"></a> Method ForDestinationUnreachable

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForDestinationUnreachable_ReactorSockets_LinkId_System_Net_Sockets_SocketError_"></a> ForDestinationUnreachable\(LinkId, SocketError\)

Builds a [`DiagnosticId.DestinationUnreachable`](ReactorSockets.DiagnosticId.md) record carrying the platform
[`SocketError`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror) reported for the send.

```csharp
public static DiagnosticRecord ForDestinationUnreachable(LinkId link, SocketError error)
```

### Parameters

`link` [LinkId](ReactorSockets.LinkId.md)

`error` [SocketError](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

