# <a id="ReactorSockets_DiagnosticRecord_ForLinkFailed_ReactorSockets_LinkId_System_Net_Sockets_SocketError_"></a> Method ForLinkFailed

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForLinkFailed_ReactorSockets_LinkId_System_Net_Sockets_SocketError_"></a> ForLinkFailed\(LinkId, SocketError\)

Builds a [`DiagnosticId.LinkFailed`](ReactorSockets.DiagnosticId.md) record carrying the platform
[`SocketError`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror) that caused the failure.

```csharp
public static DiagnosticRecord ForLinkFailed(LinkId link, SocketError error)
```

### Parameters

`link` [LinkId](ReactorSockets.LinkId.md)

`error` [SocketError](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

