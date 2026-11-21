# <a id="ReactorSockets_DiagnosticRecord_ForRedialFailed_ReactorSockets_PeerId_System_Net_Sockets_SocketError_System_Int64_"></a> Method ForRedialFailed

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForRedialFailed_ReactorSockets_PeerId_System_Net_Sockets_SocketError_System_Int64_"></a> ForRedialFailed\(PeerId, SocketError, long\)

Builds a [`DiagnosticId.RedialFailed`](ReactorSockets.DiagnosticId.md) record for the Peer whose re-dial attempt
could not connect, or connected and ended before the listener confirmed the Resume.
<code>Value0</code> carries the platform [`SocketError`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror) as its underlying integer value
([`Disconnecting`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror.disconnecting) for an end of stream before confirmation, or the
read's error, [`TimedOut`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror.timedout) when the listener did not answer within the
receive-idle deadline, [`SocketError`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror.socketerror) when the Resume record could not
be sent); <code>Value1</code> is the attempt number that failed.

```csharp
public static DiagnosticRecord ForRedialFailed(PeerId peer, SocketError error, long attemptNumber)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

`error` [SocketError](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror)

`attemptNumber` [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

