# <a id="ReactorSockets_DiagnosticRecord_ForOversizedMessage_ReactorSockets_PeerId_System_Int64_System_Int64_"></a> Method ForOversizedMessage

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForOversizedMessage_ReactorSockets_PeerId_System_Int64_System_Int64_"></a> ForOversizedMessage\(PeerId, long, long\)

Builds a [`DiagnosticId.OversizedMessage`](ReactorSockets.DiagnosticId.md) record for the Peer whose completed Message
was larger than this Reactor's own <code>MaxMessageSize</code>, carrying the Message's length and that
limit.

```csharp
public static DiagnosticRecord ForOversizedMessage(PeerId peer, long messageLength, long maxMessageSize)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

`messageLength` [long](https://learn.microsoft.com/dotnet/api/system.int64)

`maxMessageSize` [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

