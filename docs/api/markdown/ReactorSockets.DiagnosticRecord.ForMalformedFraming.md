# <a id="ReactorSockets_DiagnosticRecord_ForMalformedFraming_ReactorSockets_PeerId_System_Int64_System_Int64_"></a> Method ForMalformedFraming

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForMalformedFraming_ReactorSockets_PeerId_System_Int64_System_Int64_"></a> ForMalformedFraming\(PeerId, long, long\)

Builds a [`DiagnosticId.MalformedFraming`](ReactorSockets.DiagnosticId.md) record for the Peer whose framing broke
this profile's rules. <code>Value0</code> is the offending length prefix — 0 for a truncation, where
no full prefix was ever read; <code>Value1</code> is this Reactor's own <code>MaxMessageSize</code>.

```csharp
public static DiagnosticRecord ForMalformedFraming(PeerId peer, long announcedLength, long maxMessageSize)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

`announcedLength` [long](https://learn.microsoft.com/dotnet/api/system.int64)

`maxMessageSize` [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

