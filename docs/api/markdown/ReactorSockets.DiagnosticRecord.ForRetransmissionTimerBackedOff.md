# <a id="ReactorSockets_DiagnosticRecord_ForRetransmissionTimerBackedOff_ReactorSockets_PeerId_System_Int64_System_Int64_"></a> Method ForRetransmissionTimerBackedOff

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForRetransmissionTimerBackedOff_ReactorSockets_PeerId_System_Int64_System_Int64_"></a> ForRetransmissionTimerBackedOff\(PeerId, long, long\)

Builds a [`DiagnosticId.RetransmissionTimerBackedOff`](ReactorSockets.DiagnosticId.md) record for the Peer whose
`RetransmissionTimer` just backed off. <code>Value0</code> is the new RTO in milliseconds;
<code>Value1</code> is the consecutive backoff count after this backoff.

```csharp
public static DiagnosticRecord ForRetransmissionTimerBackedOff(PeerId peer, long newRtoMilliseconds, long consecutiveBackoffs)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

`newRtoMilliseconds` [long](https://learn.microsoft.com/dotnet/api/system.int64)

`consecutiveBackoffs` [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

