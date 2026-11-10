# <a id="ReactorSockets_DiagnosticRecord_ForRedialAttempted_ReactorSockets_PeerId_System_Int64_System_Int64_"></a> Method ForRedialAttempted

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForRedialAttempted_ReactorSockets_PeerId_System_Int64_System_Int64_"></a> ForRedialAttempted\(PeerId, long, long\)

Builds a [`DiagnosticId.RedialAttempted`](ReactorSockets.DiagnosticId.md) record for the Peer beginning a fresh
re-dial attempt. <code>Value0</code> is the attempt number; <code>Value1</code> is the delay in
milliseconds this attempt actually waited before dialling — the jittered figure, not the
computed delay before jitter.

```csharp
public static DiagnosticRecord ForRedialAttempted(PeerId peer, long attemptNumber, long delayMilliseconds)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

`attemptNumber` [long](https://learn.microsoft.com/dotnet/api/system.int64)

`delayMilliseconds` [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

