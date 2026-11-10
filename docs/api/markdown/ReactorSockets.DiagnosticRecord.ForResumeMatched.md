# <a id="ReactorSockets_DiagnosticRecord_ForResumeMatched_ReactorSockets_PeerId_System_Int64_"></a> Method ForResumeMatched

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForResumeMatched_ReactorSockets_PeerId_System_Int64_"></a> ForResumeMatched\(PeerId, long\)

Builds a [`DiagnosticId.ResumeMatched`](ReactorSockets.DiagnosticId.md) record for the Peer a valid resume secret was
matched back onto. <code>Value0</code> is the number of retained outbound bytes the match rewound;
<code>Value1</code> is unused.

```csharp
public static DiagnosticRecord ForResumeMatched(PeerId peer, long bytesRewound)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

`bytesRewound` [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

