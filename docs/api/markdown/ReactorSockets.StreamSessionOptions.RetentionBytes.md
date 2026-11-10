# <a id="ReactorSockets_StreamSessionOptions_RetentionBytes"></a> Property RetentionBytes

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionOptions_RetentionBytes"></a> RetentionBytes

The bytes retained per Peer for the send path — retention <em>is</em> the send path (ADR 0011):
a <code>Send</code> frames the Message once and appends it to this Peer's ring, and what reaches the
socket is a copy from it. Required: this is the consumer trading their memory against their
tolerance for a stalled Peer, and the right answer depends on their traffic, not on anything
here. Must be at least [`StreamSessionOptions.MaxMessageSize`](ReactorSockets.StreamSessionOptions.MaxMessageSize.md#ReactorSockets_StreamSessionOptions_MaxMessageSize) plus the 4-byte length prefix, or a single
maximum Message could never be retained.

```csharp
public required int RetentionBytes { get; init; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)

