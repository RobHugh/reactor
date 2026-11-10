# <a id="ReactorSockets_StreamSessionOptions_ReceiveBufferSize"></a> Property ReceiveBufferSize

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionOptions_ReceiveBufferSize"></a> ReceiveBufferSize

The bytes held per Peer for an incoming, not-yet-complete Message. Defaults to
[`StreamSessionOptions.MaxMessageSize`](ReactorSockets.StreamSessionOptions.MaxMessageSize.md#ReactorSockets_StreamSessionOptions_MaxMessageSize) plus the 4-byte length prefix. Provenance: derived from
[`StreamSessionOptions.MaxMessageSize`](ReactorSockets.StreamSessionOptions.MaxMessageSize.md#ReactorSockets_StreamSessionOptions_MaxMessageSize) — its own floor, since that is exactly one incomplete maximum
Message and nothing narrower could ever hold one.

```csharp
public int? ReceiveBufferSize { get; init; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)?

