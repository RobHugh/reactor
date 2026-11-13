# <a id="ReactorSockets_StreamSessionOptions_MaxMessageSize"></a> Property MaxMessageSize

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionOptions_MaxMessageSize"></a> MaxMessageSize

The largest Message, in bytes, this Reactor will accept or deliver. Required: it is a
statement about the consumer's own protocol that nothing here can derive. It is also
announced to the remote as a receive limit, and floors both the receive buffer and
[`StreamSessionOptions.RetentionBytes`](ReactorSockets.StreamSessionOptions.RetentionBytes.md#ReactorSockets_StreamSessionOptions_RetentionBytes) — an arena that cannot hold one maximum Message could never
deliver one. At most 2 147 483 587, [`MaxLength`](https://learn.microsoft.com/dotnet/api/system.array.maxlength) less the 4-byte length prefix,
so one framed maximum Message fits one array.

```csharp
public required int MaxMessageSize { get; init; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)

