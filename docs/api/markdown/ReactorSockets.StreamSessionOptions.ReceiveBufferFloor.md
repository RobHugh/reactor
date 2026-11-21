# <a id="ReactorSockets_StreamSessionOptions_ReceiveBufferFloor"></a> Property ReceiveBufferFloor

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionOptions_ReceiveBufferFloor"></a> ReceiveBufferFloor

The floor both [`StreamSessionOptions.RetentionBytes`](ReactorSockets.StreamSessionOptions.RetentionBytes.md#ReactorSockets_StreamSessionOptions_RetentionBytes) and the receive buffer are held to: one maximum
Message plus its length prefix. The sum is computed in <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/integral-numeric-types">long</a> and saturates at
[`MaxValue`](https://learn.microsoft.com/dotnet/api/system.int32.maxvalue), so on options not yet validated a [`StreamSessionOptions.MaxMessageSize`](ReactorSockets.StreamSessionOptions.MaxMessageSize.md#ReactorSockets_StreamSessionOptions_MaxMessageSize) near
[`MaxValue`](https://learn.microsoft.com/dotnet/api/system.int32.maxvalue) reads as [`MaxValue`](https://learn.microsoft.com/dotnet/api/system.int32.maxvalue) rather than wrapping negative.
Construction rejects any [`StreamSessionOptions.MaxMessageSize`](ReactorSockets.StreamSessionOptions.MaxMessageSize.md#ReactorSockets_StreamSessionOptions_MaxMessageSize) whose floor would saturate.

```csharp
public int ReceiveBufferFloor { get; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)

