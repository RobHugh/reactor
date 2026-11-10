# <a id="ReactorSockets_StreamSessionOptions_ReceiveBufferFloor"></a> Property ReceiveBufferFloor

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionOptions_ReceiveBufferFloor"></a> ReceiveBufferFloor

The floor both [`StreamSessionOptions.RetentionBytes`](ReactorSockets.StreamSessionOptions.RetentionBytes.md#ReactorSockets_StreamSessionOptions_RetentionBytes) and the receive buffer are held to: one maximum Message plus its length prefix.

```csharp
public int ReceiveBufferFloor { get; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)

