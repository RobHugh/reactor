# <a id="ReactorSockets_SendResult_BytesSent"></a> Property BytesSent

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_SendResult_BytesSent"></a> BytesSent

The number of bytes the socket actually took this call. Zero when [`SendResult.IsSuccess`](ReactorSockets.SendResult.IsSuccess.md#ReactorSockets_SendResult_IsSuccess) is
<a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/bool">false</a>. Never queued or copied by the Reactor — whatever this does not account
for out of the offered payload is still the caller's to keep and retry.

```csharp
public int BytesSent { get; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)

