# <a id="ReactorSockets_SendResult_IsSuccess"></a> Property IsSuccess

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_SendResult_IsSuccess"></a> IsSuccess

Whether the call itself succeeded — not whether every offered byte was taken. A short send
([`SendResult.BytesSent`](ReactorSockets.SendResult.BytesSent.md#ReactorSockets_SendResult_BytesSent) less than what was offered) is still <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/bool">true</a>: it arms
exactly one [`RawEventKind.Writable`](ReactorSockets.RawEventKind.md) for this Link and is otherwise ordinary. This is
<a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/bool">false</a> only for an illegal call (wrong Link kind, a stale [`LinkId`](ReactorSockets.LinkId.md),
a Link already shut down for writing) or a socket-level failure.

```csharp
public bool IsSuccess { get; }
```

### Property Value

 [bool](https://learn.microsoft.com/dotnet/api/system.boolean)

