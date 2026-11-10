# <a id="ReactorSockets_StreamSessionOptions_ReceiveBudgetBytes"></a> Property ReceiveBudgetBytes

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionOptions_ReceiveBudgetBytes"></a> ReceiveBudgetBytes

The receive line of the memory bill in bytes, as a <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/integral-numeric-types">long</a> so that a
combination too large to allocate is a value this can report rather than an overflow it
silently wraps through.

```csharp
public long ReceiveBudgetBytes { get; }
```

### Property Value

 [long](https://learn.microsoft.com/dotnet/api/system.int64)

