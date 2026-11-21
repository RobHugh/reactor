# <a id="ReactorSockets_DatagramSessionOptions_UnreliableQueueBudgetBytes"></a> Property UnreliableQueueBudgetBytes

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_UnreliableQueueBudgetBytes"></a> UnreliableQueueBudgetBytes

The unreliable-queue line of the memory bill in bytes: <code>Capacity × </code>[`DatagramSessionOptions.ResolvedUnreliableQueueBytes`](ReactorSockets.DatagramSessionOptions.ResolvedUnreliableQueueBytes.md#ReactorSockets_DatagramSessionOptions_ResolvedUnreliableQueueBytes),
one region per Peer, allocated at construction.

```csharp
public long UnreliableQueueBudgetBytes { get; }
```

### Property Value

 [long](https://learn.microsoft.com/dotnet/api/system.int64)

