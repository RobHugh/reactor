# <a id="ReactorSockets_DatagramSessionOptions_SendBudgetBytes"></a> Property SendBudgetBytes

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_SendBudgetBytes"></a> SendBudgetBytes

The send line of the memory bill in bytes, reported the same way: [`DatagramSessionOptions.RetentionSlots`](ReactorSockets.DatagramSessionOptions.RetentionSlots.md#ReactorSockets_DatagramSessionOptions_RetentionSlots)
whole fragment slots per reliable Space per Peer, each priced at its payload plus the 15 bytes
of bookkeeping that records what it holds. Exact, like the receive line, so it can be below
<code>Capacity × 2 × RetentionBytes</code> when retention is not a whole number of fragments.

```csharp
public long SendBudgetBytes { get; }
```

### Property Value

 [long](https://learn.microsoft.com/dotnet/api/system.int64)

