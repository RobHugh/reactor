# <a id="ReactorSockets_DatagramSessionOptions_HeldRecordBudgetBytes"></a> Property HeldRecordBudgetBytes

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_HeldRecordBudgetBytes"></a> HeldRecordBudgetBytes

The most heap, in bytes, a Reactor's <code>ReliableOrdered</code> Spaces can hold in ordinary records
waiting behind a gap, across all its Peers: <code>Capacity × 2 × ReceiveWindowFragments ×
FragmentSize</code>, with each slot's bookkeeping. A figure above [`MaxValue`](https://learn.microsoft.com/dotnet/api/system.int64.maxvalue) reads as
[`MaxValue`](https://learn.microsoft.com/dotnet/api/system.int64.maxvalue).

```csharp
public long HeldRecordBudgetBytes { get; }
```

### Property Value

 [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Remarks

This is a ceiling reached only under traffic, not a line allocated at construction, so
`ValidateMemoryBill` does not count it. It is over and above
[`DatagramSessionOptions.ReceiveBudgetBytes`](ReactorSockets.DatagramSessionOptions.ReceiveBudgetBytes.md#ReactorSockets_DatagramSessionOptions_ReceiveBudgetBytes) and about the same size. A record past either limit is left
unacknowledged and counted on [`PeerCounters.OutOfWindowPackets`](ReactorSockets.PeerCounters.OutOfWindowPackets.md#ReactorSockets_PeerCounters_OutOfWindowPackets), so the far end's
retransmission brings it back when the Space has moved. The figure counts what stays held, not
the short-lived arrays the ring leaves behind each time it grows.

