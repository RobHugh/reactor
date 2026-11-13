# <a id="ReactorSockets_DatagramSessionOptions_UnreliableQueueBytes"></a> Property UnreliableQueueBytes

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_UnreliableQueueBytes"></a> UnreliableQueueBytes

The most bytes one Peer may have queued for [`DeliveryMode.Unreliable`](ReactorSockets.DeliveryMode.md) and
[`DeliveryMode.UnreliableSequenced`](ReactorSockets.DeliveryMode.md) until a Pump writes them, counting each Message's payload plus
its 7-byte record header. A Send that would go past it returns [`SendOutcome.SendQueueFull`](ReactorSockets.SendOutcome.md).
<a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/null">null</a> means [`DatagramSessionOptions.RetentionSlots`](ReactorSockets.DatagramSessionOptions.RetentionSlots.md#ReactorSockets_DatagramSessionOptions_RetentionSlots) × ([`DatagramSessionOptions.ResolvedFragmentSize`](ReactorSockets.DatagramSessionOptions.ResolvedFragmentSize.md#ReactorSockets_DatagramSessionOptions_ResolvedFragmentSize) + 7):
as many full unreliable Messages as one reliable Space retains Fragments. Provenance: derived from
[`DatagramSessionOptions.RetentionBytes`](ReactorSockets.DatagramSessionOptions.RetentionBytes.md#ReactorSockets_DatagramSessionOptions_RetentionBytes) and [`DatagramSessionOptions.FragmentSize`](ReactorSockets.DatagramSessionOptions.FragmentSize.md#ReactorSockets_DatagramSessionOptions_FragmentSize), so the unreliable queue holds as much as
a reliable Space does. Must be at least
[`DatagramSessionOptions.ResolvedFragmentSize`](ReactorSockets.DatagramSessionOptions.ResolvedFragmentSize.md#ReactorSockets_DatagramSessionOptions_ResolvedFragmentSize) + 7, one full unreliable Message.

```csharp
public int? UnreliableQueueBytes { get; init; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)?

### Remarks

The queue is preallocated, [`DatagramSessionOptions.UnreliableQueueBudgetBytes`](ReactorSockets.DatagramSessionOptions.UnreliableQueueBudgetBytes.md#ReactorSockets_DatagramSessionOptions_UnreliableQueueBudgetBytes) in total, so an unreliable Send
allocates nothing. A Pump writes up to one maximum UDP payload's worth of Datagrams per Peer, so a
budget above about 65 507 bytes takes more than one Pump to drain.

