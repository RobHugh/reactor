# <a id="ReactorSockets_DatagramSessionOptions_ResolvedFragmentSize"></a> Property ResolvedFragmentSize

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_ResolvedFragmentSize"></a> ResolvedFragmentSize

The resolved fragment size: [`DatagramSessionOptions.FragmentSize`](ReactorSockets.DatagramSessionOptions.FragmentSize.md#ReactorSockets_DatagramSessionOptions_FragmentSize) if set, otherwise 1200.

```csharp
public int ResolvedFragmentSize { get; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)

### Remarks

A Peer captures this value once, onto its own slot, at admission — every
[`DatagramSessionReactor.Send(PeerId, DeliveryMode, ReadOnlySpan<byte>)`](ReactorSockets.DatagramSessionReactor.Send.md#ReactorSockets_DatagramSessionReactor_Send_ReactorSockets_PeerId_ReactorSockets_DeliveryMode_System_ReadOnlySpan_System_Byte__) call for that Peer, and every retransmit, cuts by that
captured value rather than reading this property afresh, so fragment size cannot move mid-Session
even if this options instance later would resolve differently (ADR 0004).

