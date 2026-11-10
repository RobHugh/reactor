# <a id="ReactorSockets_DatagramSessionOptions_ReceiveWindowFragments"></a> Property ReceiveWindowFragments

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_ReceiveWindowFragments"></a> ReceiveWindowFragments

The fragments each reliable Space may have outstanding per Peer. Defaults to exactly the
number a maximum Message occupies. Provenance: derived from [`DatagramSessionOptions.MaxMessageSize`](ReactorSockets.DatagramSessionOptions.MaxMessageSize.md#ReactorSockets_DatagramSessionOptions_MaxMessageSize) and
[`DatagramSessionOptions.FragmentSize`](ReactorSockets.DatagramSessionOptions.FragmentSize.md#ReactorSockets_DatagramSessionOptions_FragmentSize) — ADR 0004's deadlock floor rather than a performance choice: a window narrower than one whole Message can never deliver one, so it can
never advance, so the sender can never send the rest. This resolved value is also the inbound
bound a declared Fragment <code>fragmentCount</code> is checked against: a set larger than the window
can never be reassembled, since there is no slot to hold every one of its pieces at once, so
[`DatagramSessionReactor`](ReactorSockets.DatagramSessionReactor.md) closes it before any is held.

```csharp
public int? ReceiveWindowFragments { get; init; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)?

