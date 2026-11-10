# <a id="ReactorSockets_DatagramSessionOptions_MaxMessageSize"></a> Property MaxMessageSize

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_MaxMessageSize"></a> MaxMessageSize

The largest Message, in bytes, this Reactor will accept or deliver. Required: it is a
statement about the consumer's own protocol that nothing here can derive. It is also the knob
that multiplies this profile's memory bill in more than one direction — the class remarks on
[`DatagramSessionOptions`](ReactorSockets.DatagramSessionOptions.md) say what it floors and what raising it costs.

```csharp
public required int MaxMessageSize { get; init; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)

### Remarks

Announced to the peer at the handshake precisely so an oversized claim can be refused before
this end holds anything for it: a declared Fragment <code>fragmentCount</code> larger than this
Reactor's receive window ([`DatagramSessionOptions.ResolvedReceiveWindowFragments`](ReactorSockets.DatagramSessionOptions.ResolvedReceiveWindowFragments.md#ReactorSockets_DatagramSessionOptions_ResolvedReceiveWindowFragments)) is closed on its first
Fragment, before any receive-window slot or Fragment-group bookkeeping exists for it. The bound
is the window rather than [`DatagramSessionOptions.ReceiveWindowFloor`](ReactorSockets.DatagramSessionOptions.ReceiveWindowFloor.md#ReactorSockets_DatagramSessionOptions_ReceiveWindowFloor) alone, because the floor is computed
from this end's own [`DatagramSessionOptions.ResolvedFragmentSize`](ReactorSockets.DatagramSessionOptions.ResolvedFragmentSize.md#ReactorSockets_DatagramSessionOptions_ResolvedFragmentSize) and fragment size is unilateral and
per-direction (ADR 0004): a conforming peer with a smaller fragment size legally cuts the same,
within-limit Message into more Fragments than the floor. `Validate` already requires
the window to be at least the floor, so a set at exactly this value is always admitted.

