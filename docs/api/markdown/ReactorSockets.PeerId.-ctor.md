# <a id="ReactorSockets_PeerId__ctor_System_UInt32_System_Int32_System_UInt64_"></a> Constructor PeerId

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_PeerId__ctor_System_UInt32_System_Int32_System_UInt64_"></a> PeerId\(uint, int, ulong\)

A Peer's identity inside this process: the Reactor that issued it, a zero-based Slot index, and a
generation counter that increments when the Slot is reused, so a stale [`PeerId`](ReactorSockets.PeerId.md) held
past a Peer's close compares unequal to whatever now occupies that Slot (ADR 0007) rather than
aliasing it. Shared by both Session profiles (ADR 0010), exactly as [`LinkId`](ReactorSockets.LinkId.md) is the
Raw profile's.

```csharp
public PeerId(uint Origin, int Index, ulong Generation)
```

### Parameters

`Origin` [uint](https://learn.microsoft.com/dotnet/api/system.uint32)

Which Reactor issued this [`PeerId`](ReactorSockets.PeerId.md). Opaque and process-local — it is compared for
equality and nothing else, orders nothing, and never appears on the wire. Never 0: that value is
reserved and no Reactor is ever issued it, so a <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/default">default</a> [`PeerId`](ReactorSockets.PeerId.md)
addresses nothing twice over. Not a [`Source`](ReactorSockets.Source.md), which names a remote address; this
names a Reactor of this process.

`Index` [int](https://learn.microsoft.com/dotnet/api/system.int32)

The zero-based Slot this Peer occupies.

`Generation` [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

The generation that Slot was on when this Peer was admitted. Live Slots begin at 1, so a
<a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/default">default</a> [`PeerId`](ReactorSockets.PeerId.md) — generation 0 — addresses nothing.

### Remarks

This is deliberately not the identity a datagram Session puts on the wire. A Slot index is dense,
small and guessable, and a remote that can guess one can address a Peer it was never introduced
to; the wire identity is instead a receiver-assigned random 32-bit value the two ends exchange
during the handshake, and it never appears on this type. The generation is
<a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/integral-numeric-types">ulong</a> rather than <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/integral-numeric-types">int</a> so that it cannot wrap within the life
of any process, which is what lets generation 0 stay a reserved sentinel without a check guarding
it (ADR 0015).

<p>
The Origin is what makes a [`PeerId`](ReactorSockets.PeerId.md) mean something to exactly one Reactor. Without
it, index and generation are only unique within a single Reactor's table, and every table's first
Peer is Slot 0 generation 1 — so one Reactor's [`PeerId`](ReactorSockets.PeerId.md) passed to another's
<code>Close</code> named a live Peer of that second Reactor and closed it. A consumer running two
Reactors gets the mistake back as nothing happening, which is what an id addressing no Peer has
always meant here.
</p>

