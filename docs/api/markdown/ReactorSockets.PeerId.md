# <a id="ReactorSockets_PeerId"></a> Struct PeerId

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

A Peer's identity inside this process: the Reactor that issued it, a zero-based Slot index, and a
generation counter that increments when the Slot is reused, so a stale [`PeerId`](ReactorSockets.PeerId.md) held
past a Peer's close compares unequal to whatever now occupies that Slot (ADR 0007) rather than
aliasing it. Shared by both Session profiles (ADR 0010), exactly as [`LinkId`](ReactorSockets.LinkId.md) is the
Raw profile's.

```csharp
public readonly record struct PeerId : IEquatable<PeerId>
```

#### Implements

[IEquatable<PeerId\>](https://learn.microsoft.com/dotnet/api/system.iequatable\-1)

#### Inherited Members

[object.Equals\(object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\)), 
[object.Equals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\-system\-object\)), 
[object.GetHashCode\(\)](https://learn.microsoft.com/dotnet/api/system.object.gethashcode), 
[object.GetType\(\)](https://learn.microsoft.com/dotnet/api/system.object.gettype), 
[object.ReferenceEquals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.referenceequals), 
[object.ToString\(\)](https://learn.microsoft.com/dotnet/api/system.object.tostring)

## Remarks

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

## Constructors

 [PeerId\(uint, int, ulong\)](ReactorSockets.PeerId.\-ctor.md\#ReactorSockets\_PeerId\_\_ctor\_System\_UInt32\_System\_Int32\_System\_UInt64\_)

A Peer's identity inside this process: the Reactor that issued it, a zero-based Slot index, and a
generation counter that increments when the Slot is reused, so a stale [`PeerId`](ReactorSockets.PeerId.md) held
past a Peer's close compares unequal to whatever now occupies that Slot (ADR 0007) rather than
aliasing it. Shared by both Session profiles (ADR 0010), exactly as [`LinkId`](ReactorSockets.LinkId.md) is the
Raw profile's.

## Properties

 [Generation](ReactorSockets.PeerId.Generation.md\#ReactorSockets\_PeerId\_Generation)

The generation that Slot was on when this Peer was admitted. Live Slots begin at 1, so a
<a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/default">default</a> [`PeerId`](ReactorSockets.PeerId.md) — generation 0 — addresses nothing.

 [Index](ReactorSockets.PeerId.Index.md\#ReactorSockets\_PeerId\_Index)

The zero-based Slot this Peer occupies.

 [Origin](ReactorSockets.PeerId.Origin.md\#ReactorSockets\_PeerId\_Origin)

Which Reactor issued this [`PeerId`](ReactorSockets.PeerId.md). Opaque and process-local — it is compared for
equality and nothing else, orders nothing, and never appears on the wire. Never 0: that value is
reserved and no Reactor is ever issued it, so a <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/default">default</a> [`PeerId`](ReactorSockets.PeerId.md)
addresses nothing twice over. Not a [`Source`](ReactorSockets.Source.md), which names a remote address; this
names a Reactor of this process.

