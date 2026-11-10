# <a id="ReactorSockets_LinkId"></a> Struct LinkId

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

A Link's identity: the Reactor that issued it, a zero-based slot index, and a generation counter
that increments when the slot is reused, so a stale [`LinkId`](ReactorSockets.LinkId.md) held past a Link's close
compares unequal to whatever now occupies that slot (ADR 0014) rather than aliasing it.

```csharp
public readonly record struct LinkId : IEquatable<LinkId>
```

#### Implements

[IEquatable<LinkId\>](https://learn.microsoft.com/dotnet/api/system.iequatable\-1)

#### Inherited Members

[object.Equals\(object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\)), 
[object.Equals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\-system\-object\)), 
[object.GetHashCode\(\)](https://learn.microsoft.com/dotnet/api/system.object.gethashcode), 
[object.GetType\(\)](https://learn.microsoft.com/dotnet/api/system.object.gettype), 
[object.ReferenceEquals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.referenceequals), 
[object.ToString\(\)](https://learn.microsoft.com/dotnet/api/system.object.tostring)

## Remarks

The generation is <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/integral-numeric-types">ulong</a> rather than <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/integral-numeric-types">int</a> so that it cannot
wrap within the life of any process, which is what lets generation 0 stay a reserved sentinel
without a check guarding it. ADR 0015 records the reasoning and the alternatives declined.

<p>
The Origin extends that same guarantee across Reactors. Index and generation are unique only
within one Reactor's table, and every table's first Link is Slot 0 generation 1, so without it a
[`LinkId`](ReactorSockets.LinkId.md) from one [`RawReactor`](ReactorSockets.RawReactor.md) addressed a live Link of another.
</p>

## Constructors

 [LinkId\(uint, int, ulong\)](ReactorSockets.LinkId.\-ctor.md\#ReactorSockets\_LinkId\_\_ctor\_System\_UInt32\_System\_Int32\_System\_UInt64\_)

A Link's identity: the Reactor that issued it, a zero-based slot index, and a generation counter
that increments when the slot is reused, so a stale [`LinkId`](ReactorSockets.LinkId.md) held past a Link's close
compares unequal to whatever now occupies that slot (ADR 0014) rather than aliasing it.

## Properties

 [Generation](ReactorSockets.LinkId.Generation.md\#ReactorSockets\_LinkId\_Generation)

The generation that Slot was on when this Link was admitted. Live Slots begin at 1, so a
<a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/default">default</a> [`LinkId`](ReactorSockets.LinkId.md) — generation 0 — addresses nothing.

 [Index](ReactorSockets.LinkId.Index.md\#ReactorSockets\_LinkId\_Index)

The zero-based Slot this Link occupies.

 [Origin](ReactorSockets.LinkId.Origin.md\#ReactorSockets\_LinkId\_Origin)

Which Reactor issued this [`LinkId`](ReactorSockets.LinkId.md). Opaque and process-local — compared for equality
and nothing else, ordering nothing and meaning nothing outside this process. Never 0: that value
is reserved and no Reactor is ever issued it, so a <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/default">default</a>
[`LinkId`](ReactorSockets.LinkId.md) addresses nothing twice over.

