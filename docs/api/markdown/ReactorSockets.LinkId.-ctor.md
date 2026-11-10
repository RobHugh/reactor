# <a id="ReactorSockets_LinkId__ctor_System_UInt32_System_Int32_System_UInt64_"></a> Constructor LinkId

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_LinkId__ctor_System_UInt32_System_Int32_System_UInt64_"></a> LinkId\(uint, int, ulong\)

A Link's identity: the Reactor that issued it, a zero-based slot index, and a generation counter
that increments when the slot is reused, so a stale [`LinkId`](ReactorSockets.LinkId.md) held past a Link's close
compares unequal to whatever now occupies that slot (ADR 0014) rather than aliasing it.

```csharp
public LinkId(uint Origin, int Index, ulong Generation)
```

### Parameters

`Origin` [uint](https://learn.microsoft.com/dotnet/api/system.uint32)

Which Reactor issued this [`LinkId`](ReactorSockets.LinkId.md). Opaque and process-local — compared for equality
and nothing else, ordering nothing and meaning nothing outside this process. Never 0: that value
is reserved and no Reactor is ever issued it, so a <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/default">default</a>
[`LinkId`](ReactorSockets.LinkId.md) addresses nothing twice over.

`Index` [int](https://learn.microsoft.com/dotnet/api/system.int32)

The zero-based Slot this Link occupies.

`Generation` [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

The generation that Slot was on when this Link was admitted. Live Slots begin at 1, so a
<a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/default">default</a> [`LinkId`](ReactorSockets.LinkId.md) — generation 0 — addresses nothing.

### Remarks

The generation is <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/integral-numeric-types">ulong</a> rather than <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/integral-numeric-types">int</a> so that it cannot
wrap within the life of any process, which is what lets generation 0 stay a reserved sentinel
without a check guarding it. ADR 0015 records the reasoning and the alternatives declined.

<p>
The Origin extends that same guarantee across Reactors. Index and generation are unique only
within one Reactor's table, and every table's first Link is Slot 0 generation 1, so without it a
[`LinkId`](ReactorSockets.LinkId.md) from one [`RawReactor`](ReactorSockets.RawReactor.md) addressed a live Link of another.
</p>

