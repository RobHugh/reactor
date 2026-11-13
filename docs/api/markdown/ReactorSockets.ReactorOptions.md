# <a id="ReactorSockets_ReactorOptions"></a> Class ReactorOptions

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

The budget L0 spends inside a Pump, shared by every profile. Composed as a member into each
profile's options type rather than inherited from a base (ADR 0012) — nothing protocol-specific
may ever enter this type, since a knob two of three profiles need is exactly what would turn it
into the base class ADR 0011 refused.

```csharp
public sealed class ReactorOptions
```

#### Inheritance

[object](https://learn.microsoft.com/dotnet/api/system.object) ← 
[ReactorOptions](ReactorSockets.ReactorOptions.md)

#### Inherited Members

[object.Equals\(object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\)), 
[object.Equals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\-system\-object\)), 
[object.GetHashCode\(\)](https://learn.microsoft.com/dotnet/api/system.object.gethashcode), 
[object.GetType\(\)](https://learn.microsoft.com/dotnet/api/system.object.gettype), 
[object.ReferenceEquals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.referenceequals), 
[object.ToString\(\)](https://learn.microsoft.com/dotnet/api/system.object.tostring)

## Remarks

<p>
A knob carries a default only when that default is derived — from machinery the library owns,
from another knob, or from a citable standard (ADR 0011, ADR 0012). [`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity) has
no such derivation and is therefore required, with no default. Construction throws
[`ArgumentOutOfRangeException`](https://learn.microsoft.com/dotnet/api/system.argumentoutofrangeexception) for a single bad value and [`ArgumentException`](https://learn.microsoft.com/dotnet/api/system.argumentexception)
for a combination that cannot work, because every check here is deterministic and entirely the
caller's — unlike <code>Send</code>, whose failures belong to the remote end or the network and are reported
through a <code>Result</code> instead.
</p>
<p>
<b>Every per-Pump cost a remote party can inflate is bounded at construction</b> (ADR 0012). Reading
is bounded by each share-holder's share of the arena — [`ReactorOptions.ArenaSize`](ReactorSockets.ReactorOptions.ArenaSize.md#ReactorSockets_ReactorOptions_ArenaSize) divided by
[`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity), with one whole unit always allowed — and accepts and handshakes are both
bounded by [`ReactorOptions.AdmissionCap`](ReactorSockets.ReactorOptions.AdmissionCap.md#ReactorSockets_ReactorOptions_AdmissionCap). Three mechanisms with one reason, and the checklist for
anything added later: a cost a remote party can raise inside one Pump, on the consumer's thread,
needs a bound fixed here, because the failure is invisible until someone points a load generator
at it. The datagram profile adds a fourth on its own options,
[`DatagramSessionOptions.MaxDatagramsPerPump`](ReactorSockets.DatagramSessionOptions.MaxDatagramsPerPump.md#ReactorSockets_DatagramSessionOptions_MaxDatagramsPerPump), because the arena share does not see
control traffic.
</p>

## Properties

 [AdmissionCap](ReactorSockets.ReactorOptions.AdmissionCap.md\#ReactorSockets\_ReactorOptions\_AdmissionCap)

The per-Pump bound on accepts and handshakes alike. Defaults to [`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity).
Provenance: derived from [`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity) — one Pump never spends more admission work
than a full Reactor's worth, and the remainder waits in the OS backlog or is dropped (ADR 0012).

 [ArenaSize](ReactorSockets.ReactorOptions.ArenaSize.md\#ReactorSockets\_ReactorOptions\_ArenaSize)

The frame arena's size in bytes. Left unset, a Reactor defaults it to its own profile floor.
Provenance: derived from the profile — one maximum deliverable unit, since below it that unit
could never be delivered, and only the profile's constructor knows what it must exceed
(ADR 0012). A value below that floor throws at construction, naming the arithmetic. A value
above [`MaxLength`](https://learn.microsoft.com/dotnet/api/system.array.maxlength) throws [`ArgumentOutOfRangeException`](https://learn.microsoft.com/dotnet/api/system.argumentoutofrangeexception) at
construction, because the arena is one array and the runtime cannot allocate a larger one.

 [Capacity](ReactorSockets.ReactorOptions.Capacity.md\#ReactorSockets\_ReactorOptions\_Capacity)

The number of share-holders this Reactor admits — Links in Raw, Peers in a Session
(ADR 0014) — and the multiplier on every preallocation. Required; there is no default
derivable from anything the library owns (ADR 0011, superseding ADR 0001's "default 64").
Must be at least 1.

 [MinimumLevel](ReactorSockets.ReactorOptions.MinimumLevel.md\#ReactorSockets\_ReactorOptions\_MinimumLevel)

The minimum [`DiagnosticLevel`](ReactorSockets.DiagnosticLevel.md) the installed [`ReactorOptions.Sink`](ReactorSockets.ReactorOptions.Sink.md#ReactorSockets_ReactorOptions_Sink) receives.
Must be supplied together with [`ReactorOptions.Sink`](ReactorSockets.ReactorOptions.Sink.md#ReactorSockets_ReactorOptions_Sink) — neither carries a default, since a
default level would be a guess at how much diagnostic volume the consumer wants. Provenance:
absent only alongside an absent [`ReactorOptions.Sink`](ReactorSockets.ReactorOptions.Sink.md#ReactorSockets_ReactorOptions_Sink), where no level can matter (ADR 0012).

 [ResolvedAdmissionCap](ReactorSockets.ReactorOptions.ResolvedAdmissionCap.md\#ReactorSockets\_ReactorOptions\_ResolvedAdmissionCap)

The resolved admission cap: [`ReactorOptions.AdmissionCap`](ReactorSockets.ReactorOptions.AdmissionCap.md#ReactorSockets_ReactorOptions_AdmissionCap) if set, otherwise [`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity).

 [Sink](ReactorSockets.ReactorOptions.Sink.md\#ReactorSockets\_ReactorOptions\_Sink)

The Diagnostic Sink records are sent to, or <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/null">null</a> to disable diagnostics
entirely. Must be supplied together with [`ReactorOptions.MinimumLevel`](ReactorSockets.ReactorOptions.MinimumLevel.md#ReactorSockets_ReactorOptions_MinimumLevel) — both present or both
absent. Provenance: absent means off, which is derived rather than chosen — with no Sink there
is nowhere for a record to go, so no level can matter (ADR 0012).

 [TimeProvider](ReactorSockets.ReactorOptions.TimeProvider.md\#ReactorSockets\_ReactorOptions\_TimeProvider)

The source of monotonic time for every duration this library measures. Defaults to
[`System`](https://learn.microsoft.com/dotnet/api/system.timeprovider.system); only <code>GetTimestamp</code>/<code>GetElapsedTime</code>
are ever consulted, never <code>GetUtcNow</code>. Provenance: the framework's own clock, and the only
requirement on it is that reads be monotonic, which <code>GetTimestamp</code> is (ADR 0012).

