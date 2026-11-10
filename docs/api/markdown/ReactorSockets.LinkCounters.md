# <a id="ReactorSockets_LinkCounters"></a> Struct LinkCounters

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

A point-in-time snapshot of one Link's counters (ADR 0013): monotonic figures running from that
Link's registration, with no reset, fetched by value and keyed by [`LinkId`](ReactorSockets.LinkId.md) — there is
no accessor that enumerates every Link's counters in one call.

```csharp
public readonly record struct LinkCounters : IEquatable<LinkCounters>
```

#### Implements

[IEquatable<LinkCounters\>](https://learn.microsoft.com/dotnet/api/system.iequatable\-1)

#### Inherited Members

[object.Equals\(object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\)), 
[object.Equals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\-system\-object\)), 
[object.GetHashCode\(\)](https://learn.microsoft.com/dotnet/api/system.object.gethashcode), 
[object.GetType\(\)](https://learn.microsoft.com/dotnet/api/system.object.gettype), 
[object.ReferenceEquals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.referenceequals), 
[object.ToString\(\)](https://learn.microsoft.com/dotnet/api/system.object.tostring)

## Remarks

There is no read-share figure here, unlike [`PeerCounters`](ReactorSockets.PeerCounters.md). The Raw profile reads a
ready Link once per Pump, so a Link never reaches a second reservation in one Frame and its read
share can never be exhausted. A field that could only ever read zero fails ADR 0013's counter test.

## Constructors

 [LinkCounters\(ulong, ulong\)](ReactorSockets.LinkCounters.\-ctor.md\#ReactorSockets\_LinkCounters\_\_ctor\_System\_UInt64\_System\_UInt64\_)

A point-in-time snapshot of one Link's counters (ADR 0013): monotonic figures running from that
Link's registration, with no reset, fetched by value and keyed by [`LinkId`](ReactorSockets.LinkId.md) — there is
no accessor that enumerates every Link's counters in one call.

## Properties

 [BytesReceived](ReactorSockets.LinkCounters.BytesReceived.md\#ReactorSockets\_LinkCounters\_BytesReceived)

Bytes delivered from this Link into the Arena. Not derivable: no other figure attributes received
bytes to one Link, and the Arena high-water is a per-Frame peak rather than a running total.

 [BytesSent](ReactorSockets.LinkCounters.BytesSent.md\#ReactorSockets\_LinkCounters\_BytesSent)

Bytes the transport accepted for sending on this Link. Not derivable: the Reactor-scope datagram
figures count datagrams rather than bytes, cover every Link at once, and never see a stream Link.

