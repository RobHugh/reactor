# <a id="ReactorSockets_LinkCounters__ctor_System_UInt64_System_UInt64_"></a> Constructor LinkCounters

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_LinkCounters__ctor_System_UInt64_System_UInt64_"></a> LinkCounters\(ulong, ulong\)

A point-in-time snapshot of one Link's counters (ADR 0013): monotonic figures running from that
Link's registration, with no reset, fetched by value and keyed by [`LinkId`](ReactorSockets.LinkId.md) — there is
no accessor that enumerates every Link's counters in one call.

```csharp
public LinkCounters(ulong BytesSent, ulong BytesReceived)
```

### Parameters

`BytesSent` [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

Bytes the transport accepted for sending on this Link. Not derivable: the Reactor-scope datagram
figures count datagrams rather than bytes, cover every Link at once, and never see a stream Link.

`BytesReceived` [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

Bytes delivered from this Link into the Arena. Not derivable: no other figure attributes received
bytes to one Link, and the Arena high-water is a per-Frame peak rather than a running total.

### Remarks

There is no read-share figure here, unlike [`PeerCounters`](ReactorSockets.PeerCounters.md). The Raw profile reads a
ready Link once per Pump, so a Link never reaches a second reservation in one Frame and its read
share can never be exhausted. A field that could only ever read zero fails ADR 0013's counter test.

