# <a id="ReactorSockets_RawReactor_Send_ReactorSockets_LinkId_System_ReadOnlySpan_System_Byte__"></a> Method Send

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawReactor_Send_ReactorSockets_LinkId_System_ReadOnlySpan_System_Byte__"></a> Send\(LinkId, ReadOnlySpan<byte\>\)

<code>Send</code> is stream-only and illegal on a datagram or listening Link. Takes what the socket
will accept of <code class="paramref">payload</code> right now and reports the count — never blocks, never
copies and never queues (ADR 0008): whatever the socket did not take stays with the caller, who
decides whether and when to offer it again. Queuing was rejected on principle, not merely left
out: an unbounded queue is a memory leak with a network cause, and a bounded one needs a limit
this library would have to justify plus a way to learn when to retry — which reintroduces this
same event under another name. Either way the Reactor would be inventing a per-Link FIFO policy,
which is machinery a Session has and Raw was said not to lend.

```csharp
public SendResult Send(LinkId link, ReadOnlySpan<byte> payload)
```

### Parameters

`link` [LinkId](ReactorSockets.LinkId.md)

`payload` [ReadOnlySpan](https://learn.microsoft.com/dotnet/api/system.readonlyspan\-1)<[byte](https://learn.microsoft.com/dotnet/api/system.byte)\>

### Returns

 [SendResult](ReactorSockets.SendResult.md)

### Remarks

<p>
A send that takes fewer bytes than <code class="paramref">payload</code>.Length arms exactly one
[`RawEventKind.Writable`](ReactorSockets.RawEventKind.md) for <code class="paramref">link</code>: the next [`RawReactor.Pump()`](ReactorSockets.RawReactor.Pump.md#ReactorSockets_RawReactor_Pump)
that finds the socket writable queues it and clears the mark, so a consumer using this Link
only ever sees one <code>Writable</code> per short send, never one for every Pump a socket happens to
be writable (nearly always). A Link never over-sent on produces no <code>Writable</code> at all — the
event is a reply to something the consumer did, not a status this Reactor volunteers.
</p>
<p>
<i>When</i> that one event arrives is the platform's call, not this Reactor's, and the platforms
differ by more than a rounding error: Windows offers writability again as soon as the send buffer
has any room, while Linux waits until roughly half of it is free. A Link whose counterparty reads
slowly can therefore stay unwritable on Linux long after it would have been writable on Windows,
with a large auto-tuned send buffer widening the gap. This is reported, not smoothed over —
synthesising an earlier <code>Writable</code> would mean promising room the socket has not got, and the
next send would simply come back short again.
</p>
<p>
Fails without throwing (ADR 0008), naming whichever applies: <code class="paramref">link</code> does not
address an open Link, it does but cannot take a stream send, it was already shut down for
writing by [`RawReactor.Shutdown(LinkId)`](ReactorSockets.RawReactor.Shutdown.md#ReactorSockets_RawReactor_Shutdown_ReactorSockets_LinkId_), or the socket itself failed. The open check comes first —
answering an illegality to a stale or garbage [`LinkId`](ReactorSockets.LinkId.md) would be a lie about a Link
that does not exist.
</p>

