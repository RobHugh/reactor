# Streams

A stream Link is one TCP connection, from `Connect` or from an `Accepted` event. The two behave
identically once registered.

## Sending

```csharp
SendResult result = reactor.Send(link, payload);
```

`Send` hands bytes straight to the socket. It never blocks, never copies and never queues. It reports
how many bytes the socket took.

| Result | Meaning | What to do |
|---|---|---|
| `IsSuccess`, `BytesSent == payload.Length` | All taken. | Nothing. |
| `IsSuccess`, `BytesSent < payload.Length` | Short send. The socket buffer is full. | Keep the rest. Offer it again after `Writable`. |
| `!IsSuccess` | Illegal call or socket failure. `Error` names it. | Log it. `Close` the Link if you are done with it. |

A short send is ordinary flow control, not a failure. The bytes not taken are still yours. The
Reactor holds no copy.

`Send` fails without throwing when the `LinkId` names no open Link, the Link is not a stream Link,
you already called `Shutdown`, or the socket failed. A failed `Send` does not close the Link.

## Writable

A short send arms exactly one `Writable` for that Link. The first Pump that finds the socket has room
raises it and disarms.

```csharp
case RawEventKind.Writable:
    ReadOnlySpan<byte> rest = Unsent(evt.Link);   // your own buffer
    SendResult r = reactor.Send(evt.Link, rest);
    if (r.IsSuccess) Advance(evt.Link, r.BytesSent);   // a short send re-arms
    break;
```

A Link you never over-sent on never raises `Writable`. It is a reply to your short send, not a status
report.

When it arrives is the platform's choice. Windows reports room as soon as there is any. Linux waits
until roughly half the send buffer is free. A slow reader therefore holds a Link unwritable for
longer on Linux.

## Receiving

```csharp
case RawEventKind.SegmentReceived:
    myFramer.Append(evt.Link, evt.Payload);   // copies; Payload dies at the next Pump
    while (myFramer.TryTake(evt.Link, out Message m)) Handle(m);
    break;
```

A Segment is whatever was readable at that moment, up to `scratchBufferSize` bytes. It is not a
message. One remote write can arrive as several Segments, and several writes as one.

**Framing is yours.** Raw keeps nothing across a Pump, so a partial message must be copied into your
own buffer before the next Pump. Use `StreamSessionReactor` if you want whole Messages.

Each stream Link delivers at most one Segment per Pump.

## Half-close

TCP closes one direction at a time. Raw keeps the two directions separate.

**The remote finished sending.** You get `EndOfStream`, once. The Link stays open and `Send` still
works, so you can finish replying. `EndOfStream` is not `Closed`.

**You finished sending.** `Shutdown` sends your FIN while you keep reading:

```csharp
Result r = reactor.Shutdown(link);
```

It is one-way. A later `Send` fails, and a second `Shutdown` fails. `Shutdown` fails without throwing
on a closed or non-stream Link.

A request-response exchange that uses both:

```csharp
reactor.Send(link, request);
reactor.Shutdown(link);              // end of request
// ... SegmentReceived until EndOfStream ...
reactor.Close(link);                 // both directions done
```

**You must still `Close`.** A Link after `EndOfStream` holds its Slot until you close it or dispose
the Reactor. Raw has no timer to reclaim it. A reset after `EndOfStream` still closes the Link with
`ResetByRemote`.
