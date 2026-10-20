# RawReactor — user guide

How to build an application against `ReactorSockets.RawReactor`: UDP and TCP sockets registered at
runtime, addressed as Links, advanced by your own loop. Raw is the escape hatch for writing your own
protocol layer. It invents nothing on the wire.

## What this profile gives you

- **One identity for every transport.** A bound UDP socket, a listening TCP socket and a TCP
  connection are each a `LinkId`. One lifecycle, one `Closed` event.
- **Registration at runtime.** `Bind`, `Listen` and `Connect` add Links whenever you call them.
  Nothing is declared at construction.
- **One thread, no callbacks.** The library owns no threads. Nothing reaches you except in response
  to a call you made.
- **Bounded work per Pump.** Each ready Link is read once per Pump, and accepts are capped, so no
  remote can monopolise a Frame.
- **Stable close reasons.** A `CloseReason` you can switch on, identical on Windows and Linux. The
  platform `SocketError` travels in a diagnostic beside it.

## What it does not give you

- No async, no `Task`, no `CancellationToken`. `Pump` is synchronous and never blocks.
- No thread safety. Every mutating member must be called from the thread that constructed the
  Reactor.
- No framing. A stream Link delivers runs of bytes, not Messages. Delimiting them is yours.
- No send queue. A stream `Send` takes what the socket will take and hands the rest back to you.
- No timers. No heartbeats, no connect timeout, no Linger, no reconnect.
- No Peers, Sessions, handshakes, delivery modes, encryption or authentication. Use a Session
  profile if you want any of those.

## Read in this order

1. [Construction](01-construction.md) — options, the scratch buffer, and the memory bill.
2. [The loop](02-the-loop.md) — `Pump`, `Drain`, Frame lifetime, and the event roster.
3. [Links](03-links.md) — `Bind`, `Listen`, `Connect`, accepting, and which call fits which Link.
4. [Datagrams](04-datagrams.md) — `SendTo`, `DatagramReceived`, `Source`, truncation.
5. [Streams](05-streams.md) — `Send`, short sends, `Writable`, Segments, half-close.
6. [Closing](06-closing.md) — `Close`, every `CloseReason`, and disposal.
7. [Counters and diagnostics](07-counters-and-diagnostics.md) — what to monitor and how.

## The whole API

```
RawReactor(ReactorOptions options, int? scratchBufferSize = null)
const int                MaxUdpDatagramSize
ReactorCounters          Counters { get; }
void               Pump()
RawEventEnumerator Drain()
LinkId             Bind(IPEndPoint endpoint)
LinkId             Listen(IPEndPoint endpoint)
LinkId             Connect(IPEndPoint endpoint)
Result             SendTo(LinkId link, Source destination, ReadOnlySpan<byte> payload)
SendResult         Send(LinkId link, ReadOnlySpan<byte> payload)
Result             Shutdown(LinkId link)
IPEndPoint         LocalOf(LinkId link)
IPEndPoint         RemoteOf(LinkId link)
void               Close(LinkId link)
LinkCounters       CountersOf(LinkId link)
void               Dispose()
```

## Smallest working program

A UDP echo server.

```csharp
using System.Net;
using ReactorSockets;

using var reactor = new RawReactor(new ReactorOptions { Capacity = 4 });
LinkId udp = reactor.Bind(new IPEndPoint(IPAddress.Loopback, 9000));

while (running)
{
    reactor.Pump();

    foreach (RawEvent evt in reactor.Drain())
    {
        switch (evt.Kind)
        {
            case RawEventKind.DatagramReceived:
                reactor.SendTo(evt.Link, evt.Source, evt.Payload);   // echo back
                break;

            case RawEventKind.Closed:
                Forget(evt.Link, evt.Reason);
                break;
        }
    }

    Thread.Sleep(1);   // your loop paces itself; Pump never blocks
}
```
