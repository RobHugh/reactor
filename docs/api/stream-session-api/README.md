# StreamSessionReactor — user guide

How to build an application against `ReactorSockets.StreamSessionReactor`: Sessions over TCP,
addressed as Peers, delivering whole Messages instead of byte runs.

## What this profile gives you

- **Whole Messages.** A `u32` length prefix is written and parsed for you. You never see a partial
  Message and never reassemble one yourself.
- **Peers, not sockets.** You address a `PeerId`. Sockets are never handed out and never rebound.
- **One thread, no callbacks.** The library owns no threads. Nothing reaches you except in response
  to a call you made.
- **Survivable connections.** A lost transport interrupts a Peer rather than ending it. The dialling
  side reconnects on its own, and the Session resumes with no reconciliation asked of you.

## What it does not give you

- No async, no `Task`, no `CancellationToken`. `Pump` is synchronous and never blocks.
- No thread safety. Every member must be called from the thread that constructed the Reactor.
- No delivery modes, no fragmentation, no ordering knobs. TCP already provides what those are for.
- No TLS. Wrap the payloads yourself if you need confidentiality.

## Read in this order

1. [Construction](01-construction.md) — options, required knobs, and the memory bill.
2. [The loop](02-the-loop.md) — `Pump`, `Drain`, Frame lifetime, and the event roster.
3. [Messages](03-messages.md) — `Send`, `Connect`, receiving, and backpressure.
4. [Closing](04-closing.md) — `Close`, Goodbye, and every `DisconnectReason`.
5. [Interruption and Resume](05-interruption-and-resume.md) — heartbeats, Linger, re-dial, Resume.
6. [Counters and diagnostics](06-counters-and-diagnostics.md) — what to monitor and how.

## The whole API

```
StreamSessionReactor(StreamSessionOptions options)
IReadOnlyList<IPEndPoint> BoundEndpoints { get; }
ReactorCounters               Counters { get; }
void          Pump()
SessionEventEnumerator Drain()
PeerId        Connect(IPEndPoint destination)
SendOutcome   Send(PeerId peer, ReadOnlySpan<byte> message)
void          Close(PeerId peer)
PeerCounters  CountersOf(PeerId peer)
void          Dispose()
```

Eight members. Everything else in this guide describes what they do, not more of them.

## Smallest working program

```csharp
using System.Net;
using ReactorSockets;

using var reactor = new StreamSessionReactor(new StreamSessionOptions
{
    Reactor = new ReactorOptions { Capacity = 64 },
    MaxMessageSize = 64 * 1024,
    RetentionBytes = 1024 * 1024,
    SendIdleInterval = TimeSpan.FromSeconds(5),
    Linger = TimeSpan.FromSeconds(30),
    Endpoints = [new IPEndPoint(IPAddress.Loopback, 9000)],
});

while (running)
{
    reactor.Pump();

    foreach (SessionEvent evt in reactor.Drain())
    {
        switch (evt.Kind)
        {
            case SessionEventKind.PeerConnected:
                reactor.Send(evt.Peer, "hello"u8);
                break;

            case SessionEventKind.MessageReceived:
                Handle(evt.Peer, evt.Payload);   // copy out if you keep it
                break;

            case SessionEventKind.PeerClosed:
                Forget(evt.Peer, evt.Reason);
                break;
        }
    }

    Thread.Sleep(1);   // your loop paces itself; Pump never blocks
}
```
