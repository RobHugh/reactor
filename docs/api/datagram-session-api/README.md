# DatagramSessionReactor — user guide

How to build an application against `ReactorSockets.DatagramSessionReactor`: Sessions over UDP,
addressed as Peers, delivering whole Messages under one of four delivery modes.

## What this profile gives you

- **Whole Messages.** One `Send` is one `MessageReceived` at the far end. Reliable Messages larger
  than one Datagram are cut into Fragments and reassembled for you.
- **Four delivery modes.** Each `Send` picks reliable or not, ordered or not. Each mode has its own
  sequence Space, so loss in one never stalls another.
- **Peers, not sockets.** You address a `PeerId`. A Peer is a session token, not an address, so a
  Peer whose address changes keeps its Session.
- **One thread, no callbacks.** The library owns no threads. Nothing reaches you except in response
  to a call you made.
- **Survivable silence.** A Peer that goes quiet is interrupted rather than ended. The Session
  resumes when the path comes back, with no reconciliation asked of you.
- **A hardened handshake.** A stateless cookie round trip comes before any state is held, and no
  reply to an unverified sender is ever larger than what it sent.

## What it does not give you

- No async, no `Task`, no `CancellationToken`. `Pump` is synchronous and never blocks.
- No thread safety. Every member must be called from the thread that constructed the Reactor.
- No path MTU discovery. You choose the fragment size, and a size the path cannot carry is
  diagnosed, not worked around.
- No congestion control. Your send rate is your own to pace.
- No encryption and no authentication of the data plane. Protect payloads yourself if an on-path
  attacker is in your threat model.

## Read in this order

1. [Construction](01-construction.md) — options, required knobs, and the memory bill.
2. [The loop](02-the-loop.md) — `Pump`, `Drain`, Frame lifetime, and the event roster.
3. [Messages](03-messages.md) — `Connect`, `Send`, delivery modes, fragmentation, and backpressure.
4. [Closing](04-closing.md) — `Close`, Goodbye, and every `DisconnectReason`.
5. [Interruption and Resume](05-interruption-and-resume.md) — heartbeats, Linger, and Resume.
6. [Counters and diagnostics](06-counters-and-diagnostics.md) — what to monitor and how.

For choosing a delivery mode, see also [Choosing a delivery mode](../../delivery-modes.md).

## The whole API

```
DatagramSessionReactor(DatagramSessionOptions options)
IReadOnlyList<IPEndPoint> BoundEndpoints { get; }
ReactorCounters               Counters { get; }
void          Pump()
SessionEventEnumerator Drain()
PeerId        Connect(IPEndPoint destination)
SendOutcome   Send(PeerId peer, DeliveryMode mode, ReadOnlySpan<byte> message)
void          Close(PeerId peer)
PeerCounters  CountersOf(PeerId peer)
void          Dispose()
```

The same shape as the stream profile, plus a `DeliveryMode` on `Send`.

## Smallest working program

```csharp
using System.Net;
using ReactorSockets;

using var reactor = new DatagramSessionReactor(new DatagramSessionOptions
{
    Reactor = new ReactorOptions { Capacity = 64 },
    MaxMessageSize = 64 * 1024,
    RetentionBytes = 256 * 1024,
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
                reactor.Send(evt.Peer, DeliveryMode.ReliableOrdered, "hello"u8);
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
