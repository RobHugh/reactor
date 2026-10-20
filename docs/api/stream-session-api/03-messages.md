# Messages

## Dialling

```csharp
PeerId peer = reactor.Connect(new IPEndPoint(address, 9000));
```

`Connect` returns immediately with the `PeerId` that will address the Session. The Peer occupies a
capacity Slot from this moment — the handshake is work the Reactor is holding.

The dial resolves on a later Pump, as one of:

- `PeerConnected` — the Session is established.
- `PeerClosed` with `TransportFailed` — the connection never happened.
- `PeerClosed` with `VersionMismatch` or `RemoteAtCapacity` — the far end refused in protocol.

`Connect` throws `InvalidOperationException` if the Reactor is already at capacity. Check your own
Peer count before dialling if that is reachable in your application.

Accepted Peers need no call at all. Bind an endpoint and `PeerConnected` arrives.

## Sending

```csharp
SendOutcome outcome = reactor.Send(peer, payload);
```

`Send` frames the Message — a `u32` big-endian length prefix, then your bytes — and appends it to
that Peer's retention. **Nothing reaches the socket here.** The next `Pump` writes everything queued
in one write.

| `SendOutcome` | Meaning | What to do |
|---|---|---|
| `Sent` | Copied whole into retention. | Nothing. |
| `RetentionExhausted` | No room. Nothing was copied. | Retry after a later Pump. |
| `Closed` | A Goodbye already passed in one direction or the other. | Stop sending to this Peer. |
| `NotEstablished` | The handshake has not finished. Nothing was copied. | Wait for `PeerConnected`. |

A dialled Peer is addressable from `Connect`, but it accepts no Message until `PeerConnected`.
`Closed` is checked first, so a Peer you closed before its handshake finished returns `Closed`.

`Send` throws:

- `ArgumentException` if `message` is empty. A zero-length prefix is illegal on the wire.
- `ArgumentException` if `message.Length > MaxMessageSize`.
- `ArgumentException` if the `PeerId` names no open Peer.

A stale `PeerId` throwing is deliberate: sending to a Peer you were told was gone is a bug in your
code, not a race.

## Backpressure

`RetentionExhausted` is the only backpressure this library has. Retention never blocks, never grows
and never drops — a full ring fails the `Send` and nothing else.

Retention is per Peer and never shared, so a stalled Peer can only ever exhaust its own. A healthy
Peer's `Send` never fails because of somebody else's traffic.

Three things reach `RetentionExhausted`:

1. A single Message larger than the room currently free.
2. A sustained send rate above what the peer confirms receiving.
3. An interruption that outlasts your send rate — retention is held across it deliberately, and
   fills up.

The remedy is the same in all three: stop sending to that Peer, pump, and retry. Treat it as your
flow-control signal, not as an error.

## Receiving

```csharp
case SessionEventKind.MessageReceived:
    ReadOnlySpan<byte> body = evt.Payload;   // whole Message, no prefix
    Process(evt.Peer, body);
    break;
```

You get whole Messages, in order, one event each. The length prefix is stripped. There is no partial
delivery and no coalescing of two Messages into one event.

`Payload` aliases the Frame arena. It is valid until the next `Pump`. Copy it if you keep it.

`MaxMessageSize` is announced to the peer during the handshake as your receive limit, so the far end
cannot legally frame something you would have to reject. If it does anyway, the Session ends with
`ProtocolViolation`.

## Message size and framing failures

These close the Session with `ProtocolViolation`, and log `MalformedFraming` (302):

- A length prefix above the announced `MaxMessageSize`.
- A length prefix of zero.

A stream that ends mid-prefix or mid-body also closes the Session with `ProtocolViolation`, but logs
no diagnostic.

Data arriving after that peer's own Goodbye closes it too, logged as `DataAfterGoodbye` (303).

None of these throw. A remote can produce them at will, so they end that Session and leave the
Reactor untouched.
