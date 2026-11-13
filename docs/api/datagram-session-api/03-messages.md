# Messages

## Dialling

```csharp
PeerId peer = reactor.Connect(new IPEndPoint(address, 9000));
```

`Connect` sends a Hello and returns immediately with the `PeerId` that will address the Session. The
Peer occupies a capacity Slot from this moment — the handshake is work the Reactor is holding.

The handshake is a cookie round trip: Hello, Cookie, Hello-with-cookie, then Accept or Refusal. It
resolves on a later Pump, as one of:

- `PeerConnected` — the Session is established.
- `PeerClosed` with `VersionMismatch` or `RemoteAtCapacity` — the far end refused in protocol.
- `PeerClosed` with `TransportFailed` — nothing answered within this Reactor's `Linger` of `Connect`.

A pending dial resends its handshake: the Hello until a Cookie arrives, then the Hello-with-cookie until an
Accept or Refusal arrives. The first resend waits about one Pump interval, each later wait doubles up to
`Linger`, and every wait is drawn at random below that, so many dials do not retry in step. A lost Hello,
Cookie or Accept therefore costs a delay, not the dial. `Close` ends a pending dial at once and stops the
resends. Nothing is resent while your clock has not moved.

`Connect` throws `InvalidOperationException` if the Reactor is already at capacity. Check your own
Peer count before dialling if that is reachable in your application.

Accepted Peers need no call at all. Bind an endpoint and `PeerConnected` arrives.

## Sending

```csharp
SendOutcome outcome = reactor.Send(peer, DeliveryMode.ReliableOrdered, payload);
```

`Send` copies the Message and queues it on that Peer. **Nothing reaches the socket here.** The next
`Pump` packs what is queued for the Peer into Datagrams of at most `FragmentSize + 31` bytes, in
Send order, up to `65 507 / (FragmentSize + 31)` of them. What does not fit waits for the next Pump.

The bytes are copied before `Send` returns, so you own your buffer again immediately. An empty
Message is legal and is delivered as an empty `Payload`.

| `SendOutcome` | Meaning | What to do |
|---|---|---|
| `Sent` | Queued, and retained if the mode is reliable. | Nothing. |
| `RetentionExhausted` | The mode's reliable Space has too few free slots. Nothing was copied. | Retry after a later Pump. |
| `SendQueueFull` | An unreliable Message would take the Peer's queued unreliable bytes past `UnreliableQueueBytes`. Nothing was copied. | Retry after the next Pump, or drop it. |
| `Closed` | The Peer is not open, or a Goodbye has passed in one direction or the other. | Stop sending to this Peer. |
| `NotEstablished` | The handshake has not finished. Nothing was copied. | Wait for `PeerConnected`. |
| `MessageTooLarge` | An unreliable Message is larger than one fragment, the smaller of your `FragmentSize` and the peer's. | Split it, or use a reliable mode. |
| `RemoteMessageTooLarge` | A Message in any mode is larger than the `MaxMessageSize` the peer announced, or a reliable Message needs more Fragments than the `ReceiveWindowFragments` the peer announced. Nothing was retained or sequenced. | Send something smaller. |

The checks run in this order: Peer not open or peer's Goodbye delivered (`Closed`), handshake
unfinished (`NotEstablished`), your own `Close` in progress (`Closed`), size, then retention. For an
unreliable mode, `SendQueueFull` is checked last. A
refused `Send` changes nothing on the Session — no sequence number is spent and no retention is
taken.

`Send` returns `Closed` for a stale `PeerId` rather than throwing. A Peer closing under you is an
ordinary race.

`Send` throws:

- `ArgumentOutOfRangeException` if `mode` is not a defined `DeliveryMode`.
- `InvalidOperationException` if called from the wrong thread.

## Delivery modes

| Mode | Arrives | Ordered | Fragments | Retained |
|---|---|---|---|---|
| `ReliableOrdered` | always | in send order | yes | yes |
| `ReliableUnordered` | always | no | yes | yes |
| `UnreliableSequenced` | maybe | newest only | no | no |
| `Unreliable` | maybe | no | no | no |

- `ReliableOrdered` holds a Message whose predecessor has not arrived, and releases the whole run the
  moment the gap fills. A hole blocks every later Message in this mode, and only this mode.
- `ReliableUnordered` delivers each Message as it arrives.
- `UnreliableSequenced` delivers a Message only if it is strictly newer than the last one this mode
  delivered. You never receive an older one after a newer one.
- `Unreliable` delivers whatever arrives, suppressing duplicates within a window of 64 sequences. A
  duplicate delayed longer than that is delivered twice.

Each mode is its own sequence Space. Loss or a stall in one never delays another, so put traffic
with different needs under different modes. [Choosing a delivery mode](../../delivery-modes.md)
walks through the choice.

## Fragmentation

Only the reliable modes fragment. A reliable Message larger than one fragment is cut into
`ceil(length / size)` Fragments, each sequenced, retained and retransmitted on its own, and
delivered as one `MessageReceived` once all of them have arrived.

- Every mode is limited by the **peer's** announced `MaxMessageSize`, not yours. The same payload can
  be accepted for one Peer and refused for another.
- `size` is the smaller of your `FragmentSize` and the one the peer announced. It is fixed when the
  Peer reaches Established and does not change across a Resume. A reliable Message that needs more
  Fragments at that size than the peer's announced `ReceiveWindowFragments` returns
  `RemoteMessageTooLarge`. A smaller peer `FragmentSize` also means more retention slots per Message,
  so `RetentionExhausted` arrives sooner. Receive memory does not change.
- A Message is committed whole or not at all. If its mode's Space has fewer free retention slots
  than it has Fragments, `Send` returns `RetentionExhausted` and nothing is retained.
- The unreliable modes are capped at one fragment, permanently. Fragmenting what cannot be
  retransmitted multiplies the loss rate by the number of pieces. An unreliable Message above both
  one fragment and the peer's `MaxMessageSize` returns `MessageTooLarge`. One fragment is the
  smaller of your `FragmentSize` and the peer's.

Fragmentation lengthens `ReliableOrdered` head-of-line blocking: every later Message waits for the
whole fragmented Message, not one Datagram. For bulk transfer, prefer `ReliableUnordered`.

## Reliability

Every Datagram carries an acknowledgement block for each reliable Space. A reliable record is
retransmitted until acknowledged, on an adaptive timeout that backs off while records keep expiring.

- The timeout's floor is your observed Pump interval.
- The dialling end seeds its round-trip estimate from the handshake, so its first reliable record
  already has a realistic timeout.
- Retransmission resends the same record bytes, repacked into whatever Datagram goes out next.

Nothing unreliable is retransmitted.

## Backpressure

`RetentionExhausted` is the reliable modes' backpressure, and `SendQueueFull` is the unreliable modes'. Retention never blocks, never grows
and never drops — a Space without enough free slots fails the `Send` and nothing else.

Retention is per Peer and per reliable Space, never shared. A stalled Peer can exhaust only its own,
and a backlog in `ReliableOrdered` never fails a `ReliableUnordered` Send.

Three things reach `RetentionExhausted`:

1. A single Message needing more Fragments than are free.
2. A sustained send rate above what the peer acknowledges.
3. An interruption that outlasts your send rate — retention is held across it deliberately, and
   fills up.

The remedy is the same in all three: stop sending reliable Messages to that Peer, pump, and retry.
Treat it as your flow-control signal, not as an error.

The unreliable modes share one queue per Peer, `UnreliableQueueBytes`, charged at each Message's
length plus 7 bytes. A Send past it returns `SendQueueFull`. The queue empties as your Pumps write
it, not when the peer acknowledges, so pump and retry, or drop the Message if it is already stale.
Unreliable Messages wait in Send order behind any reliable Messages queued before them, so a
reliable backlog can hold the queue for more than one Pump. Reliable Sends are never charged
against it.

## Receiving

```csharp
case SessionEventKind.MessageReceived:
    ReadOnlySpan<byte> body = evt.Payload;   // whole Message
    Process(evt.Peer, body);
    break;
```

You get whole Messages, one event each, never a partial one and never two coalesced. `Payload`
aliases the Frame arena and is valid until the next `Pump`. Copy it if you keep it.

`SessionEvent` does not say which mode carried a Message. If you need to know, encode it in the
Message.

## What a peer can do wrong

None of these throw. A remote can produce them at will, so they cost that Session, that Datagram or
that Message and leave the Reactor untouched.

| What arrived | Result |
|---|---|
| A Datagram that does not parse, or names no Session | Discarded. `DatagramMalformed` (200). |
| A Fragment in an unreliable mode | Datagram discarded. `UnreliableFragment` (206) and `DatagramMalformed` (200). |
| A Fragment set claiming more Fragments than your receive window holds | `PeerClosed` with `ProtocolViolation`. `OversizedFragmentSet` (205). |
| A reliable Fragment wider than your `FragmentSize` | `PeerClosed` with `ProtocolViolation`. `OversizedFragment` (210). |
| A Message larger than your `MaxMessageSize`, whole or assembled | Discarded and still acknowledged. The Session stays open. `OversizedMessage` (207). |
| Messages after the peer's own Goodbye | Acknowledged, not delivered. |

A record that does not parse discards the whole Datagram it came in, including any good records before
it. A Message over your `MaxMessageSize` costs only itself.
