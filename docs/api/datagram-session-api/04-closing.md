# Closing

## Closing a Session yourself

```csharp
reactor.Close(peer);
```

What `Close` does depends on the Peer's state:

| Peer state | What `Close` does |
|---|---|
| Established | Queues a Goodbye and puts the Peer in Closing. `PeerClosed` comes later. |
| Still dialling | `PeerClosed` with `LocalClose`, this Frame. |
| Interrupted | `PeerClosed` with `LocalClose`, this Frame. No Goodbye is sent. |

The Goodbye is a reliable record in the `ReliableOrdered` Space, queued *behind* everything you
already sent in that mode. From that moment `Send` returns `Closed`.

A Closing Peer ends in one of two ways:

- `PeerClosed` with `LocalClose` once the Goodbye and every `ReliableOrdered` record before it are
  acknowledged.
- `PeerClosed` with `LingerExpired` if the effective Linger elapses from `Close` with the Goodbye
  still unacknowledged.

Keep pumping until you see one. A Closing Peer is never interrupted and never resumes.

`Close` is idempotent. A stale `PeerId`, or a Peer already closing, is a no-op. A Peer closing under
you, or closing the same Peer twice, is an ordinary race, not a programmer error.

`ReliableOrdered` retention full at the moment you call `Close` does not lose the Goodbye. The
append is retried every Pump until it commits.

**The Goodbye is ordered only within `ReliableOrdered`.** A `ReliableUnordered` Message sent before
`Close` can arrive after the Goodbye, and the far end discards it. Unreliable Messages are never
guaranteed anyway. If a final Message matters, send it `ReliableOrdered`.

## When the peer closes

The peer's Goodbye takes its turn in `ReliableOrdered` send order, so every `ReliableOrdered` Message
sent before it is delivered first. Then it resolves one of two ways:

- **Nothing undelivered** — `PeerClosed` with `PeerGoodbye`, in the same Pump.
- **Messages still undelivered** — `PeerClosing` first, after those Messages, then `PeerClosed` with
  `PeerGoodbye` on a later Pump once they drain.

`PeerClosing` is your signal that the Session is ending cleanly and no further `Send` is worth
making. You need not act on it. After the peer's Goodbye, `Send` returns `Closed` and further
Messages from the peer are acknowledged but not delivered.

The final Datagram acknowledges the Goodbye before the Slot is freed. If that acknowledgement is
lost, the Reactor remembers the closed Session for a while and re-acknowledges the peer's
retransmitted Goodbye, so the peer can still end with `LocalClose`.

## Both ends close at once

Each end ends with `LocalClose`. A Closing Peer that receives the remote's Goodbye acknowledges it
and raises neither `PeerClosing` nor `PeerGoodbye`.

## Every DisconnectReason

| Reason | Meaning |
|---|---|
| `LocalClose` | You called `Close`. |
| `PeerGoodbye` | The peer closed on purpose. A clean ending on both sides. |
| `VersionMismatch` | The two ends do not speak the same protocol version. The match is exact — no negotiation, no compatibility range. |
| `RemoteAtCapacity` | The far end had no Slot free and said so. Distinct from silence: retry later. |
| `ProtocolViolation` | The peer declared a Fragment set larger than your receive window could ever hold. |
| `LingerExpired` | An interrupted Peer's Linger ran out before it was heard from, or a Closing Peer's Goodbye went unacknowledged for a whole Linger. |
| `FragmentSizeUnviable` | Your `FragmentSize` is larger than the path will carry. |

`TransportFailed` and `RetentionGap` never occur on this profile. There is no connection to fail and
nothing to rewind on Resume.

`PeerClosed` is final. The `PeerId` is dead from that event onwards, and a Slot reused later carries
a new generation, so a stale id can never alias a new Peer.

## Refusals you will see as a dialler

`VersionMismatch` and `RemoteAtCapacity` are answers, not silence. A Reactor at capacity replies to a
verified Hello rather than dropping it, so you learn the difference between "full" and "gone".

A refusal only comes after the cookie round trip. A bare Hello is always answered with a Cookie and
nothing else, so nothing about a Reactor's version or occupancy is learnable without proving you
can receive at the address you claim.

The refusing end logs `VersionRefused` (202) or `CapacityExhausted` (3) and counts a capacity
refusal in `AdmissionDiscards`.

## FragmentSizeUnviable

The Reactor watches for one signature: unfragmented reliable records keep being acknowledged, which
proves the path and the peer are alive, while not one Fragment is acknowledged across 4
retransmission rounds. Only Fragments too large for the path explain that, so the Session closes.

Nothing is probed and no smaller size is tried. The fix is configuration: lower `FragmentSize` and
reconnect.

## Shutting down

```csharp
foreach (PeerId peer in myPeers) reactor.Close(peer);

while (myPeers.Count > 0)
{
    reactor.Pump();
    foreach (SessionEvent evt in reactor.Drain())
    {
        if (evt.Kind == SessionEventKind.PeerClosed) myPeers.Remove(evt.Peer);
    }
}

reactor.Dispose();
```

Disposing without this works, but the far ends hear nothing. On their side that is silence, which
interrupts the Peer and holds its Slot for up to Linger.
