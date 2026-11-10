# Closing

## Closing a Session yourself

```csharp
reactor.Close(peer);
```

`Close` announces that you are ending the Session on purpose. It appends a Goodbye *behind* whatever
you already queued for that Peer, so everything sent before it is delivered first.

From that moment `Send` returns `Closed`. The Peer is not gone yet.

`PeerClosed` with `LocalClose` arrives once the Goodbye has actually reached the wire — the same
Pump if nothing was queued, or several Pumps later if retention had a backlog. Keep pumping until
you see it.

`Close` is idempotent. A stale `PeerId`, or a Peer already closing, is a no-op. A Peer closing under
you, or closing the same Peer twice, is an ordinary race, not a programmer error.

Retention full at the moment you call `Close` does not lose the Goodbye. The append is retried every
Pump until it commits.

Closing an Interrupted Peer cannot put the Goodbye on the wire, because there is no transport. That
Peer ends with `LingerExpired` once the effective Linger elapses, not with `LocalClose`.

## When the peer closes

A Goodbye from the far end resolves one of two ways:

- **Nothing outstanding** — `PeerClosed` with `PeerGoodbye`, directly.
- **Something outstanding** — undrained Messages, buffered bytes, or your own retention still
  flushing — `PeerClosing` first, then `PeerClosed` with `PeerGoodbye` once all of it drains.

`PeerClosing` is your signal that the Session is ending cleanly and no further `Send` is worth
making. You need not act on it.

## Every DisconnectReason

| Reason | Meaning |
|---|---|
| `LocalClose` | You called `Close`. |
| `PeerGoodbye` | The peer closed on purpose. A clean ending on both sides. |
| `VersionMismatch` | The two ends do not speak the same protocol version. The match is exact — no negotiation, no compatibility range. |
| `RemoteAtCapacity` | The far end had no Slot free for you and said so, because it is full or your address is at its `MaxPeersPerAddress`. Distinct from silence: retry later. |
| `TransportFailed` | The transport died before the Session ever existed. Only ever a dial or a handshake, never an established Peer. |
| `ProtocolViolation` | The peer broke framing rules, sent data after its own Goodbye, or claimed to have received Messages you never sent. |
| `LingerExpired` | An interrupted Peer's Linger ran out before the transport came back. |
| `RetentionGap` | A Resume named a point below what you still retain. See [Interruption and Resume](05-interruption-and-resume.md). |

`PeerClosed` is final. The `PeerId` is dead from that event onwards, and a Slot reused later carries
a new generation, so a stale id can never alias a new Peer.

## Refusals you will see as a dialler

`VersionMismatch` and `RemoteAtCapacity` are answers, not silence. A Reactor at capacity replies to a
Hello rather than dropping it, so you learn the difference between "full" and "gone".

`RemoteAtCapacity` also means your address may be at the far end's `MaxPeersPerAddress`, so other
Slots may be free.

The far end logs a version or full-Reactor refusal as `PeerRefused` (300) at `Info`, and a
per-address refusal as `AddressPeerLimitReached` (208) at `Warn`. None touches an admission counter.

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

Disposing without this works, but the far ends see a dead transport rather than a Goodbye — which,
on their side, is an interruption they will spend up to Linger trying to reconnect to.
