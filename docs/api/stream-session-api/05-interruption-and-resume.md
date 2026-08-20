# Interruption and Resume

A lost transport does not end a Session. The Peer is held, the dialling side reconnects on its own,
and the Session continues where it stopped.

## What you have to do

Almost nothing.

```csharp
case SessionEventKind.PeerInterrupted:
    MarkUnreachable(evt.Peer);   // stop expecting replies; keep the PeerId
    break;

case SessionEventKind.PeerResumed:
    MarkReachable(evt.Peer);     // carry on; do not re-send anything
    break;
```

Do **not** close the Peer, drop your state for it, or re-send Messages. The Reactor reconnects,
rewinds its own retention to what the peer confirmed, and resends the difference itself.

Ignoring both events entirely is safe. Your Sends keep queueing and your Messages keep arriving; you
just will not know there was an outage.

## Heartbeats and the two timers

Every established Peer runs two independent timers.

**Send-idle.** Written nothing for `SendIdleInterval`? The Reactor sends a delivery-counter record as
a heartbeat. Unconditional, and your own Sends reset it — a busy Session never heartbeats.

**Receive-idle.** Received nothing for `peer's announced SendIdleInterval × your MissedHeartbeats`?
The Peer is Interrupted.

The deadline uses the interval the *peer* announced at handshake, not yours. Two ends with different
intervals each hold the other to what it actually promised.

With the default `MissedHeartbeats` of 3, a peer must miss three consecutive heartbeats before you
give up on it.

## What interrupts a Peer

- A FIN with no Goodbye before it.
- A connection reset.
- A local write failure.
- Your own receive-idle deadline expiring.

All four raise `PeerInterrupted`. A peer that said Goodbye first reaches `PeerClosing` or
`PeerClosed` instead — never this.

## What survives an interruption

| Survives | Does not survive |
|---|---|
| The `PeerId`, generation included | The transport |
| `CountersOf(peer)` — it keeps answering | Partially received bytes (the peer resends them) |
| Inbound Messages already delivered to the arena but undrained | |
| The whole outbound retention ring | |

`Send` still works. A Send to an Interrupted Peer is exactly as welcome as one to a live Peer —
holding retention across the outage is the entire point of not closing the Peer. The bytes queue and
go out when the transport returns.

If the outage outlasts your send rate, retention fills and `Send` returns `RetentionExhausted` as
usual. That is the signal to stop until the Peer resumes.

## Linger governs everything

At handshake both ends announce their `Linger` and the **minimum governs**. Read the negotiated
value from the `LingerNegotiated` (301) diagnostic: `Value0` is your own in milliseconds, `Value1`
is the effective one.

Linger is the only thing that ends an interruption:

- The interrupted Peer closes with `LingerExpired` once the effective Linger has elapsed.
- Re-dial attempts stop then too. There is no attempt-count limit anywhere.
- Both ends give up at the same negotiated minimum, so neither is left waiting on the other.

Longer Linger means longer outages survived and Slots held longer. Shorter means faster Slot reuse
and shorter outages survived. That trade is the whole knob.

## Re-dial, on the dialling side only

The end that called `Connect` reconnects to the same endpoint it originally dialled. There is no
rebinding and no address discovery.

The schedule is derived, never configured:

- The first delay is the Pump interval the Reactor observed from your loop — a retry sooner than
  your next Pump cannot happen.
- It doubles each attempt.
- It is ceilinged at the effective Linger.
- Every delay is drawn with full jitter across `[0, computed]`, so many instances re-dialling from
  one shared outage do not synchronise into bursts.

Watch it through `RedialAttempted` (306) — `Value0` is the attempt number, `Value1` the milliseconds
actually waited — and `RedialFailed` (307), which carries the `SocketError` and never closes the
Peer.

The accepting end does not dial. It reserves the interrupted Peer's Slot and waits.

## Capacity while a Slot is reserved

A reserved Slot is an occupied Slot. While an Interrupted Peer is held, a *fresh* dial from anyone
can be refused with `RemoteAtCapacity`.

Genuine resumes are never refused this way: the resume match runs before the capacity check, so the
Peer that is coming back always gets its own reserved Slot.

Size `Capacity` for this. A Reactor at capacity with several interrupted Peers turns new connections
away for up to Linger.

## Resume

The replacement connection presents the resume secret exchanged at the original handshake, plus the
count of Messages it has received. The listening side matches the secret onto the held Peer, rewinds
its retention to that count, and continues.

`PeerResumed` carries the same `PeerId`, generation included. No `PeerClosed` is ever observed in
between, and `PeerCounters.Resumes` increments on both ends.

Nothing is asked of you. Messages sent before the interruption arrive without you re-sending them,
because the rewind happens before the event is raised.

Watch it through `ResumeMatched` (304), where `Value0` is the number of retained bytes rewound.

## When a Resume fails

| What happened | Result |
|---|---|
| The secret matched no held Peer | The connection is closed with no explanation to it. `ResumeUnmatched` (305), `Value1` = 3. |
| A Hello presented a secret that did match | Refused as a protocol violation — a Resume has no business re-announcing parameters. `ResumeUnmatched` (305), `Value1` = 1. |
| The reported count names a point your retention has already released | `PeerClosed` with `RetentionGap`. Resuming would deliver a stream with a hole in it. |
| The reported count is above what you ever sent | `PeerClosed` with `ProtocolViolation`. `DeliveryCounterRefused` (308). |

`RetentionGap` is the actionable one: the peer asked to resume from bytes this end no longer holds.
The fix is configuration, not code — raise `RetentionBytes` so those bytes are still retained when a
peer comes back.
