# Interruption and Resume

UDP has no connection to lose, so the only sign of trouble is silence. A Peer that goes silent is
interrupted, not ended. The Peer is held, both ends keep probing, and the Session continues when
they hear each other again.

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

Do **not** close the Peer, drop your state for it, or re-send Messages. Unacknowledged reliable
records are still retained and keep being retransmitted, so they arrive without your help once the
path returns.

Ignoring both events entirely is safe. Your Sends keep queueing and your Messages keep arriving; you
just will not know there was an outage.

## Heartbeats and the two timers

Every established Peer runs two independent timers.

**Send-idle.** Sent no data Datagram for `SendIdleInterval`? The Reactor sends a Ping. A Ping is a
reliable record, so it is acknowledged and retransmitted like a Message, which proves the party
behind the Session is there rather than just some NAT mapping. Your own Sends reset the timer — a
busy Session never pings. It cannot be disabled.

**Receive-idle.** Received nothing for `peer's announced SendIdleInterval × your MissedHeartbeats`?
The Peer is Interrupted.

The deadline uses the interval the *peer* announced at handshake, not yours. Two ends with different
intervals each hold the other to what it actually promised.

The deadline does not stretch when the path degrades. Retransmission backs off on a bad path, but
the receive-idle deadline stays a plain count of missed intervals. A peer announcing 10 seconds, at
the default `MissedHeartbeats` of 3, is declared lost after 30 seconds of silence.

Only a Datagram that parses fully resets the receive-idle timer. Rubbish carrying a valid session id
does not keep a Peer alive.

## What interrupts a Peer

Only the receive-idle deadline. There is no FIN, no reset and no write failure to react to, and
ICMP errors are ignored.

A Closing Peer is never interrupted. Its Linger runs from `Close` instead. See
[Closing](04-closing.md).

## What survives an interruption

Everything. An Interrupted Peer is still an established Session that has stopped hearing its peer.

- The `PeerId`, generation included.
- `CountersOf(peer)` — it keeps answering.
- Both reliable Spaces' retention, sequence state and acknowledgement state.
- Partially reassembled Fragment sets.
- The Pings and retransmissions, which keep going out for as long as Linger holds the Peer.

`Send` still works. A Send to an Interrupted Peer is exactly as welcome as one to a live Peer. If
the outage outlasts your send rate, retention fills and `Send` returns `RetentionExhausted` as
usual. That is the signal to stop until the Peer resumes.

## Linger governs everything

At handshake both ends announce their `Linger` and the **minimum governs**. Read the negotiated
value from the `LingerNegotiated` (301) diagnostic: `Value0` is your own in milliseconds, `Value1`
is the effective one.

Linger is the only thing that ends an interruption. The interrupted Peer closes with
`LingerExpired` once the effective Linger has elapsed since it was interrupted. Both ends give up
at the same negotiated minimum.

Longer Linger means longer outages survived and Slots held longer. Shorter means faster Slot reuse
and shorter outages survived. That trade is the whole knob.

## Resume

There is no re-dial. A datagram Session resumes by being heard from.

**The dialling end** sends a Resume control Datagram on the Pump that interrupts its Peer, and
another every `SendIdleInterval` after that. The Resume names the accepting end's session id and
presents the resume secret exchanged at handshake. The dialling end resumes its own Peer when it next
receives a well-formed data Datagram for the Session.

**The accepting end** never sends Resume, because it cannot know the dialler's new address. It resumes its
Peer when it receives a well-formed data Datagram for the Session from the exact address and port it
sends to, and that moves nothing. When a Resume arrives with the right session id and secret, what
happens depends on the IP address it came from:

- **The same IP address the Peer is already sent to, on any port.** The accepting end points every
  later Datagram at the address the Resume came from, and resumes its Peer if it was interrupted.
- **A different IP address.** The accepting end first sends a PathChallenge control Datagram there,
  carrying a random nonce. The dialling end echoes the nonce in a PathResponse. Only when that echo
  arrives from the exact address the challenge went to does the accepting end move its send address and
  resume its Peer. Until then the Peer is sent to its old address.

That address move is what lets a Session survive a NAT rebinding or a change of network on the
dialling side. A rebinding that changes only the port costs nothing extra. A change of IP address
costs one extra round trip. The accepting end moves its send address only on a matched Resume, never
on a data Datagram from a new address.

The challenge exists because the secret proves who sent a Resume, not that its source address will
receive. Without it, any host that completed a handshake could name another host's address and have
the accepting end's traffic sent there ([ADR 0021](../../adr/0021-a-resume-from-a-new-ip-address-is-proven-by-a-path-challenge.md)).

`PeerResumed` carries the same `PeerId`, generation included. No `PeerClosed` is ever observed in
between, and `PeerCounters.Resumes` increments on each end that raised it.

Each end decides on its own whether it was interrupted. One end can raise `PeerInterrupted` and
`PeerResumed` while the other raises neither.

Nothing is rewound. Retransmission already resends whatever is unacknowledged, so `ResumeMatched`
(304) always reports `Value0` = 0 on this profile.

## Capacity while a Peer is interrupted

An Interrupted Peer is an occupied Slot. While it is held, a fresh handshake from anyone can be
refused with `RemoteAtCapacity`.

A Resume needs no Slot. It continues the existing one, so a Peer that is coming back is never
refused for capacity.

Size `Capacity` for this. A Reactor at capacity with several interrupted Peers turns new handshakes
away for up to Linger.

## When a Resume is refused

A refused Resume changes nothing. No event is raised, the Peer stays as it was, and the sender is
told nothing.

| What happened | `ResumeUnmatched` (305) |
|---|---|
| The session id names no Peer, or names a Peer this end dialled | `Value0` = 0, `Value1` = 6 |
| The session id names a Peer, but the secret is wrong | `Value0` = 1, `Value1` = 6 |
| The secret matched, but a Goodbye has passed in either direction | `Value0` = 1, `Value1` = 6 |

The secret is compared in constant time.

A PathResponse that names no pending challenge, comes from any address other than the one challenged,
or echoes the wrong nonce is discarded as `DatagramMalformed` and changes nothing. A correct echo for a
Peer that has passed a Goodbye in either direction since is refused as the last row above.
