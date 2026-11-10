# <a id="ReactorSockets_DisconnectReason"></a> Enum DisconnectReason

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

Why a Peer's Session ended, defined and kept stable by this library rather than borrowed from a
platform enumeration. Deliberately not shared with the Raw profile's [`CloseReason`](ReactorSockets.CloseReason.md)
(ADR 0008): most of the members a Session needs are protocol vocabulary Raw can never produce, and
a single shared enumeration would leave one profile documenting members it cannot emit.

```csharp
public enum DisconnectReason
```

## Fields

`LocalClose = 0` 

The consumer called <code>Close</code> on this Peer. In the datagram profile this also ends a Peer whose
own Close was in progress when the remote's Goodbye arrived: when both ends close at once, each
reports this and neither reports [`DisconnectReason.PeerGoodbye`](ReactorSockets.DisconnectReason.md).



`VersionMismatch = 1` 

The two ends do not speak the same protocol version. The version match is exact — there is no
negotiation, no lowest-common-version rule and no compatibility range — so this is produced
both by the end that refused and by the end whose Hello was refused.



`RemoteAtCapacity = 2` 

The remote end had no capacity Slot free for this Session and said so. Distinct from silence:
a Reactor at capacity answers a Hello it received rather than dropping it, so the dialler learns
the difference between "full" and "gone". It also means this address may be at the far end's
[`DatagramSessionOptions.MaxPeersPerAddress`](ReactorSockets.DatagramSessionOptions.MaxPeersPerAddress.md#ReactorSockets_DatagramSessionOptions_MaxPeersPerAddress) or
[`StreamSessionOptions.MaxPeersPerAddress`](ReactorSockets.StreamSessionOptions.MaxPeersPerAddress.md#ReactorSockets_StreamSessionOptions_MaxPeersPerAddress), so other Slots may be free.



`TransportFailed = 3` 

The underlying transport failed or ended before or during the handshake: a reset, a dial that
could not connect, or a stream that closed before a full handshake record arrived. Distinct
from every other member here because nothing about the protocol was at fault — the byte stream
itself did not survive to carry it. The same kind of failure on an <em>established</em> Peer
does not produce this reason at all — it moves the Peer to
[`SessionEventKind.PeerInterrupted`](ReactorSockets.SessionEventKind.md) instead, because its identity and undelivered
Messages are still worth holding for a Resume. This member now fires only for a Peer that never
finished becoming one.

The datagram profile has no transport to fail, and produces this member for a dial that got no
answer: one still pending its own [`DatagramSessionOptions.Linger`](ReactorSockets.DatagramSessionOptions.Linger.md#ReactorSockets_DatagramSessionOptions_Linger) after
[`DatagramSessionReactor.Connect(IPEndPoint)`](ReactorSockets.DatagramSessionReactor.Connect.md#ReactorSockets_DatagramSessionReactor_Connect_System_Net_IPEndPoint_), its handshake resent throughout (ADR 0009, amendment
of 2026-11-10).

`ProtocolViolation = 4` 

An established Peer's Message framing broke this profile's rules: a length prefix exceeding
the announced <code>MaxMessageSize</code>, a length prefix of zero, a stream that ended before a
length prefix or the Message body it named had fully arrived, or bytes arriving behind a
Goodbye already received from that Peer — a Peer that announced it was leaving and then kept
sending is contradicting its own announcement, so it is disconnected rather than believed.
On the stream profile it also ends a dial the far end refused for sending more than one
further handshake record's worth of bytes behind its first record before it was answered.
Distinct from [`DisconnectReason.TransportFailed`](ReactorSockets.DisconnectReason.md): the byte stream itself survived long enough to
carry the violation, and what is wrong is the bytes it carried rather than the transport
underneath it. Distinct from [`DisconnectReason.PeerGoodbye`](ReactorSockets.DisconnectReason.md), which is reserved for an ending that
stayed clean to the last byte.



`PeerGoodbye = 5` 

The remote Peer sent a Goodbye: it is ending the Session on purpose rather than failing.
Produced once whatever this end still held for it has finished flushing — immediately, if
nothing was outstanding when the Goodbye arrived, or after the <code>Closing</code> Session it entered
([`SessionEventKind.PeerClosing`](ReactorSockets.SessionEventKind.md)) has drained. The datagram profile raises it the
same way, once the Goodbye reaches its turn in <code>ReliableOrdered</code> send order, except that a Peer
whose own <code>Close</code> is already in progress acknowledges the Goodbye and ends with
[`DisconnectReason.LocalClose`](ReactorSockets.DisconnectReason.md) instead.



`LingerExpired = 6` 

An [`SessionEventKind.PeerInterrupted`](ReactorSockets.SessionEventKind.md) Peer's effective Linger — the value
<code>LingerNegotiated</code> already reported at handshake (ADR 0011), the minimum of the two ends'
announced Lingers — elapsed before the transport came back (stream profile, datagram profile).
Linger, not the loss of the transport, is what closes an Interrupted Peer: the loss itself only
produced [`SessionEventKind.PeerInterrupted`](ReactorSockets.SessionEventKind.md) and held the Peer's identity, counters
and undelivered Messages open for a Resume. This member fires once that wait has run out, and is
also what bounds a [`StreamSessionReactor.Close(PeerId)`](ReactorSockets.StreamSessionReactor.Close.md#ReactorSockets_StreamSessionReactor_Close_ReactorSockets_PeerId_) whose Goodbye can never reach a wire
that no longer exists — without it, that Peer's retention would sit pending forever. Distinct
from [`DisconnectReason.TransportFailed`](ReactorSockets.DisconnectReason.md), which means the byte stream never survived long enough to
say anything at all — this means it survived, said nothing further within the time this end had
promised to wait, and this end gave up on its own clock rather than being told to. Distinct from
[`DisconnectReason.PeerGoodbye`](ReactorSockets.DisconnectReason.md), which is an ending the remote announced on purpose while its transport
still worked — this is an ending this end declares unilaterally, after silence, with no
announcement possible because the transport that would have carried one is already gone.

The datagram profile ([`DatagramSessionReactor`](ReactorSockets.DatagramSessionReactor.md)) has no socket to lose, so an
Interrupted datagram Peer's own <code>HasLingerExpired</code> reads the same
<code>PeerSlot.EffectiveLinger</code> against <code>PeerSlot.InterruptedTimestamp</code> — the Pump that set
<code>PeerSlot.IsInterrupted</code>, never re-stamped while it continues — that the stream profile's
<code>HasExceededLinger</code> reads against its own equivalent timestamp, and this member is what
that per-Pump pass produces once the wait runs out. Everything else this member's summary says
about Linger governing the close, not the transport loss that produced the interruption, holds
identically for both profiles. A datagram Peer in Closing
([`DatagramSessionReactor.Close(PeerId)`](ReactorSockets.DatagramSessionReactor.Close.md#ReactorSockets_DatagramSessionReactor_Close_ReactorSockets_PeerId_)) also ends with this reason when its Goodbye is
still unacknowledged once its effective Linger has elapsed since <code>Close</code>.

`RetentionGap = 7` 

A Resume's reported delivery counter named a Message position below this end's retention ring —
the peer confirmed nothing further, but by the time it reconnected the bytes at its reported
position had already been released to make room for newer ones
([`SendOutcome.RetentionExhausted`](ReactorSockets.SendOutcome.md) during the interruption). Resuming anyway would
deliver a stream with a hole in it, so this end closes instead. Distinct from
[`DisconnectReason.LingerExpired`](ReactorSockets.DisconnectReason.md) and [`DisconnectReason.TransportFailed`](ReactorSockets.DisconnectReason.md) because both of those are silent
about *why* the Session ended — nothing was wrong with what either side said, only with how long
the transport stayed reachable — while this one is a direct statement about what the peer said:
its retention window was too small for the outage it just had. That makes the remedy actionable
and specific — raise this end's retention capacity or its Linger, or both — in a way neither of
those two members is. The mirror case, a Resume counter naming a Message this end never sent at
all, is not this member: that is a protocol violation ([`DisconnectReason.ProtocolViolation`](ReactorSockets.DisconnectReason.md)), not a
capacity question, because no amount of retention would have made a fabricated counter valid.



`FragmentSizeUnviable = 8` 

The configured fragment size ([`DatagramSessionOptions.FragmentSize`](ReactorSockets.DatagramSessionOptions.FragmentSize.md#ReactorSockets_DatagramSessionOptions_FragmentSize)) is larger than
this network path will carry. Detected from the fragment-size stall signature
(`IsFragmentSizeStall`): unfragmented reliable records kept
being acknowledged — proving the path and this Peer are both reachable — while not one Fragment
was acknowledged across `StallRoundCount` retransmission
rounds, which is only explained by every Fragment being lost every round because of its size
rather than by chance. The remedy is to lower [`DatagramSessionOptions.FragmentSize`](ReactorSockets.DatagramSessionOptions.FragmentSize.md#ReactorSockets_DatagramSessionOptions_FragmentSize);
nothing this library can change fixes it, since the configuration itself is what will not fit.

This member is a diagnosis, not a fault this end tries to work around: no probe datagram is sent,
no fragment size is reduced, nothing smaller is retried, and the Session simply closes with this
reason. Each of those would mean measuring a path this library deliberately declines to probe
(ADR 0004) and replacing a size the consumer configured with one this library guessed, so recovery
is left entirely to the consumer changing their own configuration and reconnecting. A Message
already cut into Fragments keeps the size it was cut at, because fragment size is fixed at
[`DatagramSessionReactor.Send(PeerId, DeliveryMode, ReadOnlySpan<byte>)`](ReactorSockets.DatagramSessionReactor.Send.md#ReactorSockets_DatagramSessionReactor_Send_ReactorSockets_PeerId_ReactorSockets_DeliveryMode_System_ReadOnlySpan_System_Byte__) and retransmits are byte-identical. Distinct from
[`DisconnectReason.TransportFailed`](ReactorSockets.DisconnectReason.md) and [`DisconnectReason.LingerExpired`](ReactorSockets.DisconnectReason.md), both of which are silent about
*why* the Session ended — nothing here said anything was wrong with the bytes, only that the
transport stopped answering — while this member is a direct statement about what was observed:
unfragmented traffic proved the path alive while every Fragment vanished. Distinct from
[`DisconnectReason.RetentionGap`](ReactorSockets.DisconnectReason.md), the other member whose remedy is a configuration change: that one
names a capacity too small for an outage already survived, this one names a size too large for
the path carrying it. Named for the setting that cannot work, [`DatagramSessionOptions.FragmentSize`](ReactorSockets.DatagramSessionOptions.FragmentSize.md#ReactorSockets_DatagramSessionOptions_FragmentSize),
rather than for the stall that revealed it, because the stall is only the symptom and the
fragment size is what the consumer can actually change.

## Remarks

<p>
Explicit values throughout, house style since <code>RawLinkLegality</code>: a member is added at the end
with the next free number, never inserted, so a value a consumer has already persisted keeps its
meaning across a library upgrade.
</p>
<p>
Every place this library maps something else onto a [`DisconnectReason`](ReactorSockets.DisconnectReason.md) — a wire
`RefusalReason`, a malformed-framing outcome, a Goodbye control record — states
that mapping as its own table with a discard arm that refuses rather than permits, the same house
rule <code>RawLinkLegality</code> follows: an input this library does not recognise fails closed instead
of being guessed at.
</p>

