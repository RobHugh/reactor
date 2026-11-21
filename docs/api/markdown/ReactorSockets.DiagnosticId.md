# <a id="ReactorSockets_DiagnosticId"></a> Enum DiagnosticId

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

A closed, permanent enumeration of diagnostic causes with explicit values in reserved blocks —
common 1-99, Raw 100-199, datagram session 200-299, stream session 300-399 (ADR 0013) — so a
future insertion cannot silently renumber an id a consumer has persisted. There is no
<code>None</code> or <code>Unknown</code> member and zero is never used: every [`DiagnosticRecord`](ReactorSockets.DiagnosticRecord.md)
has a cause by construction.

```csharp
public enum DiagnosticId
```

## Fields

`AdmissionCapReached = 1` 

The configured admission cap was hit this Pump with work still waiting to be admitted. No
subject. <code>Value0</code> is the cap, <code>Value1</code> is the count of Admissions declined,
counted as the Reactor's counters count them: once per declining where the work waits out of
reach — a listening Link or bound endpoint with a pending connection it left in the OS
backlog, never the number of connections left there, a depth unknowable without accepting —
and once per piece of work where the Reactor was holding it, whether it then waits or is
Discarded.



`ArenaExhausted = 2` 

The frame arena had no room left for a request, regardless of the read-share escape.
Subject is the Link that made the request on Raw, and the Peer whose Message could not be
reserved on either session profile (ADR 0014). <code>Value0</code> is the bytes requested,
<code>Value1</code> is the arena's bytes free at the moment of the request.



`CapacityExhausted = 3` 

Every capacity slot was occupied this Pump with work still waiting to be admitted — a
different exhaustion from [`DiagnosticId.AdmissionCapReached`](ReactorSockets.DiagnosticId.md), reachable because the admission
cap can be configured above [`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity). No subject.
<code>Value0</code> is the capacity, <code>Value1</code> is the count of Admissions declined, counted the
same way and with the same limit as [`DiagnosticId.AdmissionCapReached`](ReactorSockets.DiagnosticId.md)'s.



`LinkFailed = 100` 

A registered Link failed and closed. Subject is the Link. <code>Value0</code> carries the
platform [`SocketError`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror) as its underlying integer value;
<code>Value1</code> is unused.



`DestinationUnreachable = 101` 

An ICMP port-unreachable (or platform equivalent) was reported for a send. Subject is the
Link. <code>Value0</code> carries the platform [`SocketError`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror) as its
underlying integer value; <code>Value1</code> is unused.



`DatagramTruncated = 102` 

A datagram exceeded the Reactor's scratch buffer and was truncated. Subject is the claimed
Source. <code>Value0</code> is the scratch buffer size in bytes; <code>Value1</code> is unused.



`DatagramMalformed = 200` 

A datagram was discarded because nothing in it could be attributed to a Peer: wrong length,
an unknown control type, a body the declared type cannot fill, padding that was not zero, a
session id naming no Session, or a well-formed control reply — a Cookie, an Accept, a
Refusal — matching no dial this Reactor is waiting on. That last case is not a parse failure,
but it is the same thing to a consumer: bytes arrived, named nobody, and went nowhere.
Subject is the claimed Source. <code>Value0</code> is the bytes received; <code>Value1</code> is unused.

A remote can produce this at will by sending rubbish, which is exactly why it is a Diagnostic
and never an Event: the event stream is the consumer's protocol, and an unauthenticated
stranger must not be able to write to it. [`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md) rather than
[`DiagnosticLevel.Error`](ReactorSockets.DiagnosticLevel.md) for the same reason — nothing is wrong with this Reactor.

`CookieRejected = 201` 

A Hello carried a cookie this Reactor did not mint, or minted too long ago: a forged or
corrupted authenticator, a cookie replayed from a different source address, or one from an
epoch older than the previous. Subject is the claimed Source. <code>Value0</code> is the epoch the
cookie offered; <code>Value1</code> is the epoch current at the moment it was checked.

[`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md), never [`DiagnosticLevel.Error`](ReactorSockets.DiagnosticLevel.md): a cookie
arriving one epoch after it was minted is the mechanism working, and a Reactor visible to the
internet will see forged ones continuously. Raising this to Error would train a consumer to
ignore Error.

`VersionRefused = 202` 

A cookie-validated Hello offered a protocol version this Reactor does not speak, and was
refused. Subject is the claimed Source. <code>Value0</code> is the version offered; <code>Value1</code> is
the version this Reactor requires.

Emitted only for a Hello that passed the cookie check, because that is the only Hello a
Reactor answers at all — a bare Hello is answered with a Cookie and nothing else, whatever
version it claimed.

`FragmentSetAbandoned = 203` 

A Fragment set for an established Peer was abandoned before it ever completed. Subject is the
Peer, not the claimed Source: an incomplete set belongs to the Session that was receiving it,
not to a bare source address. <code>Value0</code> is the number of fragments actually held for the
set at the moment it was abandoned; <code>Value1</code> is the number the set's own metadata
declared it would need. Held against expected is the pair a consumer can act on — one of forty
held is a path dropping almost everything of that size, thirty-nine of forty is ordinary loss
on a set that was nearly there, and the two want different responses.

[`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md), not [`DiagnosticLevel.Error`](ReactorSockets.DiagnosticLevel.md): nothing local is
broken, an abandoned set is loss on a lossy transport working as designed, and the level's job
is to answer whether a consumer wants to see this, not whether this Reactor is at fault.

`RetransmissionTimerBackedOff = 204` 

This Peer's `RetransmissionTimer` backed off: a Pump's expiry sweep
(`CollectExpired`) found at least one overdue
sequence on either reliable Space, so `BackOff` was called once
for the Peer and every sequence the sweep reported re-armed at the single resulting RTO. Subject
is the Peer. <code>Value0</code> is the new RTO in milliseconds —
`EffectiveTimeout` at the moment of the backoff; <code>Value1</code> is
`ConsecutiveBackoffs` after this backoff.

[`DiagnosticLevel.Trace`](ReactorSockets.DiagnosticLevel.md), not [`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md): this is protocol
machinery working correctly, not a fault. A lossy path emits it continuously — every Pump that
finds an overdue sequence backs off again — and raising it above <code>Trace</code> would train a
consumer to ignore the level it sits at.

`OversizedFragmentSet = 205` 

An established Peer's Fragment declared a <code>fragmentCount</code> larger than this Reactor's own
receive window could ever hold, and the Session was closed with
[`DisconnectReason.ProtocolViolation`](ReactorSockets.DisconnectReason.md) on that Fragment's first appearance, before any
receive-window slot or Fragment-group bookkeeping was created for its set. Subject is the Peer.
<code>Value0</code> is the declared <code>fragmentCount</code>; <code>Value1</code> is the largest legal count
this Reactor's [`DatagramSessionOptions.ResolvedReceiveWindowFragments`](ReactorSockets.DatagramSessionOptions.ResolvedReceiveWindowFragments.md#ReactorSockets_DatagramSessionOptions_ResolvedReceiveWindowFragments) allows.

[`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md), for the same reason as [`DiagnosticId.DatagramMalformed`](ReactorSockets.DiagnosticId.md): a
remote can produce this at will by lying about how many Fragments a Message needs.

`UnreliableFragment = 206` 

A packet record carried the fragment shape in one of the two unreliable modes, which ADR 0004
forbids — you may only fragment what you can retransmit — and the read failed outright, taking
the whole Datagram with it. Subject is the claimed Source, not the Peer, matching the
[`DiagnosticId.DatagramMalformed`](ReactorSockets.DiagnosticId.md) that is raised alongside it: a discarded Datagram is never
attributed to the Session it named, and no byte of it is counted against that Peer.
<code>Value0</code> is the mode the record claimed, as the two mode bits' wire value — 0 for
<code>Unreliable</code>, 1 for <code>UnreliableSequenced</code>; <code>Value1</code> is unused.

<p>
[`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md), for the same reason as [`DiagnosticId.DatagramMalformed`](ReactorSockets.DiagnosticId.md): a
remote can produce this at will, and nothing is wrong with this Reactor.
</p>
<p>
Distinct from [`DiagnosticId.DatagramMalformed`](ReactorSockets.DiagnosticId.md) rather than folded into it because the two say
different things to a consumer. A malformed Datagram is rubbish from anywhere; this one is
well-formed at every level but the rule, which means the sender is running a build that
fragments in modes this profile does not — a misconfiguration a consumer can act on, and the
send path already refuses to produce.
</p>

`OversizedMessage = 207` 

A completed Message, whole or reassembled from its Fragments, was larger than this Reactor's own
[`DatagramSessionOptions.MaxMessageSize`](ReactorSockets.DatagramSessionOptions.MaxMessageSize.md#ReactorSockets_DatagramSessionOptions_MaxMessageSize) and was discarded instead of delivered.
Subject is the Peer. <code>Value0</code> is the Message's length in bytes; <code>Value1</code> is this
Reactor's <code>MaxMessageSize</code>. The Session stays open, and a discarded reliable Message stays
acknowledged, so the far end does not resend it and later Messages are unaffected.

[`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md), for the same reason as [`DiagnosticId.DatagramMalformed`](ReactorSockets.DiagnosticId.md): a
remote can produce this at will, and nothing is wrong with this Reactor.

`AddressPeerLimitReached = 208` 

A verified handshake, or on the stream profile an accepted connection, was refused because its
address already holds [`DatagramSessionOptions.MaxPeersPerAddress`](ReactorSockets.DatagramSessionOptions.MaxPeersPerAddress.md#ReactorSockets_DatagramSessionOptions_MaxPeersPerAddress) or
[`StreamSessionOptions.MaxPeersPerAddress`](ReactorSockets.StreamSessionOptions.MaxPeersPerAddress.md#ReactorSockets_StreamSessionOptions_MaxPeersPerAddress) accepted Peers while other capacity Slots
were free. On the stream profile it is also logged when an accepted connection is closed before its
first handshake record arrives, because its address already has that many other connections held
waiting for theirs. Subject is the claimed Source. <code>Value0</code> is the limit; <code>Value1</code> is the prefix
length the address was counted by: 32 for IPv4, and
[`DatagramSessionOptions.PeerLimitIPv6PrefixLength`](ReactorSockets.DatagramSessionOptions.PeerLimitIPv6PrefixLength.md#ReactorSockets_DatagramSessionOptions_PeerLimitIPv6PrefixLength) or
[`StreamSessionOptions.PeerLimitIPv6PrefixLength`](ReactorSockets.StreamSessionOptions.PeerLimitIPv6PrefixLength.md#ReactorSockets_StreamSessionOptions_PeerLimitIPv6PrefixLength) for IPv6, so every address in that
prefix shares the limit.
One record per refusal, because it names the address and the admission cap already bounds how
many refusals a Pump can make. A full Reactor reports [`DiagnosticId.CapacityExhausted`](ReactorSockets.DiagnosticId.md) on the
datagram profile and [`DiagnosticId.PeerRefused`](ReactorSockets.DiagnosticId.md) on the stream profile instead.

[`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md): a remote can produce this at will, and a consumer can act on it
by raising the limit if the address is a NAT or proxy, or by blocking it if it is not. Defined by
the datagram profile and shared by the stream profile, because it is one condition (ADR 0020).

`DatagramReadLimitReached = 209` 

A Pump made [`DatagramSessionOptions.MaxDatagramsPerPump`](ReactorSockets.DatagramSessionOptions.MaxDatagramsPerPump.md#ReactorSockets_DatagramSessionOptions_MaxDatagramsPerPump) reads on one bound socket and
stopped. What was left, if anything, waits for a later Pump. No subject. <code>Value0</code> is the
limit; <code>Value1</code> is the socket's index in
[`DatagramSessionReactor.BoundEndpoints`](ReactorSockets.DatagramSessionReactor.BoundEndpoints.md#ReactorSockets_DatagramSessionReactor_BoundEndpoints). At most one record per bound socket per Pump.

[`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md): a remote can produce this at will, and it names a configured
bound a consumer can act on. A queued socket error spends a read, so not every read was a datagram.
The socket's queue depth cannot be read past queued errors, so a Pump that read exactly the last
queued datagram also reports this.

`OversizedFragment = 210` 

An established Peer's reliable Fragment carried more payload bytes than this Reactor's own
[`DatagramSessionOptions.ResolvedFragmentSize`](ReactorSockets.DatagramSessionOptions.ResolvedFragmentSize.md#ReactorSockets_DatagramSessionOptions_ResolvedFragmentSize), and the Session was closed with
[`DisconnectReason.ProtocolViolation`](ReactorSockets.DisconnectReason.md) before anything in that Datagram was
acknowledged (ADR 0024). Subject is the Peer. <code>Value0</code> is the Fragment's payload length in
bytes; <code>Value1</code> is this Reactor's <code>ResolvedFragmentSize</code>.

[`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md), for the same reason as [`DiagnosticId.DatagramMalformed`](ReactorSockets.DiagnosticId.md): a
remote can produce this at will. The handshake announces the size, so a conforming peer never
sends a wider Fragment, and the width and the size say which end is misbuilt.

`PeerRefused = 300` 

A dialler was refused in-protocol during the stream handshake, either because it announced a
different protocol version or because every capacity Slot was occupied. Subject is the claimed
Source: at refusal time there is no Peer, only a claimed origin. For a capacity refusal,
<code>Value0</code> is this Reactor's capacity and <code>Value1</code> is unused. For a version refusal,
<code>Value0</code> is the version offered and <code>Value1</code> is the version this Reactor requires.

[`DiagnosticLevel.Info`](ReactorSockets.DiagnosticLevel.md), not [`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md): a version mismatch
on an upgrade and an ordinary burst at capacity are both routine, expected outcomes of running
this profile, not conditions a consumer needs to act on.

`LingerNegotiated = 301` 

A Session established — on either profile, stream or datagram — and the two ends' announced
Linger values were negotiated to their minimum (ADR 0011). Subject is the newly established
Peer. <code>Value0</code> is this end's own Linger in milliseconds; <code>Value1</code> is the effective
(minimum) Linger in milliseconds.



`MalformedFraming = 302` 

An established Peer's Message framing broke this profile's rules and the Session was closed
with [`DisconnectReason.ProtocolViolation`](ReactorSockets.DisconnectReason.md): an oversized or zero-length prefix, or a
stream that ended before a prefix or the Message body it named had fully arrived. Subject is
the Peer. <code>Value0</code> is the offending length prefix — 0 for a truncation, where no full
prefix was ever read; <code>Value1</code> is this Reactor's own <code>MaxMessageSize</code>.

[`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md): a remote can produce this at will by sending malformed
bytes, which is exactly why the Session ends rather than throws, and why this is a Diagnostic
rather than an Event on the consumer's own stream.

`DataAfterGoodbye = 303` 

An established Peer sent Message bytes after its own Goodbye had already been received, and
the Session was closed with [`DisconnectReason.ProtocolViolation`](ReactorSockets.DisconnectReason.md). Subject is the
Peer. Carries no claimed length — `MessageFramingLegality` reaches this
outcome before ever reading a prefix, so there is nothing to report but the contradiction
itself. <code>Value0</code> and <code>Value1</code> are unused.

[`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md), for the same reason as [`DiagnosticId.MalformedFraming`](ReactorSockets.DiagnosticId.md): a
remote can produce this at will by ignoring the Goodbye it just sent.

`ResumeMatched = 304` 

A connection presenting a valid resume secret was matched onto the Interrupted Peer whose Slot
was reserving the Session it named, and that Session continued over the replacement transport
rather than a new one being established (ADR 0011). Subject is the resumed Peer — the same
[`PeerId`](ReactorSockets.PeerId.md), generation included, the Session had before the interruption, since
nothing about a Peer's identity is renewed by a Resume. <code>Value0</code> is the number of retained
outbound bytes the match rewound to the counter the Resume reported; <code>Value1</code> is unused. On
the datagram profile it is logged when a Resume control datagram matches the Peer on the
accepting end (session id and secret), when an Interrupted accepting end receives a well-formed
data Datagram for the Session from the exact address and port it sends to, and when an
Interrupted dialling end that has sent a Resume hears its session again. Nothing is rewound, because retransmission already re-sends
anything unacknowledged, so <code>Value0</code> is 0 and <code>Value1</code> is unused.

<p>
[`DiagnosticLevel.Info`](ReactorSockets.DiagnosticLevel.md): a reconnection is the outcome reserving a Slot exists to
produce, not a condition a consumer needs to act on.
</p>
<p>
Shared with the datagram session profile rather than duplicated in the datagram block, as are
[`DiagnosticId.LingerNegotiated`](ReactorSockets.DiagnosticId.md) and [`DiagnosticId.ResumeUnmatched`](ReactorSockets.DiagnosticId.md) (ADR 0020).
</p>
<p>
Numbered 304 because 302 and 303 belong to [`DiagnosticId.MalformedFraming`](ReactorSockets.DiagnosticId.md) and
[`DiagnosticId.DataAfterGoodbye`](ReactorSockets.DiagnosticId.md), and a value published in a closed permanent enum is never
renumbered.
</p>

`ResumeUnmatched = 305` 

A connection claiming an existing Session was refused rather than resumed. Subject is the
claimed Source: a claim that is not matched comes from a party that is by construction not a
Peer, so there is no Peer to name, only a claimed origin. <code>Value0</code> is the number of
Interrupted Peers whose reservations the claim was checked against — 0 means nothing was being
held at all, which is the reconfiguration path where a once-valid secret arrives after its
Linger expired. <code>Value1</code> is the wire record type the claim arrived as: 3 for a Resume
whose secret matched no held Peer, and 1 for a Hello presenting a secret that did match, which
re-announces the parameters a Resume has no field for and is refused as a protocol violation.

<p>
[`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md) rather than the [`DiagnosticLevel.Info`](ReactorSockets.DiagnosticLevel.md) a version
mismatch or a capacity refusal carries: those are routine outcomes of running this profile and
a wrong secret is not.
</p>
<p>
"Unmatched" names the outcome, not the comparison: the claim was not matched onto a Session.
The Hello case reaches here having found the secret it announced, and is refused anyway because
the record it arrived in re-announces parameters — <code>Value1</code> is what tells the two apart.
</p>
<p>
<code>Value0</code> is reported to this end's own Sink and never to the wire: a refused party is
closed on without being told anything, so it learns neither the count nor which of the three
refusals it met.
</p>
<p>
The datagram profile reports a Resume control datagram the same way, with <code>Value1</code> 6 (the
Resume control type, <code>DatagramControlType.Resume</code>). <code>Value0</code> is 0 when the target
session id names no Peer the Reactor holds, or names a dialled Peer (never a Resume target, since only
the dialling end sends Resume), and 1 when it names a Peer but the presented secret
does not equal the one that Peer issued.
</p>

`RedialAttempted = 306` 

A dialling-side Peer began a fresh re-dial attempt after its transport was lost (ADR 0011).
Subject is the Peer. <code>Value0</code> is the attempt number, starting at 1 and rising by one per
attempt; <code>Value1</code> is the delay in milliseconds this attempt actually waited before dialling
— the jittered figure `NextDelay` drew, so what actually
elapsed is observable rather than only the computed figure before jitter.

[`DiagnosticLevel.Info`](ReactorSockets.DiagnosticLevel.md): a re-dial is the ordinary behaviour of a dialling Peer that
lost its transport, not a condition a consumer needs to act on.

`RedialFailed = 307` 

A re-dial attempt could not connect, or connected and ended before the listener confirmed the
Resume (ADR 0011). Subject is the Peer, which this never closes — it stays Interrupted and
waits out the schedule's next delay, leaving Linger, not an attempt count, the only thing that
ever closes it. <code>Value0</code> carries the platform [`SocketError`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror)
as its underlying integer value: for a connection that ended before confirmation it is
[`Disconnecting`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror.disconnecting) for an end of stream, or the read's
error. It is [`TimedOut`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror.timedout) when the listener did not
answer within the receive-idle deadline, and [`SocketError`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror.socketerror)
when the Resume record could not be sent. <code>Value1</code> is the attempt number that failed.

[`DiagnosticLevel.Info`](ReactorSockets.DiagnosticLevel.md) rather than [`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md): under this
profile's premise a failed reconnection attempt is an ordinary, expected outcome, and raising it
to Warn would train a consumer to ignore Warn.

`DeliveryCounterRefused = 308` 

An established Peer's delivery-counter record claimed to have received more Messages than this
end has ever sent it, and the Session was closed with
[`DisconnectReason.ProtocolViolation`](ReactorSockets.DisconnectReason.md) — the peer is confirming Messages this end
never queued, rather than merely repeating a count it already reported. Subject is the Peer.
<code>Value0</code> is the claimed cumulative count; <code>Value1</code> is the count of Messages (and, if
this end's own Goodbye has already been queued for this Peer, that too) this end has actually
appended to this Peer's retention — the largest count a legitimate confirmation could ever sit
at or below.

<p>
[`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md), for the same reason as [`DiagnosticId.MalformedFraming`](ReactorSockets.DiagnosticId.md): a
remote can produce this at will by inflating its own counter.
</p>
<p>
Numbered 308 rather than the next free 304: ADR 0013's roster reserves 304-307 for the Resume
and redial Diagnostics ([`DiagnosticId.ResumeMatched`](ReactorSockets.DiagnosticId.md) and [`DiagnosticId.ResumeUnmatched`](ReactorSockets.DiagnosticId.md), then
RedialAttempted and RedialFailed), and a value published in a closed permanent enum cannot be
moved later to make room.
</p>

`PendingAdmissionExpired = 309` 

A connection the stream profile accepted sent no whole first handshake record within this end's
[`StreamSessionOptions.SendIdleInterval`](ReactorSockets.StreamSessionOptions.SendIdleInterval.md#ReactorSockets_StreamSessionOptions_SendIdleInterval) × [`StreamSessionOptions.MissedHeartbeats`](ReactorSockets.StreamSessionOptions.MissedHeartbeats.md#ReactorSockets_StreamSessionOptions_MissedHeartbeats)
of being accepted, and was closed with nothing sent. Subject is the claimed Source. <code>Value0</code>
is the bytes of that record it had sent, 0 to 40; <code>Value1</code> is the deadline in milliseconds.

[`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md): a remote can produce this at will by connecting and sending
nothing, and a consumer can act on it by blocking the address or, if conforming diallers are being
closed, by raising [`StreamSessionOptions.SendIdleInterval`](ReactorSockets.StreamSessionOptions.SendIdleInterval.md#ReactorSockets_StreamSessionOptions_SendIdleInterval). One record per connection,
because it names the address. The deadline is fixed at accept, so bytes that arrive do not extend it.
At most [`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity) connections are held, so no more records than that
are logged in one Pump.

`PendingAdmissionLimitReached = 310` 

A stream Reactor already held [`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity) accepted connections waiting
for their first handshake record, so it accepted no more that Pump and left the rest in the OS
backlog. No subject. <code>Value0</code> is the limit, which is the capacity; <code>Value1</code> is the count
of Admissions declined, counted as [`DiagnosticId.AdmissionCapReached`](ReactorSockets.DiagnosticId.md)'s are: once per listening Link
left with a pending connection.

[`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md): a remote can produce this at will by opening connections and
sending nothing. One record per Pump. The held connections close at their deadline
([`DiagnosticId.PendingAdmissionExpired`](ReactorSockets.DiagnosticId.md)), so the backlog is accepted again within that time.

## Remarks

<p>
<b>Supported Sink style: range-check the blocks.</b> The enum grows inside its blocks, so a Sink
that classifies an id by the block it sits in, or that switches on members it cares about with a
default arm for the rest, keeps working when a member is added. A Sink whose <code>switch</code> assumes
it lists every member is not supported: the next additive release breaks it.
</p>
<p>
<b>A block names the profile that defined a condition, not the profile that emitted a record</b>
(ADR 0020). [`DiagnosticId.LingerNegotiated`](ReactorSockets.DiagnosticId.md), [`DiagnosticId.ResumeMatched`](ReactorSockets.DiagnosticId.md) and
[`DiagnosticId.ResumeUnmatched`](ReactorSockets.DiagnosticId.md) sit in the stream block and are emitted by the datagram session
profile too, because each is one condition and a member names a condition. A Raw-only Sink's
range check over 100-199 is exact, since nothing outside Raw shares that block. A Sink that needs
to know which profile emitted a record knows which Reactor it was given to.
</p>
<p>
Each member's documentation is the authority on its subject and value slots. ADR 0013's roster table
is the one place to check them side by side and is kept in step with these docs.
</p>

