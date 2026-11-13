# <a id="ReactorSockets_StreamSessionOptions"></a> Class StreamSessionOptions

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

The construction surface of a [`StreamSessionReactor`](ReactorSockets.StreamSessionReactor.md) (ADR 0011): the shared L0 budget
composed in as [`StreamSessionOptions.Reactor`](ReactorSockets.StreamSessionOptions.Reactor.md#ReactorSockets_StreamSessionOptions_Reactor), plus the knobs this profile alone has. Composed rather than
inherited, so that nothing protocol-specific here can ever leak into [`ReactorOptions`](ReactorSockets.ReactorOptions.md)
and turn it into the base class ADR 0011 refused.

```csharp
public sealed class StreamSessionOptions
```

#### Inheritance

[object](https://learn.microsoft.com/dotnet/api/system.object) ← 
[StreamSessionOptions](ReactorSockets.StreamSessionOptions.md)

#### Inherited Members

[object.Equals\(object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\)), 
[object.Equals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\-system\-object\)), 
[object.GetHashCode\(\)](https://learn.microsoft.com/dotnet/api/system.object.gethashcode), 
[object.GetType\(\)](https://learn.microsoft.com/dotnet/api/system.object.gettype), 
[object.ReferenceEquals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.referenceequals), 
[object.ToString\(\)](https://learn.microsoft.com/dotnet/api/system.object.tostring)

## Remarks

<p>
A knob carries a default only when that default is <em>derived</em> — from machinery this library
owns, from another knob, or from a citable standard. A value that trades the consumer's own
resources against their own traffic, and that nothing in this library can derive, is required with
no default: [`StreamSessionOptions.MaxMessageSize`](ReactorSockets.StreamSessionOptions.MaxMessageSize.md#ReactorSockets_StreamSessionOptions_MaxMessageSize), [`StreamSessionOptions.RetentionBytes`](ReactorSockets.StreamSessionOptions.RetentionBytes.md#ReactorSockets_StreamSessionOptions_RetentionBytes),
[`StreamSessionOptions.SendIdleInterval`](ReactorSockets.StreamSessionOptions.SendIdleInterval.md#ReactorSockets_StreamSessionOptions_SendIdleInterval) and [`StreamSessionOptions.Linger`](ReactorSockets.StreamSessionOptions.Linger.md#ReactorSockets_StreamSessionOptions_Linger) are all of that kind, and so is
[`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity). Guessing any of them would be guessing at the consumer's
application.
</p>
<p>
There is no cookie, no ack, no sequence space, no fragment and no RTO here: TCP already provides
what those were built for, so a knob to configure one would have nothing under it.
</p>
<p>
<b>The memory bill.</b> A Reactor built from these options holds at most
<code>Capacity × (RetentionBytes + receive buffer + Message-boundary ring)</code> bytes, all of it per
Peer and none of it shared, so a <code>Send</code> on a healthy Peer never fails because of somebody
else's traffic. Retention and the ring are allocated at construction and each receive buffer when
its Peer takes a Slot, so the bound has no traffic term. The retention line is buffered <em>twice</em> over — once in our retention, once
again in the kernel's own send buffer for the socket — so a stream Peer's real footprint is this
bound plus whatever the OS is holding, and the OS's half is not fully ours to control. The third
line is what lets retention be released by what the peer has confirmed receiving rather than by what
the local kernel merely accepted, and is driven by [`StreamSessionOptions.RetentionBytes`](ReactorSockets.StreamSessionOptions.RetentionBytes.md#ReactorSockets_StreamSessionOptions_RetentionBytes) too: one <code>uint</code>
per Message that could be outstanding in a Peer's retention region at once, which this profile's
four-byte length prefix bounds at roughly one entry per four retained bytes, so it costs about as
much as retention itself.
</p>
<p>
[`StreamSessionOptions.MaxMessageSize`](ReactorSockets.StreamSessionOptions.MaxMessageSize.md#ReactorSockets_StreamSessionOptions_MaxMessageSize) appears in three places at once: announced to the remote as a receive
limit, flooring the receive buffer, and flooring [`StreamSessionOptions.RetentionBytes`](ReactorSockets.StreamSessionOptions.RetentionBytes.md#ReactorSockets_StreamSessionOptions_RetentionBytes). It is therefore the
one knob whose careless enlargement multiplies memory in two directions rather than one.
</p>
<p>
Nagle is off on every stream Session socket and there is no knob to turn it on (ADR 0011): this
library batches by writing everything queued since the last Pump once, so a latency measurement
measures the consumer's Pump cadence, not TCP's acknowledgement clock.
</p>
<p>
Every check below is deterministic and entirely the caller's, so failures throw at construction —
[`ArgumentOutOfRangeException`](https://learn.microsoft.com/dotnet/api/system.argumentoutofrangeexception) for a single bad value and [`ArgumentException`](https://learn.microsoft.com/dotnet/api/system.argumentexception)
for a combination that cannot work — rather than being reported through a <code>Result</code>.
</p>

## Properties

 [BoundaryRingBudgetBytes](ReactorSockets.StreamSessionOptions.BoundaryRingBudgetBytes.md\#ReactorSockets\_StreamSessionOptions\_BoundaryRingBudgetBytes)

The Message-boundary ring line of the memory bill in bytes: one <code>uint</code> (4 bytes) per entry
in `BoundaryRingEntriesPerPeer`, across every Peer, reported the same way as the
other two lines. This is what makes releasing retention by what the peer confirmed receiving
possible at all — see the class remarks.

 [Endpoints](ReactorSockets.StreamSessionOptions.Endpoints.md\#ReactorSockets\_StreamSessionOptions\_Endpoints)

The endpoints to bind at construction, in the order supplied; possibly empty. Declared once
and never rebound (ADR 0014) — a Session's addresses are part of what it is, not something it
acquires later. A consumer addresses Peers, never the sockets this profile binds, so binding
returns no identity: an empty set means dial-only, a meaningful configuration rather than a
hole, and there is no rebind — a Reactor that must listen elsewhere is a new Reactor.
Provenance: empty is the absence of a declaration, not a chosen address — a dial's local port
is the one TCP's connect binds implicitly.

 [Entropy](ReactorSockets.StreamSessionOptions.Entropy.md\#ReactorSockets\_StreamSessionOptions\_Entropy)

The entropy the resume secret is drawn from. A construction knob in ADR 0011's sense — the
consumer's own resource, and a consumer with a hardware source or a policy about which
generator may be used is entitled to supply it. It carries a default only because that default
is derived rather than guessed. Provenance: the platform's own CSPRNG.

 [Linger](ReactorSockets.StreamSessionOptions.Linger.md\#ReactorSockets\_StreamSessionOptions\_Linger)

How long a closing Session stays alive to retransmit its Goodbye and absorb what is still in
flight. Required, for the same reason as [`StreamSessionOptions.SendIdleInterval`](ReactorSockets.StreamSessionOptions.SendIdleInterval.md#ReactorSockets_StreamSessionOptions_SendIdleInterval): it trades a prompt
teardown against a clean one. It also bounds a dial: a Peer this end dialled that has not
established one Linger after [`StreamSessionReactor.Connect(IPEndPoint)`](ReactorSockets.StreamSessionReactor.Connect.md#ReactorSockets_StreamSessionReactor_Connect_System_Net_IPEndPoint_) closes with
[`DisconnectReason.TransportFailed`](ReactorSockets.DisconnectReason.md), so a Linger shorter than the round trip plus the
far end's Pump interval closes a dial before its answer arrives. At most 2^32 - 1 milliseconds
(about 49.7 days), the most a handshake may announce.

 [MaxMessageSize](ReactorSockets.StreamSessionOptions.MaxMessageSize.md\#ReactorSockets\_StreamSessionOptions\_MaxMessageSize)

The largest Message, in bytes, this Reactor will accept or deliver. Required: it is a
statement about the consumer's own protocol that nothing here can derive. It is also
announced to the remote as a receive limit, and floors both the receive buffer and
[`StreamSessionOptions.RetentionBytes`](ReactorSockets.StreamSessionOptions.RetentionBytes.md#ReactorSockets_StreamSessionOptions_RetentionBytes) — an arena that cannot hold one maximum Message could never
deliver one. At most 2 147 483 587, [`MaxLength`](https://learn.microsoft.com/dotnet/api/system.array.maxlength) less the 4-byte length prefix,
so one framed maximum Message fits one array.

 [MaxPeersPerAddress](ReactorSockets.StreamSessionOptions.MaxPeersPerAddress.md\#ReactorSockets\_StreamSessionOptions\_MaxPeersPerAddress)

The most accepted Peers one address may hold at once (issue #452), so one reachable address
cannot occupy every capacity Slot. Defaults to [`StreamSessionOptions.ResolvedMaxPeersPerAddress`](ReactorSockets.StreamSessionOptions.ResolvedMaxPeersPerAddress.md#ReactorSockets_StreamSessionOptions_ResolvedMaxPeersPerAddress), one fewer
than [`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity). Provenance: derived from capacity, as the largest limit
that still keeps one address from holding every Slot (ADR 0014). How many Peers legitimately share
an address depends on the NATs and proxies in front of this Reactor, so set it lower when strangers
can reach it. At least 1. A value of [`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity) or above sets no limit,
which is what the default is at a capacity of 1.

 [MissedHeartbeats](ReactorSockets.StreamSessionOptions.MissedHeartbeats.md\#ReactorSockets\_StreamSessionOptions\_MissedHeartbeats)

How many consecutive heartbeat intervals may pass in silence before a Peer is declared gone.
Defaults to 3. Provenance: RFC 4271's hold time, three times the keepalive interval. Must be at
least 2, because a count of 1 makes a
single lost heartbeat fatal, and in .NET the stall that produces it is as ordinary as a
collection pause.

 [PeerLimitIPv6PrefixLength](ReactorSockets.StreamSessionOptions.PeerLimitIPv6PrefixLength.md\#ReactorSockets\_StreamSessionOptions\_PeerLimitIPv6PrefixLength)

How many leading bits of an IPv6 address form the key for [`StreamSessionOptions.MaxPeersPerAddress`](ReactorSockets.StreamSessionOptions.MaxPeersPerAddress.md#ReactorSockets_StreamSessionOptions_MaxPeersPerAddress), so
every address in one prefix shares one limit. Defaults to [`StreamSessionOptions.ResolvedPeerLimitIPv6PrefixLength`](ReactorSockets.StreamSessionOptions.ResolvedPeerLimitIPv6PrefixLength.md#ReactorSockets_StreamSessionOptions_ResolvedPeerLimitIPv6PrefixLength),
64. Provenance: RFC 4291 §2.5.1 fixes the interface identifier of a unicast address at 64 bits, so a
/64 is one link (ADR 0014). From 1 to 128. A value of 128 counts each address on its own.

 [Reactor](ReactorSockets.StreamSessionOptions.Reactor.md\#ReactorSockets\_StreamSessionOptions\_Reactor)

The shared L0 budget: capacity, admission cap, arena size, time source and Diagnostic Sink.
Required — a Reactor cannot be built without one, and its [`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity)
has no default of its own.

 [ReceiveBudgetBytes](ReactorSockets.StreamSessionOptions.ReceiveBudgetBytes.md\#ReactorSockets\_StreamSessionOptions\_ReceiveBudgetBytes)

The receive line of the memory bill in bytes, as a <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/integral-numeric-types">long</a> so that a
combination too large to allocate is a value this can report rather than an overflow it
silently wraps through.

 [ReceiveBufferFloor](ReactorSockets.StreamSessionOptions.ReceiveBufferFloor.md\#ReactorSockets\_StreamSessionOptions\_ReceiveBufferFloor)

The floor both [`StreamSessionOptions.RetentionBytes`](ReactorSockets.StreamSessionOptions.RetentionBytes.md#ReactorSockets_StreamSessionOptions_RetentionBytes) and the receive buffer are held to: one maximum Message plus its length prefix.

 [ReceiveBufferSize](ReactorSockets.StreamSessionOptions.ReceiveBufferSize.md\#ReactorSockets\_StreamSessionOptions\_ReceiveBufferSize)

The bytes held per Peer for an incoming, not-yet-complete Message. Defaults to
[`StreamSessionOptions.MaxMessageSize`](ReactorSockets.StreamSessionOptions.MaxMessageSize.md#ReactorSockets_StreamSessionOptions_MaxMessageSize) plus the 4-byte length prefix. Provenance: derived from
[`StreamSessionOptions.MaxMessageSize`](ReactorSockets.StreamSessionOptions.MaxMessageSize.md#ReactorSockets_StreamSessionOptions_MaxMessageSize) — its own floor, since that is exactly one incomplete maximum
Message and nothing narrower could ever hold one.

 [ResolvedMaxPeersPerAddress](ReactorSockets.StreamSessionOptions.ResolvedMaxPeersPerAddress.md\#ReactorSockets\_StreamSessionOptions\_ResolvedMaxPeersPerAddress)

The resolved per-address limit: [`StreamSessionOptions.MaxPeersPerAddress`](ReactorSockets.StreamSessionOptions.MaxPeersPerAddress.md#ReactorSockets_StreamSessionOptions_MaxPeersPerAddress) if set, otherwise one fewer than
[`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity), and at least 1.

 [ResolvedMissedHeartbeats](ReactorSockets.StreamSessionOptions.ResolvedMissedHeartbeats.md\#ReactorSockets\_StreamSessionOptions\_ResolvedMissedHeartbeats)

The resolved missed-heartbeat count: [`StreamSessionOptions.MissedHeartbeats`](ReactorSockets.StreamSessionOptions.MissedHeartbeats.md#ReactorSockets_StreamSessionOptions_MissedHeartbeats) if set, otherwise 3.

 [ResolvedPeerLimitIPv6PrefixLength](ReactorSockets.StreamSessionOptions.ResolvedPeerLimitIPv6PrefixLength.md\#ReactorSockets\_StreamSessionOptions\_ResolvedPeerLimitIPv6PrefixLength)

The resolved IPv6 grouping length: [`StreamSessionOptions.PeerLimitIPv6PrefixLength`](ReactorSockets.StreamSessionOptions.PeerLimitIPv6PrefixLength.md#ReactorSockets_StreamSessionOptions_PeerLimitIPv6PrefixLength) if set, otherwise 64.

 [ResolvedReceiveBufferSize](ReactorSockets.StreamSessionOptions.ResolvedReceiveBufferSize.md\#ReactorSockets\_StreamSessionOptions\_ResolvedReceiveBufferSize)

The resolved receive buffer: [`StreamSessionOptions.ReceiveBufferSize`](ReactorSockets.StreamSessionOptions.ReceiveBufferSize.md#ReactorSockets_StreamSessionOptions_ReceiveBufferSize) if set, otherwise [`StreamSessionOptions.ReceiveBufferFloor`](ReactorSockets.StreamSessionOptions.ReceiveBufferFloor.md#ReactorSockets_StreamSessionOptions_ReceiveBufferFloor).

 [RetentionBytes](ReactorSockets.StreamSessionOptions.RetentionBytes.md\#ReactorSockets\_StreamSessionOptions\_RetentionBytes)

The bytes retained per Peer for the send path — retention <em>is</em> the send path (ADR 0011):
a <code>Send</code> frames the Message once and appends it to this Peer's ring, and what reaches the
socket is a copy from it. Required: this is the consumer trading their memory against their
tolerance for a stalled Peer, and the right answer depends on their traffic, not on anything
here. Must be at least [`StreamSessionOptions.MaxMessageSize`](ReactorSockets.StreamSessionOptions.MaxMessageSize.md#ReactorSockets_StreamSessionOptions_MaxMessageSize) plus the 4-byte length prefix, or a single
maximum Message could never be retained.

 [SendBudgetBytes](ReactorSockets.StreamSessionOptions.SendBudgetBytes.md\#ReactorSockets\_StreamSessionOptions\_SendBudgetBytes)

The send line of the memory bill in bytes, reported the same way.

 [SendBufferSize](ReactorSockets.StreamSessionOptions.SendBufferSize.md\#ReactorSockets\_StreamSessionOptions\_SendBufferSize)

<code>SO_SNDBUF</code> for every Peer socket this Reactor accepts or dials, or <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/null">null</a>
— the default — for the OS default and its own autotuning. This is the kernel's send queue, not
[`StreamSessionOptions.RetentionBytes`](ReactorSockets.StreamSessionOptions.RetentionBytes.md#ReactorSockets_StreamSessionOptions_RetentionBytes): retention is what this end holds until the peer confirms receipt,
while this bounds how much the kernel will absorb from a flush before a write reports it would
block and the bytes stay in retention, unsent. Unset, the two are unrelated in practice on
Linux, whose send autotuning will absorb megabytes against a peer that has stopped reading; set,
a consumer who needs transport backpressure to reach their own flow control at a known point can
put it there. Provenance: unset leaves the OS's own default and autotuning in place, so the
library chooses nothing.

 [SendIdleInterval](ReactorSockets.StreamSessionOptions.SendIdleInterval.md\#ReactorSockets\_StreamSessionOptions\_SendIdleInterval)

How long a Session may go without sending before it sends a heartbeat instead. Required:
shorter costs bandwidth on an idle link, longer costs time to notice a dead Peer, and only the
consumer knows which they would rather spend. At most 2^32 - 1 milliseconds (about 49.7 days), the
most a handshake may announce.

