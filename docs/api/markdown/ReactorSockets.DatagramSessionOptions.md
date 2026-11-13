# <a id="ReactorSockets_DatagramSessionOptions"></a> Class DatagramSessionOptions

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

The construction surface of a [`DatagramSessionReactor`](ReactorSockets.DatagramSessionReactor.md) (ADR 0012): the shared L0
budget composed in as [`DatagramSessionOptions.Reactor`](ReactorSockets.DatagramSessionOptions.Reactor.md#ReactorSockets_DatagramSessionOptions_Reactor), plus the knobs this profile alone has. Composed
rather than inherited, so that nothing protocol-specific here can ever leak into
[`ReactorOptions`](ReactorSockets.ReactorOptions.md) and turn it into the base class ADR 0011 refused.

```csharp
public sealed class DatagramSessionOptions
```

#### Inheritance

[object](https://learn.microsoft.com/dotnet/api/system.object) ← 
[DatagramSessionOptions](ReactorSockets.DatagramSessionOptions.md)

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
no default: [`DatagramSessionOptions.MaxMessageSize`](ReactorSockets.DatagramSessionOptions.MaxMessageSize.md#ReactorSockets_DatagramSessionOptions_MaxMessageSize), [`DatagramSessionOptions.RetentionBytes`](ReactorSockets.DatagramSessionOptions.RetentionBytes.md#ReactorSockets_DatagramSessionOptions_RetentionBytes),
[`DatagramSessionOptions.SendIdleInterval`](ReactorSockets.DatagramSessionOptions.SendIdleInterval.md#ReactorSockets_DatagramSessionOptions_SendIdleInterval) and [`DatagramSessionOptions.Linger`](ReactorSockets.DatagramSessionOptions.Linger.md#ReactorSockets_DatagramSessionOptions_Linger) are all of that kind, and so is
[`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity). Guessing any of them would be guessing at the consumer's
application.
</p>
<p>
Values fixed by the wire format are not here at all and cannot be reached from here: the 32-bit
acknowledgement width, the 64-entry duplicate window, the four sequence Spaces, zero as the
reserved session id, the 20-byte data header and the thirty-second cookie epoch are protocol
constants. Changing one changes what a datagram means, which is a version, not a setting.
</p>
<p>
<b>The memory bill.</b> A Reactor built from these options preallocates, and the whole of it is
these three lines:
</p>
<ul><li>
receive: <code>Capacity × 2 × ReceiveWindowFragments × FragmentSize</code> — two of the four Spaces are
reliable and reassemble, and each holds a window of whole fragment slots per Peer.
</li><li>
send: <code>Capacity × 2 × RetentionBytes</code> — the same two Spaces retain what they have sent until
it is acknowledged, per Peer.
</li><li>
unreliable queue: <code>Capacity × UnreliableQueueBytes</code> — the bytes of Messages Sent in the two
unreliable modes that wait for the next Pump, per Peer.
</li></ul>
<p>
<code>FragmentSize</code> is in one line and not the other because the window counts packets and retention
counts bytes. Neither line has a traffic term: both are allocated at construction and never grow.
Each line also carries a few bytes of bookkeeping per fragment slot, which
[`DatagramSessionOptions.ReceiveBudgetBytes`](ReactorSockets.DatagramSessionOptions.ReceiveBudgetBytes.md#ReactorSockets_DatagramSessionOptions_ReceiveBudgetBytes) and [`DatagramSessionOptions.SendBudgetBytes`](ReactorSockets.DatagramSessionOptions.SendBudgetBytes.md#ReactorSockets_DatagramSessionOptions_SendBudgetBytes) include exactly — under 2% at the
default fragment size.
</p>
<p>
The frame arena ([`ReactorOptions.ArenaSize`](ReactorSockets.ReactorOptions.ArenaSize.md#ReactorSockets_ReactorOptions_ArenaSize)) is over and above those three lines and is
shared across the whole Reactor rather than multiplied by capacity.
</p>
<p>
One further cost is allocated only under traffic: ordinary <code>ReliableOrdered</code> records waiting
behind a gap. It has a ceiling of its own, [`DatagramSessionOptions.HeldRecordBudgetBytes`](ReactorSockets.DatagramSessionOptions.HeldRecordBudgetBytes.md#ReactorSockets_DatagramSessionOptions_HeldRecordBudgetBytes), and reaching it refuses
further records rather than growing.
</p>
<p>
[`DatagramSessionOptions.MaxMessageSize`](ReactorSockets.DatagramSessionOptions.MaxMessageSize.md#ReactorSockets_DatagramSessionOptions_MaxMessageSize) reaches three places at once: it is announced to the remote as a
receive limit, it floors the receive window at the fragments one maximum Message occupies
([`DatagramSessionOptions.ReceiveWindowFloor`](ReactorSockets.DatagramSessionOptions.ReceiveWindowFloor.md#ReactorSockets_DatagramSessionOptions_ReceiveWindowFloor)), and it floors the frame arena, because an arena that cannot
hold one maximum Message could never deliver one. The limit is enforced as well as announced: a
completed Message above it is discarded rather than delivered, with
[`DiagnosticId.OversizedMessage`](ReactorSockets.DiagnosticId.md). Careless enlargement grows memory in
two directions rather than one: the window floor is per Peer, so the receive line above rises
with it multiplied by capacity and by the Spaces, and the shared arena is pushed up alongside.
Retention is the exception this profile makes. [`DatagramSessionOptions.RetentionBytes`](ReactorSockets.DatagramSessionOptions.RetentionBytes.md#ReactorSockets_DatagramSessionOptions_RetentionBytes) is floored by
[`DatagramSessionOptions.ResolvedFragmentSize`](ReactorSockets.DatagramSessionOptions.ResolvedFragmentSize.md#ReactorSockets_DatagramSessionOptions_ResolvedFragmentSize) rather than by [`DatagramSessionOptions.MaxMessageSize`](ReactorSockets.DatagramSessionOptions.MaxMessageSize.md#ReactorSockets_DatagramSessionOptions_MaxMessageSize), because a
reliable Space retransmits fragment by fragment and needs only one fragment to have something to
resend.
</p>
<p>
<b>Security.</b> The session field on every datagram is a demultiplexing token, not an
authenticator: it makes blind off-path injection a 32-bit guess, and does nothing against anyone who
has seen one datagram of the Session. This library does not encrypt, and on-path attackers are out
of scope (ADR 0009). A consumer who needs confidentiality or integrity against an on-path attacker
provides it themselves, inside the Message.
</p>
<p>
Every check below is deterministic and entirely the caller's, so failures throw at construction —
[`ArgumentOutOfRangeException`](https://learn.microsoft.com/dotnet/api/system.argumentoutofrangeexception) for a single bad value and
[`ArgumentException`](https://learn.microsoft.com/dotnet/api/system.argumentexception) for a combination that cannot work — rather than being reported
through a <code>Result</code>.
</p>

## Properties

 [Endpoints](ReactorSockets.DatagramSessionOptions.Endpoints.md\#ReactorSockets\_DatagramSessionOptions\_Endpoints)

The endpoints to bind at construction, in the order supplied; possibly empty. Declared once
and never rebound (ADR 0014) — a Session's addresses are part of what it is, not something it
acquires later. An empty set binds nothing until the first dial, which binds a single
wildcard, port-zero socket in the destination's address family. Provenance: empty is the
absence of a declaration, not a chosen address — the dial-only socket's family and port are
derived from the destination and the OS, never configured.

 [Entropy](ReactorSockets.DatagramSessionOptions.Entropy.md\#ReactorSockets\_DatagramSessionOptions\_Entropy)

The entropy the cookie secrets, session ids and resume secrets are drawn from. A construction
knob in ADR 0012's sense — the consumer's own resource, and a consumer with a hardware source
or a policy about which generator may be used is entitled to supply it. It carries a default
only because that default is derived rather than guessed. Provenance: the platform's own CSPRNG.

 [FragmentSize](ReactorSockets.DatagramSessionOptions.FragmentSize.md\#ReactorSockets\_DatagramSessionOptions\_FragmentSize)

The bytes a fragment carries, and therefore the unit every buffer here is measured in.
Defaults to 1200. Provenance: QUIC's floor beneath the 1280-byte IPv6 minimum MTU (RFC 9000),
the one value with citable provenance for surviving an arbitrary path. Must lie between 576
(RFC 791's guaranteed reassembly minimum) and 65 476 (the 65 507-byte maximum UDP payload less the
20-byte Datagram header and the 11-byte Fragment record header). A data Datagram carries at most
this many payload bytes plus a 31-byte header, so at the default a Datagram is at most 1231 bytes,
which with IPv6 and UDP headers fits the 1280-byte minimum MTU.

 [HeldRecordBudgetBytes](ReactorSockets.DatagramSessionOptions.HeldRecordBudgetBytes.md\#ReactorSockets\_DatagramSessionOptions\_HeldRecordBudgetBytes)

The most heap, in bytes, a Reactor's <code>ReliableOrdered</code> Spaces can hold in ordinary records
waiting behind a gap, across all its Peers: <code>Capacity × 2 × ReceiveWindowFragments ×
FragmentSize</code>, with each slot's bookkeeping. A figure above [`MaxValue`](https://learn.microsoft.com/dotnet/api/system.int64.maxvalue) reads as
[`MaxValue`](https://learn.microsoft.com/dotnet/api/system.int64.maxvalue).

 [Linger](ReactorSockets.DatagramSessionOptions.Linger.md\#ReactorSockets\_DatagramSessionOptions\_Linger)

How long a closing Session stays alive to retransmit its Goodbye and absorb what is still in
flight. Required, for the same reason as [`DatagramSessionOptions.SendIdleInterval`](ReactorSockets.DatagramSessionOptions.SendIdleInterval.md#ReactorSockets_DatagramSessionOptions_SendIdleInterval): it trades a prompt
teardown against a clean one. At most 2^32 - 1 milliseconds (about 49.7 days), the most a handshake
may announce.

 [MaxDatagramsPerPump](ReactorSockets.DatagramSessionOptions.MaxDatagramsPerPump.md\#ReactorSockets\_DatagramSessionOptions\_MaxDatagramsPerPump)

The most reads one Pump makes on each bound socket, so a flood cannot keep a Pump from
returning. Defaults to [`DatagramSessionOptions.ResolvedMaxDatagramsPerPump`](ReactorSockets.DatagramSessionOptions.ResolvedMaxDatagramsPerPump.md#ReactorSockets_DatagramSessionOptions_ResolvedMaxDatagramsPerPump). Provenance: derived from a
full Reactor's worth of traffic, as [`ReactorOptions.AdmissionCap`](ReactorSockets.ReactorOptions.AdmissionCap.md#ReactorSockets_ReactorOptions_AdmissionCap) is (ADR 0012). At
least 1.

 [MaxMessageSize](ReactorSockets.DatagramSessionOptions.MaxMessageSize.md\#ReactorSockets\_DatagramSessionOptions\_MaxMessageSize)

The largest Message, in bytes, this Reactor will accept or deliver. Required: it is a
statement about the consumer's own protocol that nothing here can derive. It is also the knob
that multiplies this profile's memory bill in more than one direction — the class remarks on
[`DatagramSessionOptions`](ReactorSockets.DatagramSessionOptions.md) say what it floors and what raising it costs. At most
[`MaxLength`](https://learn.microsoft.com/dotnet/api/system.array.maxlength), 2 147 483 591, so the frame arena can hold one maximum Message.

 [MaxPeersPerAddress](ReactorSockets.DatagramSessionOptions.MaxPeersPerAddress.md\#ReactorSockets\_DatagramSessionOptions\_MaxPeersPerAddress)

The most accepted Peers one address may hold at once, so one reachable address cannot occupy
every capacity Slot. Defaults to [`DatagramSessionOptions.ResolvedMaxPeersPerAddress`](ReactorSockets.DatagramSessionOptions.ResolvedMaxPeersPerAddress.md#ReactorSockets_DatagramSessionOptions_ResolvedMaxPeersPerAddress), one fewer than
[`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity). Provenance: derived from capacity, as the largest limit that
still keeps one address from holding every Slot (ADR 0014). How many Peers legitimately share an
address depends on the NATs and proxies in front of this Reactor, so set it lower when strangers can
reach it. At least 1. A value of [`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity) or above sets no limit, which
is what the default is at a capacity of 1.

 [MissedHeartbeats](ReactorSockets.DatagramSessionOptions.MissedHeartbeats.md\#ReactorSockets\_DatagramSessionOptions\_MissedHeartbeats)

How many consecutive heartbeat intervals may pass in silence before a Peer is declared gone.
Defaults to 3. Provenance: RFC 4271's hold time, three times the keepalive interval. Must be at
least 2, because a count of 1 makes a single lost datagram fatal and no network is that reliable.

 [PeerLimitIPv6PrefixLength](ReactorSockets.DatagramSessionOptions.PeerLimitIPv6PrefixLength.md\#ReactorSockets\_DatagramSessionOptions\_PeerLimitIPv6PrefixLength)

How many leading bits of an IPv6 address form the key for [`DatagramSessionOptions.MaxPeersPerAddress`](ReactorSockets.DatagramSessionOptions.MaxPeersPerAddress.md#ReactorSockets_DatagramSessionOptions_MaxPeersPerAddress), so
every address in one prefix shares one limit. Defaults to [`DatagramSessionOptions.ResolvedPeerLimitIPv6PrefixLength`](ReactorSockets.DatagramSessionOptions.ResolvedPeerLimitIPv6PrefixLength.md#ReactorSockets_DatagramSessionOptions_ResolvedPeerLimitIPv6PrefixLength),
64. Provenance: RFC 4291 §2.5.1 fixes the interface identifier of a unicast address at 64 bits, so a
/64 is one link (ADR 0014). From 1 to 128. A value of 128 counts each address on its own.

 [Reactor](ReactorSockets.DatagramSessionOptions.Reactor.md\#ReactorSockets\_DatagramSessionOptions\_Reactor)

The shared L0 budget: capacity, admission cap, arena size, time source and Diagnostic Sink.
Required — a Reactor cannot be built without one, and its [`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity)
has no default of its own.

 [ReceiveBudgetBytes](ReactorSockets.DatagramSessionOptions.ReceiveBudgetBytes.md\#ReactorSockets\_DatagramSessionOptions\_ReceiveBudgetBytes)

The receive line of the memory bill in bytes, as a <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/integral-numeric-types">long</a> so that a
combination too large to allocate is a value this can report rather than an overflow it
silently wraps through. A figure above [`MaxValue`](https://learn.microsoft.com/dotnet/api/system.int64.maxvalue) reads as [`MaxValue`](https://learn.microsoft.com/dotnet/api/system.int64.maxvalue).

 [ReceiveWindowFloor](ReactorSockets.DatagramSessionOptions.ReceiveWindowFloor.md\#ReactorSockets\_DatagramSessionOptions\_ReceiveWindowFloor)

The resolved receive window: [`DatagramSessionOptions.ReceiveWindowFragments`](ReactorSockets.DatagramSessionOptions.ReceiveWindowFragments.md#ReactorSockets_DatagramSessionOptions_ReceiveWindowFragments) if set, otherwise the
fragments one maximum Message occupies.

 [ReceiveWindowFragments](ReactorSockets.DatagramSessionOptions.ReceiveWindowFragments.md\#ReactorSockets\_DatagramSessionOptions\_ReceiveWindowFragments)

The fragments each reliable Space may have outstanding per Peer. Defaults to exactly the
number a maximum Message occupies. Provenance: derived from [`DatagramSessionOptions.MaxMessageSize`](ReactorSockets.DatagramSessionOptions.MaxMessageSize.md#ReactorSockets_DatagramSessionOptions_MaxMessageSize) and
[`DatagramSessionOptions.FragmentSize`](ReactorSockets.DatagramSessionOptions.FragmentSize.md#ReactorSockets_DatagramSessionOptions_FragmentSize) — ADR 0004's deadlock floor rather than a performance choice: a window narrower than one whole Message can never deliver one, so it can
never advance, so the sender can never send the rest. This resolved value is also the inbound
bound a declared Fragment <code>fragmentCount</code> is checked against: a set larger than the window
can never be reassembled, since there is no slot to hold every one of its pieces at once, so
[`DatagramSessionReactor`](ReactorSockets.DatagramSessionReactor.md) closes it before any is held.

 [ResolvedFragmentSize](ReactorSockets.DatagramSessionOptions.ResolvedFragmentSize.md\#ReactorSockets\_DatagramSessionOptions\_ResolvedFragmentSize)

The resolved fragment size: [`DatagramSessionOptions.FragmentSize`](ReactorSockets.DatagramSessionOptions.FragmentSize.md#ReactorSockets_DatagramSessionOptions_FragmentSize) if set, otherwise 1200.

 [ResolvedMaxDatagramsPerPump](ReactorSockets.DatagramSessionOptions.ResolvedMaxDatagramsPerPump.md\#ReactorSockets\_DatagramSessionOptions\_ResolvedMaxDatagramsPerPump)

The resolved per-socket read limit: [`DatagramSessionOptions.MaxDatagramsPerPump`](ReactorSockets.DatagramSessionOptions.MaxDatagramsPerPump.md#ReactorSockets_DatagramSessionOptions_MaxDatagramsPerPump) if set, otherwise one receive
window for every Space of every Peer, plus every handshake the admission cap allows in one Pump,
clamped to [`MaxValue`](https://learn.microsoft.com/dotnet/api/system.int32.maxvalue).

 [ResolvedMaxPeersPerAddress](ReactorSockets.DatagramSessionOptions.ResolvedMaxPeersPerAddress.md\#ReactorSockets\_DatagramSessionOptions\_ResolvedMaxPeersPerAddress)

The resolved per-address limit: [`DatagramSessionOptions.MaxPeersPerAddress`](ReactorSockets.DatagramSessionOptions.MaxPeersPerAddress.md#ReactorSockets_DatagramSessionOptions_MaxPeersPerAddress) if set, otherwise one fewer than
[`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity), and at least 1.

 [ResolvedMissedHeartbeats](ReactorSockets.DatagramSessionOptions.ResolvedMissedHeartbeats.md\#ReactorSockets\_DatagramSessionOptions\_ResolvedMissedHeartbeats)

The resolved missed-heartbeat count: [`DatagramSessionOptions.MissedHeartbeats`](ReactorSockets.DatagramSessionOptions.MissedHeartbeats.md#ReactorSockets_DatagramSessionOptions_MissedHeartbeats) if set, otherwise 3.

 [ResolvedPeerLimitIPv6PrefixLength](ReactorSockets.DatagramSessionOptions.ResolvedPeerLimitIPv6PrefixLength.md\#ReactorSockets\_DatagramSessionOptions\_ResolvedPeerLimitIPv6PrefixLength)

The resolved IPv6 grouping length: [`DatagramSessionOptions.PeerLimitIPv6PrefixLength`](ReactorSockets.DatagramSessionOptions.PeerLimitIPv6PrefixLength.md#ReactorSockets_DatagramSessionOptions_PeerLimitIPv6PrefixLength) if set, otherwise 64.

 [ResolvedReceiveWindowFragments](ReactorSockets.DatagramSessionOptions.ResolvedReceiveWindowFragments.md\#ReactorSockets\_DatagramSessionOptions\_ResolvedReceiveWindowFragments)

The resolved receive window: [`DatagramSessionOptions.ReceiveWindowFragments`](ReactorSockets.DatagramSessionOptions.ReceiveWindowFragments.md#ReactorSockets_DatagramSessionOptions_ReceiveWindowFragments) if set, otherwise [`DatagramSessionOptions.ReceiveWindowFloor`](ReactorSockets.DatagramSessionOptions.ReceiveWindowFloor.md#ReactorSockets_DatagramSessionOptions_ReceiveWindowFloor).

 [ResolvedUnreliableQueueBytes](ReactorSockets.DatagramSessionOptions.ResolvedUnreliableQueueBytes.md\#ReactorSockets\_DatagramSessionOptions\_ResolvedUnreliableQueueBytes)

[`DatagramSessionOptions.UnreliableQueueBytes`](ReactorSockets.DatagramSessionOptions.UnreliableQueueBytes.md#ReactorSockets_DatagramSessionOptions_UnreliableQueueBytes) with its default applied, clamped to [`MaxValue`](https://learn.microsoft.com/dotnet/api/system.int32.maxvalue).

 [RetentionBytes](ReactorSockets.DatagramSessionOptions.RetentionBytes.md\#ReactorSockets\_DatagramSessionOptions\_RetentionBytes)

The bytes each reliable Space retains per Peer for retransmission, until what it holds is
acknowledged. Required: this is the consumer trading their memory against their tolerance for
stalling on loss, and the right answer depends on their traffic, not on anything here.

 [RetentionSlots](ReactorSockets.DatagramSessionOptions.RetentionSlots.md\#ReactorSockets\_DatagramSessionOptions\_RetentionSlots)

The uniform retention slots one reliable Space holds per Peer: [`DatagramSessionOptions.RetentionBytes`](ReactorSockets.DatagramSessionOptions.RetentionBytes.md#ReactorSockets_DatagramSessionOptions_RetentionBytes)
divided by [`DatagramSessionOptions.ResolvedFragmentSize`](ReactorSockets.DatagramSessionOptions.ResolvedFragmentSize.md#ReactorSockets_DatagramSessionOptions_ResolvedFragmentSize), truncating any remainder. Stays derived from the
byte bound rather than a knob of its own (ADR 0012) so it cannot drift from
[`DatagramSessionOptions.RetentionBytes`](ReactorSockets.DatagramSessionOptions.RetentionBytes.md#ReactorSockets_DatagramSessionOptions_RetentionBytes) when the fragment size changes. `Validate` already
requires retention to hold at least one fragment, so this is always at least 1.

 [SendBudgetBytes](ReactorSockets.DatagramSessionOptions.SendBudgetBytes.md\#ReactorSockets\_DatagramSessionOptions\_SendBudgetBytes)

The send line of the memory bill in bytes, reported the same way: [`DatagramSessionOptions.RetentionSlots`](ReactorSockets.DatagramSessionOptions.RetentionSlots.md#ReactorSockets_DatagramSessionOptions_RetentionSlots)
whole fragment slots per reliable Space per Peer, each priced at its payload plus the 15 bytes
of bookkeeping that records what it holds. Exact, like the receive line, so it can be below
<code>Capacity × 2 × RetentionBytes</code> when retention is not a whole number of fragments. A figure
above [`MaxValue`](https://learn.microsoft.com/dotnet/api/system.int64.maxvalue) reads as [`MaxValue`](https://learn.microsoft.com/dotnet/api/system.int64.maxvalue).

 [SendIdleInterval](ReactorSockets.DatagramSessionOptions.SendIdleInterval.md\#ReactorSockets\_DatagramSessionOptions\_SendIdleInterval)

How long a Session may go without sending before it sends a Ping instead. Required: shorter
costs bandwidth on an idle link, longer costs time to notice a dead Peer, and only the consumer
knows which they would rather spend. There is no option to disable this: every Established Peer
that has sent no data datagram for this long is always sent one seven-byte control record,
whatever else it did or did not receive. At most 2^32 - 1 milliseconds (about 49.7 days), the
most a handshake may announce.

 [UnreliableQueueBudgetBytes](ReactorSockets.DatagramSessionOptions.UnreliableQueueBudgetBytes.md\#ReactorSockets\_DatagramSessionOptions\_UnreliableQueueBudgetBytes)

The unreliable-queue line of the memory bill in bytes: <code>Capacity × </code>[`DatagramSessionOptions.ResolvedUnreliableQueueBytes`](ReactorSockets.DatagramSessionOptions.ResolvedUnreliableQueueBytes.md#ReactorSockets_DatagramSessionOptions_ResolvedUnreliableQueueBytes),
one region per Peer, allocated at construction.

 [UnreliableQueueBytes](ReactorSockets.DatagramSessionOptions.UnreliableQueueBytes.md\#ReactorSockets\_DatagramSessionOptions\_UnreliableQueueBytes)

The most bytes one Peer may have queued for [`DeliveryMode.Unreliable`](ReactorSockets.DeliveryMode.md) and
[`DeliveryMode.UnreliableSequenced`](ReactorSockets.DeliveryMode.md) until a Pump writes them, counting each Message's payload plus
its 7-byte record header. A Send that would go past it returns [`SendOutcome.SendQueueFull`](ReactorSockets.SendOutcome.md).
<a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/null">null</a> means [`DatagramSessionOptions.RetentionSlots`](ReactorSockets.DatagramSessionOptions.RetentionSlots.md#ReactorSockets_DatagramSessionOptions_RetentionSlots) × ([`DatagramSessionOptions.ResolvedFragmentSize`](ReactorSockets.DatagramSessionOptions.ResolvedFragmentSize.md#ReactorSockets_DatagramSessionOptions_ResolvedFragmentSize) + 7):
as many full unreliable Messages as one reliable Space retains Fragments. Provenance: derived from
[`DatagramSessionOptions.RetentionBytes`](ReactorSockets.DatagramSessionOptions.RetentionBytes.md#ReactorSockets_DatagramSessionOptions_RetentionBytes) and [`DatagramSessionOptions.FragmentSize`](ReactorSockets.DatagramSessionOptions.FragmentSize.md#ReactorSockets_DatagramSessionOptions_FragmentSize), so the unreliable queue holds as much as
a reliable Space does. Must be at least
[`DatagramSessionOptions.ResolvedFragmentSize`](ReactorSockets.DatagramSessionOptions.ResolvedFragmentSize.md#ReactorSockets_DatagramSessionOptions_ResolvedFragmentSize) + 7, one full unreliable Message.

