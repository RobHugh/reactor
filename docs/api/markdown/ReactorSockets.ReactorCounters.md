# <a id="ReactorSockets_ReactorCounters"></a> Struct ReactorCounters

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

A point-in-time snapshot of the Reactor-scope counters (ADR 0013): monotonic figures running
from construction, with no reset. Fetched by value so nothing lets a consumer hold a pointer
into live state across a Pump; diffing two snapshots gives everything a reset would, from state
the consumer already owns.

```csharp
public readonly record struct ReactorCounters : IEquatable<ReactorCounters>
```

#### Implements

[IEquatable<ReactorCounters\>](https://learn.microsoft.com/dotnet/api/system.iequatable\-1)

#### Inherited Members

[object.Equals\(object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\)), 
[object.Equals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\-system\-object\)), 
[object.GetHashCode\(\)](https://learn.microsoft.com/dotnet/api/system.object.gethashcode), 
[object.GetType\(\)](https://learn.microsoft.com/dotnet/api/system.object.gettype), 
[object.ReferenceEquals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.referenceequals), 
[object.ToString\(\)](https://learn.microsoft.com/dotnet/api/system.object.tostring)

## Remarks

The shape is shared by every profile; which members move is not. <code>DatagramsSent</code> and
<code>DatagramsReceived</code> stay at zero on a Reactor carrying no datagram transport, and the two
admission counters divide the same way. A member that never moves on a profile is reporting a
true zero, not a gap. Every member says, under "Not derivable", what it tells a consumer that the
other counters do not (ADR 0013). A field that could not be given that line would not exist.

## Constructors

 [ReactorCounters\(ulong, ulong, ulong, ulong, ulong, ulong, ulong, ulong\)](ReactorSockets.ReactorCounters.\-ctor.md\#ReactorSockets\_ReactorCounters\_\_ctor\_System\_UInt64\_System\_UInt64\_System\_UInt64\_System\_UInt64\_System\_UInt64\_System\_UInt64\_System\_UInt64\_System\_UInt64\_)

A point-in-time snapshot of the Reactor-scope counters (ADR 0013): monotonic figures running
from construction, with no reset. Fetched by value so nothing lets a consumer hold a pointer
into live state across a Pump; diffing two snapshots gives everything a reset would, from state
the consumer already owns.

## Properties

 [AdmissionDeferrals](ReactorSockets.ReactorCounters.AdmissionDeferrals.md\#ReactorSockets\_ReactorCounters\_AdmissionDeferrals)

Admissions declined that left the work where it was, for a later Pump to take. Moves on the
transports where declined work waits — Raw and the stream Session profile. On the stream profile both the admission cap and the limit of
[`ReactorOptions.Capacity`](ReactorSockets.ReactorOptions.Capacity.md#ReactorSockets_ReactorOptions_Capacity) connections held waiting for their first record decline this way.
Counted once per
declining rather than once per piece of work turned away, which makes it coarse where the work
sits in an OS backlog: one listening Link or bound endpoint declines its whole backlog with one
declining, because that depth is unknowable without accepting. Where the Reactor already holds
the work — a stream Session handshake accepted but not completed — one declining is one piece of
work and the figure is exact. Read the magnitude as "how much of the listening fleet is turning
work away", never as backlog depth. Not derivable: no other counter sees work the Reactor never
accepted, and its unit is declinings, not the pieces of work <code class="paramref">AdmissionDiscards</code>
counts.

 [AdmissionDiscards](ReactorSockets.ReactorCounters.AdmissionDiscards.md\#ReactorSockets\_ReactorCounters\_AdmissionDiscards)

Admissions declined that let the work go, because the transport gave it nowhere to wait. Moves
only on the datagram Session profile, which Discards the Hellos it cannot admit, whether the
admission cap, capacity or [`DatagramSessionOptions.MaxPeersPerAddress`](ReactorSockets.DatagramSessionOptions.MaxPeersPerAddress.md#ReactorSockets_DatagramSessionOptions_MaxPeersPerAddress) declined them. The
Diagnostics tell the three apart. Always an exact
count of work turned away, which <code class="paramref">AdmissionDeferrals</code> is not, because a Reactor
can only Discard what it had in hand. Kept apart from Deferrals because the two differ in the
one way a consumer would act on: deferred work arrives on a later Pump, Discarded work never
does. Not derivable: the two admission counters are never both non-zero on one Reactor, yet
knowing one says nothing of the other, and their units differ, so one shared field would invite a
sum or comparison that means nothing.

 [ArenaHighWaterBytes](ReactorSockets.ReactorCounters.ArenaHighWaterBytes.md\#ReactorSockets\_ReactorCounters\_ArenaHighWaterBytes)

The most Arena bytes any single Frame has used. Moves on every profile. Not derivable: byte totals
say how much arrived over time, never how much of it landed in one Frame, which is the figure that
sizes the Arena.

 [DatagramsReceived](ReactorSockets.ReactorCounters.DatagramsReceived.md\#ReactorSockets\_ReactorCounters\_DatagramsReceived)

Datagrams taken from the transport. Not derivable: it is the denominator for
<code class="paramref">UnknownSourceDatagrams</code> and <code class="paramref">MalformedSessionDatagrams</code>, and
per-Peer figures lose closed Peers and never see unattributed traffic.

 [DatagramsSent](ReactorSockets.ReactorCounters.DatagramsSent.md\#ReactorSockets\_ReactorCounters\_DatagramsSent)

Datagrams handed to the transport for sending. Not derivable: per-Peer byte figures cover only
traffic a Peer exists to charge and end with the Peer, while this counts every datagram for the
Reactor's life.

 [MalformedSessionDatagrams](ReactorSockets.ReactorCounters.MalformedSessionDatagrams.md\#ReactorSockets\_ReactorCounters\_MalformedSessionDatagrams)

Data datagrams thrown away for failing to parse after their session id named a live Session: a
short or bad header, a record that does not read, an illegal Fragment set. Kept apart from
<code class="paramref">UnknownSourceDatagrams</code> because the two say different things about who is
sending — this traffic did name a Peer, so it is either an established Peer sending bytes this
Reactor cannot read or a forgery that guessed a live id, where the other figure is traffic that
named nobody at all. Reactor-scope like its neighbour rather than per-Peer: a datagram whose body
did not parse is not proven to have come from the Peer whose id it carries, and charging it there
would let a stranger move an established Peer's figures. Moves only on the datagram Session
profile, and its Diagnostic is the same [`DiagnosticId.DatagramMalformed`](ReactorSockets.DiagnosticId.md). Not
derivable: the Peer it names never counts it, and <code class="paramref">UnknownSourceDatagrams</code>
counts only traffic that named nobody.

 [SinkFaulted](ReactorSockets.ReactorCounters.SinkFaulted.md\#ReactorSockets\_ReactorCounters\_SinkFaulted)

Times the installed Sink threw and was disabled for the Reactor's life. Not derivable: the Sink is
the only other place this could be reported, and it is the thing that failed.

 [UnknownSourceDatagrams](ReactorSockets.ReactorCounters.UnknownSourceDatagrams.md\#ReactorSockets\_ReactorCounters\_UnknownSourceDatagrams)

Datagrams taken from the transport and thrown away because nothing could be attributed to a Peer:
a malformed control datagram, or a session id naming no Session. Not a Discard in the
<code class="paramref">AdmissionDiscards</code> sense: no Admission was declined and no cap was spent, the
bytes named nobody. A data datagram whose session id <em>did</em> name a live Session and then
failed to parse is <code class="paramref">MalformedSessionDatagrams</code> instead. Reactor-scope rather than
per-Peer precisely because there is no Peer to charge them to — the whole point of the member is
the traffic that failed to name one. Moves only on the datagram Session profile. A rising figure
on a Reactor that is otherwise healthy is a scan or a flood, not a fault, which is why the
matching Diagnostics are [`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md). Not derivable: no Peer exists to
carry it, and <code class="paramref">DatagramsReceived</code> counts these together with everything else.

