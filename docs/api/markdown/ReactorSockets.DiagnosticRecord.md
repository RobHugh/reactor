# <a id="ReactorSockets_DiagnosticRecord"></a> Struct DiagnosticRecord

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

A single diagnostic emitted to an [`IReactorDiagnostics`](ReactorSockets.IReactorDiagnostics.md) Sink: a closed
[`DiagnosticId`](ReactorSockets.DiagnosticId.md), its fixed [`DiagnosticLevel`](ReactorSockets.DiagnosticLevel.md), a tagged subject — none,
Link, Source or Peer (ADR 0013) — and two numeric slots whose meaning is documented per
[`DiagnosticId`](ReactorSockets.DiagnosticId.md) member. No strings, no <code>object[]</code>, no closures — everything here
lives on the stack and is built only after [`IReactorDiagnostics.IsEnabled(DiagnosticLevel)`](ReactorSockets.IReactorDiagnostics.IsEnabled.md#ReactorSockets_IReactorDiagnostics_IsEnabled_ReactorSockets_DiagnosticLevel_) passes.

```csharp
public readonly record struct DiagnosticRecord : IEquatable<DiagnosticRecord>
```

#### Implements

[IEquatable<DiagnosticRecord\>](https://learn.microsoft.com/dotnet/api/system.iequatable\-1)

#### Inherited Members

[object.Equals\(object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\)), 
[object.Equals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\-system\-object\)), 
[object.GetHashCode\(\)](https://learn.microsoft.com/dotnet/api/system.object.gethashcode), 
[object.GetType\(\)](https://learn.microsoft.com/dotnet/api/system.object.gettype), 
[object.ReferenceEquals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.referenceequals), 
[object.ToString\(\)](https://learn.microsoft.com/dotnet/api/system.object.tostring)

## Properties

 [Id](ReactorSockets.DiagnosticRecord.Id.md\#ReactorSockets\_DiagnosticRecord\_Id)

Which condition produced this record.

 [Level](ReactorSockets.DiagnosticRecord.Level.md\#ReactorSockets\_DiagnosticRecord\_Level)

The fixed severity for [`DiagnosticRecord.Id`](ReactorSockets.DiagnosticRecord.Id.md#ReactorSockets_DiagnosticRecord_Id), per ADR 0013's roster.

 [Link](ReactorSockets.DiagnosticRecord.Link.md\#ReactorSockets\_DiagnosticRecord\_Link)

The Link this record concerns, valid only when [`DiagnosticRecord.SubjectKind`](ReactorSockets.DiagnosticRecord.SubjectKind.md#ReactorSockets_DiagnosticRecord_SubjectKind) is
[`DiagnosticSubjectKind.Link`](ReactorSockets.DiagnosticSubjectKind.md).

 [Peer](ReactorSockets.DiagnosticRecord.Peer.md\#ReactorSockets\_DiagnosticRecord\_Peer)

The Peer this record concerns, valid only when [`DiagnosticRecord.SubjectKind`](ReactorSockets.DiagnosticRecord.SubjectKind.md#ReactorSockets_DiagnosticRecord_SubjectKind) is
[`DiagnosticSubjectKind.Peer`](ReactorSockets.DiagnosticSubjectKind.md).

 [Source](ReactorSockets.DiagnosticRecord.Source.md\#ReactorSockets\_DiagnosticRecord\_Source)

The claimed Source this record concerns, valid only when [`DiagnosticRecord.SubjectKind`](ReactorSockets.DiagnosticRecord.SubjectKind.md#ReactorSockets_DiagnosticRecord_SubjectKind) is
[`DiagnosticSubjectKind.Source`](ReactorSockets.DiagnosticSubjectKind.md).

 [SubjectKind](ReactorSockets.DiagnosticRecord.SubjectKind.md\#ReactorSockets\_DiagnosticRecord\_SubjectKind)

Which of [`DiagnosticRecord.Link`](ReactorSockets.DiagnosticRecord.Link.md#ReactorSockets_DiagnosticRecord_Link), [`DiagnosticRecord.Source`](ReactorSockets.DiagnosticRecord.Source.md#ReactorSockets_DiagnosticRecord_Source) or [`DiagnosticRecord.Peer`](ReactorSockets.DiagnosticRecord.Peer.md#ReactorSockets_DiagnosticRecord_Peer), if any, is populated.

 [Value0](ReactorSockets.DiagnosticRecord.Value0.md\#ReactorSockets\_DiagnosticRecord\_Value0)

The first numeric slot; meaning documented on the [`DiagnosticId`](ReactorSockets.DiagnosticId.md) member.

 [Value1](ReactorSockets.DiagnosticRecord.Value1.md\#ReactorSockets\_DiagnosticRecord\_Value1)

The second numeric slot; meaning documented on the [`DiagnosticId`](ReactorSockets.DiagnosticId.md) member.

## Methods

 [ForAddressPeerLimitReached\(Source, int, int\)](ReactorSockets.DiagnosticRecord.ForAddressPeerLimitReached.md\#ReactorSockets\_DiagnosticRecord\_ForAddressPeerLimitReached\_ReactorSockets\_Source\_System\_Int32\_System\_Int32\_)

Builds a [`DiagnosticId.AddressPeerLimitReached`](ReactorSockets.DiagnosticId.md) record for the claimed Source whose
handshake was refused, carrying the per-address limit and the prefix length its address was
counted by: 32 for IPv4, and the IPv6 grouping length for IPv6 (ADR 0014).

 [ForAdmissionCapReached\(int, int\)](ReactorSockets.DiagnosticRecord.ForAdmissionCapReached.md\#ReactorSockets\_DiagnosticRecord\_ForAdmissionCapReached\_System\_Int32\_System\_Int32\_)

Builds a [`DiagnosticId.AdmissionCapReached`](ReactorSockets.DiagnosticId.md) record. No subject.
<code class="paramref">declined</code> counts declinings where the work waits out of reach and pieces of
work where the Reactor held it — see the remarks on
[`DiagnosticId.AdmissionCapReached`](ReactorSockets.DiagnosticId.md).

 [ForArenaExhausted\(LinkId, int, int\)](ReactorSockets.DiagnosticRecord.ForArenaExhausted.md\#ReactorSockets\_DiagnosticRecord\_ForArenaExhausted\_ReactorSockets\_LinkId\_System\_Int32\_System\_Int32\_)

Builds an [`DiagnosticId.ArenaExhausted`](ReactorSockets.DiagnosticId.md) record for the Link that made the request.

 [ForArenaExhausted\(PeerId, int, int\)](ReactorSockets.DiagnosticRecord.ForArenaExhausted.md\#ReactorSockets\_DiagnosticRecord\_ForArenaExhausted\_ReactorSockets\_PeerId\_System\_Int32\_System\_Int32\_)

Builds an [`DiagnosticId.ArenaExhausted`](ReactorSockets.DiagnosticId.md) record for the Peer whose Message could not
be reserved — the Session profile's counterpart to the Link-subject overload above (ADR 0014:
share-holder identity is per-Peer in a Session, not per-socket).

 [ForCapacityExhausted\(int, int\)](ReactorSockets.DiagnosticRecord.ForCapacityExhausted.md\#ReactorSockets\_DiagnosticRecord\_ForCapacityExhausted\_System\_Int32\_System\_Int32\_)

Builds a [`DiagnosticId.CapacityExhausted`](ReactorSockets.DiagnosticId.md) record. No subject.
<code class="paramref">declined</code> is counted exactly as
[`DiagnosticRecord.ForAdmissionCapReached(int, int)`](ReactorSockets.DiagnosticRecord.ForAdmissionCapReached.md#ReactorSockets_DiagnosticRecord_ForAdmissionCapReached_System_Int32_System_Int32_)'s is.

 [ForCookieRejected\(Source, long, long\)](ReactorSockets.DiagnosticRecord.ForCookieRejected.md\#ReactorSockets\_DiagnosticRecord\_ForCookieRejected\_ReactorSockets\_Source\_System\_Int64\_System\_Int64\_)

Builds a [`DiagnosticId.CookieRejected`](ReactorSockets.DiagnosticId.md) record for the claimed Source, carrying the
epoch the cookie offered and the epoch current when it was checked.

 [ForDataAfterGoodbye\(PeerId\)](ReactorSockets.DiagnosticRecord.ForDataAfterGoodbye.md\#ReactorSockets\_DiagnosticRecord\_ForDataAfterGoodbye\_ReactorSockets\_PeerId\_)

Builds a [`DiagnosticId.DataAfterGoodbye`](ReactorSockets.DiagnosticId.md) record for the Peer that kept sending
after its own Goodbye. No claimed length: <code>Value0</code> and <code>Value1</code> are unused.

 [ForDatagramMalformed\(Source, int\)](ReactorSockets.DiagnosticRecord.ForDatagramMalformed.md\#ReactorSockets\_DiagnosticRecord\_ForDatagramMalformed\_ReactorSockets\_Source\_System\_Int32\_)

Builds a [`DiagnosticId.DatagramMalformed`](ReactorSockets.DiagnosticId.md) record for the claimed Source, carrying
the number of bytes that arrived.

 [ForDatagramReadLimitReached\(int, int\)](ReactorSockets.DiagnosticRecord.ForDatagramReadLimitReached.md\#ReactorSockets\_DiagnosticRecord\_ForDatagramReadLimitReached\_System\_Int32\_System\_Int32\_)

Builds a [`DiagnosticId.DatagramReadLimitReached`](ReactorSockets.DiagnosticId.md) record. No subject. Carries the per-socket
read limit and the socket's index in [`DatagramSessionReactor.BoundEndpoints`](ReactorSockets.DatagramSessionReactor.BoundEndpoints.md#ReactorSockets_DatagramSessionReactor_BoundEndpoints).

 [ForDatagramTruncated\(Source, int\)](ReactorSockets.DiagnosticRecord.ForDatagramTruncated.md\#ReactorSockets\_DiagnosticRecord\_ForDatagramTruncated\_ReactorSockets\_Source\_System\_Int32\_)

Builds a [`DiagnosticId.DatagramTruncated`](ReactorSockets.DiagnosticId.md) record for the claimed Source, carrying
the scratch buffer size the datagram was truncated to.

 [ForDeliveryCounterRefused\(PeerId, long, long\)](ReactorSockets.DiagnosticRecord.ForDeliveryCounterRefused.md\#ReactorSockets\_DiagnosticRecord\_ForDeliveryCounterRefused\_ReactorSockets\_PeerId\_System\_Int64\_System\_Int64\_)

Builds a [`DiagnosticId.DeliveryCounterRefused`](ReactorSockets.DiagnosticId.md) record for the Peer whose
delivery-counter record claimed more Messages than this end ever sent it. <code>Value0</code> is the
claimed cumulative count; <code>Value1</code> is the count of Messages this end has actually appended
to this Peer's retention.

 [ForDestinationUnreachable\(LinkId, SocketError\)](ReactorSockets.DiagnosticRecord.ForDestinationUnreachable.md\#ReactorSockets\_DiagnosticRecord\_ForDestinationUnreachable\_ReactorSockets\_LinkId\_System\_Net\_Sockets\_SocketError\_)

Builds a [`DiagnosticId.DestinationUnreachable`](ReactorSockets.DiagnosticId.md) record carrying the platform
[`SocketError`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror) reported for the send.

 [ForFragmentSetAbandoned\(PeerId, long, long\)](ReactorSockets.DiagnosticRecord.ForFragmentSetAbandoned.md\#ReactorSockets\_DiagnosticRecord\_ForFragmentSetAbandoned\_ReactorSockets\_PeerId\_System\_Int64\_System\_Int64\_)

Builds a [`DiagnosticId.FragmentSetAbandoned`](ReactorSockets.DiagnosticId.md) record for the Peer whose Fragment set
was abandoned before it ever completed, carrying the fragments actually held and the number the
set's own metadata declared.

 [ForLingerNegotiated\(PeerId, long, long\)](ReactorSockets.DiagnosticRecord.ForLingerNegotiated.md\#ReactorSockets\_DiagnosticRecord\_ForLingerNegotiated\_ReactorSockets\_PeerId\_System\_Int64\_System\_Int64\_)

Builds a [`DiagnosticId.LingerNegotiated`](ReactorSockets.DiagnosticId.md) record for the newly established
<code class="paramref">peer</code>, on either profile — stream or datagram. <code>Value0</code> is this end's own
Linger in milliseconds; <code>Value1</code> is the effective (minimum of the two ends') Linger in
milliseconds.

 [ForLinkFailed\(LinkId, SocketError\)](ReactorSockets.DiagnosticRecord.ForLinkFailed.md\#ReactorSockets\_DiagnosticRecord\_ForLinkFailed\_ReactorSockets\_LinkId\_System\_Net\_Sockets\_SocketError\_)

Builds a [`DiagnosticId.LinkFailed`](ReactorSockets.DiagnosticId.md) record carrying the platform
[`SocketError`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror) that caused the failure.

 [ForMalformedFraming\(PeerId, long, long\)](ReactorSockets.DiagnosticRecord.ForMalformedFraming.md\#ReactorSockets\_DiagnosticRecord\_ForMalformedFraming\_ReactorSockets\_PeerId\_System\_Int64\_System\_Int64\_)

Builds a [`DiagnosticId.MalformedFraming`](ReactorSockets.DiagnosticId.md) record for the Peer whose framing broke
this profile's rules. <code>Value0</code> is the offending length prefix — 0 for a truncation, where
no full prefix was ever read; <code>Value1</code> is this Reactor's own <code>MaxMessageSize</code>.

 [ForOversizedFragment\(PeerId, long, long\)](ReactorSockets.DiagnosticRecord.ForOversizedFragment.md\#ReactorSockets\_DiagnosticRecord\_ForOversizedFragment\_ReactorSockets\_PeerId\_System\_Int64\_System\_Int64\_)

Builds a [`DiagnosticId.OversizedFragment`](ReactorSockets.DiagnosticId.md) record for the Peer whose reliable Fragment
was wider than this Reactor's own Fragment size, carrying the Fragment's payload length and that
size.

 [ForOversizedFragmentSet\(PeerId, long, long\)](ReactorSockets.DiagnosticRecord.ForOversizedFragmentSet.md\#ReactorSockets\_DiagnosticRecord\_ForOversizedFragmentSet\_ReactorSockets\_PeerId\_System\_Int64\_System\_Int64\_)

Builds a [`DiagnosticId.OversizedFragmentSet`](ReactorSockets.DiagnosticId.md) record for the Peer whose Fragment
declared a <code>fragmentCount</code> larger than this Reactor's own receive window could ever hold,
carrying the declared count and the largest legal count.

 [ForOversizedMessage\(PeerId, long, long\)](ReactorSockets.DiagnosticRecord.ForOversizedMessage.md\#ReactorSockets\_DiagnosticRecord\_ForOversizedMessage\_ReactorSockets\_PeerId\_System\_Int64\_System\_Int64\_)

Builds a [`DiagnosticId.OversizedMessage`](ReactorSockets.DiagnosticId.md) record for the Peer whose completed Message
was larger than this Reactor's own <code>MaxMessageSize</code>, carrying the Message's length and that
limit.

 [ForPeerRefusedCapacity\(Source, int\)](ReactorSockets.DiagnosticRecord.ForPeerRefusedCapacity.md\#ReactorSockets\_DiagnosticRecord\_ForPeerRefusedCapacity\_ReactorSockets\_Source\_System\_Int32\_)

Builds a [`DiagnosticId.PeerRefused`](ReactorSockets.DiagnosticId.md) record for the claimed Source of a stream
dialler refused because every capacity Slot was occupied. <code>Value0</code> carries this Reactor's
capacity; <code>Value1</code> is unused.

 [ForPeerRefusedVersion\(Source, long, long\)](ReactorSockets.DiagnosticRecord.ForPeerRefusedVersion.md\#ReactorSockets\_DiagnosticRecord\_ForPeerRefusedVersion\_ReactorSockets\_Source\_System\_Int64\_System\_Int64\_)

Builds a [`DiagnosticId.PeerRefused`](ReactorSockets.DiagnosticId.md) record for the claimed Source of a stream
dialler refused because it announced a version this Reactor does not speak. <code>Value0</code> is
the version offered; <code>Value1</code> is the version this Reactor requires.

 [ForRedialAttempted\(PeerId, long, long\)](ReactorSockets.DiagnosticRecord.ForRedialAttempted.md\#ReactorSockets\_DiagnosticRecord\_ForRedialAttempted\_ReactorSockets\_PeerId\_System\_Int64\_System\_Int64\_)

Builds a [`DiagnosticId.RedialAttempted`](ReactorSockets.DiagnosticId.md) record for the Peer beginning a fresh
re-dial attempt. <code>Value0</code> is the attempt number; <code>Value1</code> is the delay in
milliseconds this attempt actually waited before dialling — the jittered figure, not the
computed delay before jitter.

 [ForRedialFailed\(PeerId, SocketError, long\)](ReactorSockets.DiagnosticRecord.ForRedialFailed.md\#ReactorSockets\_DiagnosticRecord\_ForRedialFailed\_ReactorSockets\_PeerId\_System\_Net\_Sockets\_SocketError\_System\_Int64\_)

Builds a [`DiagnosticId.RedialFailed`](ReactorSockets.DiagnosticId.md) record for the Peer whose re-dial attempt
could not connect. <code>Value0</code> carries the platform [`SocketError`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror) as its
underlying integer value; <code>Value1</code> is the attempt number that failed.

 [ForResumeMatched\(PeerId, long\)](ReactorSockets.DiagnosticRecord.ForResumeMatched.md\#ReactorSockets\_DiagnosticRecord\_ForResumeMatched\_ReactorSockets\_PeerId\_System\_Int64\_)

Builds a [`DiagnosticId.ResumeMatched`](ReactorSockets.DiagnosticId.md) record for the Peer a valid resume secret was
matched back onto. <code>Value0</code> is the number of retained outbound bytes the match rewound;
<code>Value1</code> is unused.

 [ForResumeUnmatched\(Source, long, long\)](ReactorSockets.DiagnosticRecord.ForResumeUnmatched.md\#ReactorSockets\_DiagnosticRecord\_ForResumeUnmatched\_ReactorSockets\_Source\_System\_Int64\_System\_Int64\_)

Builds a [`DiagnosticId.ResumeUnmatched`](ReactorSockets.DiagnosticId.md) record for the claimed Source of a
connection that claimed an existing Session and was refused rather than resumed.
<code>Value0</code> is the number of Interrupted Peers the claim was checked against;
<code>Value1</code> is the wire record type the claim arrived as.

 [ForRetransmissionTimerBackedOff\(PeerId, long, long\)](ReactorSockets.DiagnosticRecord.ForRetransmissionTimerBackedOff.md\#ReactorSockets\_DiagnosticRecord\_ForRetransmissionTimerBackedOff\_ReactorSockets\_PeerId\_System\_Int64\_System\_Int64\_)

Builds a [`DiagnosticId.RetransmissionTimerBackedOff`](ReactorSockets.DiagnosticId.md) record for the Peer whose
`RetransmissionTimer` just backed off. <code>Value0</code> is the new RTO in milliseconds;
<code>Value1</code> is the consecutive backoff count after this backoff.

 [ForUnreliableFragment\(Source, long\)](ReactorSockets.DiagnosticRecord.ForUnreliableFragment.md\#ReactorSockets\_DiagnosticRecord\_ForUnreliableFragment\_ReactorSockets\_Source\_System\_Int64\_)

Builds a [`DiagnosticId.UnreliableFragment`](ReactorSockets.DiagnosticId.md) record for the claimed Source that put a
Fragment on the wire in an unreliable mode, carrying the mode it claimed as the two mode bits'
wire value. <code>Value1</code> is unused: the rule has no threshold to report against.

 [ForVersionRefused\(Source, long, long\)](ReactorSockets.DiagnosticRecord.ForVersionRefused.md\#ReactorSockets\_DiagnosticRecord\_ForVersionRefused\_ReactorSockets\_Source\_System\_Int64\_System\_Int64\_)

Builds a [`DiagnosticId.VersionRefused`](ReactorSockets.DiagnosticId.md) record for the claimed Source, carrying the
version offered and the version this Reactor requires.

