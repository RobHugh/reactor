# <a id="ReactorSockets"></a> Namespace ReactorSockets

### Namespaces

 [ReactorSockets.Logging](ReactorSockets.Logging.md)

### Classes

 [DatagramSessionOptions](ReactorSockets.DatagramSessionOptions.md)

The construction surface of a [`DatagramSessionReactor`](ReactorSockets.DatagramSessionReactor.md) (ADR 0012): the shared L0
budget composed in as [`DatagramSessionOptions.Reactor`](ReactorSockets.DatagramSessionOptions.Reactor.md#ReactorSockets_DatagramSessionOptions_Reactor), plus the knobs this profile alone has. Composed
rather than inherited, so that nothing protocol-specific here can ever leak into
[`ReactorOptions`](ReactorSockets.ReactorOptions.md) and turn it into the base class ADR 0011 refused.

 [DatagramSessionReactor](ReactorSockets.DatagramSessionReactor.md)

The datagram Session profile (ADR 0009): Sessions established over UDP, addressed as Peers, driven
by a single synchronous [`DatagramSessionReactor.Pump()`](ReactorSockets.DatagramSessionReactor.Pump.md#ReactorSockets_DatagramSessionReactor_Pump) from the consumer's own loop. It carries the handshake
and everything that guards it — the stateless cookie, the amplification bound, version and
capacity refusals, session identity and demultiplexing — and the data plane behind it: packet
records in four sequence Spaces, one per [`DeliveryMode`](ReactorSockets.DeliveryMode.md), fragmentation of reliable
Messages, acknowledgement and retransmission, heartbeats, Goodbye, interruption and Resume.

 [DiagnosticFormatter](ReactorSockets.DiagnosticFormatter.md)

The one authoritative rendering of a [`DiagnosticRecord`](ReactorSockets.DiagnosticRecord.md) to text (ADR 0005, ADR 0013),
so a structured Sink consumes the typed fields and never formats, and a text Sink gets this
library's wording rather than inventing its own. Holds no state: nothing is allocated until
[`DiagnosticFormatter.Format(in DiagnosticRecord)`](ReactorSockets.DiagnosticFormatter.Format.md#ReactorSockets_DiagnosticFormatter_Format_ReactorSockets_DiagnosticRecord__) is called, and a consumer who never formats pays nothing.

 [RawReactor](ReactorSockets.RawReactor.md)

The escape hatch (ADR 0007): a consumer registers UDP sockets at runtime, gets a [`LinkId`](ReactorSockets.LinkId.md)
back for each, and advances everything with a single synchronous [`RawReactor.Pump()`](ReactorSockets.RawReactor.Pump.md#ReactorSockets_RawReactor_Pump) call from their
own loop. Nothing is delivered except in response to a call the consumer made, on the thread that
constructed this Reactor. TCP joins the datagram surface here: a listening Link and the connections
it accepts are registered and admitted the same way, and a stream Link's reads drain as
[`RawEventKind.SegmentReceived`](ReactorSockets.RawEventKind.md) and [`RawEventKind.EndOfStream`](ReactorSockets.RawEventKind.md) exactly like a
datagram Link's. A stream Link's [`RawReactor.Send(LinkId, ReadOnlySpan<byte>)`](ReactorSockets.RawReactor.Send.md#ReactorSockets_RawReactor_Send_ReactorSockets_LinkId_System_ReadOnlySpan_System_Byte__) takes what the socket will take and reports the
count, arming exactly one [`RawEventKind.Writable`](ReactorSockets.RawEventKind.md) when it took fewer bytes than
offered; [`RawReactor.Shutdown(LinkId)`](ReactorSockets.RawReactor.Shutdown.md#ReactorSockets_RawReactor_Shutdown_ReactorSockets_LinkId_) sends this Link's own FIN while it stays open for reading.

 [ReactorOptions](ReactorSockets.ReactorOptions.md)

The budget L0 spends inside a Pump, shared by every profile. Composed as a member into each
profile's options type rather than inherited from a base (ADR 0012) — nothing protocol-specific
may ever enter this type, since a knob two of three profiles need is exactly what would turn it
into the base class ADR 0011 refused.

 [StreamSessionOptions](ReactorSockets.StreamSessionOptions.md)

The construction surface of a [`StreamSessionReactor`](ReactorSockets.StreamSessionReactor.md) (ADR 0011): the shared L0 budget
composed in as [`StreamSessionOptions.Reactor`](ReactorSockets.StreamSessionOptions.Reactor.md#ReactorSockets_StreamSessionOptions_Reactor), plus the knobs this profile alone has. Composed rather than
inherited, so that nothing protocol-specific here can ever leak into [`ReactorOptions`](ReactorSockets.ReactorOptions.md)
and turn it into the base class ADR 0011 refused.

 [StreamSessionReactor](ReactorSockets.StreamSessionReactor.md)

The stream Session profile (ADR 0010): Sessions established over TCP, addressed as Peers,
delivering <code>u32</code> length-prefixed Messages rather than the byte runs the transport gives.
Construction validates [`StreamSessionOptions`](ReactorSockets.StreamSessionOptions.md) and its allocation bill and binds the
declared endpoint set; each Peer is carried through the handshake to an established Session. Each
established Peer reassembles Messages in its own persistent receive buffer and delivers each whole
one as a [`SessionEventKind.MessageReceived`](ReactorSockets.SessionEventKind.md) event copied into the shared frame arena.

### Structs

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

A single diagnostic emitted to an [`IReactorDiagnostics`](ReactorSockets.IReactorDiagnostics.md) Sink: a closed
[`DiagnosticId`](ReactorSockets.DiagnosticId.md), its fixed [`DiagnosticLevel`](ReactorSockets.DiagnosticLevel.md), a tagged subject — none,
Link, Source or Peer (ADR 0013) — and two numeric slots whose meaning is documented per
[`DiagnosticId`](ReactorSockets.DiagnosticId.md) member. No strings, no <code>object[]</code>, no closures — everything here
lives on the stack and is built only after [`IReactorDiagnostics.IsEnabled(DiagnosticLevel)`](ReactorSockets.IReactorDiagnostics.IsEnabled.md#ReactorSockets_IReactorDiagnostics_IsEnabled_ReactorSockets_DiagnosticLevel_) passes.

 [LinkCounters](ReactorSockets.LinkCounters.md)

A point-in-time snapshot of one Link's counters (ADR 0013): monotonic figures running from that
Link's registration, with no reset, fetched by value and keyed by [`LinkId`](ReactorSockets.LinkId.md) — there is
no accessor that enumerates every Link's counters in one call.

 [LinkId](ReactorSockets.LinkId.md)

A Link's identity: the Reactor that issued it, a zero-based slot index, and a generation counter
that increments when the slot is reused, so a stale [`LinkId`](ReactorSockets.LinkId.md) held past a Link's close
compares unequal to whatever now occupies that slot (ADR 0014) rather than aliasing it.

 [PeerCounters](ReactorSockets.PeerCounters.md)

A point-in-time snapshot of one Peer's counters (ADR 0013): monotonic figures running from that
Peer's establishment, with no reset, fetched by value and keyed by [`PeerId`](ReactorSockets.PeerId.md) — mirrors
[`LinkCounters`](ReactorSockets.LinkCounters.md)'s Raw-profile shape and the same "no accessor enumerates every Peer's
counters in one call" rule.

 [PeerId](ReactorSockets.PeerId.md)

A Peer's identity inside this process: the Reactor that issued it, a zero-based Slot index, and a
generation counter that increments when the Slot is reused, so a stale [`PeerId`](ReactorSockets.PeerId.md) held
past a Peer's close compares unequal to whatever now occupies that Slot (ADR 0007) rather than
aliasing it. Shared by both Session profiles (ADR 0010), exactly as [`LinkId`](ReactorSockets.LinkId.md) is the
Raw profile's.

 [RawEvent](ReactorSockets.RawEvent.md)

One event drained from a [`RawReactor`](ReactorSockets.RawReactor.md). A <code>ref struct</code> because
[`RawEvent.Payload`](ReactorSockets.RawEvent.Payload.md#ReactorSockets_RawEvent_Payload) aliases the frame arena directly and must never outlive the Frame it was
drained in (ADR 0008). [`RawEvent.Kind`](ReactorSockets.RawEvent.Kind.md#ReactorSockets_RawEvent_Kind) determines exactly which other fields are populated —
the kinds are [`RawEventKind.DatagramReceived`](ReactorSockets.RawEventKind.md), [`RawEventKind.Closed`](ReactorSockets.RawEventKind.md),
[`RawEventKind.Accepted`](ReactorSockets.RawEventKind.md), [`RawEventKind.Connected`](ReactorSockets.RawEventKind.md),
[`RawEventKind.SegmentReceived`](ReactorSockets.RawEventKind.md), [`RawEventKind.EndOfStream`](ReactorSockets.RawEventKind.md) and
[`RawEventKind.Writable`](ReactorSockets.RawEventKind.md) — the complete Raw profile roster.

 [RawEventEnumerator](ReactorSockets.RawEventEnumerator.md)

The zero-allocation enumerator [`RawReactor.Drain()`](ReactorSockets.RawReactor.Drain.md#ReactorSockets_RawReactor_Drain) returns. A <code>ref struct</code>
so that a <code>foreach</code> over it can yield [`RawEvent`](ReactorSockets.RawEvent.md) — itself a <code>ref struct</code>
— without a heap-allocated <code>IEnumerator&lt;T&gt;</code>, following the same pattern as
[`Span`1`](https://learn.microsoft.com/dotnet/api/system.span-1)'s own enumerator.

 [ReactorCounters](ReactorSockets.ReactorCounters.md)

A point-in-time snapshot of the Reactor-scope counters (ADR 0013): monotonic figures running
from construction, with no reset. Fetched by value so nothing lets a consumer hold a pointer
into live state across a Pump; diffing two snapshots gives everything a reset would, from state
the consumer already owns.

 [Result](ReactorSockets.Result.md)

The outcome of an operation whose failures are the caller's to check rather than the runtime's to
throw — a wrong-kind call on a Link (<code>Send</code> on a datagram Link) or a send the remote end or
the network refused. [`Result.Error`](ReactorSockets.Result.Error.md#ReactorSockets_Result_Error) names the illegality or the platform failure in prose; there
is no error code enumeration here because, unlike [`CloseReason`](ReactorSockets.CloseReason.md), nothing about this
failure needs to survive as permanent public API — a consumer branches on [`Result.IsSuccess`](ReactorSockets.Result.IsSuccess.md#ReactorSockets_Result_IsSuccess)
and logs or surfaces [`Result.Error`](ReactorSockets.Result.Error.md#ReactorSockets_Result_Error) as-is.

 [SendResult](ReactorSockets.SendResult.md)

The outcome of one [`RawReactor.Send(LinkId, ReadOnlySpan<byte>)`](ReactorSockets.RawReactor.Send.md#ReactorSockets_RawReactor_Send_ReactorSockets_LinkId_System_ReadOnlySpan_System_Byte__) call. Unlike [`Result`](ReactorSockets.Result.md), a stream send
has a quantity worth reporting even on success: the Reactor holds no outbound bytes (ADR 0008), so
<code>Send</code> takes what the socket will take right now and hands back the count — the caller keeps
whatever was not taken, and [`SendResult.BytesSent`](ReactorSockets.SendResult.BytesSent.md#ReactorSockets_SendResult_BytesSent) is how it learns how much that is.

 [SessionEvent](ReactorSockets.SessionEvent.md)

One event drained from a Session Reactor. A <code>ref struct</code> because
[`SessionEvent.Payload`](ReactorSockets.SessionEvent.Payload.md#ReactorSockets_SessionEvent_Payload) aliases the frame arena directly, and must never outlive the Frame it was
drained in (ADR 0008). [`SessionEvent.Kind`](ReactorSockets.SessionEvent.Kind.md#ReactorSockets_SessionEvent_Kind) determines exactly which other fields are populated.

 [SessionEventEnumerator](ReactorSockets.SessionEventEnumerator.md)

The zero-allocation enumerator every Session Reactor's <code>Drain</code> returns (ADR 0010), shared by
both profiles via `ISessionEventSource` rather than duplicated per profile. A
<code>ref struct</code> so that a <code>foreach</code> over it can yield [`SessionEvent`](ReactorSockets.SessionEvent.md) —
itself a <code>ref struct</code> — without a heap-allocated <code>IEnumerator&lt;T&gt;</code>,
following the same pattern as [`Span`1`](https://learn.microsoft.com/dotnet/api/system.span-1)'s own enumerator.

 [Source](ReactorSockets.Source.md)

Who a datagram claims to be from — an unauthenticated hint about origin, never an identity
(ADR 0014). Carried on the record path as a value type: <code>{ family, 16 address bytes, scope id,
port }</code>, roughly 56 bytes, so a Diagnostic or an Event never allocates to name a Source.

### Interfaces

 [IReactorDiagnostics](ReactorSockets.IReactorDiagnostics.md)

The Diagnostic Sink contract. Implementations are called synchronously, on the consumer's
thread, inside <code>Pump</code> — a Sink that stalls stalls the Frame, and a Sink that throws is
caught, counted as <code>SinkFaulted</code>, and disabled for the life of the Reactor (ADR 0005).
Calling back into the Reactor from a Sink is a programmer error and throws.

### Enums

 [CloseReason](ReactorSockets.CloseReason.md)

Why a Link closed, defined and kept stable by this library rather than borrowed from a platform
enumeration (ADR 0008): a consumer's permanent logic switches on this instead of a
[`SocketError`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror) that differs between Windows and Unix. The exact
[`SocketError`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror) travels in the [`DiagnosticRecord`](ReactorSockets.DiagnosticRecord.md) emitted
at the same moment, linked to this only by the Link and the Pump it happened in.

 [DeliveryMode](ReactorSockets.DeliveryMode.md)

Which of a datagram Session's four delivery modes carries a Message sent through
[`DatagramSessionReactor.Send(PeerId, DeliveryMode, ReadOnlySpan<byte>)`](ReactorSockets.DatagramSessionReactor.Send.md#ReactorSockets_DatagramSessionReactor_Send_ReactorSockets_PeerId_ReactorSockets_DeliveryMode_System_ReadOnlySpan_System_Byte__) (ADR 0009). See
<code>docs/delivery-modes.md</code> for the consumer-facing guide to choosing one and the full guarantees
each mode makes: ordering, duplicate suppression, and what a late arrival does. Each mode is carried
in its own Space.

 [DiagnosticId](ReactorSockets.DiagnosticId.md)

A closed, permanent enumeration of diagnostic causes with explicit values in reserved blocks —
common 1-99, Raw 100-199, datagram session 200-299, stream session 300-399 (ADR 0013) — so a
future insertion cannot silently renumber an id a consumer has persisted. There is no
<code>None</code> or <code>Unknown</code> member and zero is never used: every [`DiagnosticRecord`](ReactorSockets.DiagnosticRecord.md)
has a cause by construction.

 [DiagnosticLevel](ReactorSockets.DiagnosticLevel.md)

The minimum severity a [`IReactorDiagnostics`](ReactorSockets.IReactorDiagnostics.md) Sink was installed to receive.
Ordered by ascending severity so that a level is enabled when it is at least as severe
as the installed <code>MinimumLevel</code>.

 [DiagnosticSubjectKind](ReactorSockets.DiagnosticSubjectKind.md)

Which field of a [`DiagnosticRecord`](ReactorSockets.DiagnosticRecord.md)'s tagged subject is populated (ADR 0013). A
record has exactly one subject, so a Sink or formatter switches on this rather than checking
several optional fields for which one is set.

 [DisconnectReason](ReactorSockets.DisconnectReason.md)

Why a Peer's Session ended, defined and kept stable by this library rather than borrowed from a
platform enumeration. Deliberately not shared with the Raw profile's [`CloseReason`](ReactorSockets.CloseReason.md)
(ADR 0008): most of the members a Session needs are protocol vocabulary Raw can never produce, and
a single shared enumeration would leave one profile documenting members it cannot emit.

 [RawEventKind](ReactorSockets.RawEventKind.md)

Which kind of [`RawEvent`](ReactorSockets.RawEvent.md) a drained event is (ADR 0008). [`RawEventKind.Accepted`](ReactorSockets.RawEventKind.md) comes
from a listening Link, [`RawEventKind.Connected`](ReactorSockets.RawEventKind.md) from dialling, [`RawEventKind.SegmentReceived`](ReactorSockets.RawEventKind.md) and
[`RawEventKind.EndOfStream`](ReactorSockets.RawEventKind.md) from stream reads, and [`RawEventKind.Writable`](ReactorSockets.RawEventKind.md) from partial sends.

 [SendOutcome](ReactorSockets.SendOutcome.md)

The outcome of one [`StreamSessionReactor.Send(PeerId, ReadOnlySpan<byte>)`](ReactorSockets.StreamSessionReactor.Send.md#ReactorSockets_StreamSessionReactor_Send_ReactorSockets_PeerId_System_ReadOnlySpan_System_Byte__) call. A bare
enum rather than a result struct (unlike [`SendResult`](ReactorSockets.SendResult.md)): a Session-level send has no
partial-completion quantity worth reporting back — the framed Message either fit in the Peer's
retention region whole, or it did not, so the outcome is the whole story.

 [SessionEventKind](ReactorSockets.SessionEventKind.md)

Which kind of [`SessionEvent`](ReactorSockets.SessionEvent.md) was drained, and therefore which of its fields carry
meaning. Shared by both Session profiles (ADR 0010).

 [SourceFamily](ReactorSockets.SourceFamily.md)

The address family of a [`Source`](ReactorSockets.Source.md), carried as an explicit tag rather than inferred
from the address bytes (ADR 0014). With <code>DualMode</code> never set, nothing in a 16-byte field
distinguishes a zero-padded IPv4 address from an IPv6 one.

