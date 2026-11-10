# <a id="ReactorSockets_RawEvent"></a> Struct RawEvent

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

One event drained from a [`RawReactor`](ReactorSockets.RawReactor.md). A <code>ref struct</code> because
[`RawEvent.Payload`](ReactorSockets.RawEvent.Payload.md#ReactorSockets_RawEvent_Payload) aliases the frame arena directly and must never outlive the Frame it was
drained in (ADR 0008). [`RawEvent.Kind`](ReactorSockets.RawEvent.Kind.md#ReactorSockets_RawEvent_Kind) determines exactly which other fields are populated —
the kinds are [`RawEventKind.DatagramReceived`](ReactorSockets.RawEventKind.md), [`RawEventKind.Closed`](ReactorSockets.RawEventKind.md),
[`RawEventKind.Accepted`](ReactorSockets.RawEventKind.md), [`RawEventKind.Connected`](ReactorSockets.RawEventKind.md),
[`RawEventKind.SegmentReceived`](ReactorSockets.RawEventKind.md), [`RawEventKind.EndOfStream`](ReactorSockets.RawEventKind.md) and
[`RawEventKind.Writable`](ReactorSockets.RawEventKind.md) — the complete Raw profile roster.

```csharp
public readonly ref struct RawEvent
```

#### Inherited Members

[object.Equals\(object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\)), 
[object.Equals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\-system\-object\)), 
[object.GetHashCode\(\)](https://learn.microsoft.com/dotnet/api/system.object.gethashcode), 
[object.GetType\(\)](https://learn.microsoft.com/dotnet/api/system.object.gettype), 
[object.ReferenceEquals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.referenceequals), 
[object.ToString\(\)](https://learn.microsoft.com/dotnet/api/system.object.tostring)

## Properties

 [Kind](ReactorSockets.RawEvent.Kind.md\#ReactorSockets\_RawEvent\_Kind)

Which kind of event this is.

 [Link](ReactorSockets.RawEvent.Link.md\#ReactorSockets\_RawEvent\_Link)

The Link this event concerns. Always populated — for [`RawEventKind.Accepted`](ReactorSockets.RawEventKind.md) this
is the new connection's identity, not the listening Link's (see [`RawEvent.ListeningLink`](ReactorSockets.RawEvent.ListeningLink.md#ReactorSockets_RawEvent_ListeningLink)
for that).

 [ListeningLink](ReactorSockets.RawEvent.ListeningLink.md\#ReactorSockets\_RawEvent\_ListeningLink)

The listening Link that produced this accepted connection. Populated only when
[`RawEvent.Kind`](ReactorSockets.RawEvent.Kind.md#ReactorSockets_RawEvent_Kind) is [`RawEventKind.Accepted`](ReactorSockets.RawEventKind.md); a consumer listening on several
Links needs this to know which one just fired. Named for the Link rather than a "Listener"
deliberately: listening is a role a Link plays, not a kind of thing this library names.

 [Payload](ReactorSockets.RawEvent.Payload.md\#ReactorSockets\_RawEvent\_Payload)

The received bytes, valid only until the next [`RawReactor.Pump()`](ReactorSockets.RawReactor.Pump.md#ReactorSockets_RawReactor_Pump). Populated when
[`RawEvent.Kind`](ReactorSockets.RawEvent.Kind.md#ReactorSockets_RawEvent_Kind) is [`RawEventKind.DatagramReceived`](ReactorSockets.RawEventKind.md) or
[`RawEventKind.SegmentReceived`](ReactorSockets.RawEventKind.md). For a Segment this is exactly the run of bytes that
arrived on this Link in this Frame — no claim of being a whole anything, and nothing is held
across a Pump to make one (ADR 0008).

 [Reason](ReactorSockets.RawEvent.Reason.md\#ReactorSockets\_RawEvent\_Reason)

Why the Link closed. Populated only when [`RawEvent.Kind`](ReactorSockets.RawEvent.Kind.md#ReactorSockets_RawEvent_Kind) is [`RawEventKind.Closed`](ReactorSockets.RawEventKind.md).

 [Source](ReactorSockets.RawEvent.Source.md\#ReactorSockets\_RawEvent\_Source)

Who claims to have sent this datagram — an unauthenticated hint, never an identity. Populated
only when [`RawEvent.Kind`](ReactorSockets.RawEvent.Kind.md#ReactorSockets_RawEvent_Kind) is [`RawEventKind.DatagramReceived`](ReactorSockets.RawEventKind.md).

