# <a id="ReactorSockets_SessionEvent"></a> Struct SessionEvent

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

One event drained from a Session Reactor. A <code>ref struct</code> because
[`SessionEvent.Payload`](ReactorSockets.SessionEvent.Payload.md#ReactorSockets_SessionEvent_Payload) aliases the frame arena directly, and must never outlive the Frame it was
drained in (ADR 0008). [`SessionEvent.Kind`](ReactorSockets.SessionEvent.Kind.md#ReactorSockets_SessionEvent_Kind) determines exactly which other fields are populated.

```csharp
public readonly ref struct SessionEvent
```

#### Inherited Members

[object.Equals\(object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\)), 
[object.Equals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\-system\-object\)), 
[object.GetHashCode\(\)](https://learn.microsoft.com/dotnet/api/system.object.gethashcode), 
[object.GetType\(\)](https://learn.microsoft.com/dotnet/api/system.object.gettype), 
[object.ReferenceEquals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.referenceequals), 
[object.ToString\(\)](https://learn.microsoft.com/dotnet/api/system.object.tostring)

## Properties

 [Kind](ReactorSockets.SessionEvent.Kind.md\#ReactorSockets\_SessionEvent\_Kind)

Which kind of event this is.

 [Payload](ReactorSockets.SessionEvent.Payload.md\#ReactorSockets\_SessionEvent\_Payload)

The whole Message this event carries, aliasing the frame arena. Populated only when
[`SessionEvent.Kind`](ReactorSockets.SessionEvent.Kind.md#ReactorSockets_SessionEvent_Kind) is [`SessionEventKind.MessageReceived`](ReactorSockets.SessionEventKind.md), and valid only until the
next Pump (ADR 0008) — copy it out before draining or pumping again.

 [Peer](ReactorSockets.SessionEvent.Peer.md\#ReactorSockets\_SessionEvent\_Peer)

The Peer this event concerns. Always populated.

 [Reason](ReactorSockets.SessionEvent.Reason.md\#ReactorSockets\_SessionEvent\_Reason)

Why the Session ended. Populated only when [`SessionEvent.Kind`](ReactorSockets.SessionEvent.Kind.md#ReactorSockets_SessionEvent_Kind) is
[`SessionEventKind.PeerClosed`](ReactorSockets.SessionEventKind.md).

