# <a id="ReactorSockets_CloseReason"></a> Enum CloseReason

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

Why a Link closed, defined and kept stable by this library rather than borrowed from a platform
enumeration (ADR 0008): a consumer's permanent logic switches on this instead of a
[`SocketError`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror) that differs between Windows and Unix. The exact
[`SocketError`](https://learn.microsoft.com/dotnet/api/system.net.sockets.socketerror) travels in the [`DiagnosticRecord`](ReactorSockets.DiagnosticRecord.md) emitted
at the same moment, linked to this only by the Link and the Pump it happened in.

```csharp
public enum CloseReason
```

## Fields

`LocalClose = 0` 

The consumer called <code>Close</code> on this Link.



`ResetByRemote = 1` 

The remote end reset the connection. Named for the remote rather than a "peer" deliberately:
Peer is a Session word, and Raw leaves it free for whatever a consumer's own protocol layer
calls its remote party. This is the same "remote" [`RawReactor.RemoteOf(LinkId)`](ReactorSockets.RawReactor.RemoteOf.md#ReactorSockets_RawReactor_RemoteOf_ReactorSockets_LinkId_) answers for.



`Refused = 2` 

The remote end refused the connection.



`Unreachable = 3` 

The destination was reported unreachable.



`Failed = 4` 

The Link failed for a reason not covered by the other members.



