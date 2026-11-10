# <a id="ReactorSockets_DiagnosticSubjectKind"></a> Enum DiagnosticSubjectKind

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

Which field of a [`DiagnosticRecord`](ReactorSockets.DiagnosticRecord.md)'s tagged subject is populated (ADR 0013). A
record has exactly one subject, so a Sink or formatter switches on this rather than checking
several optional fields for which one is set.

```csharp
public enum DiagnosticSubjectKind
```

## Fields

`None = 0` 

The record concerns no particular Link or Source.



`Link = 1` 

The record concerns a registered Link, carried in [`DiagnosticRecord.Link`](ReactorSockets.DiagnosticRecord.Link.md#ReactorSockets_DiagnosticRecord_Link).



`Source = 2` 

The record concerns an unauthenticated claimed origin, carried in
[`DiagnosticRecord.Source`](ReactorSockets.DiagnosticRecord.Source.md#ReactorSockets_DiagnosticRecord_Source).



`Peer = 3` 

The record concerns an established Peer, carried in [`DiagnosticRecord.Peer`](ReactorSockets.DiagnosticRecord.Peer.md#ReactorSockets_DiagnosticRecord_Peer).
Distinct from [`DiagnosticSubjectKind.Source`](ReactorSockets.DiagnosticSubjectKind.md): a Peer exists only once a handshake has completed, while a
Source is merely a claimed origin — a refusal made before a Peer exists tags [`DiagnosticSubjectKind.Source`](ReactorSockets.DiagnosticSubjectKind.md),
never this.



