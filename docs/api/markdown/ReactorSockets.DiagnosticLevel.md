# <a id="ReactorSockets_DiagnosticLevel"></a> Enum DiagnosticLevel

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

The minimum severity a [`IReactorDiagnostics`](ReactorSockets.IReactorDiagnostics.md) Sink was installed to receive.
Ordered by ascending severity so that a level is enabled when it is at least as severe
as the installed <code>MinimumLevel</code>.

```csharp
public enum DiagnosticLevel
```

## Fields

`Trace = 0` 

The most verbose level: everything a Sink installed at this level receives.



`Info = 1` 

Routine, expected events worth recording but not acting on.



`Warn = 2` 

Recoverable conditions a consumer may want to notice.



`Error = 3` 

Conditions that end a Link or a Peer.



