# <a id="ReactorSockets_PeerCounters_Resumes"></a> Property Resumes

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_PeerCounters_Resumes"></a> Resumes

How many times this Peer's transport has been successfully resumed after an Interruption (ADR 0013).
Incremented once per [`SessionEventKind.PeerResumed`](ReactorSockets.SessionEventKind.md) raised for this Peer, on both the
listening and dialling sides, and never on a refused resume. Earns its own member rather than being
derived by the consumer counting [`SessionEventKind.PeerResumed`](ReactorSockets.SessionEventKind.md) Events: a Session
profile drains Events into whatever cadence the consumer chooses, so counting them back out is not
generally possible once drained, while this figure is a Counter — read fresh by value at any time,
the same as every other figure here. Monotonic and never reset: a resume carries no counter's
history away with it, so a retransmission count spanning several resumes is how a flapping path
shows itself instead of being hidden by a zeroing on every reconnect. Not derivable: no other
counter moves only on a Resume.

```csharp
public ulong Resumes { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

