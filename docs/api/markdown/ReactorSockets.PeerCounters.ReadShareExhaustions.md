# <a id="ReactorSockets_PeerCounters_ReadShareExhaustions"></a> Property ReadShareExhaustions

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_PeerCounters_ReadShareExhaustions"></a> ReadShareExhaustions

How many times this Peer had a whole Message ready and was refused Arena room because it had
already used its read share for the Frame (ADR 0013, where <code>ReadShareReached</code> became this
counter). Counted once per refused reservation. What becomes of the Message differs by profile. The
stream profile keeps it buffered and delivers it on a later Pump. On the datagram profile an
unreliable record is lost. A reliable one is delivered on a later Pump: a record still held for send
order or reassembly is retried from where it is held, and any other is left unacknowledged so the
far end retransmits it. Recurs every Pump while one Peer outpaces its share, which is why it is a
Counter and not a Diagnostic. Moves on both Session profiles. Monotonic and never reset, exactly
like [`PeerCounters.HorizonDeferrals`](ReactorSockets.PeerCounters.HorizonDeferrals.md#ReactorSockets_PeerCounters_HorizonDeferrals): its lifetime is the Peer's, not the transport's, so a Resume
does not zero it. Not derivable: bytes received and [`PeerCounters.MessagesReceived`](ReactorSockets.PeerCounters.MessagesReceived.md#ReactorSockets_PeerCounters_MessagesReceived) show that
delivery lagged arrival, but not that the read share was the cause rather than the Arena itself
running out.

```csharp
public ulong ReadShareExhaustions { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

