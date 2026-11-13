# <a id="ReactorSockets_ReactorCounters_AdmissionDeferrals"></a> Property AdmissionDeferrals

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_ReactorCounters_AdmissionDeferrals"></a> AdmissionDeferrals

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

```csharp
public ulong AdmissionDeferrals { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

