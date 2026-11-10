# <a id="ReactorSockets_PeerCounters_OutOfWindowPackets"></a> Property OutOfWindowPackets

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_PeerCounters_OutOfWindowPackets"></a> OutOfWindowPackets

How many packet records this Peer's per-Space receive window has classified as out of window —
a sequence number too far ahead of, or behind, that Space's receive window base
to be tracked against it (ADR 0013). Recurs every Pump under sustained loss or reordering, which is
exactly why it is a Counter and never a Diagnostic. Incremented by the parse loop's classification
of each arriving record; a freshly connected Peer reads zero. Note that only the two reliable Spaces
raise this on a far-ahead arrival: the unreliable Spaces slide their window forward instead and
count the retirement as loss, so for those Spaces out-of-window means a sequence below the base
alone. A <code>ReliableOrdered</code> record the window would admit but the Space will not hold behind a
gap counts here too, because it is refused the same way and comes back by retransmission. Moves only
on the datagram profile. Not derivable: bytes received counts these records together with admitted
ones, and no other figure separates reordering beyond the window from repeats.

```csharp
public ulong OutOfWindowPackets { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

