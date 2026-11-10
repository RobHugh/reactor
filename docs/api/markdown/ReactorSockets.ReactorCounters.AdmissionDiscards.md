# <a id="ReactorSockets_ReactorCounters_AdmissionDiscards"></a> Property AdmissionDiscards

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_ReactorCounters_AdmissionDiscards"></a> AdmissionDiscards

Admissions declined that let the work go, because the transport gave it nowhere to wait. Moves
only on the datagram Session profile, which Discards the Hellos it cannot admit, whether the
admission cap, capacity or [`DatagramSessionOptions.MaxPeersPerAddress`](ReactorSockets.DatagramSessionOptions.MaxPeersPerAddress.md#ReactorSockets_DatagramSessionOptions_MaxPeersPerAddress) declined them. The
Diagnostics tell the three apart. Always an exact
count of work turned away, which <code class="paramref">AdmissionDeferrals</code> is not, because a Reactor
can only Discard what it had in hand. Kept apart from Deferrals because the two differ in the
one way a consumer would act on: deferred work arrives on a later Pump, Discarded work never
does. Not derivable: the two admission counters are never both non-zero on one Reactor, yet
knowing one says nothing of the other, and their units differ, so one shared field would invite a
sum or comparison that means nothing.

```csharp
public ulong AdmissionDiscards { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

