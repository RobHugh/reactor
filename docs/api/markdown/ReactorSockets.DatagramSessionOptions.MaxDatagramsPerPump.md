# <a id="ReactorSockets_DatagramSessionOptions_MaxDatagramsPerPump"></a> Property MaxDatagramsPerPump

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_MaxDatagramsPerPump"></a> MaxDatagramsPerPump

The most reads one Pump makes on each bound socket, so a flood cannot keep a Pump from
returning. Defaults to [`DatagramSessionOptions.ResolvedMaxDatagramsPerPump`](ReactorSockets.DatagramSessionOptions.ResolvedMaxDatagramsPerPump.md#ReactorSockets_DatagramSessionOptions_ResolvedMaxDatagramsPerPump). Provenance: derived from a
full Reactor's worth of traffic, as [`ReactorOptions.AdmissionCap`](ReactorSockets.ReactorOptions.AdmissionCap.md#ReactorSockets_ReactorOptions_AdmissionCap) is (ADR 0012). At
least 1.

```csharp
public int? MaxDatagramsPerPump { get; init; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)?

### Remarks

What this leaves unread waits in the socket's receive buffer for a later Pump. If that buffer
fills, the operating system drops what arrives next and no counter here sees it. A queued socket
error spends a read too. Reads are first come, first served, so a live Peer's datagrams wait behind
a flood from anyone else on that socket, and a long enough wait looks to the receive-idle deadline
like silence. Lower this to bound how long one Pump can take, at the cost of a longer wait. A Pump
that reaches it reports [`DiagnosticId.DatagramReadLimitReached`](ReactorSockets.DiagnosticId.md).

