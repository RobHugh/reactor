# <a id="ReactorSockets_ReactorCounters_UnknownSourceDatagrams"></a> Property UnknownSourceDatagrams

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_ReactorCounters_UnknownSourceDatagrams"></a> UnknownSourceDatagrams

Datagrams taken from the transport and thrown away because nothing could be attributed to a Peer:
a malformed control datagram, or a session id naming no Session. Not a Discard in the
<code class="paramref">AdmissionDiscards</code> sense: no Admission was declined and no cap was spent, the
bytes named nobody. A data datagram whose session id <em>did</em> name a live Session and then
failed to parse is <code class="paramref">MalformedSessionDatagrams</code> instead. Reactor-scope rather than
per-Peer precisely because there is no Peer to charge them to — the whole point of the member is
the traffic that failed to name one. Moves only on the datagram Session profile. A rising figure
on a Reactor that is otherwise healthy is a scan or a flood, not a fault, which is why the
matching Diagnostics are [`DiagnosticLevel.Warn`](ReactorSockets.DiagnosticLevel.md). Not derivable: no Peer exists to
carry it, and <code class="paramref">DatagramsReceived</code> counts these together with everything else.

```csharp
public ulong UnknownSourceDatagrams { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

