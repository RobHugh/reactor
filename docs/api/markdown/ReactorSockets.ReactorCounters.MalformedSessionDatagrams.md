# <a id="ReactorSockets_ReactorCounters_MalformedSessionDatagrams"></a> Property MalformedSessionDatagrams

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_ReactorCounters_MalformedSessionDatagrams"></a> MalformedSessionDatagrams

Data datagrams thrown away for failing to parse after their session id named a live Session: a
short or bad header, a record that does not read, an illegal Fragment set. Kept apart from
<code class="paramref">UnknownSourceDatagrams</code> because the two say different things about who is
sending — this traffic did name a Peer, so it is either an established Peer sending bytes this
Reactor cannot read or a forgery that guessed a live id, where the other figure is traffic that
named nobody at all. Reactor-scope like its neighbour rather than per-Peer: a datagram whose body
did not parse is not proven to have come from the Peer whose id it carries, and charging it there
would let a stranger move an established Peer's figures. Moves only on the datagram Session
profile, and its Diagnostic is the same [`DiagnosticId.DatagramMalformed`](ReactorSockets.DiagnosticId.md). Not
derivable: the Peer it names never counts it, and <code class="paramref">UnknownSourceDatagrams</code>
counts only traffic that named nobody.

```csharp
public ulong MalformedSessionDatagrams { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

