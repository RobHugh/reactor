# <a id="ReactorSockets_StreamSessionOptions_SendBufferSize"></a> Property SendBufferSize

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionOptions_SendBufferSize"></a> SendBufferSize

<code>SO_SNDBUF</code> for every Peer socket this Reactor accepts or dials, or <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/null">null</a>
— the default — for the OS default and its own autotuning. This is the kernel's send queue, not
[`StreamSessionOptions.RetentionBytes`](ReactorSockets.StreamSessionOptions.RetentionBytes.md#ReactorSockets_StreamSessionOptions_RetentionBytes): retention is what this end holds until the peer confirms receipt,
while this bounds how much the kernel will absorb from a flush before a write reports it would
block and the bytes stay in retention, unsent. Unset, the two are unrelated in practice on
Linux, whose send autotuning will absorb megabytes against a peer that has stopped reading; set,
a consumer who needs transport backpressure to reach their own flow control at a known point can
put it there. Provenance: unset leaves the OS's own default and autotuning in place, so the
library chooses nothing.

```csharp
public int? SendBufferSize { get; init; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)?

### Remarks

A request, not an exact size — Linux doubles the value and holds it to its own <code>tcp_wmem</code>
floor, so nothing may depend on a precise resulting byte count. It also does not bound
correctness of anything this profile does: a Reactor sends whatever the socket accepts and keeps
the rest, whatever the buffer's size.

