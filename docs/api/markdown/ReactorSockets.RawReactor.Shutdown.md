# <a id="ReactorSockets_RawReactor_Shutdown_ReactorSockets_LinkId_"></a> Method Shutdown

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawReactor_Shutdown_ReactorSockets_LinkId_"></a> Shutdown\(LinkId\)

<code>Shutdown</code> is stream-only and illegal on a datagram or listening Link. Sends this Link's
own FIN, closing the send half while the Link stays open for reading — this is what lets a
consumer build a request-response protocol that signals end-of-request by closing the write
half, and it is a one-way door: a further [`RawReactor.Send(LinkId, ReadOnlySpan<byte>)`](ReactorSockets.RawReactor.Send.md#ReactorSockets_RawReactor_Send_ReactorSockets_LinkId_System_ReadOnlySpan_System_Byte__) on this Link fails afterwards.
Fails without throwing (ADR 0008), naming whichever applies: <code class="paramref">link</code> does not
address an open Link, or it does and that Link cannot be shut down. The open check comes first,
for the reason on [`RawReactor.Send(LinkId, ReadOnlySpan<byte>)`](ReactorSockets.RawReactor.Send.md#ReactorSockets_RawReactor_Send_ReactorSockets_LinkId_System_ReadOnlySpan_System_Byte__).

```csharp
public Result Shutdown(LinkId link)
```

### Parameters

`link` [LinkId](ReactorSockets.LinkId.md)

### Returns

 [Result](ReactorSockets.Result.md)

