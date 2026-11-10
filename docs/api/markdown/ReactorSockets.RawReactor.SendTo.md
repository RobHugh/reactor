# <a id="ReactorSockets_RawReactor_SendTo_ReactorSockets_LinkId_ReactorSockets_Source_System_ReadOnlySpan_System_Byte__"></a> Method SendTo

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_RawReactor_SendTo_ReactorSockets_LinkId_ReactorSockets_Source_System_ReadOnlySpan_System_Byte__"></a> SendTo\(LinkId, Source, ReadOnlySpan<byte\>\)

Sends <code class="paramref">payload</code> to <code class="paramref">destination</code> in one datagram, all-or-nothing
(ADR 0008). Fails without throwing when <code class="paramref">link</code> does not address an open Link or
the send itself fails.

```csharp
public Result SendTo(LinkId link, Source destination, ReadOnlySpan<byte> payload)
```

### Parameters

`link` [LinkId](ReactorSockets.LinkId.md)

`destination` [Source](ReactorSockets.Source.md)

`payload` [ReadOnlySpan](https://learn.microsoft.com/dotnet/api/system.readonlyspan\-1)<[byte](https://learn.microsoft.com/dotnet/api/system.byte)\>

### Returns

 [Result](ReactorSockets.Result.md)

