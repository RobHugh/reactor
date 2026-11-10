# <a id="ReactorSockets_LinkCounters_BytesSent"></a> Property BytesSent

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_LinkCounters_BytesSent"></a> BytesSent

Bytes the transport accepted for sending on this Link. Not derivable: the Reactor-scope datagram
figures count datagrams rather than bytes, cover every Link at once, and never see a stream Link.

```csharp
public ulong BytesSent { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

