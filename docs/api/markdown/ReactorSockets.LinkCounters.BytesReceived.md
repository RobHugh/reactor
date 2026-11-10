# <a id="ReactorSockets_LinkCounters_BytesReceived"></a> Property BytesReceived

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_LinkCounters_BytesReceived"></a> BytesReceived

Bytes delivered from this Link into the Arena. Not derivable: no other figure attributes received
bytes to one Link, and the Arena high-water is a per-Frame peak rather than a running total.

```csharp
public ulong BytesReceived { get; init; }
```

### Property Value

 [ulong](https://learn.microsoft.com/dotnet/api/system.uint64)

