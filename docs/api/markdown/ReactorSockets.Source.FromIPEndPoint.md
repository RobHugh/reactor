# <a id="ReactorSockets_Source_FromIPEndPoint_System_Net_IPEndPoint_"></a> Method FromIPEndPoint

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_Source_FromIPEndPoint_System_Net_IPEndPoint_"></a> FromIPEndPoint\(IPEndPoint\)

Converts an [`IPEndPoint`](https://learn.microsoft.com/dotnet/api/system.net.ipendpoint) — the type the consumer already holds on the cold
bind/dial surface — into the value type carried on the record path (ADR 0014). Throws
[`ArgumentException`](https://learn.microsoft.com/dotnet/api/system.argumentexception) for anything other than IPv4 or IPv6, since <code>DualMode</code>
is never set and no mapped form can appear here.

```csharp
public static Source FromIPEndPoint(IPEndPoint endpoint)
```

### Parameters

`endpoint` [IPEndPoint](https://learn.microsoft.com/dotnet/api/system.net.ipendpoint)

### Returns

 [Source](ReactorSockets.Source.md)

