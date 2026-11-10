# <a id="ReactorSockets_DatagramSessionOptions_Linger"></a> Property Linger

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_Linger"></a> Linger

How long a closing Session stays alive to retransmit its Goodbye and absorb what is still in
flight. Required, for the same reason as [`DatagramSessionOptions.SendIdleInterval`](ReactorSockets.DatagramSessionOptions.SendIdleInterval.md#ReactorSockets_DatagramSessionOptions_SendIdleInterval): it trades a prompt
teardown against a clean one. At most 2^32 - 1 milliseconds (about 49.7 days), the most a handshake
may announce.

```csharp
public required TimeSpan Linger { get; init; }
```

### Property Value

 [TimeSpan](https://learn.microsoft.com/dotnet/api/system.timespan)

