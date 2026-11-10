# <a id="ReactorSockets_DiagnosticRecord_ForDeliveryCounterRefused_ReactorSockets_PeerId_System_Int64_System_Int64_"></a> Method ForDeliveryCounterRefused

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForDeliveryCounterRefused_ReactorSockets_PeerId_System_Int64_System_Int64_"></a> ForDeliveryCounterRefused\(PeerId, long, long\)

Builds a [`DiagnosticId.DeliveryCounterRefused`](ReactorSockets.DiagnosticId.md) record for the Peer whose
delivery-counter record claimed more Messages than this end ever sent it. <code>Value0</code> is the
claimed cumulative count; <code>Value1</code> is the count of Messages this end has actually appended
to this Peer's retention.

```csharp
public static DiagnosticRecord ForDeliveryCounterRefused(PeerId peer, long claimedCount, long appendedMessageCount)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

`claimedCount` [long](https://learn.microsoft.com/dotnet/api/system.int64)

`appendedMessageCount` [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

