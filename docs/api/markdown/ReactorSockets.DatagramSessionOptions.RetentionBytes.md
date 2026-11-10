# <a id="ReactorSockets_DatagramSessionOptions_RetentionBytes"></a> Property RetentionBytes

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_RetentionBytes"></a> RetentionBytes

The bytes each reliable Space retains per Peer for retransmission, until what it holds is
acknowledged. Required: this is the consumer trading their memory against their tolerance for
stalling on loss, and the right answer depends on their traffic, not on anything here.

```csharp
public required int RetentionBytes { get; init; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)

