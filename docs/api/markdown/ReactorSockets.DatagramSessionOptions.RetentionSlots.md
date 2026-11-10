# <a id="ReactorSockets_DatagramSessionOptions_RetentionSlots"></a> Property RetentionSlots

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_RetentionSlots"></a> RetentionSlots

The uniform retention slots one reliable Space holds per Peer: [`DatagramSessionOptions.RetentionBytes`](ReactorSockets.DatagramSessionOptions.RetentionBytes.md#ReactorSockets_DatagramSessionOptions_RetentionBytes)
divided by [`DatagramSessionOptions.ResolvedFragmentSize`](ReactorSockets.DatagramSessionOptions.ResolvedFragmentSize.md#ReactorSockets_DatagramSessionOptions_ResolvedFragmentSize), truncating any remainder. Stays derived from the
byte bound rather than a knob of its own (ADR 0012) so it cannot drift from
[`DatagramSessionOptions.RetentionBytes`](ReactorSockets.DatagramSessionOptions.RetentionBytes.md#ReactorSockets_DatagramSessionOptions_RetentionBytes) when the fragment size changes. `Validate` already
requires retention to hold at least one fragment, so this is always at least 1.

```csharp
public int RetentionSlots { get; }
```

### Property Value

 [int](https://learn.microsoft.com/dotnet/api/system.int32)

