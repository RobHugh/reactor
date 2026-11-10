# <a id="ReactorSockets_StreamSessionOptions_BoundaryRingBudgetBytes"></a> Property BoundaryRingBudgetBytes

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_StreamSessionOptions_BoundaryRingBudgetBytes"></a> BoundaryRingBudgetBytes

The Message-boundary ring line of the memory bill in bytes: one <code>uint</code> (4 bytes) per entry
in `BoundaryRingEntriesPerPeer`, across every Peer, reported the same way as the
other two lines. This is what makes releasing retention by what the peer confirmed receiving
possible at all — see the class remarks.

```csharp
public long BoundaryRingBudgetBytes { get; }
```

### Property Value

 [long](https://learn.microsoft.com/dotnet/api/system.int64)

