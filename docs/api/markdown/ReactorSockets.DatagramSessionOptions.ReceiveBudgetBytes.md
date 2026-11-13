# <a id="ReactorSockets_DatagramSessionOptions_ReceiveBudgetBytes"></a> Property ReceiveBudgetBytes

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DatagramSessionOptions_ReceiveBudgetBytes"></a> ReceiveBudgetBytes

The receive line of the memory bill in bytes, as a <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/integral-numeric-types">long</a> so that a
combination too large to allocate is a value this can report rather than an overflow it
silently wraps through. A figure above [`MaxValue`](https://learn.microsoft.com/dotnet/api/system.int64.maxvalue) reads as [`MaxValue`](https://learn.microsoft.com/dotnet/api/system.int64.maxvalue).

```csharp
public long ReceiveBudgetBytes { get; }
```

### Property Value

 [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Remarks

Four things per Peer scale with the window, and this figure covers all four. Payload slots —
[`DatagramSessionOptions.ResolvedFragmentSize`](ReactorSockets.DatagramSessionOptions.ResolvedFragmentSize.md#ReactorSockets_DatagramSessionOptions_ResolvedFragmentSize) bytes each — exist only for the
`ReliableSpaceCount` Spaces that hold fragments until a Message is whole. The
one-byte-per-slot admission record a receive window classifies against exists for all
`SpaceCount` Spaces, because all four classify whether or not they retain.
`FragmentLengthBytes` per slot records how many bytes of a held Fragment's payload
are actually occupied, and `FragmentGroupEntryBytes` per slot names the group that
payload belongs to — both exist only for the same `ReliableSpaceCount` Spaces as the
payload they describe, and both are the receive window's own reassembly bookkeeping, not a
separate buffer: the length says a piece was held, the group-identity entry is what still lets
an abandoned set's held and declared counts be read once that slot is reused. The six-byte
waiting-group entry — `PendingGroupEntryBytes` — exists for the same two Spaces too,
but only half a window's worth each (`PendingGroupsPerSpace`), since a
completed-but-undelivered group holds at least two Fragments. This figure is an exact statement
of what the receive arena takes at construction. Ordinary records held behind a gap are the one
receive-side cost outside it, and they have their own ceiling,
[`DatagramSessionOptions.HeldRecordBudgetBytes`](ReactorSockets.DatagramSessionOptions.HeldRecordBudgetBytes.md#ReactorSockets_DatagramSessionOptions_HeldRecordBudgetBytes).

