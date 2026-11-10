# <a id="ReactorSockets_DiagnosticRecord_ForArenaExhausted_ReactorSockets_LinkId_System_Int32_System_Int32_"></a> Method ForArenaExhausted

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForArenaExhausted_ReactorSockets_LinkId_System_Int32_System_Int32_"></a> ForArenaExhausted\(LinkId, int, int\)

Builds an [`DiagnosticId.ArenaExhausted`](ReactorSockets.DiagnosticId.md) record for the Link that made the request.

```csharp
public static DiagnosticRecord ForArenaExhausted(LinkId link, int bytesRequested, int bytesFree)
```

### Parameters

`link` [LinkId](ReactorSockets.LinkId.md)

`bytesRequested` [int](https://learn.microsoft.com/dotnet/api/system.int32)

`bytesFree` [int](https://learn.microsoft.com/dotnet/api/system.int32)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

## <a id="ReactorSockets_DiagnosticRecord_ForArenaExhausted_ReactorSockets_PeerId_System_Int32_System_Int32_"></a> ForArenaExhausted\(PeerId, int, int\)

Builds an [`DiagnosticId.ArenaExhausted`](ReactorSockets.DiagnosticId.md) record for the Peer whose Message could not
be reserved — the Session profile's counterpart to the Link-subject overload above (ADR 0014:
share-holder identity is per-Peer in a Session, not per-socket).

```csharp
public static DiagnosticRecord ForArenaExhausted(PeerId peer, int bytesRequested, int bytesFree)
```

### Parameters

`peer` [PeerId](ReactorSockets.PeerId.md)

`bytesRequested` [int](https://learn.microsoft.com/dotnet/api/system.int32)

`bytesFree` [int](https://learn.microsoft.com/dotnet/api/system.int32)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

