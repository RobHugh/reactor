# <a id="ReactorSockets_DiagnosticRecord_ForDatagramReadLimitReached_System_Int32_System_Int32_"></a> Method ForDatagramReadLimitReached

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForDatagramReadLimitReached_System_Int32_System_Int32_"></a> ForDatagramReadLimitReached\(int, int\)

Builds a [`DiagnosticId.DatagramReadLimitReached`](ReactorSockets.DiagnosticId.md) record. No subject. Carries the per-socket
read limit and the socket's index in [`DatagramSessionReactor.BoundEndpoints`](ReactorSockets.DatagramSessionReactor.BoundEndpoints.md#ReactorSockets_DatagramSessionReactor_BoundEndpoints).

```csharp
public static DiagnosticRecord ForDatagramReadLimitReached(int limit, int socketIndex)
```

### Parameters

`limit` [int](https://learn.microsoft.com/dotnet/api/system.int32)

`socketIndex` [int](https://learn.microsoft.com/dotnet/api/system.int32)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

