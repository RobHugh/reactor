# <a id="ReactorSockets_DiagnosticRecord_ForUnreliableFragment_ReactorSockets_Source_System_Int64_"></a> Method ForUnreliableFragment

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticRecord_ForUnreliableFragment_ReactorSockets_Source_System_Int64_"></a> ForUnreliableFragment\(Source, long\)

Builds a [`DiagnosticId.UnreliableFragment`](ReactorSockets.DiagnosticId.md) record for the claimed Source that put a
Fragment on the wire in an unreliable mode, carrying the mode it claimed as the two mode bits'
wire value. <code>Value1</code> is unused: the rule has no threshold to report against.

```csharp
public static DiagnosticRecord ForUnreliableFragment(Source source, long modeBits)
```

### Parameters

`source` [Source](ReactorSockets.Source.md)

`modeBits` [long](https://learn.microsoft.com/dotnet/api/system.int64)

### Returns

 [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

