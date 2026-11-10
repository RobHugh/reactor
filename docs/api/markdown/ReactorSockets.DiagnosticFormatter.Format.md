# <a id="ReactorSockets_DiagnosticFormatter_Format_ReactorSockets_DiagnosticRecord__"></a> Method Format

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

## <a id="ReactorSockets_DiagnosticFormatter_Format_ReactorSockets_DiagnosticRecord__"></a> Format\(in DiagnosticRecord\)

Renders <code class="paramref">record</code> to text, leaving the record untouched. Throws
[`ArgumentOutOfRangeException`](https://learn.microsoft.com/dotnet/api/system.argumentoutofrangeexception) for an id or subject kind outside the roster, which
only a <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/default">default</a> or hand-forged record can carry — every record the library
builds has a cause by construction.

```csharp
public static string Format(in DiagnosticRecord record)
```

### Parameters

`record` [DiagnosticRecord](ReactorSockets.DiagnosticRecord.md)

### Returns

 [string](https://learn.microsoft.com/dotnet/api/system.string)

