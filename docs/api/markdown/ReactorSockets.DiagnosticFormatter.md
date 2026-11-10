# <a id="ReactorSockets_DiagnosticFormatter"></a> Class DiagnosticFormatter

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

The one authoritative rendering of a [`DiagnosticRecord`](ReactorSockets.DiagnosticRecord.md) to text (ADR 0005, ADR 0013),
so a structured Sink consumes the typed fields and never formats, and a text Sink gets this
library's wording rather than inventing its own. Holds no state: nothing is allocated until
[`DiagnosticFormatter.Format(in DiagnosticRecord)`](ReactorSockets.DiagnosticFormatter.Format.md#ReactorSockets_DiagnosticFormatter_Format_ReactorSockets_DiagnosticRecord__) is called, and a consumer who never formats pays nothing.

```csharp
public static class DiagnosticFormatter
```

#### Inheritance

[object](https://learn.microsoft.com/dotnet/api/system.object) ← 
[DiagnosticFormatter](ReactorSockets.DiagnosticFormatter.md)

#### Inherited Members

[object.Equals\(object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\)), 
[object.Equals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\-system\-object\)), 
[object.GetHashCode\(\)](https://learn.microsoft.com/dotnet/api/system.object.gethashcode), 
[object.GetType\(\)](https://learn.microsoft.com/dotnet/api/system.object.gettype), 
[object.MemberwiseClone\(\)](https://learn.microsoft.com/dotnet/api/system.object.memberwiseclone), 
[object.ReferenceEquals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.referenceequals), 
[object.ToString\(\)](https://learn.microsoft.com/dotnet/api/system.object.tostring)

## Remarks

<p>
The output is permanent public surface — consumers grep it, alert on it and paste it into issues —
and its shape is <code>{Level} {Id} ({number}), {subject}: {what happened} ({value slots})</code>, the
subject segment omitted when the record has none. Numbers are rendered in the invariant culture.
</p>
<p>
A Source is rendered as a claim — <code>claimed source 203.0.113.7:4242 (unauthenticated)</code> — never
as a fact like "from 203.0.113.7:4242". A Source is a hint about origin that an attacker chooses,
and the wording must not invite a consumer to firewall on it (<code>CONTEXT.md</code>, ADR 0013).
</p>

## Methods

 [Format\(in DiagnosticRecord\)](ReactorSockets.DiagnosticFormatter.Format.md\#ReactorSockets\_DiagnosticFormatter\_Format\_ReactorSockets\_DiagnosticRecord\_\_)

Renders <code class="paramref">record</code> to text, leaving the record untouched. Throws
[`ArgumentOutOfRangeException`](https://learn.microsoft.com/dotnet/api/system.argumentoutofrangeexception) for an id or subject kind outside the roster, which
only a <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/default">default</a> or hand-forged record can carry — every record the library
builds has a cause by construction.

