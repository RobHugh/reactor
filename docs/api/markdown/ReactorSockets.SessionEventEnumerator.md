# <a id="ReactorSockets_SessionEventEnumerator"></a> Struct SessionEventEnumerator

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

The zero-allocation enumerator every Session Reactor's <code>Drain</code> returns (ADR 0010), shared by
both profiles via `ISessionEventSource` rather than duplicated per profile. A
<code>ref struct</code> so that a <code>foreach</code> over it can yield [`SessionEvent`](ReactorSockets.SessionEvent.md) —
itself a <code>ref struct</code> — without a heap-allocated <code>IEnumerator&lt;T&gt;</code>,
following the same pattern as [`Span`1`](https://learn.microsoft.com/dotnet/api/system.span-1)'s own enumerator.

```csharp
public ref struct SessionEventEnumerator
```

#### Inherited Members

[object.Equals\(object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\)), 
[object.Equals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\-system\-object\)), 
[object.GetHashCode\(\)](https://learn.microsoft.com/dotnet/api/system.object.gethashcode), 
[object.GetType\(\)](https://learn.microsoft.com/dotnet/api/system.object.gettype), 
[object.ReferenceEquals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.referenceequals), 
[object.ToString\(\)](https://learn.microsoft.com/dotnet/api/system.object.tostring)

## Properties

 [Current](ReactorSockets.SessionEventEnumerator.Current.md\#ReactorSockets\_SessionEventEnumerator\_Current)

The event at the enumerator's current position.

## Methods

 [GetEnumerator\(\)](ReactorSockets.SessionEventEnumerator.GetEnumerator.md\#ReactorSockets\_SessionEventEnumerator\_GetEnumerator)

Returns this enumerator, so <code>Drain()</code> itself is usable directly in a <code>foreach</code>.

 [MoveNext\(\)](ReactorSockets.SessionEventEnumerator.MoveNext.md\#ReactorSockets\_SessionEventEnumerator\_MoveNext)

Advances to the next pending event, if one remains this Frame.

