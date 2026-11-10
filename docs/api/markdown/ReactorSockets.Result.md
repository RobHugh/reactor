# <a id="ReactorSockets_Result"></a> Struct Result

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

The outcome of an operation whose failures are the caller's to check rather than the runtime's to
throw — a wrong-kind call on a Link (<code>Send</code> on a datagram Link) or a send the remote end or
the network refused. [`Result.Error`](ReactorSockets.Result.Error.md#ReactorSockets_Result_Error) names the illegality or the platform failure in prose; there
is no error code enumeration here because, unlike [`CloseReason`](ReactorSockets.CloseReason.md), nothing about this
failure needs to survive as permanent public API — a consumer branches on [`Result.IsSuccess`](ReactorSockets.Result.IsSuccess.md#ReactorSockets_Result_IsSuccess)
and logs or surfaces [`Result.Error`](ReactorSockets.Result.Error.md#ReactorSockets_Result_Error) as-is.

```csharp
public readonly record struct Result : IEquatable<Result>
```

#### Implements

[IEquatable<Result\>](https://learn.microsoft.com/dotnet/api/system.iequatable\-1)

#### Inherited Members

[object.Equals\(object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\)), 
[object.Equals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\-system\-object\)), 
[object.GetHashCode\(\)](https://learn.microsoft.com/dotnet/api/system.object.gethashcode), 
[object.GetType\(\)](https://learn.microsoft.com/dotnet/api/system.object.gettype), 
[object.ReferenceEquals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.referenceequals), 
[object.ToString\(\)](https://learn.microsoft.com/dotnet/api/system.object.tostring)

## Properties

 [Error](ReactorSockets.Result.Error.md\#ReactorSockets\_Result\_Error)

The failure reason, or <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/null">null</a> when [`Result.IsSuccess`](ReactorSockets.Result.IsSuccess.md#ReactorSockets_Result_IsSuccess) is <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/bool">true</a>.

 [IsSuccess](ReactorSockets.Result.IsSuccess.md\#ReactorSockets\_Result\_IsSuccess)

Whether the operation succeeded.

## Methods

 [Failure\(string\)](ReactorSockets.Result.Failure.md\#ReactorSockets\_Result\_Failure\_System\_String\_)

Builds a failed result naming <code class="paramref">error</code> as the reason.

 [Success\(\)](ReactorSockets.Result.Success.md\#ReactorSockets\_Result\_Success)

Builds a successful result.

