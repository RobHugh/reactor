# <a id="ReactorSockets_SendResult"></a> Struct SendResult

Namespace: [ReactorSockets](ReactorSockets.md)  
Assembly: ReactorSockets.dll  

The outcome of one [`RawReactor.Send(LinkId, ReadOnlySpan<byte>)`](ReactorSockets.RawReactor.Send.md#ReactorSockets_RawReactor_Send_ReactorSockets_LinkId_System_ReadOnlySpan_System_Byte__) call. Unlike [`Result`](ReactorSockets.Result.md), a stream send
has a quantity worth reporting even on success: the Reactor holds no outbound bytes (ADR 0008), so
<code>Send</code> takes what the socket will take right now and hands back the count — the caller keeps
whatever was not taken, and [`SendResult.BytesSent`](ReactorSockets.SendResult.BytesSent.md#ReactorSockets_SendResult_BytesSent) is how it learns how much that is.

```csharp
public readonly record struct SendResult : IEquatable<SendResult>
```

#### Implements

[IEquatable<SendResult\>](https://learn.microsoft.com/dotnet/api/system.iequatable\-1)

#### Inherited Members

[object.Equals\(object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\)), 
[object.Equals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\-system\-object\)), 
[object.GetHashCode\(\)](https://learn.microsoft.com/dotnet/api/system.object.gethashcode), 
[object.GetType\(\)](https://learn.microsoft.com/dotnet/api/system.object.gettype), 
[object.ReferenceEquals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.referenceequals), 
[object.ToString\(\)](https://learn.microsoft.com/dotnet/api/system.object.tostring)

## Properties

 [BytesSent](ReactorSockets.SendResult.BytesSent.md\#ReactorSockets\_SendResult\_BytesSent)

The number of bytes the socket actually took this call. Zero when [`SendResult.IsSuccess`](ReactorSockets.SendResult.IsSuccess.md#ReactorSockets_SendResult_IsSuccess) is
<a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/bool">false</a>. Never queued or copied by the Reactor — whatever this does not account
for out of the offered payload is still the caller's to keep and retry.

 [Error](ReactorSockets.SendResult.Error.md\#ReactorSockets\_SendResult\_Error)

The failure reason, or <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/keywords/null">null</a> when [`SendResult.IsSuccess`](ReactorSockets.SendResult.IsSuccess.md#ReactorSockets_SendResult_IsSuccess) is <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/bool">true</a>.

 [IsSuccess](ReactorSockets.SendResult.IsSuccess.md\#ReactorSockets\_SendResult\_IsSuccess)

Whether the call itself succeeded — not whether every offered byte was taken. A short send
([`SendResult.BytesSent`](ReactorSockets.SendResult.BytesSent.md#ReactorSockets_SendResult_BytesSent) less than what was offered) is still <a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/bool">true</a>: it arms
exactly one [`RawEventKind.Writable`](ReactorSockets.RawEventKind.md) for this Link and is otherwise ordinary. This is
<a href="https://learn.microsoft.com/dotnet/csharp/language-reference/builtin-types/bool">false</a> only for an illegal call (wrong Link kind, a stale [`LinkId`](ReactorSockets.LinkId.md),
a Link already shut down for writing) or a socket-level failure.

## Methods

 [Failure\(string\)](ReactorSockets.SendResult.Failure.md\#ReactorSockets\_SendResult\_Failure\_System\_String\_)

Builds a failed result naming <code class="paramref">error</code> as the reason.

 [Success\(int\)](ReactorSockets.SendResult.Success.md\#ReactorSockets\_SendResult\_Success\_System\_Int32\_)

Builds a successful result reporting how many bytes the socket actually took.

