# <a id="ReactorSockets_Logging_LoggerSink"></a> Class LoggerSink

Namespace: [ReactorSockets.Logging](ReactorSockets.Logging.md)  
Assembly: ReactorSockets.Logging.dll  

An [`IReactorDiagnostics`](ReactorSockets.IReactorDiagnostics.md) Sink that writes each record to an [`ILogger`](https://learn.microsoft.com/dotnet/api/microsoft.extensions.logging.ilogger). The
message is [`DiagnosticFormatter.Format(in DiagnosticRecord)`](ReactorSockets.DiagnosticFormatter.Format.md#ReactorSockets_DiagnosticFormatter_Format_ReactorSockets_DiagnosticRecord__)'s text, never wording of this package's own, and
the state carries the record's typed fields for a structured provider. One type serves every profile:
install one per Reactor, each over a logger with its own category from one
[`ILoggerFactory`](https://learn.microsoft.com/dotnet/api/microsoft.extensions.logging.iloggerfactory), and every Reactor writes one stream with each record attributed by
category.

```csharp
public sealed class LoggerSink : IReactorDiagnostics
```

#### Inheritance

[object](https://learn.microsoft.com/dotnet/api/system.object) ← 
[LoggerSink](ReactorSockets.Logging.LoggerSink.md)

#### Implements

[IReactorDiagnostics](ReactorSockets.IReactorDiagnostics.md)

#### Inherited Members

[object.Equals\(object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\)), 
[object.Equals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.equals\#system\-object\-equals\(system\-object\-system\-object\)), 
[object.GetHashCode\(\)](https://learn.microsoft.com/dotnet/api/system.object.gethashcode), 
[object.GetType\(\)](https://learn.microsoft.com/dotnet/api/system.object.gettype), 
[object.ReferenceEquals\(object?, object?\)](https://learn.microsoft.com/dotnet/api/system.object.referenceequals), 
[object.ToString\(\)](https://learn.microsoft.com/dotnet/api/system.object.tostring)

## Remarks

<p>
This Sink is bound by the same contract as any other (ADR 0005): the Reactor calls it synchronously,
on the thread that called <code>Pump</code>, inside the Frame. Every provider the [`ILogger`](https://learn.microsoft.com/dotnet/api/microsoft.extensions.logging.ilogger) is
wired to runs there too, so a provider that writes to a console or a file synchronously stalls the
Frame for as long as the write takes. Configure providers that hand the write off and return.
</p>
<p>
The category is the attribution. A record with a Link or Peer subject also names its Reactor in the
<code>Reactor</code> field and the message, but a record with no subject or a Source subject names none, so
one instance shared between Reactors leaves those unattributed.
</p>

## Constructors

 [LoggerSink\(ILogger\)](ReactorSockets.Logging.LoggerSink.\-ctor.md\#ReactorSockets\_Logging\_LoggerSink\_\_ctor\_Microsoft\_Extensions\_Logging\_ILogger\_)

An [`IReactorDiagnostics`](ReactorSockets.IReactorDiagnostics.md) Sink that writes each record to an [`ILogger`](https://learn.microsoft.com/dotnet/api/microsoft.extensions.logging.ilogger). The
message is [`DiagnosticFormatter.Format(in DiagnosticRecord)`](ReactorSockets.DiagnosticFormatter.Format.md#ReactorSockets_DiagnosticFormatter_Format_ReactorSockets_DiagnosticRecord__)'s text, never wording of this package's own, and
the state carries the record's typed fields for a structured provider. One type serves every profile:
install one per Reactor, each over a logger with its own category from one
[`ILoggerFactory`](https://learn.microsoft.com/dotnet/api/microsoft.extensions.logging.iloggerfactory), and every Reactor writes one stream with each record attributed by
category.

## Methods

 [IsEnabled\(DiagnosticLevel\)](ReactorSockets.Logging.LoggerSink.IsEnabled.md\#ReactorSockets\_Logging\_LoggerSink\_IsEnabled\_ReactorSockets\_DiagnosticLevel\_)

Asks <code class="paramref">level</code>'s [`LogLevel`](https://learn.microsoft.com/dotnet/api/microsoft.extensions.logging.loglevel) of the logger, so a level the logger filters out is never built.

 [Log\(in DiagnosticRecord\)](ReactorSockets.Logging.LoggerSink.Log.md\#ReactorSockets\_Logging\_LoggerSink\_Log\_ReactorSockets\_DiagnosticRecord\_\_)

Writes <code class="paramref">record</code> at its mapped [`LogLevel`](https://learn.microsoft.com/dotnet/api/microsoft.extensions.logging.loglevel), with the
[`DiagnosticId`](ReactorSockets.DiagnosticId.md)'s value and name as the [`EventId`](https://learn.microsoft.com/dotnet/api/microsoft.extensions.logging.eventid).

