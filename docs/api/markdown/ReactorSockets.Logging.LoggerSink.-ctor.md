# <a id="ReactorSockets_Logging_LoggerSink__ctor_Microsoft_Extensions_Logging_ILogger_"></a> Constructor LoggerSink

Namespace: [ReactorSockets.Logging](ReactorSockets.Logging.md)  
Assembly: ReactorSockets.Logging.dll  

## <a id="ReactorSockets_Logging_LoggerSink__ctor_Microsoft_Extensions_Logging_ILogger_"></a> LoggerSink\(ILogger\)

An [`IReactorDiagnostics`](ReactorSockets.IReactorDiagnostics.md) Sink that writes each record to an [`ILogger`](https://learn.microsoft.com/dotnet/api/microsoft.extensions.logging.ilogger). The
message is [`DiagnosticFormatter.Format(in DiagnosticRecord)`](ReactorSockets.DiagnosticFormatter.Format.md#ReactorSockets_DiagnosticFormatter_Format_ReactorSockets_DiagnosticRecord__)'s text, never wording of this package's own, and
the state carries the record's typed fields for a structured provider. One type serves every profile:
install one per Reactor, each over a logger with its own category from one
[`ILoggerFactory`](https://learn.microsoft.com/dotnet/api/microsoft.extensions.logging.iloggerfactory), and every Reactor writes one stream with each record attributed by
category.

```csharp
public LoggerSink(ILogger logger)
```

### Parameters

`logger` [ILogger](https://learn.microsoft.com/dotnet/api/microsoft.extensions.logging.ilogger)

The logger every record is written to.

### Remarks

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

