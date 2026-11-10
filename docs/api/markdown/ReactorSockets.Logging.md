# <a id="ReactorSockets_Logging"></a> Namespace ReactorSockets.Logging

### Classes

 [LoggerSink](ReactorSockets.Logging.LoggerSink.md)

An [`IReactorDiagnostics`](ReactorSockets.IReactorDiagnostics.md) Sink that writes each record to an [`ILogger`](https://learn.microsoft.com/dotnet/api/microsoft.extensions.logging.ilogger). The
message is [`DiagnosticFormatter.Format(in DiagnosticRecord)`](ReactorSockets.DiagnosticFormatter.Format.md#ReactorSockets_DiagnosticFormatter_Format_ReactorSockets_DiagnosticRecord__)'s text, never wording of this package's own, and
the state carries the record's typed fields for a structured provider. One type serves every profile:
install one per Reactor, each over a logger with its own category from one
[`ILoggerFactory`](https://learn.microsoft.com/dotnet/api/microsoft.extensions.logging.iloggerfactory), and every Reactor writes one stream with each record attributed by
category.

