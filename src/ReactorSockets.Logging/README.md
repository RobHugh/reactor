# ReactorSockets.Logging

Writes ReactorSockets Diagnostics to a `Microsoft.Extensions.Logging` `ILogger`.

```csharp
ILogger logger = loggerFactory.CreateLogger("ReactorSockets");
LoggerSink sink = new(logger);

ReactorOptions options = new()
{
    Capacity = 64,
    Sink = sink,
    MinimumLevel = DiagnosticLevel.Info,
};
```

## Your logging providers run inside `Pump`

Every Diagnostic is written while your call to `Pump` is still running, on the thread that called it.
`Pump` does not return until every provider your `ILogger` is wired to has finished with the record.
A console or file provider that writes synchronously makes `Pump` take as long as that write, and
everything you do after `Pump` in that iteration of your loop waits for it.

Use providers that queue the write and return. Check that before you raise `MinimumLevel` to `Trace`.

A provider that throws is caught. The Reactor counts it in `SinkFaulted` and stops calling this Sink
for the rest of its life. Do not call back into the Reactor from a provider: that throws.

## What you get

- **The message** is the text `DiagnosticFormatter.Format` renders, the same wording every other Sink
  gets. This package writes none of its own.
- **The event id** is the `DiagnosticId`'s value and name.
- **The level** maps `Trace` to `Trace`, `Info` to `Information`, `Warn` to `Warning` and `Error` to
  `Error`.
- **The state** is the record's typed fields for structured providers: `DiagnosticId`,
  `DiagnosticLevel`, `SubjectKind`, the subject's fields, then `Value0` and `Value1`. A Link or Peer
  subject adds `Reactor`, `Slot` and `Generation`. A Source subject adds `ClaimedSource`: an address
  the sender chose, not one anything verified.

## One stream for every Reactor

Install one `LoggerSink` on Reactors of any profile and their Diagnostics arrive in one stream. A record
about a Link or a Peer names the Reactor that emitted it in its `Reactor` field. A record with no
subject or a Source subject does not. To attribute those as well, give each Reactor its own
`LoggerSink` over a logger with its own category from the same `ILoggerFactory`.

## Why this is a separate package

The `ReactorSockets` package takes no dependencies (ADR 0005), so it cannot reference
`Microsoft.Extensions.Logging.Abstractions`. If you do not use `ILogger`, implement
`IReactorDiagnostics` yourself. That costs you a small adapter, and the core stays free of a
dependency many consumers would have accepted.

This package versions in lockstep with `ReactorSockets` and follows the major versions of
`Microsoft.Extensions.Logging.Abstractions`.
