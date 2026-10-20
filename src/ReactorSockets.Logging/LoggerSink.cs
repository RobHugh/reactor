using Microsoft.Extensions.Logging;

namespace ReactorSockets.Logging;

/// <summary>
/// An <see cref="IReactorDiagnostics"/> Sink that writes each record to an <see cref="ILogger"/>. The
/// message is <see cref="DiagnosticFormatter.Format"/>'s text, never wording of this package's own, and
/// the state carries the record's typed fields for a structured provider. One type serves every profile:
/// install one per Reactor, each over a logger with its own category from one
/// <see cref="ILoggerFactory"/>, and every Reactor writes one stream with each record attributed by
/// category.
/// </summary>
/// <remarks>
/// <para>
/// This Sink is bound by the same contract as any other (ADR 0005): the Reactor calls it synchronously,
/// on the thread that called <c>Pump</c>, inside the Frame. Every provider the <see cref="ILogger"/> is
/// wired to runs there too, so a provider that writes to a console or a file synchronously stalls the
/// Frame for as long as the write takes. Configure providers that hand the write off and return.
/// </para>
/// <para>
/// The category is the attribution. A record with a Link or Peer subject also names its Reactor in the
/// <c>Reactor</c> field and the message, but a record with no subject or a Source subject names none, so
/// one instance shared between Reactors leaves those unattributed.
/// </para>
/// </remarks>
/// <param name="logger">The logger every record is written to.</param>
public sealed class LoggerSink(ILogger logger) : IReactorDiagnostics
{
    /// <summary>Asks <paramref name="level"/>'s <see cref="LogLevel"/> of the logger, so a level the logger filters out is never built.</summary>
    public bool IsEnabled(DiagnosticLevel level) => logger.IsEnabled(ToLogLevel(level));

    /// <summary>
    /// Writes <paramref name="record"/> at its mapped <see cref="LogLevel"/>, with the
    /// <see cref="DiagnosticId"/>'s value and name as the <see cref="EventId"/>.
    /// </summary>
    public void Log(in DiagnosticRecord record) =>
        logger.Log(
            ToLogLevel(record.Level),
            new EventId((int)record.Id, record.Id.ToString()),
            new DiagnosticFields(record),
            exception: null,
            static (fields, _) => DiagnosticFormatter.Format(fields.Record));

    /// <summary>Maps each <see cref="DiagnosticLevel"/> to the <see cref="LogLevel"/> of the same name or meaning.</summary>
    private static LogLevel ToLogLevel(DiagnosticLevel level) => level switch
    {
        DiagnosticLevel.Trace => LogLevel.Trace,
        DiagnosticLevel.Info => LogLevel.Information,
        DiagnosticLevel.Warn => LogLevel.Warning,
        DiagnosticLevel.Error => LogLevel.Error,
        _ => throw new ArgumentOutOfRangeException(nameof(level), level, $"{level} is not a recognised DiagnosticLevel."),
    };
}
