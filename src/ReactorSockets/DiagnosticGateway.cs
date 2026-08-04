namespace ReactorSockets;

/// <summary>
/// Wraps a consumer-installed <see cref="IReactorDiagnostics"/> Sink together with its configured
/// <see cref="DiagnosticLevel"/> floor (ADR 0005): <see cref="IsEnabled"/> is the single
/// pre-construction gate every caller must pass before building a <see cref="DiagnosticRecord"/>,
/// and <see cref="Log"/> is the only place the Sink is ever invoked. A Sink that throws is caught,
/// counted as <c>SinkFaulted</c> on the supplied <see cref="ReactorCounterSet"/>, and disabled for
/// the life of this gateway — a deliberate exception to bugs-always-throw, so the Frame that
/// provoked the fault still completes normally.
/// </summary>
public sealed class DiagnosticGateway
{
    private readonly IReactorDiagnostics? _sink;
    private readonly DiagnosticLevel _minimumLevel;
    private readonly ReactorCounterSet _counters;
    private bool _faulted;

    /// <summary>
    /// Constructs a gateway around <paramref name="sink"/> and <paramref name="minimumLevel"/>. Both
    /// are <see langword="null"/> together to disable diagnostics entirely — the pairing
    /// <see cref="ReactorOptions.Validate"/> enforces at construction.
    /// </summary>
    public DiagnosticGateway(IReactorDiagnostics? sink, DiagnosticLevel? minimumLevel, ReactorCounterSet counters)
    {
        ArgumentNullException.ThrowIfNull(counters);

        _sink = sink;
        _minimumLevel = minimumLevel ?? DiagnosticLevel.Error;
        _counters = counters;
    }

    /// <summary>
    /// Whether a record at <paramref name="level"/> would reach the Sink: a Sink is installed, has
    /// not faulted, <paramref name="level"/> is at least as severe as the configured minimum, and the
    /// Sink's own <see cref="IReactorDiagnostics.IsEnabled"/> agrees. With no Sink installed this
    /// costs one comparison and always answers <see langword="false"/>, so a disabled level never
    /// causes a <see cref="DiagnosticRecord"/> to be built.
    /// </summary>
    public bool IsEnabled(DiagnosticLevel level) =>
        _sink is not null && !_faulted && level >= _minimumLevel && _sink.IsEnabled(level);

    /// <summary>
    /// Forwards <paramref name="record"/> to the installed Sink. Callers are expected to have
    /// already checked <see cref="IsEnabled"/> for <paramref name="record"/>'s level; this method
    /// still no-ops safely if no Sink is installed or the Sink has already faulted. An exception
    /// thrown by the Sink is caught, counted as <c>SinkFaulted</c>, and the Sink is disabled for the
    /// life of this gateway.
    /// </summary>
    public void Log(in DiagnosticRecord record)
    {
        if (_sink is null || _faulted)
        {
            return;
        }

        try
        {
            _sink.Log(record);
        }
        catch
        {
            _faulted = true;
            _counters.RecordSinkFaulted();
        }
    }
}
