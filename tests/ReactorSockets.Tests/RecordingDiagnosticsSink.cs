namespace ReactorSockets.Tests;

/// <summary>The recording Sink test double tests/CLAUDE.md's testing decisions call for: collects every record by value.</summary>
internal sealed class RecordingDiagnosticsSink : IReactorDiagnostics
{
    public List<DiagnosticRecord> Received { get; } = [];

    /// <summary>What this Sink answers to <see cref="IsEnabled"/>, independent of the Reactor's configured minimum level.</summary>
    public bool Enabled { get; init; } = true;

    public bool IsEnabled(DiagnosticLevel level) => Enabled;

    public void Log(in DiagnosticRecord record) => Received.Add(record);
}

/// <summary>
/// A Sink that calls back into the Reactor that is reporting to it — the re-entrancy ADR 0005
/// forbids. <see cref="Observed"/> captures what the Reactor threw at it, unless
/// <see cref="LetTheThrowEscape"/> is set, in which case the throw leaves <see cref="Log"/> and the
/// Sink is faulted for it like any other misbehaving Sink.
/// </summary>
internal sealed class ReentrantDiagnosticsSink : IReactorDiagnostics
{
    public required Action<RawReactor> ReentrantCall { get; init; }

    public bool LetTheThrowEscape { get; init; }

    public RawReactor? Reactor { get; set; }

    public Exception? Observed { get; private set; }

    public int CallCount { get; private set; }

    public bool IsEnabled(DiagnosticLevel level) => true;

    public void Log(in DiagnosticRecord record)
    {
        CallCount++;
        if (LetTheThrowEscape)
        {
            ReentrantCall(Reactor!);
            return;
        }

        try
        {
            ReentrantCall(Reactor!);
        }
        catch (Exception exception)
        {
            Observed = exception;
        }
    }
}

/// <summary>
/// A Sink that throws on every <see cref="Log"/> call, for the fault-and-disable rule (ADR 0005): the
/// Reactor must catch the throw, count it once, and never call this Sink again.
/// </summary>
internal sealed class ThrowingDiagnosticsSink : IReactorDiagnostics
{
    public int CallCount { get; private set; }

    public bool IsEnabled(DiagnosticLevel level) => true;

    public void Log(in DiagnosticRecord record)
    {
        CallCount++;
        throw new InvalidOperationException("Sink deliberately throws for the fault-and-disable test.");
    }
}
