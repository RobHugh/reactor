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

        // Not `Reactor!`: the Sink has to exist before the Reactor it calls back into, so this
        // genuinely can be null if a test forgets to set it. Saying so beats a NullReferenceException
        // surfacing as a faulted Sink, which is exactly what these tests are trying to distinguish.
        RawReactor reactor = Reactor
            ?? throw new InvalidOperationException($"{nameof(Reactor)} was never set on this Sink — assign it after constructing the Reactor.");

        if (LetTheThrowEscape)
        {
            ReentrantCall(reactor);
            return;
        }

        try
        {
            ReentrantCall(reactor);
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
