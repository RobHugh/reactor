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
