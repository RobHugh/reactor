namespace ReactorSockets.Tests;

/// <summary>The recording Sink test double tests/CLAUDE.md's testing decisions call for: collects every record by value.</summary>
internal sealed class RecordingDiagnosticsSink : IReactorDiagnostics
{
    public List<DiagnosticRecord> Received { get; } = [];

    public bool IsEnabled(DiagnosticLevel level) => true;

    public void Log(in DiagnosticRecord record) => Received.Add(record);
}
