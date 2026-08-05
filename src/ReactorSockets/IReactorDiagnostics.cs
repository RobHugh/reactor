namespace ReactorSockets;

/// <summary>
/// The Diagnostic Sink contract. Implementations are called synchronously, on the consumer's
/// thread, inside <c>Pump</c> — a Sink that stalls stalls the Frame, and a Sink that throws is
/// caught, counted as <c>SinkFaulted</c>, and disabled for the life of the Reactor (ADR 0005).
/// Calling back into the Reactor from a Sink is a programmer error and throws.
/// </summary>
public interface IReactorDiagnostics
{
    /// <summary>
    /// Gates construction of a <see cref="DiagnosticRecord"/> before it is built, so a disabled
    /// level costs a comparison and never reaches <see cref="Log"/>.
    /// </summary>
    bool IsEnabled(DiagnosticLevel level);

    /// <summary>Receives a record already known to satisfy <see cref="IsEnabled"/>.</summary>
    void Log(in DiagnosticRecord record);
}
