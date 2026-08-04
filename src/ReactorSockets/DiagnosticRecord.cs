namespace ReactorSockets;

/// <summary>
/// A single diagnostic emitted to an <see cref="IReactorDiagnostics"/> Sink.
/// This shared value type establishes the Sink contract's shape; the full roster of
/// fields — the <c>DiagnosticId</c> enum, the level, and the tagged subject ADR 0013
/// describes — is populated by the diagnostics facade.
/// </summary>
public readonly record struct DiagnosticRecord;
