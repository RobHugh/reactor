namespace ReactorSockets;

/// <summary>
/// A point-in-time snapshot of the Reactor-scope counters (ADR 0013): monotonic figures running
/// from construction, with no reset. Fetched by value so nothing lets a consumer hold a pointer
/// into live state across a Pump; diffing two snapshots gives everything a reset would, from state
/// the consumer already owns.
/// </summary>
public readonly record struct ReactorCounters(
    ulong SinkFaulted,
    ulong DatagramsSent,
    ulong DatagramsReceived,
    ulong ArenaHighWaterBytes,
    ulong AdmissionDeferrals);
