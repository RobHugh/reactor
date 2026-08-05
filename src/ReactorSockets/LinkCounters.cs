namespace ReactorSockets;

/// <summary>
/// A point-in-time snapshot of one Link's counters (ADR 0013): monotonic figures running from that
/// Link's registration, with no reset, fetched by value and keyed by <see cref="LinkId"/> — there is
/// no accessor that enumerates every Link's counters in one call.
/// </summary>
public readonly record struct LinkCounters(
    ulong BytesSent,
    ulong BytesReceived,
    ulong ReadShareExhaustions);
