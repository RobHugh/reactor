namespace ReactorSockets;

/// <summary>
/// What <see cref="SessionEventEnumerator"/> drains from — the pending-event storage every Session
/// Reactor keeps for the Frame in progress. Internal: this is plumbing between a profile's Reactor and
/// the shared enumerator, never a surface a consumer implements. Both <see cref="DatagramSessionReactor"/>
/// and <see cref="StreamSessionReactor"/> implement it, which is what lets <see cref="SessionEvent"/> and
/// <see cref="SessionEventEnumerator"/> be genuinely shared (ADR 0010) rather than duplicated per profile.
/// </summary>
internal interface ISessionEventSource
{
    /// <summary>How many events this Frame has produced so far.</summary>
    int PendingEventCount { get; }

    /// <summary>Builds the event at <paramref name="index"/> from this Frame's pending-event storage.</summary>
    SessionEvent GetPendingEvent(int index);
}
