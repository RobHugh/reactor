namespace ReactorSockets.Core;

/// <summary>
/// The mutable Reactor-scope counter storage a Reactor holds for its own lifetime, incremented on
/// the hot path with a single add and read only through <see cref="Snapshot"/> (ADR 0013). There is
/// no reset: two readers sampling this independently — a metrics export and a debug overlay — must
/// both see a monotonic figure, or whichever resets first silently destroys the other's baseline.
/// </summary>
internal sealed class ReactorCounterSet
{
    private ulong _sinkFaulted;
    private ulong _datagramsSent;
    private ulong _datagramsReceived;
    private ulong _arenaHighWaterBytes;
    private ulong _admissionsDeferred;

    /// <summary>Records that the installed Sink threw and has been disabled for the life of the Reactor.</summary>
    public void RecordSinkFaulted() => _sinkFaulted++;

    /// <summary>Records that one datagram was sent.</summary>
    public void RecordDatagramSent() => _datagramsSent++;

    /// <summary>Records that one datagram was received.</summary>
    public void RecordDatagramReceived() => _datagramsReceived++;

    /// <summary>Raises the arena high-water mark if <paramref name="bytesUsed"/> exceeds it.</summary>
    public void ObserveArenaUsage(int bytesUsed)
    {
        ulong used = (ulong)bytesUsed;
        if (used > _arenaHighWaterBytes)
        {
            _arenaHighWaterBytes = used;
        }
    }

    /// <summary>Records that one admission (accept or handshake) was deferred past the admission cap.</summary>
    public void RecordAdmissionDeferred() => _admissionsDeferred++;

    /// <summary>Takes a point-in-time snapshot of every Reactor-scope counter.</summary>
    public ReactorCounters Snapshot() =>
        new(_sinkFaulted, _datagramsSent, _datagramsReceived, _arenaHighWaterBytes, _admissionsDeferred);
}
