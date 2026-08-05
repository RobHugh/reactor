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
    private ulong _admissionDeferrals;

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

    /// <summary>
    /// Records one deferral: a Link that had somewhere to admit an inbound connection from and
    /// declined to, because either the admission cap or capacity was spent. It counts deferring
    /// Links per Frame, not connections left in the backlog — a Reactor cannot know that depth
    /// without accepting, which is the thing a deferral consists of not doing. The two causes share
    /// this counter and are told apart by <see cref="DiagnosticId.AdmissionCapReached"/> versus
    /// <see cref="DiagnosticId.CapacityExhausted"/>.
    /// </summary>
    public void RecordAdmissionDeferral() => _admissionDeferrals++;

    /// <summary>Takes a point-in-time snapshot of every Reactor-scope counter.</summary>
    public ReactorCounters Snapshot() =>
        new(_sinkFaulted, _datagramsSent, _datagramsReceived, _arenaHighWaterBytes, _admissionDeferrals);
}
