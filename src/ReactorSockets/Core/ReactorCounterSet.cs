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
    private ulong _admissionDiscards;
    private ulong _unknownSourceDatagrams;
    private ulong _malformedSessionDatagrams;

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
    /// Records one deferral: an Admission declined because the admission cap or capacity was spent,
    /// where the work stays put for a later Pump. One call is one declining, not one piece of work
    /// turned away — a listening Link or bound endpoint declines its whole backlog with one call,
    /// because that depth is unknowable without accepting, which is the thing a deferral consists of
    /// not doing. Where the Reactor already holds the work the two coincide and the figure is exact.
    /// Work the Reactor cannot leave waiting is a Discard, not a deferral: see
    /// <see cref="RecordAdmissionDiscard"/>. The two causes share this counter and are told apart by
    /// <see cref="DiagnosticId.AdmissionCapReached"/> versus <see cref="DiagnosticId.CapacityExhausted"/>.
    /// </summary>
    public void RecordAdmissionDeferral() => _admissionDeferrals++;

    /// <summary>
    /// Records one Discard: one piece of work the Reactor was holding and let go, because the
    /// admission cap, capacity or a per-address limit was spent and the transport gives it nowhere to wait. Always exact,
    /// unlike <see cref="RecordAdmissionDeferral"/> — a Reactor can only Discard what it had in
    /// hand. Called only where declined work does not survive to a later Pump: a datagram Session
    /// dropping a handshake datagram it cannot admit.
    /// </summary>
    public void RecordAdmissionDiscard() => _admissionDiscards++;

    /// <summary>
    /// Records one datagram taken from the transport and thrown away because nothing in it could be
    /// attributed to a Peer — a malformed control datagram, a session id naming no Session, or a
    /// well-formed control reply matching no live dial. A data datagram whose session id did name a
    /// live Session is <see cref="RecordMalformedSessionDatagram"/> instead. Kept apart from
    /// <see cref="RecordAdmissionDiscard"/> because this is not a declined Admission: nothing was
    /// turned away, the bytes simply meant nothing. Budget may still have been spent getting here —
    /// a Hello-with-cookie is charged an Admission before its cookie is verified, precisely so that
    /// the verifying work is bounded — but that unit bought an answer to a question, not a refusal.
    /// </summary>
    public void RecordUnknownSourceDatagram() => _unknownSourceDatagrams++;

    /// <summary>
    /// Records one data datagram thrown away because it failed to parse after its session id had
    /// already resolved to a live Session. Split from <see cref="RecordUnknownSourceDatagram"/> so the
    /// two causes stay distinguishable to someone reading counters in the field: that figure is
    /// traffic that named nobody, this is traffic that named somebody and then said nothing readable.
    /// Reactor-scope rather than charged to the named Peer, because a datagram that did not parse is
    /// no proof of who sent it.
    /// </summary>
    public void RecordMalformedSessionDatagram() => _malformedSessionDatagrams++;

    /// <summary>Takes a point-in-time snapshot of every Reactor-scope counter.</summary>
    public ReactorCounters Snapshot() =>
        new(
            _sinkFaulted,
            _datagramsSent,
            _datagramsReceived,
            _arenaHighWaterBytes,
            _admissionDeferrals,
            _admissionDiscards,
            _unknownSourceDatagrams,
            _malformedSessionDatagrams);
}
