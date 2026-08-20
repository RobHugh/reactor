namespace ReactorSockets;

/// <summary>
/// The classification a <see cref="ReceiveWindow"/> reports for one arriving sequence (#40, #170):
/// whether it is being seen for the first time, has already been seen, or falls outside the span the
/// window currently covers. The window only classifies — a caller such as #172/#173 is the one that
/// counts each outcome.
/// </summary>
public enum ReceiveWindowOutcome
{
    /// <summary>
    /// The sequence lay within the window and had not been recorded before. It is now recorded, and
    /// <see cref="ReceiveWindow.Base"/> has advanced past it and every already-admitted sequence
    /// contiguous above it.
    /// </summary>
    Admitted,

    /// <summary>
    /// The sequence lay within the window but was already recorded — a retransmission of a sequence
    /// this Space has already admitted. The window is unchanged.
    /// </summary>
    Duplicate,

    /// <summary>
    /// The sequence lay outside the window: below <see cref="ReceiveWindow.Base"/> (already retired)
    /// or at or beyond the window's far edge (not yet reachable). The window is unchanged; in
    /// particular a far-ahead sequence never advances the base past data that has not arrived.
    /// </summary>
    OutOfWindow,
}

/// <summary>
/// The receive window for one Space of one Peer (#40, #170, ADR 0009): a fixed span of sequence
/// numbers, starting at <see cref="Base"/>, over which it tracks what has already been admitted and
/// classifies each arriving sequence as new, a duplicate, or outside the span. Sharing one window
/// across Spaces would reintroduce, through shared memory, the head-of-line blocking that separate
/// Spaces exist to remove, so each Space owns one of these. Ordering is decided exclusively by
/// <see cref="Session.SequenceNumber.IsNewerThan"/>, so the wrap-aware comparison exists in one place.
/// </summary>
/// <remarks>
/// <para>
/// Public precisely so this behaviour can be pinned by constructing one directly and calling
/// <see cref="Classify"/>, with no <c>InternalsVisibleTo</c> and no interface invented for the
/// purpose — the same reason <see cref="PumpIntervalObserver"/> is public (ADR 0015). This ticket
/// (#170) builds the structure alone; nothing in this slice calls it yet, so there is no seam through
/// a Reactor to observe it through. Wiring it to the data path is #172, and preallocating the two
/// reliable Spaces' windows in one arena is #171.
/// </para>
/// <para>
/// <b>Fixed at construction.</b> The window's size — in practice
/// <see cref="DatagramSessionOptions.ResolvedReceiveWindowFragments"/>, already validated against its
/// floor by <see cref="DatagramSessionOptions.Validate"/> — never changes after construction. A
/// sequence the window cannot currently hold is not an error it resolves: <see cref="Classify"/>
/// reports <see cref="ReceiveWindowOutcome.OutOfWindow"/> and leaves <see cref="Base"/> exactly where
/// it was, so a far-ahead packet can never drag the base forward past a hole that has not been
/// filled.
/// </para>
/// <para>
/// <b>No allocation after construction.</b> The only storage this type owns is the <c>bool[]</c>
/// backing array allocated once in the constructor. <see cref="Classify"/> only reads and writes that
/// array and a handful of value-typed fields, so no sequence of arrivals ever allocates.
/// </para>
/// </remarks>
public sealed class ReceiveWindow
{
    /// <summary>
    /// Whether the sequence at each slot has been admitted. Indexed by ring position, not by offset
    /// from <see cref="Base"/>: <see cref="_baseSlot"/> is the ring position <see cref="Base"/>
    /// currently occupies, and a slot is reused once the base advances past it.
    /// </summary>
    private readonly bool[] _admitted;

    /// <summary>The sequence at the window's lower edge. See <see cref="Base"/>.</summary>
    private uint _base;

    /// <summary>The ring position in <see cref="_admitted"/> that <see cref="_base"/> currently occupies.</summary>
    private int _baseSlot;

    /// <summary>
    /// Builds a window of <paramref name="capacityFragments"/> sequence slots, based at
    /// <paramref name="initialBase"/>. Allocates its backing storage once; nothing after this
    /// allocates.
    /// </summary>
    /// <param name="capacityFragments">
    /// The number of sequence numbers the window spans, fixed for the window's lifetime. Must be at
    /// least 1. In practice this is
    /// <see cref="DatagramSessionOptions.ResolvedReceiveWindowFragments"/>, already validated against
    /// its floor by <see cref="DatagramSessionOptions.Validate"/>; this constructor re-validates
    /// rather than trusting the caller, because a window is cheap to misuse from a test or a future
    /// caller that never went through those options.
    /// </param>
    /// <param name="initialBase">
    /// The sequence <see cref="Base"/> starts at. Defaults to 0 — where every Space's sequence
    /// numbering begins (ADR 0009) — but is a constructor parameter rather than a hardcoded zero so a
    /// window can be built already positioned anywhere in the sequence space, including a test that
    /// places it at the <c>u32</c> wrap boundary without walking every sequence up to it.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacityFragments"/> is less than 1.</exception>
    public ReceiveWindow(int capacityFragments, uint initialBase = 0)
    {
        if (capacityFragments < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(capacityFragments),
                capacityFragments,
                $"a receive window of {capacityFragments} slots cannot hold anything — it must be at least 1.");
        }

        Capacity = capacityFragments;
        _admitted = new bool[capacityFragments];
        _base = initialBase;
        _baseSlot = 0;
    }

    /// <summary>
    /// The sequence at the window's lower edge: the oldest sequence this Space has not yet admitted.
    /// Every sequence below this has already been admitted or retired; every sequence from here up to
    /// but not including <c>Base + Capacity</c> lies within the window's current span. Starts at zero
    /// and only ever advances, by exactly one sequence per already-admitted slot <see cref="Classify"/>
    /// passes over.
    /// </summary>
    public uint Base => _base;

    /// <summary>The number of sequence slots the window spans, fixed at construction.</summary>
    public int Capacity { get; }

    /// <summary>
    /// Classifies <paramref name="sequence"/> against the window's current span. When it is new and
    /// within the span, it is admitted: recorded, and then <see cref="Base"/> is advanced across it
    /// and every already-admitted sequence contiguous above it, stopping at the first hole. Nothing
    /// else changes the window's state.
    /// </summary>
    /// <param name="sequence">The arriving record's <c>u32</c> sequence number.</param>
    /// <returns>
    /// <see cref="ReceiveWindowOutcome.Admitted"/> the first time a sequence inside the window is
    /// seen; <see cref="ReceiveWindowOutcome.Duplicate"/> if it was already recorded;
    /// <see cref="ReceiveWindowOutcome.OutOfWindow"/> if it lies below <see cref="Base"/> or at or
    /// beyond the window's far edge.
    /// </returns>
    public ReceiveWindowOutcome Classify(uint sequence)
    {
        bool atOrAboveBase = sequence == _base || Session.SequenceNumber.IsNewerThan(sequence, _base);
        if (!atOrAboveBase)
        {
            // Below the base: already admitted or already retired. Never a duplicate report — a
            // duplicate is a sequence still tracked inside the window, not one the base has already
            // passed.
            return ReceiveWindowOutcome.OutOfWindow;
        }

        // Unsigned subtraction wraps exactly the way the sequence space does. Combined with the
        // ordering check above — which already establishes sequence is at or after base within the
        // half-space IsNewerThan resolves correctly — this recovers the true forward distance even
        // across the u32 wrap.
        uint offset = unchecked(sequence - _base);
        if (offset >= (uint)Capacity)
        {
            // At or beyond the far edge: not yet reachable. The base stays exactly where it was, so a
            // far-ahead packet can never drag it forward past data that has not arrived.
            return ReceiveWindowOutcome.OutOfWindow;
        }

        int slot = (int)((_baseSlot + offset) % (uint)Capacity);
        if (_admitted[slot])
        {
            return ReceiveWindowOutcome.Duplicate;
        }

        _admitted[slot] = true;
        AdvanceBase();
        return ReceiveWindowOutcome.Admitted;
    }

    /// <summary>
    /// Advances <see cref="_base"/> one slot at a time while the slot it currently occupies is
    /// admitted, clearing each slot as it passes so the ring can reuse it once the base wraps back
    /// around. Stops at the first hole — a slot not yet admitted — which is exactly where the next
    /// arrival still needs to land. A single admitted arrival that fills the last hole below a run of
    /// earlier arrivals therefore advances the base across the whole run in one call.
    /// </summary>
    private void AdvanceBase()
    {
        while (_admitted[_baseSlot])
        {
            _admitted[_baseSlot] = false;
            _baseSlot = _baseSlot + 1 == Capacity ? 0 : _baseSlot + 1;
            _base = unchecked(_base + 1);
        }
    }
}
