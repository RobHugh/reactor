namespace ReactorSockets.Session;

/// <summary>
/// The classification a <see cref="ReceiveWindow"/> reports for one arriving sequence (#40, #170):
/// whether it is being seen for the first time, has already been seen, or falls outside the span the
/// window currently covers. The window only classifies — a caller such as #172/#173 is the one that
/// counts each outcome.
/// </summary>
internal enum ReceiveWindowOutcome
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
    /// The sequence lay outside the window: below <see cref="ReceiveWindow.Base"/> (already retired),
    /// or — for a window built with <c>slides: false</c> (#40, #170) — at or beyond the far edge
    /// (not yet reachable), in which case the window is unchanged and a far-ahead sequence never
    /// advances the base past data that has not arrived. A window built with <c>slides: true</c>
    /// (#173) never reports this for a far-ahead sequence: it slides instead, so for such a window
    /// this outcome means only "below the base".
    /// </summary>
    OutOfWindow,
}

/// <summary>
/// The receive window for one Space of one Peer (#40, #170, ADR 0009): a fixed span of sequence
/// numbers, starting at <see cref="Base"/>, over which it tracks what has already been admitted and
/// classifies each arriving sequence as new, a duplicate, or outside the span. Sharing one window
/// across Spaces would reintroduce, through shared memory, the head-of-line blocking that separate
/// Spaces exist to remove, so each Space owns one of these. Ordering is decided exclusively by
/// <see cref="SequenceNumber.IsNewerThan"/>, so the wrap-aware comparison exists in one place.
/// </summary>
/// <remarks>
/// <para>
/// The type is internal, so the consumer's surface stays as small as ADR 0003 leaves it — nothing
/// past <see cref="DatagramSessionReactor"/> and <see cref="DatagramSessionReactor.CountersOf"/> is a
/// permanent commitment. Wired into the data path by #172 and into loss counting by #173, both
/// reachable through that seam, with no <c>InternalsVisibleTo</c> and no interface invented for the
/// purpose (ADR 0015).
/// </para>
/// <para>
/// <b>Fixed at construction.</b> The window's size — in practice
/// <see cref="DatagramSessionOptions.ResolvedReceiveWindowFragments"/>, already validated against its
/// floor by <see cref="DatagramSessionOptions.Validate"/> — never changes after construction. Whether
/// the window slides is fixed at construction too (#173): a <c>slides: false</c> window treats a
/// sequence it cannot currently hold as nothing to resolve — <see cref="Classify"/> reports
/// <see cref="ReceiveWindowOutcome.OutOfWindow"/> and leaves <see cref="Base"/> exactly where it was,
/// so a far-ahead packet can never drag the base forward past a hole that has not been filled. A
/// <c>slides: true</c> window instead slides its base forward to make room, retiring whatever the
/// slide passes by un-admitted; see <see cref="LastSlideLoss"/>.
/// </para>
/// <para>
/// <b>No allocation of its own, ever (#179).</b> The only storage this type touches is the
/// <see cref="Memory{T}"/> run <see cref="Core.ReceiveArena.GetAdmissionRun"/> hands it at
/// construction — one byte per slot, zero for not-admitted and non-zero for admitted. <see cref="Classify"/>
/// only reads and writes that run and a handful of value-typed fields, so no sequence of arrivals ever
/// allocates. A slide walks at most <see cref="Capacity"/> slots — the same bound
/// <see cref="AdvanceBase"/> already has — so it stays allocation-free and its cost is bounded by
/// construction, not by how far ahead the arriving sequence is. Because the arena's storage outlives
/// any one Peer, a Slot reused by a later Peer must not inherit the previous occupant's admissions —
/// that is what <see cref="Reset"/> is for.
/// </para>
/// </remarks>
internal sealed class ReceiveWindow
{
    /// <summary>
    /// Whether the sequence at each slot has been admitted: zero for not-admitted, non-zero for
    /// admitted. Indexed by ring position, not by offset from <see cref="Base"/>:
    /// <see cref="_baseSlot"/> is the ring position <see cref="Base"/> currently occupies, and a slot
    /// is reused once the base advances past it. Backed by the receive arena (#179), not owned by this
    /// type.
    /// </summary>
    private readonly Memory<byte> _admitted;

    /// <summary>
    /// Whether this window slides its base forward to admit a far-ahead arrival instead of refusing
    /// it (#173). Fixed at construction; see <see cref="ReceiveWindow(Memory{byte}, bool)"/>.
    /// </summary>
    private readonly bool _slides;

    /// <summary>The sequence at the window's lower edge. See <see cref="Base"/>.</summary>
    private uint _base;

    /// <summary>The ring position in <see cref="_admitted"/> that <see cref="_base"/> currently occupies.</summary>
    private int _baseSlot;

    /// <summary>
    /// Builds a window over <paramref name="admitted"/>'s slots, based at sequence zero — where every
    /// Space's numbering begins (ADR 0009). Allocates nothing: <paramref name="admitted"/> is the
    /// window's entire storage, on loan from the receive arena for the calling Peer's lifetime.
    /// </summary>
    /// <param name="admitted">
    /// The admission run this window classifies into, one byte per sequence slot — see
    /// <see cref="Core.ReceiveArena.GetAdmissionRun"/>. <see cref="Capacity"/> is
    /// <c><paramref name="admitted"/>.Length</c>. Must not be empty.
    /// </param>
    /// <param name="slides">
    /// Whether a sequence at or beyond the window's far edge slides the base forward to admit it,
    /// rather than being refused (#173). Defaults to <see langword="false"/> — today's behaviour for
    /// every window built before this parameter existed, and the one <c>ReliableOrdered</c> and
    /// <c>ReliableUnordered</c> keep: retransmission fills their holes, so sliding would retire data
    /// that was about to arrive. <c>Unreliable</c> and <c>UnreliableSequenced</c> pass
    /// <see langword="true"/>, because nothing retransmits into them and a window that never advances
    /// past a hole would stall forever after the first real loss.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="admitted"/> is empty.</exception>
    internal ReceiveWindow(Memory<byte> admitted, bool slides = false)
    {
        if (admitted.Length < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(admitted),
                admitted.Length,
                $"a receive window of {admitted.Length} slots cannot hold anything — it must be at least 1.");
        }

        Capacity = admitted.Length;
        _admitted = admitted;
        _base = 0;
        _baseSlot = 0;
        _slides = slides;
    }

    /// <summary>
    /// The sequence at the window's lower edge: the oldest sequence this Space has not yet admitted.
    /// Every sequence below this has already been admitted or retired; every sequence from here up to
    /// but not including <c>Base + Capacity</c> lies within the window's current span. Starts at zero
    /// and only ever advances, by exactly one sequence per already-admitted slot <see cref="Classify"/>
    /// passes over.
    /// </summary>
    internal uint Base => _base;

    /// <summary>The number of sequence slots the window spans, fixed at construction.</summary>
    internal int Capacity { get; }

    /// <summary>
    /// How many sequences the most recent <see cref="Classify"/> call retired un-admitted by sliding
    /// the base forward (#173) — zero unless that call both slid the window and this window was built
    /// with <c>slides: true</c>. Read after <see cref="Classify"/> returns; a caller that wants to
    /// raise a loss figure by the right amount reads this once per call rather than assuming a slide
    /// only ever loses one sequence, since a jump can retire several holes at once. Reset to zero at
    /// the start of every <see cref="Classify"/> call, so a call that does not slide reports zero even
    /// if an earlier call slid.
    /// </summary>
    internal ulong LastSlideLoss { get; private set; }

    /// <summary>
    /// Classifies <paramref name="sequence"/> against the window's current span. When it is new and
    /// within the span, it is admitted: recorded, and then <see cref="Base"/> is advanced across it
    /// and every already-admitted sequence contiguous above it, stopping at the first hole. For a
    /// window built with <c>slides: true</c> (#173), a sequence at or beyond the far edge instead
    /// slides the base forward until the sequence fits, retiring whatever the slide passes by
    /// un-admitted — see <see cref="LastSlideLoss"/> — and then admits it. Nothing else changes the
    /// window's state.
    /// </summary>
    /// <param name="sequence">The arriving record's <c>u32</c> sequence number.</param>
    /// <returns>
    /// <see cref="ReceiveWindowOutcome.Admitted"/> the first time a sequence inside the window is
    /// seen, including one admitted by a slide; <see cref="ReceiveWindowOutcome.Duplicate"/> if it was
    /// already recorded; <see cref="ReceiveWindowOutcome.OutOfWindow"/> if it lies below
    /// <see cref="Base"/>, or — only for a window built with <c>slides: false</c> — at or beyond the
    /// window's far edge.
    /// </returns>
    internal ReceiveWindowOutcome Classify(uint sequence)
    {
        LastSlideLoss = 0;

        bool atOrAboveBase = sequence == _base || SequenceNumber.IsNewerThan(sequence, _base);
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
            if (!_slides)
            {
                // At or beyond the far edge: not yet reachable. The base stays exactly where it was,
                // so a far-ahead packet can never drag it forward past data that has not arrived.
                return ReceiveWindowOutcome.OutOfWindow;
            }

            SlideTo(sequence);
            offset = unchecked(sequence - _base);
        }

        int slot = (int)((_baseSlot + offset) % (uint)Capacity);
        Span<byte> admitted = _admitted.Span;
        if (admitted[slot] != 0)
        {
            return ReceiveWindowOutcome.Duplicate;
        }

        admitted[slot] = 1;
        AdvanceBase();
        return ReceiveWindowOutcome.Admitted;
    }

    /// <summary>
    /// Describes this window's current state as an ack block (issue #183, ADR 0009): the cumulative
    /// base it has reached, plus a 32-bit field describing the 32 sequences above that base — bit 0
    /// for the first sequence above the base (<c>Base + 1</c>), up to bit 31 for the thirty-second
    /// (<c>Base + 32</c>). A sequence more than 32 above the base has no bit to hold it and is simply
    /// not described; the horizon rule that governs what a caller does with that fact (issue #41) is
    /// not this method's concern.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A pure read: it classifies nothing, admits nothing, and moves no base. It is generated fresh
    /// from <see cref="_admitted"/>, <see cref="_base"/> and <see cref="_baseSlot"/> on every call
    /// rather than accumulated, so a window that has just retired a hole through
    /// <see cref="AdvanceBase"/> describes the new base and the bitfield that goes with it, never the
    /// state from before that call.
    /// </para>
    /// <para>
    /// Wrap correctness is inherited, not reinvented: <see cref="_base"/> and <see cref="_baseSlot"/>
    /// already track the window correctly across the <c>u32</c> wrap — that is what
    /// <see cref="SequenceNumber.IsNewerThan"/> inside <see cref="Classify"/> establishes — so this
    /// method only ever walks ring positions relative to <see cref="_baseSlot"/> and never compares
    /// sequence numbers itself.
    /// </para>
    /// <para>
    /// Allocation-free: the returned tuple is a value type, and the only storage touched is the
    /// <see cref="_admitted"/> span already backing this window.
    /// </para>
    /// </remarks>
    /// <returns>
    /// <see cref="Base"/> is a window's current base sequence, and a bitfield whose bit <c>n</c>
    /// (0-based) is set exactly when <c>Base + n + 1</c> has been admitted. Bits beyond
    /// <see cref="Capacity"/><c> - 1</c> sequences above the base are always clear, since the window
    /// itself cannot hold anything that far ahead.
    /// </returns>
    internal (uint Base, uint Bits) DescribeAsAckBlock()
    {
        Span<byte> admitted = _admitted.Span;
        int reachable = Capacity - 1 < 32 ? Capacity - 1 : 32;

        uint bits = 0;
        for (int offset = 1; offset <= reachable; offset++)
        {
            int slot = _baseSlot + offset;
            slot = slot >= Capacity ? slot - Capacity : slot;
            if (admitted[slot] != 0)
            {
                bits |= 1u << (offset - 1);
            }
        }

        return (_base, bits);
    }

    /// <summary>
    /// Clears this window's admission run, and resets <see cref="Base"/>, its ring position and
    /// <see cref="LastSlideLoss"/> to their construction-time values. The arena storage a window is
    /// built over outlives the Peer that used it (#179): a Slot a new Peer reuses must classify from a
    /// clean window, not inherit whatever the previous occupant had admitted. Mirrors the division
    /// <see cref="Core.ReceiveArena.GetFragmentSlot"/> already draws — the arena keeps the bytes, the
    /// occupant clears them.
    /// </summary>
    internal void Reset()
    {
        _admitted.Span.Clear();
        _base = 0;
        _baseSlot = 0;
        LastSlideLoss = 0;
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
        Span<byte> admitted = _admitted.Span;
        while (admitted[_baseSlot] != 0)
        {
            admitted[_baseSlot] = 0;
            _baseSlot = _baseSlot + 1 == Capacity ? 0 : _baseSlot + 1;
            _base = unchecked(_base + 1);
        }
    }

    /// <summary>
    /// Slides the base forward until <paramref name="sequence"/> sits exactly at the window's new far
    /// edge, called only for a <c>slides: true</c> window when <paramref name="sequence"/> arrived at
    /// or beyond the current far edge. Every slot the slide passes by that was not admitted is one
    /// lost sequence, tallied into <see cref="LastSlideLoss"/>; a slot that was admitted is data
    /// already received, so it is cleared and reused rather than counted as loss. The walk over
    /// <see cref="_admitted"/> is bounded at <see cref="Capacity"/> slots regardless of how far ahead
    /// <paramref name="sequence"/> is — a jump wider than the window retires the whole ring in one
    /// pass and folds every sequence beyond it straight into the loss tally, since those were never
    /// tracked by a slot at all.
    /// </summary>
    private void SlideTo(uint sequence)
    {
        uint newBase = unchecked(sequence - (uint)Capacity + 1);
        uint retiredCount = unchecked(newBase - _base);
        int slotsToWalk = retiredCount < (uint)Capacity ? (int)retiredCount : Capacity;

        Span<byte> admitted = _admitted.Span;
        ulong lost = 0;
        for (int i = 0; i < slotsToWalk; i++)
        {
            if (admitted[_baseSlot] != 0)
            {
                admitted[_baseSlot] = 0;
            }
            else
            {
                lost++;
            }

            _baseSlot = _baseSlot + 1 == Capacity ? 0 : _baseSlot + 1;
        }

        if (retiredCount > (uint)Capacity)
        {
            lost += retiredCount - (uint)Capacity;
        }

        _base = newBase;
        LastSlideLoss = lost;
    }
}
