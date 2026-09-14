using System.Buffers.Binary;

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
/// sequence it cannot currently hold as nothing to resolve — <see cref="Classify(uint)"/> reports
/// <see cref="ReceiveWindowOutcome.OutOfWindow"/> and leaves <see cref="Base"/> exactly where it was,
/// so a far-ahead packet can never drag the base forward past a hole that has not been filled. A
/// <c>slides: true</c> window instead slides its base forward to make room, retiring whatever the
/// slide passes by un-admitted; see <see cref="LastSlideLoss"/>.
/// </para>
/// <para>
/// <b>No allocation of its own, ever (#179).</b> The only storage this type touches is the
/// <see cref="Memory{T}"/> run <see cref="Core.ReceiveArena.GetAdmissionRun"/> hands it at
/// construction — one byte per slot, zero for not-admitted and non-zero for admitted — plus, for a
/// window built over a retaining Space, the length run <see cref="Core.ReceiveArena.GetFragmentLengthRun"/>
/// hands it (#257). <see cref="Classify(uint)"/> only reads and writes those runs and a handful of
/// value-typed fields, so no sequence of arrivals ever allocates. A slide walks at most
/// <see cref="Capacity"/> slots — the same bound <see cref="AdvanceBase"/> already has — so it stays
/// allocation-free and its cost is bounded by construction, not by how far ahead the arriving sequence
/// is. Because the arena's storage outlives any one Peer, a Slot reused by a later Peer must not
/// inherit the previous occupant's admissions — that is what <see cref="Reset"/> is for.
/// </para>
/// <para>
/// <b>This window is the reassembly storage for a Fragment (issue #257, ADR 0004).</b> A separate
/// reassembly buffer is refused by design, not merely left unbuilt: it would be new attack surface
/// rather than a defence against one, since a sender flooding partial Fragment sets would fill memory
/// this Reactor took on their behalf instead of the receive window it already bounds and already
/// polices. Holding a Fragment's payload in the same slot its own admission already occupies means a
/// hostile peer can never hold more Fragments than an honest one already could — the window's own
/// capacity is the only bound either ever meets. <see cref="RecordFragmentLength"/> and
/// <see cref="GetFragmentLength"/> are the narrow surface that makes this possible: a slot's admission
/// byte is cleared the instant <see cref="AdvanceBase"/> passes it, so it cannot by itself say whether
/// a slot still holds an unfinished Fragment — the length run is what a caller reads instead, and it
/// stays whatever it was last set to until the slot is reused or the Fragment set completes (#258).
/// </para>
/// <para>
/// <b>No expiry timer for an incomplete group, ever (issue #261).</b> A held Fragment's length entry is
/// cleared only by <see cref="ReleaseGroup"/> — its own group completing — or by a fresh admission
/// reusing the same physical ring slot for an unrelated, later sequence (see the admission path in
/// <see cref="Classify(uint, out int)"/>). No clock, per-group timestamp, or expiry constant exists to
/// clear it any other way, because none is needed: the window's own <see cref="Capacity"/> already
/// bounds how many groups a sender flooding partial sets can hold open at once — the same bound an
/// honest sender is already held to — so a timer would defend against nothing the bound does not
/// already defend against, at the cost of a duration nobody could size honestly (too short discards a
/// genuinely slow Message; too long defends nothing a hostile flood could not already do faster by
/// simply filling the window). A group that never completes therefore occupies its slots for exactly as
/// long as the window's ordinary rules leave them occupied — until the ring wraps back around to them
/// for a later sequence, or the Peer's Session ends — the same lifetime as any other admitted-but-not-
/// yet-retired sequence in this window, with no separate accounting of its own.
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
    /// The Fragment-length run for this window's Space, two bytes per slot, ring-indexed exactly as
    /// <see cref="_admitted"/> is — <see cref="Core.ReceiveArena.GetFragmentLengthRun"/> (#257). Empty
    /// for a window built over a Space that never retains a Fragment (today, the two unreliable
    /// Spaces, which ADR 0004 never routes a Fragment into): <see cref="RecordFragmentLength"/> and
    /// <see cref="GetFragmentLength"/> both throw for such a window rather than silently accepting a
    /// call that could never be meaningful.
    /// </summary>
    private readonly Memory<byte> _fragmentLengths;

    /// <summary>
    /// The Fragment-group run for this window's Space, six bytes per slot, ring-indexed exactly as
    /// <see cref="_fragmentLengths"/> is — <see cref="Core.ReceiveArena.GetFragmentGroupRun"/> (issue
    /// #297): a <c>u32</c> first sequence and a <c>u16</c> declared Fragment count naming the group the
    /// slot's held Fragment belongs to. Empty exactly when <see cref="_fragmentLengths"/> is, for the
    /// same reason: a Space that never retains a Fragment never needs to say which set one belonged to.
    /// This is what lets <see cref="Classify(uint, out int)"/> answer how much of an abandoned set was
    /// held and how much it declared at the moment its slot is reused — the length region alone says a
    /// piece was held, not which set it was a piece of or how large that set claimed to be.
    /// </summary>
    private readonly Memory<byte> _fragmentGroups;

    /// <summary>
    /// Whether this window slides its base forward to admit a far-ahead arrival instead of refusing
    /// it (#173). Fixed at construction; see
    /// <see cref="ReceiveWindow(Memory{byte}, bool, Memory{byte}, Memory{byte})"/>.
    /// </summary>
    private readonly bool _slides;

    /// <summary>The ring position in <see cref="_admitted"/> that <see cref="Base"/> currently occupies.</summary>
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
    /// <param name="fragmentLengths">
    /// The Fragment-length run this window records a held Fragment's byte length into, two bytes per
    /// sequence slot — see <see cref="Core.ReceiveArena.GetFragmentLengthRun"/>. Defaults to
    /// <see langword="default"/> (empty) for a window built over a Space that never retains a Fragment
    /// (ADR 0004); <see cref="RecordFragmentLength"/> and <see cref="GetFragmentLength"/> throw for
    /// such a window. When supplied, its length must be exactly twice <paramref name="admitted"/>'s.
    /// </param>
    /// <param name="fragmentGroups">
    /// The Fragment-group run this window records a held Fragment's group identity into, six bytes per
    /// sequence slot — see <see cref="Core.ReceiveArena.GetFragmentGroupRun"/> (issue #297). Defaults to
    /// <see langword="default"/> (empty) for a window built over a Space that never retains a Fragment
    /// (ADR 0004), the same condition <paramref name="fragmentLengths"/> is empty under. When supplied,
    /// its length must be exactly six times <paramref name="admitted"/>'s.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="admitted"/> is empty, <paramref name="fragmentLengths"/> is non-empty and its
    /// length is not exactly twice <paramref name="admitted"/>'s, or <paramref name="fragmentGroups"/> is
    /// non-empty and its length is not exactly six times <paramref name="admitted"/>'s.
    /// </exception>
    internal ReceiveWindow(
        Memory<byte> admitted, bool slides = false, Memory<byte> fragmentLengths = default, Memory<byte> fragmentGroups = default)
    {
        if (admitted.Length < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(admitted),
                admitted.Length,
                $"a receive window of {admitted.Length} slots cannot hold anything — it must be at least 1.");
        }

        if (!fragmentLengths.IsEmpty && fragmentLengths.Length != admitted.Length * 2)
        {
            throw new ArgumentOutOfRangeException(
                nameof(fragmentLengths),
                fragmentLengths.Length,
                $"a Fragment-length run of {fragmentLengths.Length} bytes does not match {admitted.Length} " +
                $"admission slots at 2 bytes each ({admitted.Length * 2}).");
        }

        if (!fragmentGroups.IsEmpty && fragmentGroups.Length != admitted.Length * 6)
        {
            throw new ArgumentOutOfRangeException(
                nameof(fragmentGroups),
                fragmentGroups.Length,
                $"a Fragment-group run of {fragmentGroups.Length} bytes does not match {admitted.Length} " +
                $"admission slots at 6 bytes each ({admitted.Length * 6}).");
        }

        Capacity = admitted.Length;
        _admitted = admitted;
        _fragmentLengths = fragmentLengths;
        _fragmentGroups = fragmentGroups;
        Base = 0;
        _baseSlot = 0;
        _slides = slides;
    }

    /// <summary>
    /// The sequence at the window's lower edge: the oldest sequence this Space has not yet admitted.
    /// Every sequence below this has already been admitted or retired; every sequence from here up to
    /// but not including <c>Base + Capacity</c> lies within the window's current span. Starts at zero
    /// and only ever advances, by exactly one sequence per already-admitted slot <see cref="Classify(uint)"/>
    /// passes over.
    /// </summary>
    internal uint Base { get; private set; }

    /// <summary>The number of sequence slots the window spans, fixed at construction.</summary>
    internal int Capacity { get; }

    /// <summary>
    /// How many sequences the most recent <see cref="Classify(uint)"/> call retired un-admitted by sliding
    /// the base forward (#173) — zero unless that call both slid the window and this window was built
    /// with <c>slides: true</c>. Read after <see cref="Classify(uint)"/> returns; a caller that wants to
    /// raise a loss figure by the right amount reads this once per call rather than assuming a slide
    /// only ever loses one sequence, since a jump can retire several holes at once. Reset to zero at
    /// the start of every <see cref="Classify(uint)"/> call, so a call that does not slide reports zero even
    /// if an earlier call slid.
    /// </summary>
    internal ulong LastSlideLoss { get; private set; }

    /// <summary>
    /// Whether the most recent <see cref="Classify(uint, out int)"/> call discovered an incomplete
    /// Fragment set abandoned (issue #297) — a fresh admission clobbered a physical ring slot that still
    /// carried a live, never-completed occupant's Fragment-length residue — and if so, how many of that
    /// set's sequences were actually held at that instant (<c>Held</c>) versus how many it declared
    /// (<c>Expected</c>). <c>Occurred</c> is <see langword="false"/> and <c>Held</c>/<c>Expected</c> are
    /// both zero unless that call's admission clobbered such residue. Reset to that default at the start
    /// of every <see cref="Classify(uint, out int)"/> call, so a call that abandons nothing reports it
    /// even if an earlier call did — mirrors <see cref="LastSlideLoss"/>, and for the same reason: this
    /// window has no <see cref="PeerId"/> to subject a Diagnostic to, so
    /// <see cref="DatagramSessionReactor.ClassifyRecord"/> reads this and logs
    /// <see cref="DiagnosticRecord.ForFragmentSetAbandoned"/> from it instead.
    /// </summary>
    internal (bool Occurred, long Held, long Expected) LastAbandonedFragmentSet { get; private set; }

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
    internal ReceiveWindowOutcome Classify(uint sequence) => Classify(sequence, out _);

    /// <summary>
    /// <see cref="Classify(uint)"/>, additionally reporting the ring slot <paramref name="sequence"/>
    /// resolves to — the same slot a caller holding a Fragment's bytes passes to
    /// <see cref="RecordFragmentLength"/> and a <see cref="Core.ReceiveArena.GetFragmentSlot"/> call
    /// (#257). Keeping the ring arithmetic here, rather than letting a caller re-derive it from
    /// <see cref="Base"/>, matters because <see cref="Classify(uint)"/> itself moves <see cref="Base"/>
    /// before returning: a caller cannot recompute the slot afterward from the window's new state.
    /// </summary>
    /// <param name="sequence">The arriving record's <c>u32</c> sequence number.</param>
    /// <param name="ringSlot">
    /// The ring position <paramref name="sequence"/> occupies, valid whenever the return value is
    /// <see cref="ReceiveWindowOutcome.Admitted"/> or <see cref="ReceiveWindowOutcome.Duplicate"/>; -1
    /// for <see cref="ReceiveWindowOutcome.OutOfWindow"/>, since a sequence outside the span never
    /// resolves to one of this window's slots.
    /// </param>
    /// <returns>Exactly what <see cref="Classify(uint)"/> returns.</returns>
    internal ReceiveWindowOutcome Classify(uint sequence, out int ringSlot)
    {
        LastSlideLoss = 0;
        LastAbandonedFragmentSet = default;
        ringSlot = -1;

        bool atOrAboveBase = sequence == Base || SequenceNumber.IsNewerThan(sequence, Base);
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
        uint offset = unchecked(sequence - Base);
        if (offset >= (uint)Capacity)
        {
            if (!_slides)
            {
                // At or beyond the far edge: not yet reachable. The base stays exactly where it was,
                // so a far-ahead packet can never drag it forward past data that has not arrived.
                return ReceiveWindowOutcome.OutOfWindow;
            }

            SlideTo(sequence);
            offset = unchecked(sequence - Base);
        }

        int slot = (int)((_baseSlot + offset) % (uint)Capacity);
        ringSlot = slot;
        Span<byte> admitted = _admitted.Span;
        if (admitted[slot] != 0)
        {
            return ReceiveWindowOutcome.Duplicate;
        }

        admitted[slot] = 1;
        if (!_fragmentLengths.IsEmpty)
        {
            // A fresh admission at this ring slot is the last moment anyone can say how much of a
            // previous, never-completed occupant's Fragment set had arrived (issue #297): once the
            // length below is zeroed, that occupant's own residue is gone. A non-zero length here means
            // exactly that — a live piece of a set that will now never complete, because the ring has
            // just reused its slot and a sequence the base has already passed can never be re-admitted.
            ushort residualLength = BinaryPrimitives.ReadUInt16LittleEndian(_fragmentLengths.Span.Slice(slot * 2, 2));
            if (residualLength != 0)
            {
                HandleFragmentSetAbandonment(slot);
            }

            // A fresh admission at this ring slot invalidates whatever Fragment-length residue a
            // previous, never-completed occupant of the same physical slot left behind (issue #259):
            // without this, a slot the ring later reuses for an unrelated sequence could still read
            // non-zero from a group that never actually completed, and FragmentGroupCompleteness.IsComplete
            // would misread that leftover byte as this admission's own piece. Cleared unconditionally,
            // for a Fragment and an ordinary record alike, because an ordinary record never calls
            // RecordFragmentLength to overwrite it itself; HoldFragment writes the real length right
            // after this call returns, for the one case that needs one. The group-identity entry is
            // cleared alongside it for the same reason (issue #297).
            BinaryPrimitives.WriteUInt16LittleEndian(_fragmentLengths.Span.Slice(slot * 2, 2), 0);
            ClearFragmentGroup(slot);
        }

        AdvanceBase();
        return ReceiveWindowOutcome.Admitted;
    }

    /// <summary>
    /// Records that the Fragment occupying ring slot <paramref name="ringSlot"/> — as reported by
    /// <see cref="Classify(uint, out int)"/> for the sequence it was admitted at — is
    /// <paramref name="byteLength"/> bytes long (#257). This is the write half of the reassembly
    /// bookkeeping this window's class doc describes: the length run outlives
    /// <see cref="AdvanceBase"/> clearing that same slot's admission byte, so it is what later tells a
    /// held Fragment apart from an empty slot.
    /// </summary>
    /// <param name="ringSlot">A ring position below <see cref="Capacity"/>, from <see cref="Classify(uint, out int)"/>.</param>
    /// <param name="byteLength">The Fragment's payload length in bytes.</param>
    /// <exception cref="InvalidOperationException">
    /// This window was built without a Fragment-length run — see
    /// <see cref="ReceiveWindow(Memory{byte}, bool, Memory{byte}, Memory{byte})"/> — because its Space never retains
    /// a Fragment (ADR 0004).
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ringSlot"/> is outside <see cref="Capacity"/>.</exception>
    internal void RecordFragmentLength(int ringSlot, ushort byteLength)
    {
        Span<byte> slot = FragmentLengthSlot(ringSlot);
        BinaryPrimitives.WriteUInt16LittleEndian(slot, byteLength);
    }

    /// <summary>
    /// Reads back what <see cref="RecordFragmentLength"/> most recently recorded for ring slot
    /// <paramref name="ringSlot"/> — zero if nothing has, which is exactly what a slot never written to
    /// (or cleared by <see cref="Reset"/>) reads as (#257, #258).
    /// </summary>
    /// <param name="ringSlot">A ring position below <see cref="Capacity"/>.</param>
    /// <exception cref="InvalidOperationException">
    /// This window was built without a Fragment-length run — see
    /// <see cref="ReceiveWindow(Memory{byte}, bool, Memory{byte}, Memory{byte})"/> — because its Space never retains
    /// a Fragment (ADR 0004).
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ringSlot"/> is outside <see cref="Capacity"/>.</exception>
    internal ushort GetFragmentLength(int ringSlot)
    {
        Span<byte> slot = FragmentLengthSlot(ringSlot);
        return BinaryPrimitives.ReadUInt16LittleEndian(slot);
    }

    /// <summary>
    /// Resolves <paramref name="ringSlot"/>'s two bytes in <see cref="_fragmentLengths"/>, shared by
    /// <see cref="RecordFragmentLength"/> and <see cref="GetFragmentLength"/> so the bounds check and
    /// the "this Space never retains" guard exist in exactly one place.
    /// </summary>
    private Span<byte> FragmentLengthSlot(int ringSlot)
    {
        if (_fragmentLengths.IsEmpty)
        {
            throw new InvalidOperationException(
                "this receive window was built over a Space that never retains a Fragment (ADR 0004), " +
                "so it holds no Fragment-length run to read or write.");
        }

        if (ringSlot < 0 || ringSlot >= Capacity)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ringSlot),
                ringSlot,
                $"ring slot {ringSlot} is outside this window's capacity of {Capacity}.");
        }

        return _fragmentLengths.Span.Slice(ringSlot * 2, 2);
    }

    /// <summary>
    /// Records that the Fragment occupying ring slot <paramref name="ringSlot"/> belongs to the group
    /// starting at <paramref name="groupFirstSequence"/> and declaring <paramref name="fragmentCount"/>
    /// pieces (issue #297). Written alongside <see cref="RecordFragmentLength"/> at the same point
    /// <see cref="DatagramSessionReactor.HoldFragment"/> holds a piece, so the two runs never disagree
    /// about which slots currently hold a piece of which set.
    /// </summary>
    /// <param name="ringSlot">A ring position below <see cref="Capacity"/>, from <see cref="Classify(uint, out int)"/>.</param>
    /// <param name="groupFirstSequence">The group's first sequence — see <see cref="FragmentGroup.FirstSequenceOf"/>.</param>
    /// <param name="fragmentCount">The group's declared Fragment count.</param>
    /// <exception cref="InvalidOperationException">
    /// This window was built without a Fragment-group run — see
    /// <see cref="ReceiveWindow(Memory{byte}, bool, Memory{byte}, Memory{byte})"/> — because its Space
    /// never retains a Fragment (ADR 0004).
    /// </exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="ringSlot"/> is outside <see cref="Capacity"/>.</exception>
    internal void RecordFragmentGroup(int ringSlot, uint groupFirstSequence, ushort fragmentCount)
    {
        Span<byte> slot = FragmentGroupSlot(ringSlot);
        BinaryPrimitives.WriteUInt32LittleEndian(slot[..4], groupFirstSequence);
        BinaryPrimitives.WriteUInt16LittleEndian(slot.Slice(4, 2), fragmentCount);
    }

    /// <summary>
    /// Reads back what <see cref="RecordFragmentGroup"/> most recently recorded for ring slot
    /// <paramref name="ringSlot"/> — a first sequence and declared count of zero if nothing has, which
    /// is exactly what a slot never written to (or cleared by <see cref="Reset"/> or
    /// <see cref="ClearFragmentGroup"/>) reads as.
    /// </summary>
    private (uint FirstSequence, ushort FragmentCount) ReadFragmentGroup(int ringSlot)
    {
        Span<byte> slot = FragmentGroupSlot(ringSlot);
        uint firstSequence = BinaryPrimitives.ReadUInt32LittleEndian(slot[..4]);
        ushort fragmentCount = BinaryPrimitives.ReadUInt16LittleEndian(slot.Slice(4, 2));
        return (firstSequence, fragmentCount);
    }

    /// <summary>
    /// Zeroes ring slot <paramref name="ringSlot"/>'s Fragment-group entry — called everywhere the
    /// matching length entry is cleared (a fresh admission clobbering it, <see cref="ReleaseGroup"/>
    /// releasing a delivered set, or <see cref="Reset"/>), so the two runs are cleared together and
    /// never drift out of step.
    /// </summary>
    private void ClearFragmentGroup(int ringSlot) => FragmentGroupSlot(ringSlot).Clear();

    /// <summary>
    /// Resolves <paramref name="ringSlot"/>'s six bytes in <see cref="_fragmentGroups"/>, shared by
    /// <see cref="RecordFragmentGroup"/>, <see cref="ReadFragmentGroup"/> and
    /// <see cref="ClearFragmentGroup"/> so the bounds check and the "this Space never retains" guard
    /// exist in exactly one place, mirroring <see cref="FragmentLengthSlot"/>.
    /// </summary>
    private Span<byte> FragmentGroupSlot(int ringSlot)
    {
        if (_fragmentGroups.IsEmpty)
        {
            throw new InvalidOperationException(
                "this receive window was built over a Space that never retains a Fragment (ADR 0004), " +
                "so it holds no Fragment-group run to read or write.");
        }

        if (ringSlot < 0 || ringSlot >= Capacity)
        {
            throw new ArgumentOutOfRangeException(
                nameof(ringSlot),
                ringSlot,
                $"ring slot {ringSlot} is outside this window's capacity of {Capacity}.");
        }

        return _fragmentGroups.Span.Slice(ringSlot * 6, 6);
    }

    /// <summary>
    /// Reports how much of the incomplete Fragment set occupying ring slot <paramref name="ringSlot"/>
    /// was actually held at the instant its physical slot is about to be reused for an unrelated, later
    /// sequence (issue #297) — the last moment anyone can still answer that, since the caller is about to
    /// zero the very residue this reads. Sets <see cref="LastAbandonedFragmentSet"/> and clears every one
    /// of the set's own still-addressable slots whose recorded group still names it, so that no later
    /// clobber of a sibling slot can ever report the same set abandoned a second time.
    /// </summary>
    /// <param name="ringSlot">
    /// The ring slot a fresh admission is about to clobber, whose Fragment-length residue was found
    /// non-zero — the set's own record of its first sequence and declared count is read from here.
    /// </param>
    /// <remarks>
    /// A set member's sequence that no longer resolves to any ring slot (<see cref="TryGetRingSlot"/>
    /// returns <see langword="false"/>) is skipped rather than counted: it is too far away, in either
    /// direction, for any slot to still be able to answer for it. A set member whose resolved slot's
    /// recorded group names a different first sequence is skipped too — that physical slot holds
    /// something other than this set's own piece (a hole this set never filled, or residue from a still
    /// older, unrelated occupant), so it is neither counted as held nor cleared as part of this set.
    /// </remarks>
    private void HandleFragmentSetAbandonment(int ringSlot)
    {
        (uint groupFirstSequence, ushort declaredCount) = ReadFragmentGroup(ringSlot);
        long held = 0;
        for (int i = 0; i < declaredCount; i++)
        {
            uint sequence = unchecked(groupFirstSequence + (uint)i);
            if (!TryGetRingSlot(sequence, out int memberSlot))
            {
                continue;
            }

            ushort length = BinaryPrimitives.ReadUInt16LittleEndian(_fragmentLengths.Span.Slice(memberSlot * 2, 2));
            (uint memberFirstSequence, _) = ReadFragmentGroup(memberSlot);
            if (length == 0 || memberFirstSequence != groupFirstSequence)
            {
                continue;
            }

            held++;
            BinaryPrimitives.WriteUInt16LittleEndian(_fragmentLengths.Span.Slice(memberSlot * 2, 2), 0);
            ClearFragmentGroup(memberSlot);
        }

        LastAbandonedFragmentSet = (true, held, declaredCount);
    }

    /// <summary>
    /// Resolves <paramref name="sequence"/> to the ring slot it occupies in this window right now
    /// (issue #258): <see cref="FragmentGroupCompleteness.TryGetRingSlot"/> given this window's own
    /// <see cref="Capacity"/>, <see cref="Base"/> and ring position. Unlike
    /// <see cref="Classify(uint, out int)"/>, this never admits, moves <see cref="Base"/>, or writes
    /// to any run — it only answers where a sequence <em>would</em> be, addressing both a sequence
    /// still inside the live span and one up to <see cref="Capacity"/> slots behind
    /// <see cref="Base"/> that this window already admitted and, for a retaining Space, may still hold
    /// a Fragment for (see <see cref="GetFragmentLength"/>).
    /// </summary>
    /// <param name="sequence">The sequence to resolve.</param>
    /// <param name="ringSlot">
    /// The ring position <paramref name="sequence"/> occupies, valid only when this method returns
    /// <see langword="true"/>; -1 otherwise.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if <paramref name="sequence"/> is addressable — see
    /// <see cref="FragmentGroupCompleteness.TryGetRingSlot"/> for the exact bound in each direction;
    /// <see langword="false"/> if it is too far ahead or too far behind for any slot to still be able
    /// to answer for it.
    /// </returns>
    internal bool TryGetRingSlot(uint sequence, out int ringSlot) =>
        FragmentGroupCompleteness.TryGetRingSlot(sequence, Capacity, Base, _baseSlot, out ringSlot);

    /// <summary>
    /// Whether every Fragment of the group starting at <paramref name="groupFirstSequence"/> and
    /// declaring <paramref name="fragmentCount"/> pieces is currently held in this window (issue #258)
    /// — delegates entirely to <see cref="FragmentGroupCompleteness.IsComplete"/> over this window's
    /// own <see cref="_fragmentLengths"/> run, <see cref="Capacity"/>, <see cref="Base"/> and ring
    /// position. Reads window state and nothing else: no ack, retransmission or retention state is
    /// consulted, which is what keeps a lost Fragment costing exactly one Datagram to repair — that
    /// machinery repairs by sequence number alone and does not know a Fragment exists, so nothing
    /// here may teach it otherwise by routing through it.
    /// </summary>
    /// <param name="groupFirstSequence">
    /// The group's first sequence — see <see cref="FragmentGroup.FirstSequenceOf"/>. Never open-code
    /// <c>sequence - fragmentIndex</c>; derive it there instead.
    /// </param>
    /// <param name="fragmentCount">The group's declared Fragment count.</param>
    /// <returns>
    /// <see langword="true"/> if every one of the group's sequences resolves to a ring slot that
    /// currently holds a Fragment; <see langword="false"/> if any piece is missing, or if the group is
    /// wider than this window can currently address — never throws for either reason.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// This window was built without a Fragment-length run — see
    /// <see cref="ReceiveWindow(Memory{byte}, bool, Memory{byte}, Memory{byte})"/> — because its Space never retains
    /// a Fragment (ADR 0004).
    /// </exception>
    internal bool IsGroupComplete(uint groupFirstSequence, ushort fragmentCount)
    {
        if (_fragmentLengths.IsEmpty)
        {
            throw new InvalidOperationException(
                "this receive window was built over a Space that never retains a Fragment (ADR 0004), " +
                "so no Fragment set can ever complete in it.");
        }

        return FragmentGroupCompleteness.IsComplete(
            _admitted.Span, _fragmentLengths.Span, Capacity, Base, _baseSlot, groupFirstSequence, fragmentCount);
    }

    /// <summary>
    /// Zeroes the Fragment-length and Fragment-group entries for every sequence of the group starting at
    /// <paramref name="groupFirstSequence"/> (issue #259, extended by #297) — the release half of the
    /// reassembly bookkeeping this window's class doc describes, called once a completed group's pieces
    /// have all been copied out into a delivered Message. Releasing is what keeps a later Fragment that
    /// reuses one of these ring slots from ever being mistaken for a leftover of the group just
    /// delivered: without it, <see cref="IsGroupComplete"/> could read a stale non-zero length left over
    /// from this group and report a future, different group complete before every one of its own pieces
    /// had actually arrived, and a later abandonment could misreport this already-delivered set's residue
    /// as still abandoned. A sequence too far away to resolve to a ring slot — already fully retired, or
    /// never reachable — is simply skipped rather than throwing, since there is then nothing left in
    /// this window that still needs releasing for it.
    /// </summary>
    /// <param name="groupFirstSequence">
    /// The group's first sequence — see <see cref="FragmentGroup.FirstSequenceOf"/>.
    /// </param>
    /// <param name="fragmentCount">The group's declared Fragment count.</param>
    /// <exception cref="InvalidOperationException">
    /// This window was built without a Fragment-length run — see
    /// <see cref="ReceiveWindow(Memory{byte}, bool, Memory{byte}, Memory{byte})"/> — because its Space never retains
    /// a Fragment (ADR 0004).
    /// </exception>
    internal void ReleaseGroup(uint groupFirstSequence, ushort fragmentCount)
    {
        if (_fragmentLengths.IsEmpty)
        {
            throw new InvalidOperationException(
                "this receive window was built over a Space that never retains a Fragment (ADR 0004), " +
                "so it holds no Fragment set to release.");
        }

        for (int i = 0; i < fragmentCount; i++)
        {
            uint sequence = unchecked(groupFirstSequence + (uint)i);
            if (TryGetRingSlot(sequence, out int ringSlot))
            {
                RecordFragmentLength(ringSlot, 0);
                ClearFragmentGroup(ringSlot);
            }
        }
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
    /// from <see cref="_admitted"/>, <see cref="Base"/> and <see cref="_baseSlot"/> on every call
    /// rather than accumulated, so a window that has just retired a hole through
    /// <see cref="AdvanceBase"/> describes the new base and the bitfield that goes with it, never the
    /// state from before that call.
    /// </para>
    /// <para>
    /// Wrap correctness is inherited, not reinvented: <see cref="Base"/> and <see cref="_baseSlot"/>
    /// already track the window correctly across the <c>u32</c> wrap — that is what
    /// <see cref="SequenceNumber.IsNewerThan"/> inside <see cref="Classify(uint)"/> establishes — so this
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

        return (Base, bits);
    }

    /// <summary>
    /// Clears this window's admission run and its Fragment-length and Fragment-group runs, if it has
    /// them, and resets <see cref="Base"/>, its ring position and <see cref="LastSlideLoss"/> to their
    /// construction-time values. The arena storage a window is built over outlives the Peer that used it
    /// (#179): a Slot a new Peer reuses must classify from a clean window, not inherit whatever the
    /// previous occupant had admitted or held. Mirrors the division
    /// <see cref="Core.ReceiveArena.GetFragmentSlot"/> already draws — the arena keeps the bytes, the
    /// occupant clears them. Does not emit — the Session is over and there is no Peer left to subject a
    /// Diagnostic to, so <see cref="LastAbandonedFragmentSet"/> is simply reset to its construction-time
    /// default along with everything else, exactly as it would after any other <see cref="Classify(uint)"/> call.
    /// </summary>
    internal void Reset()
    {
        _admitted.Span.Clear();
        _fragmentLengths.Span.Clear();
        _fragmentGroups.Span.Clear();
        Base = 0;
        _baseSlot = 0;
        LastSlideLoss = 0;
        LastAbandonedFragmentSet = default;
    }

    /// <summary>
    /// Advances <see cref="Base"/> one slot at a time while the slot it currently occupies is
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
            Base = unchecked(Base + 1);
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
        uint retiredCount = unchecked(newBase - Base);
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

        Base = newBase;
        LastSlideLoss = lost;
    }
}
