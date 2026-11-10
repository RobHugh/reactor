using System.Diagnostics;
using ReactorSockets.Session;

namespace ReactorSockets;

/// <summary>
/// Checks every Fragment in one data datagram's record body as part of the set it claims to belong to,
/// not only individually (ADR 0009). Contiguity stands in for a message ID no wire byte carries, so two
/// shapes of forgery break it: Fragments naming the same group (the pair of that group's mode and
/// <see cref="FragmentGroup.FirstSequenceOf"/>) that disagree on their declared count or repeat an
/// index, and two distinct claimed groups <em>in the same mode</em> whose <em>declared</em> ranges
/// overlap, meaning some one wire sequence could be read as belonging to either, whether or not a
/// Fragment has actually arrived there yet.
/// </summary>
/// <remarks>
/// <para>
/// Internal (ADR 0022). The wrap-aware overlap sweep and the per-mode group identity are proved through
/// the receive path, by datagrams a hand-built client sends. <see cref="DatagramSessionReactor"/> owns one
/// instance and calls <see cref="AreWellFormed"/> once per data datagram, after every record has proved
/// individually legal and before any Space is touched.
/// </para>
/// <para>
/// <b>A group is identified by (mode, first sequence), not first sequence alone.</b> Each
/// <c>PacketRecordMode</c> is an independent sequence Space, so a sequence admitted under one mode says
/// nothing about the same sequence under another. Two groups in different modes can never conflict,
/// however their first sequences or declared ranges line up, so the overlap sweep only ever compares two
/// groups already known to share a mode.
/// </para>
/// <para>
/// <b>Scope is one datagram.</b> A group split across datagrams, or one a later datagram contradicts,
/// needs state only reassembly owns. A partial set is not a violation, and neither is a gap in the
/// indices it does carry: selective retransmission packs whichever Fragments of a group expired, which
/// is a subset chosen by what was acked rather than a run. What each group's members must agree on is
/// their declared count, and no index may appear twice.
/// </para>
/// <para>
/// <b>Overlap is judged on each same-mode group's declared range, not on what is observed.</b> Group
/// firstSeq 100 declaring count 5 (100..104) and group firstSeq 103 declaring count 2 (103..104) can
/// arrive with only 100, 101 and 103 present, so their observed points never collide. Sequence 104 is
/// still claimed by both, and nothing observed so far can tell which group a Fragment landing there
/// belongs to.
/// </para>
/// <para>
/// <b>Bounded cost, no per-call allocation.</b> One linear pass collects each Fragment's (mode, first
/// sequence) key and index-and-count, and the two arrays are sorted together by that key (mode in the
/// high bits, so one mode's entries never interleave with another's). Every Fragment of one group then
/// forms one contiguous run, checked for a disagreed count and a repeated index in O(n log n) overall.
/// Each passing run contributes one distinct group, and the declared-range overlap sweep runs once per
/// mode's contiguous slice of those groups: an interval-scheduling sweep, O(g log g) in the number of
/// distinct groups. Every scratch array is sized once at construction from <see cref="MaxBodySize"/>
/// divided by the smallest Fragment record, so no body this type accepts can hold more Fragments than
/// the scratch holds entries for. Four arrays of that size cost about 140 KB per instance. Not
/// thread-safe: the scratch is reused across calls.
/// </para>
/// </remarks>
internal sealed class FragmentSetValidator
{
    /// <summary>
    /// The longest record body this validator accepts: the largest UDP datagram
    /// (<see cref="RawReactor.MaxUdpDatagramSize"/>) minus the data datagram header. A longer body is
    /// refused by <see cref="AreWellFormed"/>.
    /// </summary>
    public const int MaxBodySize = RawReactor.MaxUdpDatagramSize - DatagramSessionProtocol.DatagramHeaderSize;

    /// <summary>
    /// 2^32, the size of the <c>u32</c> sequence circle. The overlap sweep measures distances around that
    /// circle, and 2^32 does not fit in the <see cref="uint"/> the circle is counted in.
    /// </summary>
    private const ulong OneSequenceSpace = 1UL << 32;

    private const int MaxFragmentRecords = MaxBodySize / DatagramSessionProtocol.FragmentRecordSize;

    /// <summary>One entry per Fragment: mode in the high bits, the group's first sequence in the low 32.</summary>
    private readonly ulong[] _fragmentGroupKey = new ulong[MaxFragmentRecords];

    /// <summary>One entry per Fragment, paired with <see cref="_fragmentGroupKey"/>: index in the high 16 bits, count in the low 16.</summary>
    private readonly uint[] _fragmentIndexAndCount = new uint[MaxFragmentRecords];

    /// <summary>One entry per distinct group: mode in bits 48-55, first sequence in bits 16-47, declared count in bits 0-15.</summary>
    private readonly ulong[] _distinctGroups = new ulong[MaxFragmentRecords];

    /// <summary>Each distinct group's declared range start relative to its mode slice's wrap-aware anchor.</summary>
    private readonly int[] _distinctGroupSortKey = new int[MaxFragmentRecords];

    /// <summary>
    /// Reports whether every Fragment in <paramref name="body"/> belongs to a well-formed claimed set:
    /// members of one (mode, first sequence) group agree on their declared count and repeat no index, and
    /// no two groups in the same mode declare overlapping ranges. A body with no Fragments is well-formed.
    /// A body longer than <see cref="MaxBodySize"/>, or one holding a record that does not parse, is not.
    /// </summary>
    /// <param name="body">The packet records of one data datagram, after its header.</param>
    public bool AreWellFormed(ReadOnlySpan<byte> body)
    {
        if (body.Length > MaxBodySize)
        {
            return false;
        }

        int fragmentCount = 0;
        PacketRecordReader reader = new(body);
        while (reader.TryReadNext(out PacketRecord record))
        {
            if (record.IsFragment)
            {
                // Cannot happen: MaxFragmentRecords is MaxBodySize divided by the smallest Fragment
                // record, and a longer body was refused above.
                Debug.Assert(fragmentCount < MaxFragmentRecords, "More Fragments than the scratch was sized for.");

                _fragmentGroupKey[fragmentCount] = PackFragmentGroupKey(
                    (byte)record.Mode, FragmentGroup.FirstSequenceOf(record.Sequence, record.FragmentIndex));
                _fragmentIndexAndCount[fragmentCount] = ((uint)record.FragmentIndex << 16) | record.FragmentCount;
                fragmentCount++;
            }
        }

        if (!reader.IsAtEnd)
        {
            return false;
        }

        if (fragmentCount == 0)
        {
            return true;
        }

        Array.Sort(_fragmentGroupKey, _fragmentIndexAndCount, 0, fragmentCount);

        int distinctGroupCount = 0;
        int runStart = 0;
        for (int i = 1; i <= fragmentCount; i++)
        {
            if (i == fragmentCount || _fragmentGroupKey[i] != _fragmentGroupKey[runStart])
            {
                if (!TryCloseRun(runStart, i, ref distinctGroupCount))
                {
                    return false;
                }

                runStart = i;
            }
        }

        return NoTwoDeclaredRangesOverlap(distinctGroupCount);
    }

    /// <summary>Packs a Fragment's group identity: mode in bits 32-39, so it dominates the sort, first sequence in bits 0-31.</summary>
    private static ulong PackFragmentGroupKey(byte mode, uint firstSequence) => ((ulong)mode << 32) | firstSequence;

    private static byte FragmentGroupKeyMode(ulong key) => (byte)(key >> 32);

    private static uint FragmentGroupKeyFirstSequence(ulong key) => (uint)key;

    private static ushort IndexOf(uint indexAndCount) => (ushort)(indexAndCount >> 16);

    private static ushort CountOf(uint indexAndCount) => (ushort)indexAndCount;

    private static byte GroupMode(ulong group) => (byte)(group >> 48);

    // Truncating to uint after the >>16 shift drops the mode bits along with it, leaving exactly the
    // first-sequence bits placed at 16-47.
    private static uint GroupFirstSequence(ulong group) => (uint)(group >> 16);

    private static ushort GroupDeclaredCount(ulong group) => (ushort)group;

    /// <summary>
    /// Closes the run <c>[runStart, runEnd)</c>: every Fragment sharing one group's mode and first
    /// sequence, now contiguous in the sorted scratch. Fails if any member disagrees on the declared count
    /// or an index appears twice. The indices present need only be a subset of <c>[0, declaredCount)</c>
    /// with no duplicates, not a gap-free run. On success, appends one entry to
    /// <see cref="_distinctGroups"/>.
    /// </summary>
    private bool TryCloseRun(int runStart, int runEnd, ref int distinctGroupCount)
    {
        ushort declaredCount = CountOf(_fragmentIndexAndCount[runStart]);

        for (int i = runStart; i < runEnd; i++)
        {
            if (CountOf(_fragmentIndexAndCount[i]) != declaredCount)
            {
                return false;
            }
        }

        // Sorting the run by its packed index-and-count puts any repeated index beside its twin. Every key
        // in the run is equal by construction, so reordering only the values leaves each pair intact.
        Array.Sort(_fragmentIndexAndCount, runStart, runEnd - runStart);
        for (int i = runStart + 1; i < runEnd; i++)
        {
            if (IndexOf(_fragmentIndexAndCount[i]) == IndexOf(_fragmentIndexAndCount[i - 1]))
            {
                return false;
            }
        }

        ulong key = _fragmentGroupKey[runStart];
        byte mode = FragmentGroupKeyMode(key);
        uint firstSequence = FragmentGroupKeyFirstSequence(key);
        _distinctGroups[distinctGroupCount] = ((ulong)mode << 48) | ((ulong)firstSequence << 16) | declaredCount;
        distinctGroupCount++;
        return true;
    }

    /// <summary>
    /// Sweeps the first <paramref name="groupCount"/> distinct groups for a declared-range overlap, one
    /// mode's contiguous slice at a time. Groups were appended in (mode, first sequence) order, so each
    /// mode's groups already sit together.
    /// </summary>
    private bool NoTwoDeclaredRangesOverlap(int groupCount)
    {
        int sliceStart = 0;
        for (int i = 1; i <= groupCount; i++)
        {
            if (i == groupCount || GroupMode(_distinctGroups[i]) != GroupMode(_distinctGroups[sliceStart]))
            {
                if (!NoTwoDeclaredRangesOverlapWithinOneMode(sliceStart, i))
                {
                    return false;
                }

                sliceStart = i;
            }
        }

        return true;
    }

    /// <summary>
    /// The declared-range overlap sweep over one mode's slice <c>[sliceStart, sliceEnd)</c>. Sorts the
    /// slice by declared range start relative to a wrap-aware anchor, then sweeps once tracking the
    /// furthest declared end seen: O(k log k), correct for nested as well as adjacent ranges.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The anchor is derived, not assumed.</b> Taking the slice's numerically smallest value is wrong:
    /// a hostile peer adds one decoy group to drag that point away, which splits a genuinely overlapping
    /// pair to opposite ends of the sorted slice so the sweep never compares them. The anchor is the
    /// value immediately after the largest circular gap between consecutive first sequences, wrap gap
    /// included, so every offset from it is a genuine forward distance around the circle.
    /// </para>
    /// <para>
    /// <b>A slice too wide to order is refused.</b> No wrap-aware order exists over sequences spanning half
    /// the space (2^31) or more, the same boundary <see cref="SequenceNumber"/> names. An honest sender's
    /// receive window is far smaller, so such a slice is refused rather than swept in an order the peer
    /// chose.
    /// </para>
    /// <para>
    /// Offsets are <see cref="long"/>: a declared end is its start plus up to
    /// <see cref="ushort.MaxValue"/>, which a slice spanning close to 2^31 would push past
    /// <see cref="int.MaxValue"/>.
    /// </para>
    /// </remarks>
    private bool NoTwoDeclaredRangesOverlapWithinOneMode(int sliceStart, int sliceEnd)
    {
        int sliceCount = sliceEnd - sliceStart;
        if (sliceCount <= 1)
        {
            return true;
        }

        // The slice is in ascending numeric first-sequence order and its groups are distinct, so the
        // largest circular gap is found over adjacent pairs plus the wrap gap from the last back to the first.
        uint smallest = GroupFirstSequence(_distinctGroups[sliceStart]);
        uint largest = GroupFirstSequence(_distinctGroups[sliceEnd - 1]);

        ulong largestGap = OneSequenceSpace - largest + smallest;
        uint anchor = smallest;

        for (int i = sliceStart + 1; i < sliceEnd; i++)
        {
            ulong gap = GroupFirstSequence(_distinctGroups[i]) - GroupFirstSequence(_distinctGroups[i - 1]);
            if (gap > largestGap)
            {
                largestGap = gap;
                anchor = GroupFirstSequence(_distinctGroups[i]);
            }
        }

        // "At or wider", not merely "wider": a span of exactly 2^31 puts the furthest group at an offset
        // the int sort key cannot hold, so it would wrap negative and sort to the wrong end.
        if (OneSequenceSpace - largestGap >= OneSequenceSpace / 2)
        {
            return false;
        }

        for (int i = sliceStart; i < sliceEnd; i++)
        {
            _distinctGroupSortKey[i] = unchecked((int)(GroupFirstSequence(_distinctGroups[i]) - anchor));
        }

        Array.Sort(_distinctGroupSortKey, _distinctGroups, sliceStart, sliceCount);

        long maxDeclaredEndSoFar = long.MinValue;
        for (int i = sliceStart; i < sliceEnd; i++)
        {
            long start = unchecked(GroupFirstSequence(_distinctGroups[i]) - anchor);
            long end = start + GroupDeclaredCount(_distinctGroups[i]) - 1;

            if (i > sliceStart && start <= maxDeclaredEndSoFar)
            {
                return false;
            }

            maxDeclaredEndSoFar = Math.Max(maxDeclaredEndSoFar, end);
        }

        return true;
    }
}
