using System.Buffers.Binary;
using System.Diagnostics;

namespace ReactorSockets.Session;

/// <summary>
/// The completed Fragment groups of one reliable Space of one Peer that have not yet reached the
/// consumer: each one waiting for its turn in that Space's send-order queue, for frame arena room, or
/// both. A group is named by the pair every one of its Fragments already agrees on — its first sequence
/// and its declared Fragment count (<see cref="FragmentGroup"/>) — so nothing here needs a message ID
/// the wire does not carry.
/// </summary>
/// <remarks>
/// <para>
/// <b>More than one group can be waiting at once.</b> A group completes when its own pieces have all
/// arrived, and that is independent of whether an earlier group in the same Space is still missing
/// pieces: the Fragments of a later Message can arrive, whole, while a hole sits in front of them. A
/// single waiting slot per Space would let the second group to complete overwrite the first, and the
/// first would then never be delivered and never have its receive-window slots released — a Message
/// lost with nothing reported. Holding them all is what makes "delivered exactly once, and only when
/// complete" true of every group rather than only of the most recent one.
/// </para>
/// <para>
/// <b>The bound is proved by construction, not chosen.</b> A window of <c>n</c> slots can never have
/// more than <c>n / 2</c> groups waiting at once, which is exactly
/// <see cref="Core.ReceiveArena.PendingGroupsPerSpace"/>, because three rules hold. A group declares at
/// least two pieces, since <see cref="PacketRecordCodec.TryRead"/> refuses a count of zero or one, and
/// never more than the window has slots, since the datagram parse refuses that too. Two waiting groups
/// never share a slot, since <see cref="FragmentGroupCompleteness.IsComplete"/> counts a piece only for
/// the group it named on arrival. A waiting group keeps its slots until it is delivered, since
/// <see cref="ReceiveWindow.WouldOverwriteDeliverableFragment"/> turns away any record that would overwrite
/// one and no ack covers that record, so the far end's retransmission brings it back once the group has
/// gone. A group whose turn never comes keeps its slots for good, which stops only that Space of the
/// Peer that sent it. <see cref="Add"/> asserts rather than grows, in the house style for an invariant
/// designed away rather than defended (ADR 0015).
/// </para>
/// <para>
/// <b>No allocation of its own, ever.</b> The only storage this type touches is the
/// <see cref="Memory{T}"/> run <see cref="Core.ReceiveArena.GetPendingGroupRun"/> hands it at
/// construction, six bytes per entry. It lives in the arena rather than in an array of its own
/// because its size follows the receive window, and a window-proportional cost outside the arena
/// would be a cost outside <see cref="DatagramSessionOptions.ReceiveBudgetBytes"/> — the figure the
/// memory bill states. A hostile peer flooding groups is bounded by the same receive window that
/// already bounds its Fragments, so nothing here grows under a flood.
/// </para>
/// </remarks>
internal sealed class PendingFragmentGroups
{
    /// <summary>Bytes one entry occupies: a <c>u32</c> first sequence then a <c>u16</c> Fragment count.</summary>
    private const int EntrySize = 6;

    private readonly Memory<byte> _entries;

    /// <summary>
    /// Builds a set over <paramref name="entries"/>, which is its entire storage — on loan from the
    /// receive arena for the calling Peer's lifetime.
    /// </summary>
    /// <param name="entries">
    /// The waiting-group run for this Space, six bytes per entry — see
    /// <see cref="Core.ReceiveArena.GetPendingGroupRun"/>. Must hold at least one whole entry.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="entries"/> cannot hold one entry.</exception>
    internal PendingFragmentGroups(Memory<byte> entries)
    {
        if (entries.Length < EntrySize)
        {
            throw new ArgumentOutOfRangeException(
                nameof(entries),
                entries.Length,
                $"a waiting-group run of {entries.Length} bytes cannot hold one {EntrySize}-byte entry.");
        }

        _entries = entries;
        Capacity = entries.Length / EntrySize;
    }

    /// <summary>The number of groups this set can have waiting at once, fixed at construction.</summary>
    internal int Capacity { get; }

    /// <summary>How many completed groups are currently waiting to be delivered.</summary>
    internal int Count { get; private set; }

    /// <summary>
    /// Records <paramref name="firstSequence"/>'s group as waiting, unless it already is. A repeat is
    /// ignored rather than stored twice, so one group can never be delivered twice from this set.
    /// </summary>
    internal void Add(uint firstSequence, ushort fragmentCount)
    {
        Span<byte> entries = _entries.Span;
        for (int i = 0; i < Count; i++)
        {
            if (FirstSequenceAt(entries, i) == firstSequence)
            {
                return;
            }
        }

        Debug.Assert(
            Count < Capacity,
            "A receive window of n slots cannot have more than n / 2 Fragment groups waiting: each holds at least two slots of its own until delivered.");

        Write(entries, Count, firstSequence, fragmentCount);
        Count++;
    }

    /// <summary>
    /// The waiting group whose turn has come in a <c>ReliableOrdered</c> Space: the one starting
    /// exactly at <paramref name="nextSequence"/>, the sequence that Space's queue is next owed. A
    /// group whose turn has not arrived is left exactly where it is, its pieces still held.
    /// </summary>
    /// <returns><see langword="true"/> if a waiting group starts at <paramref name="nextSequence"/>.</returns>
    internal bool TryGetDueAt(uint nextSequence, out ushort fragmentCount)
    {
        Span<byte> entries = _entries.Span;
        for (int i = 0; i < Count; i++)
        {
            if (FirstSequenceAt(entries, i) == nextSequence)
            {
                fragmentCount = FragmentCountAt(entries, i);
                return true;
            }
        }

        fragmentCount = 0;
        return false;
    }

    /// <summary>
    /// The waiting group at <paramref name="index"/>, for a caller walking the set to retry each one
    /// in turn. Order is arrival order and carries no meaning — a <c>ReliableUnordered</c> Space has no
    /// turn to wait for, so any group waiting there is deliverable the moment the arena has room.
    /// </summary>
    internal (uint FirstSequence, ushort FragmentCount) At(int index)
    {
        Span<byte> entries = _entries.Span;
        return (FirstSequenceAt(entries, index), FragmentCountAt(entries, index));
    }

    /// <summary>
    /// Forgets <paramref name="firstSequence"/>'s group, called once it has actually been delivered.
    /// The last entry moves into the freed position, so the set stays a dense prefix and nothing
    /// shifts; a caller walking the set while removing must therefore walk it backwards.
    /// </summary>
    internal void Remove(uint firstSequence)
    {
        Span<byte> entries = _entries.Span;
        for (int i = 0; i < Count; i++)
        {
            if (FirstSequenceAt(entries, i) != firstSequence)
            {
                continue;
            }

            Write(entries, i, FirstSequenceAt(entries, Count - 1), FragmentCountAt(entries, Count - 1));
            Count--;
            return;
        }
    }

    /// <summary>
    /// Forgets every waiting group. The arena run this set is built over outlives the Peer that used
    /// it, so a Slot a later Peer occupies must not inherit the previous occupant's waiting groups —
    /// the same reason <see cref="ReceiveWindow.Reset"/> exists.
    /// </summary>
    internal void Reset() => Count = 0;

    private static uint FirstSequenceAt(ReadOnlySpan<byte> entries, int index) =>
        BinaryPrimitives.ReadUInt32LittleEndian(entries[(index * EntrySize)..]);

    private static ushort FragmentCountAt(ReadOnlySpan<byte> entries, int index) =>
        BinaryPrimitives.ReadUInt16LittleEndian(entries[((index * EntrySize) + 4)..]);

    private static void Write(Span<byte> entries, int index, uint firstSequence, ushort fragmentCount)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(entries[(index * EntrySize)..], firstSequence);
        BinaryPrimitives.WriteUInt16LittleEndian(entries[((index * EntrySize) + 4)..], fragmentCount);
    }
}
