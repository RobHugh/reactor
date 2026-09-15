using System.Buffers.Binary;
using ReactorSockets.Session;

namespace ReactorSockets;

/// <summary>
/// The arithmetic that decides whether every Fragment of one group has been held in a
/// <see cref="ReceiveWindow"/> (issue #258, part of #49's reassembly work). Internal, and tested only
/// through <see cref="DatagramSessionReactor"/> (ADR 0003, ADR 0015, issue #278): its inputs are the
/// receive arena's run layouts and a ring index, which mean nothing to a consumer.
/// </summary>
/// <remarks>
/// <para>
/// <b>One type owns every piece of ring/wrap arithmetic reassembly needs.</b>
/// <see cref="ReceiveWindow.TryGetRingSlot"/> and <see cref="ReceiveWindow.IsGroupComplete"/> are thin
/// wrappers that hand this type their window's own <c>Capacity</c>, <c>Base</c> and ring position and
/// nothing else — they never recompute a modulus themselves. That keeps a caller such as
/// <see cref="DatagramSessionReactor"/> from ever open-coding <c>(baseSlot + offset) % Capacity</c>.
/// </para>
/// <para>
/// <b>The <c>u32</c> wrap is not a separate case.</b> Every decision here reads a sequence only as its
/// wrapped distance from <c>windowBase</c>, <c>unchecked(sequence - windowBase)</c>, and never compares
/// two sequences directly. Adding the same constant to every sequence and to the base therefore changes
/// no answer, so a group straddling the <c>u32</c> wrap behaves exactly like the same group near zero,
/// which the seam reaches cheaply. A reliable window never slides, so reaching the wrap itself through
/// the seam would take 2^32 records.
/// </para>
/// <para>
/// <b>Reads window state and nothing else.</b> Every parameter here is either a plain value already
/// held by the window (capacity, base, base's ring slot) or one of its runs. Nothing about an ack, a
/// retransmission timer or the retention arena passes through this type — see
/// <see cref="ReceiveWindow"/>'s own remarks on why that separation matters.
/// </para>
/// <para>
/// <b>A sequence is addressable from two directions.</b> One at or above the window's base resolves
/// forward; one below the base resolves backward, by how far behind it sits. Both are bounded at
/// <c>capacity</c> slots — ahead of that the sequence has not arrived yet, and behind it by more than one
/// full lap the ring has already reused its slot. <see cref="TryGetRingSlot"/> reports that as "not
/// found" rather than throwing.
/// </para>
/// </remarks>
internal static class FragmentGroupCompleteness
{
    /// <summary>
    /// Resolves <paramref name="sequence"/> to the ring slot it occupies in a window of
    /// <paramref name="capacity"/> slots currently based at <paramref name="windowBase"/>, whose ring
    /// position <paramref name="baseSlot"/> occupies.
    /// </summary>
    /// <param name="sequence">The sequence to resolve.</param>
    /// <param name="capacity">The window's capacity in slots.</param>
    /// <param name="windowBase">The window's current base sequence.</param>
    /// <param name="baseSlot">The ring position <paramref name="windowBase"/> currently occupies.</param>
    /// <param name="ringSlot">
    /// The ring position <paramref name="sequence"/> occupies, valid only when this method returns
    /// <see langword="true"/>; -1 otherwise.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if <paramref name="sequence"/> is fewer than <paramref name="capacity"/>
    /// slots ahead of <paramref name="windowBase"/>, or at most <paramref name="capacity"/> slots behind
    /// it; <see langword="false"/> if it lies further away in either direction.
    /// </returns>
    internal static bool TryGetRingSlot(uint sequence, int capacity, uint windowBase, int baseSlot, out int ringSlot)
    {
        uint distance = unchecked(sequence - windowBase);
        if (IsAtOrAboveBase(distance))
        {
            if (distance < (uint)capacity)
            {
                ringSlot = (int)((baseSlot + distance) % (uint)capacity);
                return true;
            }
        }
        else
        {
            uint backward = unchecked(0u - distance);
            if (backward <= (uint)capacity)
            {
                ringSlot = ((baseSlot - (int)backward) % capacity + capacity) % capacity;
                return true;
            }
        }

        ringSlot = -1;
        return false;
    }

    /// <summary>
    /// Whether every sequence of the group starting at <paramref name="groupFirstSequence"/> and
    /// running for <paramref name="fragmentCount"/> sequences has a Fragment held in
    /// <paramref name="fragmentLengths"/>. No structure is allocated to answer this: each sequence is
    /// resolved to its ring slot by <see cref="TryGetRingSlot"/> and that slot's entries are read directly.
    /// </summary>
    /// <param name="admitted">
    /// The window's admission run — see <see cref="Core.ReceiveArena.GetAdmissionRun"/> — one byte per
    /// ring slot, non-zero exactly where that slot's sequence is currently admitted. Needed only to
    /// corroborate a piece still at or ahead of <paramref name="windowBase"/> (issue #259): see remarks.
    /// </param>
    /// <param name="fragmentLengths">
    /// The window's Fragment-length run — see <see cref="Core.ReceiveArena.GetFragmentLengthRun"/> —
    /// two bytes per slot, <c>capacity * 2</c> bytes long.
    /// </param>
    /// <param name="capacity">The window's capacity in slots.</param>
    /// <param name="windowBase">The window's current base sequence.</param>
    /// <param name="baseSlot">The ring position <paramref name="windowBase"/> currently occupies.</param>
    /// <param name="groupFirstSequence">
    /// The group's first sequence — see <see cref="FragmentGroup.FirstSequenceOf"/>.
    /// </param>
    /// <param name="fragmentCount">The group's declared Fragment count.</param>
    /// <returns>
    /// <see langword="true"/> if every one of the group's sequences resolves to a ring slot whose recorded
    /// length is non-zero and, for a sequence at or ahead of <paramref name="windowBase"/>, whose admission
    /// entry is also set; <see langword="false"/> otherwise, including when the group is wider than the
    /// window can currently address.
    /// </returns>
    /// <remarks>
    /// A piece already behind <paramref name="windowBase"/> is trusted on its length alone:
    /// <see cref="ReceiveWindow.AdvanceBase"/> clears a passed slot's admission entry, so the length run is
    /// what survives to answer this, and a length reused since is cleared at the next fresh admission of
    /// that slot (issue #259). A piece at or ahead of the base is different: the ring slot a not-yet-arrived
    /// sequence resolves to can still hold a non-zero length left by an earlier, abandoned occupant whose
    /// admission entry was cleared when the base swept past it. A live forward piece always has its entry
    /// set, so requiring it rejects exactly the stale case.
    /// </remarks>
    internal static bool IsComplete(
        ReadOnlySpan<byte> admitted,
        ReadOnlySpan<byte> fragmentLengths,
        int capacity,
        uint windowBase,
        int baseSlot,
        uint groupFirstSequence,
        ushort fragmentCount)
    {
        for (int i = 0; i < fragmentCount; i++)
        {
            uint sequence = unchecked(groupFirstSequence + (uint)i);
            if (!TryGetRingSlot(sequence, capacity, windowBase, baseSlot, out int ringSlot))
            {
                return false;
            }

            ushort length = BinaryPrimitives.ReadUInt16LittleEndian(fragmentLengths.Slice(ringSlot * 2, 2));
            if (length == 0)
            {
                return false;
            }

            if (IsAtOrAboveBase(unchecked(sequence - windowBase)) && admitted[ringSlot] == 0)
            {
                // Forward of the base, but nothing is currently admitted at this ring slot: the non-zero
                // length is stale residue from an earlier, abandoned occupant — see remarks.
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Whether a sequence whose wrapped distance from the base is <paramref name="distance"/> sits at or
    /// above the base — the same half-space <see cref="SequenceNumber.IsNewerThan"/> resolves.
    /// </summary>
    private static bool IsAtOrAboveBase(uint distance) => unchecked((int)distance) >= 0;
}
