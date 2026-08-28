using System.Buffers.Binary;
using ReactorSockets.Session;

namespace ReactorSockets;

/// <summary>
/// The arithmetic that decides whether every Fragment of one group has been held in a
/// <see cref="ReceiveWindow"/> (issue #258, part of #49's reassembly work), extracted as a pure
/// function for the same reason <see cref="FragmentPlan.Resolve"/> is (ADR 0015): a test pins the
/// ring-wrap and <c>u32</c>-wrap boundary conditions by calling <see cref="IsComplete"/> and
/// <see cref="TryGetRingSlot"/> directly with hand-picked window states, with no
/// <c>InternalsVisibleTo</c> and no interface invented for the purpose. Neither method needs a
/// <see cref="ReceiveWindow"/>, a Peer or a Reactor — both are total over their inputs and never
/// throw for a window shape that could actually exist.
/// </summary>
/// <remarks>
/// <para>
/// <b>One type owns every piece of ring/wrap arithmetic reassembly needs.</b>
/// <see cref="ReceiveWindow.TryGetRingSlot"/> and <see cref="ReceiveWindow.IsGroupComplete"/> are thin
/// wrappers that hand this type their window's own <c>Capacity</c>, <c>Base</c> and ring position and
/// nothing else — they never recompute a modulus themselves. That keeps a caller such as
/// <see cref="DatagramSessionReactor"/> from ever open-coding <c>(baseSlot + offset) % Capacity</c>,
/// and keeps the one place this arithmetic lives directly testable despite <see cref="ReceiveWindow"/>
/// itself staying <see langword="internal"/> (ADR 0003): this type answers the question, the window
/// only supplies the state to ask it of.
/// </para>
/// <para>
/// <b>Reads window state and nothing else.</b> Every parameter here is either a plain value already
/// held by the window (capacity, base, base's ring slot) or the length run itself. Nothing about an
/// ack, a retransmission timer or the retention arena passes through this type, which is what keeps
/// watching for a complete set from reaching into the machinery that repairs a lost Fragment — see
/// <see cref="ReceiveWindow"/>'s own remarks on why that separation is load-bearing.
/// </para>
/// <para>
/// <b>A sequence is addressable from two directions.</b> One at or above the window's base resolves
/// forward, the same offset <see cref="ReceiveWindow.Classify(uint)"/> already computes; one below the
/// base resolves backward, by how far behind it sits. Both are bounded at <see cref="int"/>
/// <c>capacity</c> slots — a sequence further than that in either direction has no addressable ring
/// slot, because nothing this far away is still the Fragment it once was: ahead of the window it has
/// not arrived yet, and behind it by more than one full lap the ring has already reused its slot for
/// something else. <see cref="TryGetRingSlot"/> reports that as "not found" rather than throwing,
/// which is exactly the "incomplete rather than throwing" acceptance criterion for a group whose
/// declared count exceeds what the window can currently span.
/// </para>
/// </remarks>
public static class FragmentGroupCompleteness
{
    /// <summary>
    /// Resolves <paramref name="sequence"/> to the ring slot it occupies in a window of
    /// <paramref name="capacity"/> slots currently based at <paramref name="windowBase"/>, whose ring
    /// position <paramref name="baseSlot"/> occupies — the same three values a
    /// <see cref="ReceiveWindow"/> already tracks for itself.
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
    /// <see langword="true"/> if <paramref name="sequence"/> is at or within <paramref name="capacity"/>
    /// slots ahead of <paramref name="windowBase"/> (still inside the window's live span), or up to
    /// <paramref name="capacity"/> slots behind it (already retired from the span, but not yet reused
    /// by a later lap of the ring); <see langword="false"/> if it lies further away in either
    /// direction, where no ring slot can safely be said to still hold what it once did.
    /// </returns>
    public static bool TryGetRingSlot(uint sequence, int capacity, uint windowBase, int baseSlot, out int ringSlot)
    {
        bool atOrAboveBase = sequence == windowBase || SequenceNumber.IsNewerThan(sequence, windowBase);
        if (atOrAboveBase)
        {
            uint forward = unchecked(sequence - windowBase);
            if (forward < (uint)capacity)
            {
                ringSlot = (int)((baseSlot + forward) % (uint)capacity);
                return true;
            }
        }
        else
        {
            uint backward = unchecked(windowBase - sequence);
            if (backward <= (uint)capacity)
            {
                int raw = ((baseSlot - (int)backward) % capacity + capacity) % capacity;
                ringSlot = raw;
                return true;
            }
        }

        ringSlot = -1;
        return false;
    }

    /// <summary>
    /// Whether every sequence of the group starting at <paramref name="groupFirstSequence"/> and
    /// running for <paramref name="fragmentCount"/> sequences has a Fragment held in
    /// <paramref name="fragmentLengths"/> — the window's own length run (issue #257), two bytes per
    /// ring slot, non-zero exactly where a Fragment is currently held. No structure is allocated to
    /// answer this: each of the group's sequences is resolved to its ring slot by
    /// <see cref="TryGetRingSlot"/> and that slot's two bytes are read directly.
    /// </summary>
    /// <param name="admitted">
    /// The window's own admission run — see <see cref="Core.ReceiveArena.GetAdmissionRun"/> — one byte
    /// per ring slot, non-zero exactly where that slot's sequence is currently admitted. Needed only to
    /// corroborate a piece still ahead of <paramref name="windowBase"/> (issue #259): see remarks.
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
    /// <see langword="true"/> if every one of the group's <paramref name="fragmentCount"/> sequences
    /// resolves to a ring slot whose recorded length is non-zero — and, for a sequence still at or
    /// ahead of <paramref name="windowBase"/>, whose admission bit is also set; <see langword="false"/>
    /// if any sequence is only partly present, or if the group is wider than the window can currently
    /// address — the same "incomplete rather than throwing" answer either way.
    /// </returns>
    /// <remarks>
    /// A piece already behind <paramref name="windowBase"/> is trusted on its length alone: the
    /// admission bit for a passed slot is cleared the instant <see cref="ReceiveWindow.AdvanceBase"/>
    /// walks over it, precisely so the length run — not the admission bit — is what survives to answer
    /// this question, and a length reused since is caught by the caller clearing it at the next fresh
    /// admission of that slot (issue #259). A piece still at or ahead of the base is different: the
    /// same physical ring slot a not-yet-arrived sequence resolves to (issue #259's own bug) can still
    /// hold a non-zero length left behind by an earlier, permanently-abandoned occupant of that slot —
    /// one whose own group never completed and whose gap this window is still stuck behind, so nothing
    /// has cleared it since. That old occupant's admission bit was already cleared the moment
    /// <see cref="ReceiveWindow.AdvanceBase"/> swept past it (which is what moved it from "ahead of
    /// base" to "behind base" in the first place); a slot that is still forward of the base and
    /// genuinely holds a live piece — one still waiting for an earlier gap to fill — has its bit set the
    /// entire time, because nothing has had a reason to touch it since it arrived. Requiring the
    /// admission bit for the forward case therefore rejects exactly the stale case and never the live
    /// one.
    /// </remarks>
    public static bool IsComplete(
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

            bool atOrAboveBase = sequence == windowBase || SequenceNumber.IsNewerThan(sequence, windowBase);
            if (atOrAboveBase && admitted[ringSlot] == 0)
            {
                // Forward of the base, but nothing is currently tracked as admitted at this ring slot:
                // the non-zero length is stale residue from an earlier, abandoned occupant of the same
                // physical slot, not this sequence's own piece — see remarks.
                return false;
            }
        }

        return true;
    }
}
