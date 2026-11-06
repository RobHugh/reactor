using System.Diagnostics;
using ReactorSockets.Session;

namespace ReactorSockets;

/// <summary>
/// A Fragment's group identity (ADR 0009): no message ID exists on the wire, so a group is named by
/// where it starts rather than by a number stamped on it. <see cref="FirstSequenceOf"/> is the one
/// place <c>firstSeq = seq - fragmentIndex</c> is computed, and <see cref="AssertContiguousSet"/> is
/// the debug check the send path calls when it emits a set, so a sender bug that would make the naming
/// scheme lie is caught where the set is built rather than inferred later by a receiver.
/// </summary>
/// <remarks>
/// <para>
/// Public for the same reason <see cref="FragmentPlan"/> is (ADR 0015): a test pins the <c>u32</c>-wrap
/// identity by calling <see cref="FirstSequenceOf"/> directly with hand-picked sequences and fragment
/// indices, with no <c>InternalsVisibleTo</c> and no interface invented for the purpose. It is not part
/// of a consumer's request path — nothing here is reached by name from application code — but the seam
/// this library tests through has no other way to reach arithmetic this precise.
/// </para>
/// <para>
/// Added as a sibling to <see cref="SequenceNumber"/> rather than folded into it: that type answers
/// one question, whether <c>a</c> is newer than <c>b</c>, and has no notion of a Fragment or a group.
/// This type answers a different one, which group a Fragment belongs to and whether a claimed group is
/// well-formed, and reuses <see cref="SequenceNumber"/>'s house rule — wrap-aware arithmetic is
/// <c>unchecked</c> <c>u32</c> subtraction, never a signed or widened comparison — rather than
/// open-coding it a second time. Keeping the two types separate keeps each file's XML doc answering one
/// question instead of two.
/// </para>
/// </remarks>
public static class FragmentGroup
{
    /// <summary>
    /// The first sequence of the group a Fragment at <paramref name="sequence"/> and
    /// <paramref name="fragmentIndex"/> belongs to: <c>firstSeq = seq - fragmentIndex</c>, computed as
    /// unsigned <c>u32</c> subtraction so a group straddling the wrap — one whose first sequence is
    /// near <see cref="uint.MaxValue"/> and whose later pieces have wrapped back around to a small
    /// number — still names itself as the same first sequence every piece agrees on, rather than a
    /// value corrupted by treating the wrap as a discontinuity.
    /// </summary>
    /// <remarks>
    /// This is the whole naming scheme. No message ID exists on the wire (ADR 0009): contiguity is the
    /// invariant standing in for one — every Fragment of one Message occupies a sequence one past the
    /// previous, so the group's first sequence is recoverable from any single piece plus its index, and
    /// two pieces with the same <paramref name="sequence"/> minus <paramref name="fragmentIndex"/> are,
    /// by construction, pieces of the same Message. A four-byte message ID would buy nothing over this:
    /// it would still need contiguity to reassemble the pieces in order, so it would cost four bytes on
    /// every Fragment of every Message to duplicate an identity this subtraction already recovers for
    /// free from fields the wire carries anyway.
    /// </remarks>
    /// <param name="sequence">The Fragment's own <c>u32</c> sequence number.</param>
    /// <param name="fragmentIndex">The Fragment's zero-based position within its group.</param>
    /// <returns>The group's first sequence — the sequence its piece at index 0 carries.</returns>
    public static uint FirstSequenceOf(uint sequence, ushort fragmentIndex) => unchecked(sequence - fragmentIndex);

    /// <summary>
    /// Debug-only assertion the send path calls when it emits a set of Fragments for one Message: the
    /// sequences claimed as one group must be contiguous — each one exactly one past the last in the
    /// sequence space's own wrap-aware arithmetic — and their count must match
    /// <paramref name="declaredFragmentCount"/>, the value the wire's fragment count field will carry.
    /// A future sender bug that hands this a gap, a duplicate or a set of the wrong length is caught
    /// here, at the point the set is built, rather than left for a receiver to infer from a symptom
    /// downstream.
    /// </summary>
    /// <remarks>
    /// A no-op outside <c>DEBUG</c> builds, matching every other debug-only guard in this codebase: the
    /// contiguity invariant is enforced by construction in the send path that builds these sets, so this
    /// exists to catch a defect in that construction, not to police an untrusted input — a remote peer's
    /// Fragments are validated by <see cref="PacketRecordCodec.TryRead"/>, not by this method. Public for
    /// the same reason <see cref="FirstSequenceOf"/> is (ADR 0015): the send path only ever hands this
    /// a set it just built correctly, so the happy path exercised through
    /// <see cref="DatagramSessionReactor.Send"/> can never reach the gap or length-mismatch it guards
    /// against. Those two failure shapes are unreachable through the public seam at any cost — not
    /// expensive, unreachable — so a test pins them by calling this method directly with a hand-built
    /// bad set, with no <c>InternalsVisibleTo</c> and no interface invented for the purpose.
    /// </remarks>
    /// <param name="sequencesInFragmentIndexOrder">
    /// The group's sequences, ordered by ascending fragment index — <c>sequencesInFragmentIndexOrder[i]</c>
    /// is the sequence of the Fragment at index <c>i</c>.
    /// </param>
    /// <param name="declaredFragmentCount">The fragment count the set is being emitted to claim on the wire.</param>
    [Conditional("DEBUG")]
    public static void AssertContiguousSet(ReadOnlySpan<uint> sequencesInFragmentIndexOrder, ushort declaredFragmentCount)
    {
        Debug.Assert(
            sequencesInFragmentIndexOrder.Length == declaredFragmentCount,
            "A Fragment set's length must match the fragment count it declares on the wire.");

        for (int i = 1; i < sequencesInFragmentIndexOrder.Length; i++)
        {
            Debug.Assert(
                unchecked(sequencesInFragmentIndexOrder[i] - sequencesInFragmentIndexOrder[i - 1]) == 1,
                "A Fragment set's sequences must be contiguous: each one exactly one past the last.");
        }
    }
}
