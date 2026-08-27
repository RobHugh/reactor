namespace ReactorSockets.Session;

/// <summary>
/// Plain <see cref="DeliveryMode.Unreliable"/>'s duplicate suppression: a bounded sliding window of
/// the 64 most recent sequences, which admits a sequence it has not seen and suppresses one it has
/// (subtask 6.6.12, issue #222; ADR 0017). Suppression is bounded rather than absolute — a duplicate
/// delayed by more than 64 sequences falls out of the window and is delivered, which is the honest
/// promise the mode makes rather than a claim it cannot keep.
/// </summary>
/// <remarks>
/// <para>
/// <b>This is not the receive window.</b> The receive window's capacity is
/// <see cref="DatagramSessionOptions.ResolvedReceiveWindowFragments"/>, derived from the consumer's
/// own <see cref="DatagramSessionOptions.MaxMessageSize"/> and fragment size, while 64 here is a
/// protocol constant every Session on both ends agrees to without negotiating it. Sizing suppression
/// from the receive window would make the promise in <c>docs/delivery-modes.md</c> — a duplicate
/// within 64 is suppressed — true only for consumers who happened to choose the matching options, so
/// the two windows are kept apart and this one is not a knob. ADR 0017 has the full argument.
/// </para>
/// <para>
/// Exactly 64 entries because that is what one <see cref="ulong"/> holds: bit <c>i</c> stands for the
/// sequence <c>i</c> below <see cref="Highest"/>, so the whole window is one shift and one test, it
/// allocates nothing, and the width is not a number that could quietly drift from the constant the
/// protocol names. Comparisons go through <see cref="SequenceNumber.IsNewerThan"/>, so the window
/// stays correct across the <c>u32</c> wrap.
/// </para>
/// </remarks>
internal sealed class UnreliableDuplicateWindow
{
    /// <summary>
    /// How many sequences this window suppresses within — a protocol constant, not a knob, and equal
    /// to the bit width of the <see cref="ulong"/> that holds it.
    /// </summary>
    public const int Capacity = 64;

    private ulong _seen;
    private bool _any;

    /// <summary>
    /// The newest sequence this window has admitted. Meaningless until something has been admitted,
    /// which <see cref="_any"/> is what distinguishes: sequence zero is a real sequence a Session's
    /// first <see cref="DeliveryMode.Unreliable"/> Send uses, so a zero default could not stand in for
    /// "nothing yet".
    /// </summary>
    public uint Highest { get; private set; }

    /// <summary>
    /// Records <paramref name="sequence"/> and reports whether it should be delivered:
    /// <see langword="true"/> for a sequence this window has not seen, <see langword="false"/> for one
    /// it has and still holds. A sequence older than the window's 64 entries is admitted rather than
    /// suppressed — outside the window this type has no memory to suppress from, and delivering is what
    /// bounded suppression means.
    /// </summary>
    /// <param name="sequence">The arriving record's <c>u32</c> sequence number.</param>
    /// <returns><see langword="true"/> to deliver; <see langword="false"/> to suppress as a duplicate.</returns>
    public bool TryAdmit(uint sequence)
    {
        if (!_any)
        {
            _any = true;
            Highest = sequence;
            _seen = 1UL;
            return true;
        }

        if (SequenceNumber.IsNewerThan(sequence, Highest))
        {
            uint advance = sequence - Highest;

            // A jump of 64 or more leaves nothing of the old window in range, and shifting by 64 or
            // more is undefined for a ulong in C# (the count is masked to 6 bits), so the whole window
            // is cleared rather than shifted.
            _seen = advance >= Capacity ? 0UL : _seen << (int)advance;
            _seen |= 1UL;
            Highest = sequence;
            return true;
        }

        uint distance = Highest - sequence;
        if (distance >= Capacity)
        {
            // Older than anything this window still remembers. Suppression is bounded, so this is
            // delivered — and deliberately not recorded, since there is no bit for it to occupy.
            return true;
        }

        ulong bit = 1UL << (int)distance;
        if ((_seen & bit) != 0)
        {
            return false;
        }

        _seen |= bit;
        return true;
    }
}
