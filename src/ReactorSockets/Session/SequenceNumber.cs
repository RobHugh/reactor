namespace ReactorSockets.Session;

/// <summary>
/// Serial comparison for the datagram data path's <c>u32</c> sequence numbers (ADR 0009): the one place
/// the receive path asks "is this sequence newer than that one." Every "newer than" decision in the
/// datagram data path goes through <see cref="IsNewerThan"/> so the wrap-aware arithmetic exists
/// exactly once.
/// </summary>
/// <remarks>
/// A sequence counter wraps in every fixed width; <c>u32</c> does not remove that, it only makes the
/// wrap unreachable in practice. "Newer" is decided by which of the two directions around the
/// circle from <c>b</c> to <c>a</c> is shorter, computed as the signed difference of the unsigned
/// values: <c>a</c> is newer than <c>b</c> exactly when <c>unchecked((int)(a - b))</c> is positive.
/// This is the same rule TCP uses for its 32-bit sequence space (RFC 1982), and it only gives a
/// wrong answer when the two sequences are more than half the space (2^31) apart — a distance a
/// receive window bounded at construction never lets the two sides reach. A <c>u16</c> would put that
/// same guarantee in the consumer's hands, since it would depend on the send rate staying below one
/// wrap per window; <c>u32</c> makes it structural instead.
/// </remarks>
internal static class SequenceNumber
{
    /// <summary>
    /// Whether <paramref name="a"/> is newer than <paramref name="b"/> in serial order, correct
    /// across the wrap boundary: a sequence just above the wrap is newer than one just below it.
    /// </summary>
    /// <remarks>
    /// The subtraction is unsigned <c>u32</c> arithmetic that wraps by design, then reinterpreted as
    /// a signed <c>i32</c> to read off its direction. This is deliberately not <c>a &gt; b</c>: a
    /// plain numeric comparison treats the sequence space as a line with an edge at 0/2^32-1, which
    /// is wrong exactly at the wrap the comparison exists to handle. Equal sequences are not newer
    /// than each other, matching a receive window that must treat a duplicate as not-newer rather
    /// than as an advance.
    /// </remarks>
    public static bool IsNewerThan(uint a, uint b) => unchecked((int)(a - b)) > 0;
}
