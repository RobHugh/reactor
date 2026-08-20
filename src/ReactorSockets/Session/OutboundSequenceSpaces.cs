namespace ReactorSockets.Session;

/// <summary>
/// One Peer's four outbound sequence allocators (ADR 0009, issue #40): one independent <c>u32</c>
/// counter per delivery mode's Space, keyed by the same <see cref="PacketRecordMode"/> a packet
/// record's flags byte carries on the wire. Four Spaces exist, rather than one shared counter,
/// because a receiver missing sequence 12 cannot know what mode 12 was — the flags of a packet that
/// never arrived are not available — so whatever shares a Space shares head-of-line blocking whether
/// or not that was intended. Plain <see cref="PacketRecordMode.Unreliable"/> gets a Space and a
/// sequence number like every other mode; the independence costs nothing on the wire because the mode
/// bits already identify the Space.
/// </summary>
/// <remarks>
/// Each Space starts at zero (settled in 5.4, not a random initial sequence number) and wraps through
/// the full <c>u32</c> range: allocation is an explicitly <c>unchecked</c> post-increment, so the
/// counter returns to zero after <see cref="uint.MaxValue"/> rather than throwing, and stays that way
/// if the project ever turns on <c>CheckForOverflowUnderflow</c>. Ordering across that wrap is decided
/// solely by <see cref="SequenceNumber.IsNewerThan"/>; this type does no comparison of its own. Not
/// thread-safe, matching every other piece of a Peer's state (ADR 0009): a Peer is driven from one
/// Reactor's single synchronous <c>Pump</c>.
/// </remarks>
internal sealed class OutboundSequenceSpaces
{
    private uint _unreliable;
    private uint _unreliableSequenced;
    private uint _reliableUnordered;
    private uint _reliableOrdered;

    /// <summary>
    /// Allocates the next sequence number in <paramref name="mode"/>'s Space and advances that Space
    /// by one. The first value allocated in a Space is zero. Allocating in one Space never moves any
    /// other Space's next sequence, and the counter wraps through the full <c>u32</c> range rather
    /// than overflowing.
    /// </summary>
    /// <param name="mode">Which of the four Spaces to allocate from.</param>
    /// <returns>The sequence number this allocation claimed.</returns>
    public uint Allocate(PacketRecordMode mode) => mode switch
    {
        PacketRecordMode.Unreliable => unchecked(_unreliable++),
        PacketRecordMode.UnreliableSequenced => unchecked(_unreliableSequenced++),
        PacketRecordMode.ReliableUnordered => unchecked(_reliableUnordered++),
        PacketRecordMode.ReliableOrdered => unchecked(_reliableOrdered++),
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown delivery mode."),
    };
}
