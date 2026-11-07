using ReactorSockets.Core;

namespace ReactorSockets.Session;

/// <summary>
/// One Peer's four inbound receive windows (ADR 0009): one independent <see cref="ReceiveWindow"/> per
/// delivery mode's Space, keyed by the same <see cref="PacketRecordMode"/> a packet record's flags byte
/// carries on the wire and mirroring <see cref="OutboundSequenceSpaces"/>'s shape on the receive side.
/// Four windows exist, rather than one shared window, for the same reason four outbound Spaces do: a
/// sequence admitted under one mode says nothing about the same sequence under another, so sharing one
/// window would let one mode's traffic classify — and duplicate-suppress — another's.
/// </summary>
/// <remarks>
/// Every Space gets a window here, including the two unreliable ones that get no payload storage:
/// classification is the same act whether or not anything is retained, and giving all four a window is
/// what lets the same sequence be admitted independently in all four. Not thread-safe, matching every
/// other piece of a Peer's state: a Peer is driven from one Reactor's single synchronous <c>Pump</c>.
/// </remarks>
internal sealed class ReceiveWindowSpaces
{
    private readonly ReceiveWindow _unreliable;
    private readonly ReceiveWindow _unreliableSequenced;
    private readonly ReceiveWindow _reliableUnordered;
    private readonly ReceiveWindow _reliableOrdered;
    private readonly PendingFragmentGroups _pendingReliableUnordered;
    private readonly PendingFragmentGroups _pendingReliableOrdered;

    /// <summary>
    /// The reliable Space index <see cref="ReceiveArena.GetFragmentSlot"/> and
    /// <see cref="ReceiveArena.GetFragmentLengthRun"/> address a retaining Space under —
    /// <c>ReliableOrdered</c> first, <c>ReliableUnordered</c> second, the same order
    /// <see cref="GenerateAckBlocks"/> and <see cref="DatagramSessionReactor.ParseDataDatagram"/>'s ack
    /// application already use, so send and receive never address the two reliable Spaces differently.
    /// </summary>
    private static int RetainingSpaceIndex(PacketRecordMode mode) => mode switch
    {
        PacketRecordMode.ReliableOrdered => 0,
        PacketRecordMode.ReliableUnordered => 1,
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Not a reliable delivery mode."),
    };

    /// <summary>
    /// Builds four windows for <paramref name="shareHolderId"/>'s Slot, one per
    /// <see cref="PacketRecordMode"/>, each over the admission run
    /// <see cref="ReceiveArena.GetAdmissionRun"/> hands back for that Space — nothing here allocates.
    /// The two reliable windows additionally take the Fragment-length run
    /// <see cref="ReceiveArena.GetFragmentLengthRun"/> hands back for the matching retaining Space and
    /// the Fragment-group run <see cref="ReceiveArena.GetFragmentGroupRun"/> hands back for the same
    /// Space; the two unreliable windows take neither, since ADR 0004 never routes a Fragment into
    /// either.
    /// </summary>
    /// <param name="arena">The receive arena a Slot's admission and Fragment-length runs come from.</param>
    /// <param name="shareHolderId">The Slot index this instance's four windows are built over.</param>
    public ReceiveWindowSpaces(ReceiveArena arena, int shareHolderId)
    {
        // Unreliable and UnreliableSequenced slide (#173): nothing retransmits into either, so a
        // window that only ever refused a far-ahead arrival would stall forever after the first real
        // loss. ReliableUnordered and ReliableOrdered do not slide: retransmission fills their holes,
        // so sliding would retire data that was about to arrive.
        _unreliable = new ReceiveWindow(
            arena.GetAdmissionRun(shareHolderId, (int)PacketRecordMode.Unreliable), slides: true);
        _unreliableSequenced = new ReceiveWindow(
            arena.GetAdmissionRun(shareHolderId, (int)PacketRecordMode.UnreliableSequenced), slides: true);
        _reliableUnordered = new ReceiveWindow(
            arena.GetAdmissionRun(shareHolderId, (int)PacketRecordMode.ReliableUnordered),
            fragmentLengths: arena.GetFragmentLengthRun(shareHolderId, RetainingSpaceIndex(PacketRecordMode.ReliableUnordered)),
            fragmentGroups: arena.GetFragmentGroupRun(shareHolderId, RetainingSpaceIndex(PacketRecordMode.ReliableUnordered)));
        _reliableOrdered = new ReceiveWindow(
            arena.GetAdmissionRun(shareHolderId, (int)PacketRecordMode.ReliableOrdered),
            fragmentLengths: arena.GetFragmentLengthRun(shareHolderId, RetainingSpaceIndex(PacketRecordMode.ReliableOrdered)),
            fragmentGroups: arena.GetFragmentGroupRun(shareHolderId, RetainingSpaceIndex(PacketRecordMode.ReliableOrdered)));

        _pendingReliableOrdered = new PendingFragmentGroups(
            arena.GetPendingGroupRun(shareHolderId, RetainingSpaceIndex(PacketRecordMode.ReliableOrdered)));
        _pendingReliableUnordered = new PendingFragmentGroups(
            arena.GetPendingGroupRun(shareHolderId, RetainingSpaceIndex(PacketRecordMode.ReliableUnordered)));
    }

    /// <summary>
    /// <paramref name="mode"/>'s own window. Classifying against it touches that Space alone: the other
    /// three Spaces' windows are untouched, which is what keeps the four Spaces from disturbing one
    /// another.
    /// </summary>
    /// <param name="mode">Which of the four Spaces' windows to reach.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode"/> is not a delivery mode.</exception>
    public ReceiveWindow Window(PacketRecordMode mode) => mode switch
    {
        PacketRecordMode.Unreliable => _unreliable,
        PacketRecordMode.UnreliableSequenced => _unreliableSequenced,
        PacketRecordMode.ReliableUnordered => _reliableUnordered,
        PacketRecordMode.ReliableOrdered => _reliableOrdered,
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown delivery mode."),
    };

    /// <summary>
    /// <paramref name="mode"/>'s window, restricted to the two reliable Spaces: the only two that never
    /// slide and that ADR 0004 ever routes a Fragment into. Callers asking whether a sequence would be
    /// admitted, or reading and writing Fragment bookkeeping, reach the window through this, so asking an
    /// unreliable Space is refused rather than answered by a sliding window.
    /// </summary>
    /// <param name="mode">Which reliable Space's window to reach.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode"/> is not a reliable mode.</exception>
    public ReceiveWindow ReliableWindow(PacketRecordMode mode) => mode switch
    {
        PacketRecordMode.ReliableOrdered => _reliableOrdered,
        PacketRecordMode.ReliableUnordered => _reliableUnordered,
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Only reliable delivery modes retain a Fragment."),
    };

    /// <summary>
    /// Generates both ack blocks for this Peer, fresh from the current state of the two reliable
    /// Spaces' windows (ADR 0009): <c>ReliableOrdered</c>'s block first, then
    /// <c>ReliableUnordered</c>'s — the order <see cref="DatagramHeaderCodec.WriteHeader"/> lays
    /// them out in. Both are always produced, whatever has been received: the wire format has no
    /// field to say one is absent, so a Peer with nothing yet admitted in a Space still gets that
    /// Space's zeroed block rather than no block at all. The two unreliable Spaces are observed,
    /// never acknowledged, and play no part in either block.
    /// </summary>
    /// <remarks>
    /// A pure read, delegating to <see cref="ReceiveWindow.DescribeAsAckBlock"/> on each of the two
    /// reliable windows: nothing here classifies, admits, or moves a base, and nothing is
    /// accumulated across calls — every call reflects only the two windows' present state, so an
    /// admission in either Space between two calls shows up immediately and an unchanged Space
    /// reports the identical block again. The two reliable windows are independent: an admission in one
    /// changes neither the base nor the bits this method reports for the other. Allocation-free, since
    /// each block is <see cref="ReceiveWindow.DescribeAsAckBlock"/>'s value-typed result.
    /// </remarks>
    /// <returns>
    /// The <c>ReliableOrdered</c> Space's ack block, then the <c>ReliableUnordered</c> Space's —
    /// each a cumulative base plus the 32-bit field describing the sequences above it, per
    /// <see cref="ReceiveWindow.DescribeAsAckBlock"/>.
    /// </returns>
    public (AckBlock ReliableOrdered, AckBlock ReliableUnordered) GenerateAckBlocks()
    {
        return (_reliableOrdered.DescribeAsAckBlock(), _reliableUnordered.DescribeAsAckBlock());
    }

    /// <summary>
    /// Resets all four windows (<see cref="ReceiveWindow.Reset"/>) so a Slot a new Peer reuses
    /// classifies from a clean state, inheriting none of the previous occupant's admissions. Called at
    /// both admission sites — <see cref="DatagramSessionReactor.Connect"/> and the accepted-Peer path
    /// in <see cref="DatagramSessionReactor.OnHelloWithCookie"/> — right after the Slot's handle is
    /// assigned.
    /// </summary>
    public void Reset()
    {
        _unreliable.Reset();
        _unreliableSequenced.Reset();
        _reliableUnordered.Reset();
        _reliableOrdered.Reset();
        _pendingReliableUnordered.Reset();
        _pendingReliableOrdered.Reset();
    }

    /// <summary>
    /// The completed-but-undelivered Fragment groups of <paramref name="mode"/>'s Space — the ones
    /// waiting for their turn in that Space's send-order queue, for frame arena room, or both. Held
    /// here, beside the windows, because a waiting group's pieces are held in the window itself and the
    /// two are reset together when a Slot is reused.
    /// </summary>
    /// <param name="mode">Which reliable Space's waiting groups to reach.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode"/> is not a reliable mode; only those ever fragment (ADR 0004).</exception>
    public PendingFragmentGroups PendingGroupsFor(PacketRecordMode mode) => mode switch
    {
        PacketRecordMode.ReliableOrdered => _pendingReliableOrdered,
        PacketRecordMode.ReliableUnordered => _pendingReliableUnordered,
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Not a reliable delivery mode."),
    };
}
