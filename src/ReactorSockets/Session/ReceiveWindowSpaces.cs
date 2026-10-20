using ReactorSockets.Core;

namespace ReactorSockets.Session;

/// <summary>
/// One Peer's four inbound receive windows (ADR 0009, issue #40): one independent
/// <see cref="ReceiveWindow"/> per delivery mode's Space, keyed by the same
/// <see cref="PacketRecordMode"/> a packet record's flags byte carries on the wire and mirroring
/// <see cref="OutboundSequenceSpaces"/>'s shape on the receive side. Four windows exist, rather than
/// one shared window, for the same reason four outbound Spaces do: a sequence admitted under one mode
/// says nothing about the same sequence under another, so sharing one window would let one mode's
/// traffic classify — and duplicate-suppress — another's.
/// </summary>
/// <remarks>
/// Every Space gets a window here, including the two unreliable ones that #171 never gives payload
/// storage: classification is the same act whether or not anything is retained, and giving all four a
/// window is what lets the same sequence be admitted independently in all four (subtask 6.2.5, issue
/// #172). Not thread-safe, matching every other piece of a Peer's state: a Peer is driven from one
/// Reactor's single synchronous <c>Pump</c>.
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
    /// <see cref="ReceiveArena.GetAdmissionRun"/> hands back for that Space (#179) — nothing here
    /// allocates. The two reliable windows additionally take the Fragment-length run
    /// <see cref="ReceiveArena.GetFragmentLengthRun"/> hands back for the matching retaining Space
    /// (#257) and the Fragment-group run <see cref="ReceiveArena.GetFragmentGroupRun"/> hands back for
    /// the same Space (issue #297); the two unreliable windows take neither, since ADR 0004 never routes
    /// a Fragment into either.
    /// </summary>
    /// <param name="arena">The receive arena a Slot's admission and Fragment-length runs come from.</param>
    /// <param name="shareHolderId">The Slot index this instance's four windows are built over.</param>
    public ReceiveWindowSpaces(ReceiveArena arena, int shareHolderId)
    {
        // Unreliable and UnreliableSequenced slide (#173): nothing retransmits into either, so a
        // window that only ever refused a far-ahead arrival would stall forever after the first real
        // loss. ReliableUnordered and ReliableOrdered keep today's behaviour — retransmission (6.4/6.5)
        // is what fills their holes, so sliding would retire data that was about to arrive.
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
    /// How many sequences <see cref="Classify(PacketRecordMode, uint)"/>'s most recent call retired un-admitted by sliding the
    /// touched Space's window forward (#173) — zero unless that call touched a sliding Space (today,
    /// <c>Unreliable</c> or <c>UnreliableSequenced</c>) and that Space's window slid. Mirrors
    /// <see cref="ReceiveWindow.LastSlideLoss"/> one level up, since a caller here does not hold a
    /// reference to whichever of the four windows <see cref="Classify(PacketRecordMode, uint)"/> touched.
    /// </summary>
    public ulong LastSlideLoss { get; private set; }

    /// <summary>
    /// Whether <see cref="Classify(PacketRecordMode, uint, out int)"/>'s most recent call discovered an
    /// incomplete Fragment set abandoned (issue #297), and if so, how much of it was held versus how
    /// much it declared — mirrors <see cref="ReceiveWindow.LastAbandonedFragmentSet"/> one level up, for
    /// the same reason <see cref="LastSlideLoss"/> does: a caller here does not hold a reference to
    /// whichever of the four windows <see cref="Classify(PacketRecordMode, uint, out int)"/> touched.
    /// </summary>
    public (bool Occurred, long Held, long Expected) LastAbandonedFragmentSet { get; private set; }

    /// <summary>
    /// Classifies <paramref name="sequence"/> against <paramref name="mode"/>'s Space alone — the other
    /// three Spaces' windows are untouched by this call, which is what keeps the four Spaces from
    /// disturbing one another. <see cref="LastSlideLoss"/> reflects whatever this call did before it
    /// returns.
    /// </summary>
    /// <param name="mode">Which of the four Spaces to classify against.</param>
    /// <param name="sequence">The arriving record's <c>u32</c> sequence number.</param>
    /// <returns>The outcome <paramref name="mode"/>'s window reports for <paramref name="sequence"/>.</returns>
    public ReceiveWindowOutcome Classify(PacketRecordMode mode, uint sequence) => Classify(mode, sequence, out _);

    /// <summary>
    /// Whether <paramref name="mode"/>'s window would admit <paramref name="sequence"/> right now —
    /// see <see cref="ReceiveWindow.WouldAdmit"/>. Changes nothing, so <see cref="LastSlideLoss"/> and
    /// <see cref="LastAbandonedFragmentSet"/> keep what the last <see cref="Classify(PacketRecordMode, uint)"/>
    /// left. <paramref name="mode"/> must be <c>ReliableOrdered</c> or <c>ReliableUnordered</c>, the
    /// two Spaces whose windows do not slide.
    /// </summary>
    public bool WouldAdmit(PacketRecordMode mode, uint sequence) => mode switch
    {
        PacketRecordMode.ReliableUnordered => _reliableUnordered.WouldAdmit(sequence),
        PacketRecordMode.ReliableOrdered => _reliableOrdered.WouldAdmit(sequence),
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Only the two reliable Spaces' windows can be asked."),
    };

    /// <summary>
    /// <see cref="Classify(PacketRecordMode, uint)"/>, additionally reporting the ring slot
    /// <paramref name="sequence"/> resolves to in <paramref name="mode"/>'s window — see
    /// <see cref="ReceiveWindow.Classify(uint, out int)"/>. A caller holding a Fragment's payload
    /// passes this back into <see cref="RecordFragmentLength"/> and
    /// <see cref="ReceiveArena.GetFragmentSlot"/> to divert it (#257).
    /// </summary>
    /// <param name="mode">Which of the four Spaces to classify against.</param>
    /// <param name="sequence">The arriving record's <c>u32</c> sequence number.</param>
    /// <param name="ringSlot">The ring position <paramref name="sequence"/> resolves to; -1 for <see cref="ReceiveWindowOutcome.OutOfWindow"/>.</param>
    /// <returns>The outcome <paramref name="mode"/>'s window reports for <paramref name="sequence"/>.</returns>
    public ReceiveWindowOutcome Classify(PacketRecordMode mode, uint sequence, out int ringSlot)
    {
        ReceiveWindow window = mode switch
        {
            PacketRecordMode.Unreliable => _unreliable,
            PacketRecordMode.UnreliableSequenced => _unreliableSequenced,
            PacketRecordMode.ReliableUnordered => _reliableUnordered,
            PacketRecordMode.ReliableOrdered => _reliableOrdered,
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown delivery mode."),
        };

        ReceiveWindowOutcome outcome = window.Classify(sequence, out ringSlot);
        LastSlideLoss = window.LastSlideLoss;
        LastAbandonedFragmentSet = window.LastAbandonedFragmentSet;
        return outcome;
    }

    /// <summary>
    /// Records that the Fragment occupying ring slot <paramref name="ringSlot"/> of
    /// <paramref name="mode"/>'s window is <paramref name="byteLength"/> bytes long (#257) — see
    /// <see cref="ReceiveWindow.RecordFragmentLength"/>. <paramref name="mode"/> must be
    /// <c>ReliableOrdered</c> or <c>ReliableUnordered</c>, the only two Spaces ADR 0004 ever routes a
    /// Fragment into.
    /// </summary>
    public void RecordFragmentLength(PacketRecordMode mode, int ringSlot, ushort byteLength) =>
        ReliableWindow(mode).RecordFragmentLength(ringSlot, byteLength);

    /// <summary>
    /// Records that the Fragment occupying ring slot <paramref name="ringSlot"/> of
    /// <paramref name="mode"/>'s window belongs to the group starting at
    /// <paramref name="groupFirstSequence"/> and declaring <paramref name="fragmentCount"/> pieces
    /// (issue #297) — see <see cref="ReceiveWindow.RecordFragmentGroup"/>. <paramref name="mode"/> must
    /// be <c>ReliableOrdered</c> or <c>ReliableUnordered</c>, the only two Spaces ADR 0004 ever routes a
    /// Fragment into.
    /// </summary>
    public void RecordFragmentGroup(PacketRecordMode mode, int ringSlot, uint groupFirstSequence, ushort fragmentCount) =>
        ReliableWindow(mode).RecordFragmentGroup(ringSlot, groupFirstSequence, fragmentCount);

    /// <summary>
    /// Reads back what <see cref="RecordFragmentLength"/> most recently recorded for ring slot
    /// <paramref name="ringSlot"/> of <paramref name="mode"/>'s window (#257, #258) — see
    /// <see cref="ReceiveWindow.GetFragmentLength"/>.
    /// </summary>
    public ushort GetFragmentLength(PacketRecordMode mode, int ringSlot) =>
        ReliableWindow(mode).GetFragmentLength(ringSlot);

    /// <summary>
    /// Whether every Fragment of the group starting at <paramref name="groupFirstSequence"/> and
    /// declaring <paramref name="fragmentCount"/> pieces is currently held in <paramref name="mode"/>'s
    /// window (issue #258) — see <see cref="ReceiveWindow.IsGroupComplete"/>. Reads that one window's
    /// state alone: the other three Spaces, and every piece of ack or retransmit state, play no part.
    /// </summary>
    /// <param name="mode">Which reliable Space's window to test — <c>ReliableOrdered</c> or <c>ReliableUnordered</c>.</param>
    /// <param name="groupFirstSequence">The group's first sequence — see <see cref="FragmentGroup.FirstSequenceOf"/>.</param>
    /// <param name="fragmentCount">The group's declared Fragment count.</param>
    public bool IsGroupComplete(PacketRecordMode mode, uint groupFirstSequence, ushort fragmentCount) =>
        ReliableWindow(mode).IsGroupComplete(groupFirstSequence, fragmentCount);

    /// <summary>
    /// Resolves <paramref name="sequence"/> to the ring slot it occupies in <paramref name="mode"/>'s
    /// window right now (issue #259) — see <see cref="ReceiveWindow.TryGetRingSlot"/>. What a caller
    /// delivering a completed group's pieces uses to find each piece's payload slot
    /// (<see cref="Core.ReceiveArena.GetFragmentSlot"/>) and recorded length
    /// (<see cref="GetFragmentLength"/>) without re-deriving the window's ring arithmetic itself.
    /// </summary>
    /// <param name="mode">Which reliable Space's window to resolve against — <c>ReliableOrdered</c> or <c>ReliableUnordered</c>.</param>
    /// <param name="sequence">The sequence to resolve.</param>
    /// <param name="ringSlot">The ring position <paramref name="sequence"/> occupies; -1 if not addressable.</param>
    public bool TryGetRingSlot(PacketRecordMode mode, uint sequence, out int ringSlot) =>
        ReliableWindow(mode).TryGetRingSlot(sequence, out ringSlot);

    /// <summary>
    /// Releases every sequence of the group starting at <paramref name="groupFirstSequence"/> in
    /// <paramref name="mode"/>'s window (issue #259) — see <see cref="ReceiveWindow.ReleaseGroup"/>.
    /// Called once a completed group's pieces have all been copied into one delivered Message, so the
    /// window's space is returned and a later Fragment reusing one of these ring slots is never
    /// mistaken for a leftover of the group just delivered.
    /// </summary>
    /// <param name="mode">Which reliable Space's window to release from — <c>ReliableOrdered</c> or <c>ReliableUnordered</c>.</param>
    /// <param name="groupFirstSequence">The group's first sequence — see <see cref="FragmentGroup.FirstSequenceOf"/>.</param>
    /// <param name="fragmentCount">The group's declared Fragment count.</param>
    public void ReleaseGroup(PacketRecordMode mode, uint groupFirstSequence, ushort fragmentCount) =>
        ReliableWindow(mode).ReleaseGroup(groupFirstSequence, fragmentCount);

    /// <summary>The reliable window <paramref name="mode"/> names, for the two Fragment-length accessors above.</summary>
    private ReceiveWindow ReliableWindow(PacketRecordMode mode) => mode switch
    {
        PacketRecordMode.ReliableOrdered => _reliableOrdered,
        PacketRecordMode.ReliableUnordered => _reliableUnordered,
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Only reliable delivery modes retain a Fragment."),
    };

    /// <summary>
    /// Generates both ack blocks for this Peer, fresh from the current state of the two reliable
    /// Spaces' windows (issue #186, ADR 0009): <c>ReliableOrdered</c>'s block first, then
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
    /// reports the identical block again. The two reliable windows are independent (#40): an
    /// admission in one changes neither the base nor the bits this method reports for the other.
    /// Allocation-free, since each block is <see cref="ReceiveWindow.DescribeAsAckBlock"/>'s
    /// value-typed result.
    /// </remarks>
    /// <returns>
    /// The <c>ReliableOrdered</c> Space's ack block, then the <c>ReliableUnordered</c> Space's —
    /// each a cumulative base plus the 32-bit field describing the sequences above it, per
    /// <see cref="ReceiveWindow.DescribeAsAckBlock"/>.
    /// </returns>
    public ((uint Base, uint Bits) ReliableOrdered, (uint Base, uint Bits) ReliableUnordered) GenerateAckBlocks()
    {
        return (_reliableOrdered.DescribeAsAckBlock(), _reliableUnordered.DescribeAsAckBlock());
    }

    /// <summary>
    /// Resets all four windows (<see cref="ReceiveWindow.Reset"/>) so a Slot a new Peer reuses
    /// classifies from a clean state, inheriting none of the previous occupant's admissions (#179).
    /// Called at both admission sites — <see cref="DatagramSessionReactor.Connect"/> and the
    /// accepted-Peer path in <see cref="DatagramSessionReactor.OnHelloWithCookie"/> — right after the
    /// Slot's handle is assigned.
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
    /// The completed-but-undelivered Fragment groups of <paramref name="mode"/>'s Space (issue #259) —
    /// the ones waiting for their turn in that Space's send-order queue, for frame arena room, or both.
    /// Held here, beside the windows, because a waiting group's pieces are held in the window itself
    /// and the two are reset together when a Slot is reused.
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
