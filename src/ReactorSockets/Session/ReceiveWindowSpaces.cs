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

    /// <summary>
    /// Builds four windows, each of <paramref name="capacityFragments"/> sequence slots starting at
    /// sequence zero — where every Space's numbering begins (ADR 0009).
    /// </summary>
    /// <param name="capacityFragments">
    /// The span, in sequence numbers, each of the four windows covers. Passed straight to
    /// <see cref="ReceiveWindow(int, bool)"/>, which re-validates it.
    /// </param>
    public ReceiveWindowSpaces(int capacityFragments)
    {
        // Unreliable and UnreliableSequenced slide (#173): nothing retransmits into either, so a
        // window that only ever refused a far-ahead arrival would stall forever after the first real
        // loss. ReliableUnordered and ReliableOrdered keep today's behaviour — retransmission (6.4/6.5)
        // is what fills their holes, so sliding would retire data that was about to arrive.
        _unreliable = new ReceiveWindow(capacityFragments, slides: true);
        _unreliableSequenced = new ReceiveWindow(capacityFragments, slides: true);
        _reliableUnordered = new ReceiveWindow(capacityFragments);
        _reliableOrdered = new ReceiveWindow(capacityFragments);
    }

    /// <summary>
    /// How many sequences <see cref="Classify"/>'s most recent call retired un-admitted by sliding the
    /// touched Space's window forward (#173) — zero unless that call touched a sliding Space (today,
    /// <c>Unreliable</c> or <c>UnreliableSequenced</c>) and that Space's window slid. Mirrors
    /// <see cref="ReceiveWindow.LastSlideLoss"/> one level up, since a caller here does not hold a
    /// reference to whichever of the four windows <see cref="Classify"/> touched.
    /// </summary>
    public ulong LastSlideLoss { get; private set; }

    /// <summary>
    /// Classifies <paramref name="sequence"/> against <paramref name="mode"/>'s Space alone — the other
    /// three Spaces' windows are untouched by this call, which is what keeps the four Spaces from
    /// disturbing one another. <see cref="LastSlideLoss"/> reflects whatever this call did before it
    /// returns.
    /// </summary>
    /// <param name="mode">Which of the four Spaces to classify against.</param>
    /// <param name="sequence">The arriving record's <c>u32</c> sequence number.</param>
    /// <returns>The outcome <paramref name="mode"/>'s window reports for <paramref name="sequence"/>.</returns>
    public ReceiveWindowOutcome Classify(PacketRecordMode mode, uint sequence)
    {
        ReceiveWindow window = mode switch
        {
            PacketRecordMode.Unreliable => _unreliable,
            PacketRecordMode.UnreliableSequenced => _unreliableSequenced,
            PacketRecordMode.ReliableUnordered => _reliableUnordered,
            PacketRecordMode.ReliableOrdered => _reliableOrdered,
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown delivery mode."),
        };

        ReceiveWindowOutcome outcome = window.Classify(sequence);
        LastSlideLoss = window.LastSlideLoss;
        return outcome;
    }
}
