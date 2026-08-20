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
    /// <see cref="ReceiveWindow(int, uint)"/>, which re-validates it.
    /// </param>
    public ReceiveWindowSpaces(int capacityFragments)
    {
        _unreliable = new ReceiveWindow(capacityFragments);
        _unreliableSequenced = new ReceiveWindow(capacityFragments);
        _reliableUnordered = new ReceiveWindow(capacityFragments);
        _reliableOrdered = new ReceiveWindow(capacityFragments);
    }

    /// <summary>
    /// Classifies <paramref name="sequence"/> against <paramref name="mode"/>'s Space alone — the other
    /// three Spaces' windows are untouched by this call, which is what keeps the four Spaces from
    /// disturbing one another.
    /// </summary>
    /// <param name="mode">Which of the four Spaces to classify against.</param>
    /// <param name="sequence">The arriving record's <c>u32</c> sequence number.</param>
    /// <returns>The outcome <paramref name="mode"/>'s window reports for <paramref name="sequence"/>.</returns>
    public ReceiveWindowOutcome Classify(PacketRecordMode mode, uint sequence) => mode switch
    {
        PacketRecordMode.Unreliable => _unreliable.Classify(sequence),
        PacketRecordMode.UnreliableSequenced => _unreliableSequenced.Classify(sequence),
        PacketRecordMode.ReliableUnordered => _reliableUnordered.Classify(sequence),
        PacketRecordMode.ReliableOrdered => _reliableOrdered.Classify(sequence),
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown delivery mode."),
    };
}
