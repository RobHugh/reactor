namespace ReactorSockets;

/// <summary>
/// One Peer's raw material for the fragment-size stall signature (subtask 7.5.3.1, issue #294; parent
/// #269): three facts — when an unfragmented record was last acked, whether any Fragment has ever been
/// acked, and how many retransmission rounds have elapsed since the first still-unacked Fragment was
/// sent — tracked from the existing ack and retransmission paths with no timer or sweep of its own. This
/// type holds state only; the predicate that reads it and decides whether the signature fires is a
/// separate ticket (issue #298).
/// </summary>
/// <remarks>
/// <para>
/// Public for the same reason <see cref="RetransmissionTimer"/>, <see cref="RoundTripEstimator"/> and
/// <see cref="PumpIntervalObserver"/> are (ADR 0015): a test drives this directly by constructing one and
/// calling <see cref="RecordFragmentSent"/> / <see cref="RecordUnfragmentedRecordAcked"/> /
/// <see cref="RecordFragmentAcked"/> / <see cref="RecordRetransmissionRound"/> against hand-picked
/// timestamps, with no <c>InternalsVisibleTo</c> and no interface invented for the purpose.
/// </para>
/// <para>
/// <b>Why an acked Fragment latches permanently.</b> The signature this state feeds exists to recognise a
/// path that cannot carry a Fragment at all — a size boundary, not ordinary loss. A path that has carried
/// even one Fragment is demonstrably not such a path, however badly it behaves afterwards, so
/// <see cref="RecordFragmentAcked"/> sets <see cref="HasAckedFragment"/> for this Peer's whole remaining
/// lifetime. However late that ack arrives, the state can never again produce the signature: a predicate
/// that still fired after proof to the contrary would be diagnosing history, not the path as it stands.
/// </para>
/// <para>
/// Both timestamps are monotonic <see cref="TimeProvider.GetTimestamp"/> readings, never
/// <see cref="TimeProvider.GetUtcNow"/>, matching <see cref="Session.OutstandingReliableSequences"/>'s own
/// discipline (issue #42): a wall-clock reading is exactly the state a clock adjustment could use to move,
/// and a monotonic timestamp cannot be moved by one. <see langword="null"/> means the fact has never
/// happened, not "happened at time zero".
/// </para>
/// <para>
/// Not thread-safe, matching every other piece of a Peer's state (ADR 0009): a Peer is driven from one
/// Reactor's single synchronous Pump. Allocates nothing after construction.
/// </para>
/// </remarks>
public sealed class FragmentStallSignature
{
    /// <summary>
    /// The number of elapsed retransmission rounds (<see cref="RecordRetransmissionRound"/>, one per
    /// Peer per sweep that found something expired, not one per Fragment) <see
    /// cref="RoundsSinceFirstUnackedFragment"/> must reach before <see cref="IsFragmentSizeStall"/> can
    /// fire. Four.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Chosen against a false positive first. A Message split into Fragments has at least two of them,
    /// so on a path that ordinarily loses one packet in ten, going four rounds with every Fragment lost
    /// in every round means losing on the order of eight independent packets in a row: roughly
    /// 0.1 to the 8th power. Unfragmented records kept being acked the whole time this was happening.
    /// Ordinary loss does not reach four rounds of that; a size boundary does, because every Fragment
    /// is lost every round by construction rather than by chance.
    /// </para>
    /// <para>
    /// Chosen against firing too late second. <see cref="RetransmissionTimer"/>'s backoff doubles on
    /// each expiry, so four rounds elapse in roughly fifteen RTOs total (1 + 2 + 4 + 8). That is far
    /// short of any Session liveness timeout, so a consumer learns the fragment size is the problem
    /// long before a Linger-driven timeout would have told them only that the Peer was gone.
    /// </para>
    /// </remarks>
    public const int StallRoundCount = 4;

    private readonly TimeProvider _time;

    /// <summary>
    /// Builds a fresh signature for one Peer, with no history yet: no ack of either kind has been
    /// recorded and no Fragment window is open.
    /// </summary>
    /// <param name="time">The monotonic time source every timestamp recorded here is read from.</param>
    public FragmentStallSignature(TimeProvider time)
    {
        ArgumentNullException.ThrowIfNull(time);
        _time = time;
    }

    /// <summary>
    /// The moment (<see cref="TimeProvider.GetTimestamp"/>) an unfragmented reliable record was last
    /// acknowledged, or <see langword="null"/> if none has been yet. This is the half of the stall
    /// signature that rules out a dead path: unfragmented traffic still being acked proves the Peer is
    /// reachable and its acks are arriving, so a Fragment's silence cannot be attributed to the path
    /// being down.
    /// </summary>
    public long? LastUnfragmentedAckTimestamp { get; private set; }

    /// <summary>
    /// The moment (<see cref="TimeProvider.GetTimestamp"/>) the first Fragment of the current unacked
    /// window was sent, or <see langword="null"/> when no Fragment window is open — either none has been
    /// sent yet, or the last one open was closed by <see cref="RecordFragmentAcked"/>. Marks where
    /// <see cref="RoundsSinceFirstUnackedFragment"/> starts counting from.
    /// </summary>
    public long? FirstUnackedFragmentSentTimestamp { get; private set; }

    /// <summary>
    /// Whether any Fragment has ever been acknowledged for this Peer. Once <see langword="true"/> it
    /// never returns to <see langword="false"/> — see the type remarks on why the latch is permanent.
    /// This is the half of the stall signature that rules out ordinary loss: a Fragment having ever
    /// arrived means the path can carry Fragments, so silence afterwards is not the size boundary the
    /// signature looks for.
    /// </summary>
    public bool HasAckedFragment { get; private set; }

    /// <summary>
    /// How many retransmission rounds have elapsed for this Peer since
    /// <see cref="FirstUnackedFragmentSentTimestamp"/> opened, with no Fragment acked since. Zero
    /// whenever no Fragment window is open. This is what turns "a Fragment hasn't been acked yet" into
    /// "a Fragment hasn't been acked across several rounds", the count the predicate over this state
    /// compares against a chosen threshold to distinguish a size boundary from an ordinary retransmit in
    /// flight.
    /// </summary>
    public int RoundsSinceFirstUnackedFragment { get; private set; }

    /// <summary>
    /// Records that a reliable Fragment was just packed and sent — including a retransmit of one already
    /// sent. Opens the unacked-Fragment window (<see cref="FirstUnackedFragmentSentTimestamp"/>) only if
    /// none is already open, so calling this for every Fragment packed, retransmits included, still marks
    /// only the first send of the window.
    /// </summary>
    public void RecordFragmentSent()
    {
        FirstUnackedFragmentSentTimestamp ??= _time.GetTimestamp();
    }

    /// <summary>
    /// Records that an unfragmented reliable record was just acknowledged, updating
    /// <see cref="LastUnfragmentedAckTimestamp"/> to now.
    /// </summary>
    public void RecordUnfragmentedRecordAcked()
    {
        LastUnfragmentedAckTimestamp = _time.GetTimestamp();
    }

    /// <summary>
    /// Records that a Fragment was just acknowledged: latches <see cref="HasAckedFragment"/> permanently
    /// (see the type remarks) and closes the unacked-Fragment window, clearing
    /// <see cref="FirstUnackedFragmentSentTimestamp"/> to <see langword="null"/> and
    /// <see cref="RoundsSinceFirstUnackedFragment"/> to zero. However late this ack arrives, the state is
    /// left unable to produce the stall signature again.
    /// </summary>
    public void RecordFragmentAcked()
    {
        HasAckedFragment = true;
        FirstUnackedFragmentSentTimestamp = null;
        RoundsSinceFirstUnackedFragment = 0;
    }

    /// <summary>
    /// Records one elapsed retransmission round for this Peer — called once per Peer per retransmission
    /// sweep that found something expired, whatever expired, not once per Fragment. Only advances
    /// <see cref="RoundsSinceFirstUnackedFragment"/> while a Fragment window is open and unacked
    /// (<see cref="FirstUnackedFragmentSentTimestamp"/> is not <see langword="null"/> and
    /// <see cref="HasAckedFragment"/> is <see langword="false"/>); otherwise a no-op, since there is
    /// nothing for a round to count against once a Fragment has been acked or none has been sent.
    /// </summary>
    public void RecordRetransmissionRound()
    {
        if (FirstUnackedFragmentSentTimestamp is not null && !HasAckedFragment)
        {
            RoundsSinceFirstUnackedFragment++;
        }
    }

    /// <summary>
    /// Whether this Peer's state matches the fragment-size stall signature: a Fragment window open, no
    /// Fragment ever acked, an unfragmented record acked at or after the window opened, and at least
    /// <see cref="StallRoundCount"/> retransmission rounds elapsed since. Read-only; nothing here acts
    /// on the result (issue #270 does that).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Stays <see langword="false"/> in three states a caller should be able to trust it to stay quiet
    /// in: while any Fragment has ever been acked (<see cref="HasAckedFragment"/> latches permanently,
    /// so this can never fire again for this Peer once one arrives), while nothing has been acked at
    /// all (an ordinary path outage looks the same as a size boundary until something else confirms the
    /// Peer is reachable), and short of <see cref="StallRoundCount"/> rounds (an ordinary retransmit
    /// still in flight).
    /// </para>
    /// <para>
    /// <b>Why the unfragmented ack must postdate the window.</b> Issue #269 asks for <em>recently</em>
    /// acked unfragmented records, and the moment the first unacked Fragment went out is what "recently"
    /// is measured against here, rather than a wall-clock window nothing in this design has a budget
    /// for. An unfragmented record acked only before the Fragments started is a Peer that <em>was</em>
    /// reachable: a path that went silent the moment the Fragments began is the dead path a Session's
    /// own liveness machinery owns, and calling it a fragment-size problem would send a consumer to
    /// change the one setting that is not the cause. So the comparison is against
    /// <see cref="FirstUnackedFragmentSentTimestamp"/>, and it is at-or-after rather than strictly
    /// after: two events one Pump processes without the clock moving between them are both real, and a
    /// strict comparison would turn the signature on tick granularity instead of on what happened.
    /// </para>
    /// </remarks>
    public bool IsFragmentSizeStall =>
        FirstUnackedFragmentSentTimestamp is not null
        && !HasAckedFragment
        && LastUnfragmentedAckTimestamp is not null
        && LastUnfragmentedAckTimestamp >= FirstUnackedFragmentSentTimestamp
        && RoundsSinceFirstUnackedFragment >= StallRoundCount;
}
