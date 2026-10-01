namespace ReactorSockets.Session;

/// <summary>
/// One Peer's tracker of reliable sequences that have been sent and not yet acknowledged (subtask
/// 6.3.3, issue #185; ADR 0009), and the horizon rule that decides which of them are being judged for
/// retransmission at all. Only the two reliable Spaces are tracked —
/// <see cref="PacketRecordMode.ReliableUnordered"/> and <see cref="PacketRecordMode.ReliableOrdered"/>
/// — since the unreliable Spaces are never acknowledged and so are never retransmitted.
/// </summary>
/// <remarks>
/// <para>
/// <b>The horizon rule.</b> A received ack block describes a cumulative base plus a 32-bit bitfield of
/// the sequences above it (subtask 6.1) — it can only ever speak for the span
/// <c>[base, base + 32]</c>. A sequence sent inside that span is judged: <see cref="RecordSent"/> arms
/// its retransmit deadline immediately, taken from the injected <see cref="TimeProvider"/> plus the
/// effective RTO passed in at that call. A sequence sent beyond that span cannot yet be described by
/// any ack the peer could send, so arming its deadline now would retransmit it the moment the timeout
/// elapsed even though nothing was lost — it is recorded as outstanding but left unjudged, and
/// <see cref="HorizonDeferrals"/> counts the deferral. Only once a later ack advances the base far
/// enough that the horizon slides over it does its deadline arm, from that moment, at the RTO current
/// at the slide — <see cref="ApplyAck"/> (subtask 6.3.5, issue #187; subtask 6.4.7, issue #197).
/// </para>
/// <para>
/// Without this rule, a Message spanning more than 33 Fragments would have its later Fragments arrive
/// and sit undescribable by either ack block while an early Fragment is still missing, and every one of
/// those later Fragments would be retransmitted once its timeout elapsed — although nothing was lost.
/// This is unconditional machinery on the normal reliable-send path, not an edge case reached only under
/// loss.
/// </para>
/// <para>
/// <b>The adaptive RTO, composed with the horizon (issue #197).</b> There is no timeout captured at
/// construction any more (issue #42 finding R1: a captured or fallback RTO is how a fixed timeout
/// survives into a design that rejected one — see <c>DatagramSessionProtocol</c>'s now-deleted
/// <c>DefaultRetransmitTimeout</c>). Every call that arms a deadline — <see cref="RecordSent"/> for a
/// sequence already inside the horizon, and the newly-covered arm inside <see cref="ApplyAck"/> for a
/// sequence the horizon has just slid over — takes the effective RTO as a parameter, read by the caller
/// at that exact moment from <see cref="RetransmissionTimer.EffectiveTimeout"/>. The horizon gate and
/// the adaptive timer are therefore one path: a deferred sequence has no deadline and so never touches
/// the RTO at all until the moment it arms, and it arms at whatever the RTO is <em>then</em> — not the
/// RTO current when it was originally sent, and not backdated. Two sequences armed at different moments
/// can and do get different deadlines; arming a later sequence never retroactively moves an earlier
/// one's deadline, because each slot's deadline is written only by the call that armed it.
/// </para>
/// <para>
/// <b>Feeding the estimator.</b> Releasing an armed sequence — by <see cref="ApplyAck"/>, whether
/// named by a bit or swept up by the cumulative base advancing past it — feeds exactly one sample into
/// the injected <see cref="RetransmissionTimer"/>'s <see cref="RetransmissionTimer.Estimator"/>: the
/// elapsed time from when that sequence was armed to this release, and whether it was ever marked
/// retransmitted (<see cref="MarkRetransmitted"/>), so <see cref="RoundTripEstimator.Sample(TimeSpan,bool)"/>'s
/// Karn's-rule discard applies. A sequence released while still deferred — never armed, because the
/// horizon never slid over it before it was acknowledged — feeds no sample at all: there was no arming
/// moment to measure from. A sequence already released by an earlier bit and later swept out of the ring
/// by the base advancing past it is not sampled twice.
/// </para>
/// <para>
/// <b>Reporting what a release covered.</b> <see cref="RecordSent"/> also takes whether the sequence
/// being recorded is a Fragment (subtask 7.5.3.1, issue #294), and every sequence one <see cref="ApplyAck"/>
/// call actually releases — named by a bit or swept up by the base advancing past it, once each,
/// matching the estimator-sampling rule above — contributes to <see cref="LastAckRelease"/> according to
/// its stored flag. This is what lets a Peer's fragment-stall signature (issue #269) tell a Fragment ack
/// from an unfragmented one without <see cref="ApplyAck"/> itself knowing anything about that signature.
/// </para>
/// <para>
/// Deadlines are always computed from the injected <see cref="TimeProvider"/>, never the ambient clock,
/// so a test can advance time deterministically and so a real Reactor's single time source stays the
/// only clock a Peer ever reads. Every timing read here is monotonic — <see cref="TimeProvider.GetTimestamp"/>
/// and <see cref="TimeProvider.GetElapsedTime(long,long)"/> only, never <c>GetUtcNow</c> (issue #42): a
/// deadline stored as a wall-clock <see cref="DateTimeOffset"/> is exactly the state a clock adjustment
/// could use to fire every armed deadline at once, and a monotonic timestamp cannot be moved by one. Not
/// thread-safe, matching every other piece of a Peer's state (ADR 0009): a Peer is driven from one
/// Reactor's single synchronous <c>Pump</c>.
/// </para>
/// <para>
/// <b>Allocation.</b> Each reliable Space's outstanding sequences are held in a growable ring indexed by
/// offset from the oldest still-outstanding sequence, mirroring <see cref="ReceiveWindow"/>'s
/// offset-from-base indexing on the receive side. The ring only grows — doubling its backing arrays —
/// when more sequences are outstanding at once than it has ever held before; <see cref="RecordSent"/>,
/// <see cref="IsOutstanding"/> and <see cref="TryGetDeadlineTimestamp"/> allocate nothing once a
/// Space's ring has grown to cover its steady-state outstanding count. <see cref="ApplyAck"/> never
/// grows the ring — it only releases sequences, feeds estimator samples and moves the tracked window's
/// edges — so it allocates nothing regardless of how many times it has been called. Neither does
/// <see cref="CollectExpired"/> (issue #198): its result is a reused scratch buffer sized alongside the
/// ring's own arrays, including when nothing has expired.
/// </para>
/// </remarks>
internal sealed class OutstandingReliableSequences
{
    private readonly TimeProvider _time;
    private readonly RetransmissionTimer _timer;
    private readonly ReliableSpaceTracker _reliableUnordered = new();
    private readonly ReliableSpaceTracker _reliableOrdered = new();

    /// <summary>
    /// Builds a tracker with both reliable Spaces empty and both horizons at their initial position
    /// — sequence zero, matching every Space's numbering start (ADR 0009) — so before any ack is ever
    /// received, sequences <c>0</c> through <c>32</c> arm and <c>33</c> upward defer.
    /// </summary>
    /// <param name="time">The monotonic time source every armed deadline and every estimator sample is computed from.</param>
    /// <param name="timer">
    /// This Peer's own <see cref="RetransmissionTimer"/> (issue #194, #197): a release feeds its
    /// <see cref="RetransmissionTimer.Estimator"/> exactly one sample, and its own effective RTO is what
    /// every arming call is given by the caller — this tracker never reads <see cref="RetransmissionTimer.Timeout"/>
    /// or <see cref="RetransmissionTimer.EffectiveTimeout"/> itself, since only the Reactor knows the
    /// observed Pump cadence and Linger that <c>EffectiveTimeout</c> clamps against.
    /// </param>
    public OutstandingReliableSequences(TimeProvider time, RetransmissionTimer timer)
    {
        ArgumentNullException.ThrowIfNull(time);
        ArgumentNullException.ThrowIfNull(timer);
        _time = time;
        _timer = timer;
    }

    /// <summary>
    /// The monotonic count of sequences whose retransmit deadline has been withheld by the horizon rule
    /// so far, summed across both reliable Spaces — a sequence recorded by <see cref="RecordSent"/>
    /// while beyond its Space's horizon, once each. Never decremented: a sequence later brought inside
    /// the horizon by a sliding ack arms from that point on, it does not retroactively un-defer.
    /// </summary>
    public ulong HorizonDeferrals { get; private set; }

    /// <summary>
    /// What the most recent <see cref="ApplyAck"/> call actually released, one Space's worth at a time
    /// (subtask 7.5.3.1, issue #294): whether any of the sequences it released was a Fragment, and
    /// whether any was an unfragmented record. Written by every <see cref="ApplyAck"/> call, including a
    /// stale or empty one, which writes <see langword="default"/> — <c>(false, false)</c> — rather than
    /// leaving the previous call's value standing. One value across both reliable Spaces, since each
    /// <see cref="ApplyAck"/> call names exactly one Space: read this immediately after the call it
    /// belongs to, mirroring <see cref="ReceiveWindow.LastSlideLoss"/> and
    /// <see cref="ReceiveWindow.LastAbandonedFragmentSet"/>'s own idiom.
    /// </summary>
    public (bool ReleasedFragment, bool ReleasedUnfragmentedRecord) LastAckRelease { get; private set; }

    /// <summary>
    /// Records that <paramref name="sequence"/> was just sent on <paramref name="mode"/>'s reliable
    /// Space and is now outstanding. Arms its retransmit deadline — <paramref name="sequence"/>'s send
    /// time is not tracked, only the moment this call arms from — at <paramref name="effectiveRto"/>
    /// when the Space's current horizon covers it; otherwise leaves it unjudged and counts the deferral
    /// in <see cref="HorizonDeferrals"/>. Expects the sequences recorded for one Space to arrive in the
    /// order they were allocated (<see cref="OutboundSequenceSpaces.Allocate"/> is the only source),
    /// the same precondition <see cref="ReceiveWindow"/> relies on for its own offset arithmetic.
    /// </summary>
    /// <param name="mode">Which reliable Space <paramref name="sequence"/> was sent on.</param>
    /// <param name="sequence">The <c>u32</c> sequence number that was just sent.</param>
    /// <param name="effectiveRto">
    /// The effective RTO to arm with, read fresh by the caller at the moment of this call (issue #197)
    /// — never captured earlier and never a value fixed at construction.
    /// </param>
    /// <param name="isFragment">
    /// Whether <paramref name="sequence"/> names one Fragment of a cut Message rather than a whole
    /// record (subtask 7.5.3.1, issue #294). Stored against the slot so a later <see cref="ApplyAck"/>
    /// release can report it through <see cref="LastAckRelease"/> without this tracker knowing anything
    /// about what reads that value.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode"/> is not a reliable Space.</exception>
    public void RecordSent(PacketRecordMode mode, uint sequence, TimeSpan effectiveRto, bool isFragment)
    {
        if (SpaceOf(mode).RecordSent(sequence, _time, effectiveRto, isFragment))
        {
            HorizonDeferrals++;
        }
    }

    /// <summary>
    /// Whether <paramref name="sequence"/> was sent on <paramref name="mode"/>'s reliable Space and has
    /// not yet been released by an acknowledging ack block — regardless of whether its deadline is
    /// currently armed. A deferred-but-outstanding sequence and an armed one both report
    /// <see langword="true"/> here; <see cref="TryGetDeadlineTimestamp"/> is what distinguishes them.
    /// </summary>
    /// <param name="mode">Which reliable Space to query.</param>
    /// <param name="sequence">The <c>u32</c> sequence number to query.</param>
    /// <returns><see langword="true"/> if <paramref name="sequence"/> is still outstanding.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode"/> is not a reliable Space.</exception>
    public bool IsOutstanding(PacketRecordMode mode, uint sequence) => SpaceOf(mode).IsOutstanding(sequence);

    /// <summary>
    /// Whether any sequence at or below <paramref name="sequence"/> on <paramref name="mode"/>'s reliable
    /// Space is still outstanding, that is, sent and not yet released by an acknowledging ack block. False
    /// once the cumulative ack has passed <paramref name="sequence"/>, or once every sequence up to it has
    /// been released selectively.
    /// </summary>
    /// <param name="mode">Which reliable Space to query.</param>
    /// <param name="sequence">The <c>u32</c> sequence number to query up to, inclusive.</param>
    /// <returns><see langword="true"/> if any sequence at or below <paramref name="sequence"/> is still outstanding.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode"/> is not a reliable Space.</exception>
    public bool HasOutstandingAtOrBelow(PacketRecordMode mode, uint sequence) => SpaceOf(mode).HasOutstandingAtOrBelow(sequence);

    /// <summary>
    /// Attempts to read <paramref name="sequence"/>'s current retransmit deadline, as a monotonic
    /// <see cref="TimeProvider.GetTimestamp"/> value, on <paramref name="mode"/>'s reliable Space. Fails
    /// when <paramref name="sequence"/> is not outstanding at all, and also fails while it is
    /// outstanding but deferred by the horizon rule — the mechanism the horizon tests assert on, since a
    /// deferred sequence has no deadline to report until a later ack's horizon slide arms one. Internal,
    /// and reached only through the Reactor's own expiry sweep (issue #198) — never a
    /// <see cref="DateTimeOffset"/>, matching this whole type's monotonic-only discipline (issue #42).
    /// </summary>
    /// <param name="mode">Which reliable Space to query.</param>
    /// <param name="sequence">The <c>u32</c> sequence number to query.</param>
    /// <param name="deadlineTimestamp">
    /// The armed retransmit deadline as a <see cref="TimeProvider.GetTimestamp"/> value, valid only
    /// when this method returns <see langword="true"/>. Compare against a fresh
    /// <see cref="TimeProvider.GetTimestamp"/> with <see cref="TimeProvider.GetElapsedTime(long,long)"/>
    /// to test whether it has passed.
    /// </param>
    /// <returns><see langword="true"/> if <paramref name="sequence"/> is outstanding and armed.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode"/> is not a reliable Space.</exception>
    internal bool TryGetDeadlineTimestamp(PacketRecordMode mode, uint sequence, out long deadlineTimestamp) =>
        SpaceOf(mode).TryGetDeadlineTimestamp(sequence, out deadlineTimestamp);

    /// <summary>
    /// Marks <paramref name="sequence"/> on <paramref name="mode"/>'s reliable Space as having been
    /// retransmitted at least once, so a later <see cref="ApplyAck"/> release feeds its sample to the
    /// estimator as ambiguous (Karn's rule, issue #193) rather than as a clean round trip. A no-op if
    /// <paramref name="sequence"/> is not currently tracked — released or never sent. Internal: called
    /// only from <see cref="CollectExpired"/>'s own sweep (issue #198), which decides <em>when</em> a
    /// sequence has expired and calls this once it does.
    /// </summary>
    /// <param name="mode">Which reliable Space <paramref name="sequence"/> was sent on.</param>
    /// <param name="sequence">The <c>u32</c> sequence number that was retransmitted.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode"/> is not a reliable Space.</exception>
    internal void MarkRetransmitted(PacketRecordMode mode, uint sequence) =>
        SpaceOf(mode).MarkRetransmitted(sequence);

    /// <summary>
    /// Applies one received ack block to <paramref name="mode"/>'s reliable Space (subtask 6.3.5, issue
    /// #187; subtask 6.4.7, issue #197): releases every sequence the block acknowledges, feeding the
    /// injected <see cref="RetransmissionTimer"/>'s estimator one sample per armed sequence released,
    /// slides the Space's horizon to <paramref name="ackBase"/>, and arms any deferred sequence the
    /// slide newly covers at <paramref name="effectiveRto"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>What is released.</b> <paramref name="ackBase"/> is cumulative — every outstanding sequence
    /// strictly below it is released in this one call, whether or not any bit ever described it. Bit
    /// <c>n</c> (0-based) of <paramref name="ackBits"/> additionally releases the single sequence
    /// <c>ackBase + n + 1</c> when set, matching the projection <see cref="ReceiveWindow.DescribeAsAckBlock"/>
    /// builds on the receive side. An unset bit releases nothing, and a bit describing a sequence this
    /// Space never recorded (untrusted peer input) is silently ignored rather than throwing.
    /// </para>
    /// <para>
    /// <b>Sampling on release.</b> Every released sequence that was armed feeds one sample — the elapsed
    /// time since it armed, and whether <see cref="MarkRetransmitted"/> was ever called for it — into
    /// <see cref="RetransmissionTimer.Estimator"/>. A released sequence that was never armed (still
    /// deferred) feeds nothing. A sequence already released by an earlier call's bit, later swept out of
    /// the ring here as the base advances past it, is not sampled a second time.
    /// </para>
    /// <para>
    /// <b>Staleness.</b> <paramref name="ackBase"/> is compared against the base of the last ack this
    /// Space applied, using <see cref="SequenceNumber.IsNewerThan"/> so the comparison is correct across
    /// the <c>u32</c> wrap. A base that is not newer than (and not equal to) the base already applied is
    /// stale: the whole block is ignored, moving nothing, releasing nothing and sampling nothing, since
    /// an equal base can still carry new bits worth applying but an older one cannot be trusted at all.
    /// </para>
    /// <para>
    /// <b>The slide.</b> Once <paramref name="ackBase"/> is accepted, it becomes this Space's horizon.
    /// Any sequence still outstanding after the release above, not yet armed, and now within
    /// <c>[ackBase, ackBase + 32]</c> arms at this moment — its deadline is taken from the injected
    /// <see cref="TimeProvider"/> plus <paramref name="effectiveRto"/> <i>now</i>, not backdated to when
    /// it was originally sent, and not at whatever RTO was current back then.
    /// </para>
    /// <para>Allocates nothing: it only flips flags, feeds estimator samples and moves the tracked window's edges.</para>
    /// </remarks>
    /// <param name="mode">Which reliable Space the ack block describes.</param>
    /// <param name="ackBase">The ack block's cumulative base.</param>
    /// <param name="ackBits">
    /// The ack block's 32-bit field; bit <c>n</c> describes sequence <c>ackBase + n + 1</c>.
    /// </param>
    /// <param name="effectiveRto">
    /// The effective RTO any newly-covered sequence arms at, read fresh by the caller at the moment of
    /// this call (issue #197) — never a value fixed at construction.
    /// </param>
    /// <remarks>
    /// Always writes <see cref="LastAckRelease"/>, including <see langword="default"/> for a stale or
    /// empty block — see that property's own doc.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode"/> is not a reliable Space.</exception>
    public void ApplyAck(PacketRecordMode mode, uint ackBase, uint ackBits, TimeSpan effectiveRto) =>
        LastAckRelease = SpaceOf(mode).ApplyAck(ackBase, ackBits, _time, effectiveRto, _timer);

    /// <summary>
    /// The sequence numbers <paramref name="mode"/>'s reliable Space actually released, once each — by a
    /// bit or swept up by the cumulative base advancing past it — during the most recent
    /// <see cref="ApplyAck"/> call made for this <paramref name="mode"/> (issue #345). Read this
    /// immediately after the <see cref="ApplyAck"/> call it belongs to, mirroring
    /// <see cref="LastAckRelease"/>'s own idiom: <see cref="ReactorSockets.Core.RetentionPool.Release"/>
    /// is what a caller feeds each entry to, one call per sequence, to free the retention slot every
    /// released record held — nothing else in this tracker frees one.
    /// </summary>
    /// <remarks>
    /// Allocates nothing: like <see cref="CollectExpired"/>, this is a reused scratch buffer sized
    /// alongside the Space's own ring, overwritten by every <see cref="ApplyAck"/> call including a
    /// stale or empty one, which leaves it empty rather than the previous call's contents standing. The
    /// span is only valid until the next <see cref="ApplyAck"/> call on this Space.
    /// </remarks>
    /// <param name="mode">Which reliable Space's most recent release to read.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode"/> is not a reliable Space.</exception>
    public ReadOnlySpan<uint> LastAckReleasedSequences(PacketRecordMode mode) => SpaceOf(mode).LastReleasedSequences;

    /// <summary>
    /// Reports every sequence on <paramref name="mode"/>'s reliable Space whose retransmit deadline has
    /// passed as of now (subtask 6.4.8, issue #198), so the caller knows what must be resent. A
    /// deferred-but-outstanding sequence — outstanding but never armed, because the horizon has not slid
    /// over it — is never reported: this reads the same armed deadline <see cref="TryGetDeadlineTimestamp"/>
    /// does, so the horizon gate and the timer are one path, not two that happen to agree.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Call order.</b> This method is half of a two-call sweep and does not stand alone. Per Peer,
    /// per Pump, the order is: <see cref="CollectExpired"/> for <em>every</em> reliable Space first,
    /// then — only if the Spaces reported something between them — one
    /// <see cref="RetransmissionTimer.BackOff"/> and one
    /// <see cref="RetransmissionTimer.EffectiveTimeout"/> read, then <see cref="ReArmExpired"/> for
    /// every Space that was swept, all at that one RTO. Sweeping and re-arming one Space before
    /// sweeping the other would put the second Space's findings behind a backoff the first Space
    /// caused, which is the compounding this split exists to prevent. The split is what makes the
    /// backoff one decision per Peer per Pump rather than one per Space; the cost is that the order
    /// is a contract this type documents rather than one the signatures enforce.
    /// <see cref="DatagramSessionReactor"/>'s per-Peer sweep is the reference caller.
    /// </para>
    /// <para>
    /// Each reported sequence is marked retransmitted (<see cref="MarkRetransmitted"/>) before this
    /// returns, so a later <see cref="ApplyAck"/> release feeds Karn's rule the ambiguous case rather
    /// than a clean sample. This call does <em>not</em> back off <see cref="RetransmissionTimer"/> and
    /// does not re-arm what it finds — <see cref="ReArmExpired"/> does both, once the caller has swept
    /// every reliable Space and decided whether this Peer backs off at all this Pump (subtask 6.4.9,
    /// issue #199, fixing a defect in the original 6.4.8 shape: with the backoff decided inside this
    /// call, a Pump where both reliable Spaces expired something backed off the shared
    /// <see cref="RetransmissionTimer"/> twice, doubling the RTO twice for what is one loss event, not
    /// two). Call <see cref="ReArmExpired"/> for <paramref name="mode"/> before this Space is swept
    /// again — a second <see cref="CollectExpired"/> call before re-arming would simply re-report the
    /// same sequences, since nothing here changes <see cref="TryGetDeadlineTimestamp"/>'s answer for
    /// them.
    /// </para>
    /// <para>
    /// Allocates nothing: the returned span is backed by a buffer sized alongside this Space's ring and
    /// reused by every call, exactly like <see cref="RecordSent"/> and <see cref="ApplyAck"/> allocate
    /// nothing once the ring has grown to cover the steady-state outstanding count. The span is only
    /// valid until the next call on this Space — copy it out before calling again if it must outlive
    /// that.
    /// </para>
    /// </remarks>
    /// <param name="mode">Which reliable Space to sweep for expired deadlines.</param>
    /// <returns>
    /// The <c>u32</c> sequence numbers on <paramref name="mode"/>'s Space that just expired, in the
    /// order they were originally sent. Empty when nothing has expired.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode"/> is not a reliable Space.</exception>
    public ReadOnlySpan<uint> CollectExpired(PacketRecordMode mode) =>
        SpaceOf(mode).CollectExpired(_time);

    /// <summary>
    /// Re-arms whatever <see cref="CollectExpired"/> most recently found on <paramref name="mode"/>'s
    /// reliable Space, at <paramref name="effectiveRto"/>, from the same moment that sweep read
    /// (subtask 6.4.9, issue #199) — mirroring how a horizon slide arms every sequence it newly covers
    /// from one shared moment and one shared RTO (<see cref="ApplyAck"/>'s own <c>ArmNewlyCovered</c>)
    /// rather than reading the clock or the timer once per sequence. The caller decides
    /// <paramref name="effectiveRto"/> itself — typically <see cref="RetransmissionTimer.EffectiveTimeout"/>
    /// read once after backing off <see cref="RetransmissionTimer"/> at most once for however many
    /// Spaces reported anything this Pump, so two reliable Spaces expiring in the same Pump share one
    /// backoff and one resulting RTO rather than compounding. A no-op if <see cref="CollectExpired"/>
    /// found nothing on this Space, or has not been called since the last re-arm.
    /// <para>
    /// <b>Call order.</b> This is the second half of <see cref="CollectExpired"/>'s two-call sweep and
    /// must follow it for the same <paramref name="mode"/> within the same Pump — see
    /// <see cref="CollectExpired"/> for the full ordering contract. A sequence this Space reported and
    /// that is never re-armed keeps its already-passed deadline, so it is reported again on the next
    /// Pump and on every Pump after: skipping this call does not leave a sequence un-armed, it leaves
    /// it permanently expired.
    /// </para>
    /// </summary>
    /// <param name="mode">Which reliable Space to re-arm the most recent sweep's findings on.</param>
    /// <param name="effectiveRto">The single RTO every re-armed sequence on <paramref name="mode"/> arms at.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="mode"/> is not a reliable Space.</exception>
    public void ReArmExpired(PacketRecordMode mode, TimeSpan effectiveRto) =>
        SpaceOf(mode).ReArmExpired(_time, effectiveRto);

    /// <summary>
    /// Resolves <paramref name="mode"/> to its reliable Space's tracker. Only the two reliable modes are
    /// valid — the two unreliable modes are never acknowledged and so are never tracked here — matching
    /// how <see cref="OutboundSequenceSpaces.Allocate"/> rejects a mode it does not recognize.
    /// </summary>
    private ReliableSpaceTracker SpaceOf(PacketRecordMode mode) => mode switch
    {
        PacketRecordMode.ReliableUnordered => _reliableUnordered,
        PacketRecordMode.ReliableOrdered => _reliableOrdered,
        _ => throw new ArgumentOutOfRangeException(
            nameof(mode), mode, "Only the two reliable delivery modes are tracked for retransmission."),
    };

    /// <summary>
    /// Converts <paramref name="span"/> into <paramref name="time"/>'s own provider-tick units, the way
    /// a <see cref="TimeProvider.GetTimestamp"/> reading is added to. There is no built-in
    /// "timestamp plus TimeSpan" operation on <see cref="TimeProvider"/> — only the inverse,
    /// <see cref="TimeProvider.GetElapsedTime(long,long)"/> — so this performs the documented conversion
    /// through <see cref="TimeProvider.TimestampFrequency"/> once per arm. Saturates at
    /// <see cref="long.MaxValue"/> rather than producing an undefined result if <paramref name="span"/>
    /// is (or is close to) <see cref="TimeSpan.MaxValue"/>, even though every live caller's
    /// <paramref name="span"/> is already finite — <see cref="RetransmissionTimer.EffectiveTimeout"/>
    /// always clamps to a finite Linger.
    /// </summary>
    private static long ToProviderTicks(TimeProvider time, TimeSpan span)
    {
        if (span >= TimeSpan.MaxValue)
        {
            return long.MaxValue;
        }

        double providerTicks = span.Ticks * ((double)time.TimestampFrequency / TimeSpan.TicksPerSecond);
        return providerTicks >= long.MaxValue ? long.MaxValue : (long)providerTicks;
    }

    /// <summary>
    /// One reliable Space's outstanding sequences and horizon (see the enclosing type's remarks). Holds
    /// a contiguous run of sequences — from the oldest not yet released up to the newest sent — indexed
    /// by their offset from the oldest, in a ring that grows by doubling rather than a fixed capacity,
    /// since nothing bounds how many sequences a Space can have outstanding at once.
    /// </summary>
    private sealed class ReliableSpaceTracker
    {
        /// <summary>
        /// The horizon's width: an ack block describes its base plus this many sequences above it
        /// (subtask 6.1) — the span <see cref="RecordSent"/> tests a sequence against.
        /// </summary>
        private const uint HorizonWidth = 32;

        private const int InitialCapacity = 16;

        private bool[] _armed = new bool[InitialCapacity];
        private bool[] _released = new bool[InitialCapacity];
        private bool[] _retransmitted = new bool[InitialCapacity];
        private bool[] _isFragment = new bool[InitialCapacity];
        private long[] _armedAtTimestamp = new long[InitialCapacity];
        private long[] _deadlineTimestamp = new long[InitialCapacity];

        /// <summary>
        /// Scratch buffer <see cref="CollectExpired"/> writes its result into, reused by every call and
        /// grown alongside the other per-slot arrays in <see cref="EnsureCapacity"/> rather than sized
        /// independently — the number of sequences one sweep can report is bounded by <see cref="_count"/>,
        /// which is already bounded by <see cref="_capacity"/>, so no separate growth path is needed.
        /// Its contents past the count <see cref="CollectExpired"/> last returned are meaningless.
        /// </summary>
        private uint[] _expiredSequences = new uint[InitialCapacity];

        /// <summary>
        /// Scratch buffer <see cref="ApplyAck"/> writes the sequences it actually released into (issue
        /// #345), reused by every call and grown alongside the other per-slot arrays in
        /// <see cref="EnsureCapacity"/> — the number of sequences one <see cref="ApplyAck"/> call can
        /// release is bounded by <see cref="_count"/>, which is already bounded by <see cref="_capacity"/>,
        /// exactly like <see cref="_expiredSequences"/>. Its contents past
        /// <see cref="_releasedSequenceCount"/> are meaningless.
        /// </summary>
        private uint[] _releasedSequences = new uint[InitialCapacity];

        /// <summary>
        /// How many entries in <see cref="_releasedSequences"/> the most recent <see cref="ApplyAck"/>
        /// call wrote. Reset to zero at the start of every <see cref="ApplyAck"/> call, including a stale
        /// one, so <see cref="LastReleasedSequences"/> never reports a previous call's releases.
        /// </summary>
        private int _releasedSequenceCount;

        private int _capacity = InitialCapacity;

        /// <summary>
        /// How many entries in <see cref="_expiredSequences"/> the most recent <see cref="CollectExpired"/>
        /// call reported and left un-armed, awaiting <see cref="ReArmExpired"/> (subtask 6.4.9, issue
        /// #199). Reset to zero once <see cref="ReArmExpired"/> arms them, so a re-arm call with nothing
        /// pending is a no-op rather than re-arming stale entries.
        /// </summary>
        private int _pendingRearmCount;

        /// <summary>
        /// The monotonic moment <see cref="_pendingRearmCount"/>'s entries were found expired, so
        /// <see cref="ReArmExpired"/> arms them from the same instant they were swept at rather than a
        /// later one — matching <see cref="ArmNewlyCovered"/>'s one-shared-moment shape.
        /// </summary>
        private long _pendingRearmTimestamp;

        /// <summary>The ring position the oldest tracked sequence currently occupies.</summary>
        private int _headSlot;

        /// <summary>The oldest sequence still tracked. Meaningful only while <see cref="_count"/> is nonzero.</summary>
        private uint _oldest;

        /// <summary>How many sequences, starting at <see cref="_oldest"/>, are currently tracked.</summary>
        private int _count;

        /// <summary>
        /// The base of the last ack block applied to this Space — the horizon's lower edge. Zero until
        /// the first ack arrives, matching every Space's numbering start. Advanced only by
        /// <see cref="ApplyAck"/>, and only forward: a stale ack (subtask 6.3.5, issue #187) never moves
        /// it back.
        /// </summary>
        private uint _ackBase;

        /// <summary>
        /// Records <paramref name="sequence"/> as newly outstanding, arming its deadline at
        /// <paramref name="effectiveRto"/> when the current horizon covers it. Returns whether it was
        /// deferred instead, so the caller can fold that into <see cref="HorizonDeferrals"/> without this
        /// tracker needing to know about the counter it feeds. Stores <paramref name="isFragment"/>
        /// against the slot, read back when this sequence is released (see the enclosing type's
        /// <c>LastAckRelease</c>).
        /// </summary>
        public bool RecordSent(uint sequence, TimeProvider time, TimeSpan effectiveRto, bool isFragment)
        {
            if (_count == 0)
            {
                _oldest = sequence;
                _headSlot = 0;
            }

            uint offset = unchecked(sequence - _oldest);
            EnsureCapacity(offset);
            int slot = SlotFor(offset);

            bool withinHorizon = unchecked(sequence - _ackBase) <= HorizonWidth;
            _released[slot] = false;
            _retransmitted[slot] = false;
            _isFragment[slot] = isFragment;
            if (withinHorizon)
            {
                Arm(slot, time.GetTimestamp(), time, effectiveRto);
            }
            else
            {
                _armed[slot] = false;
            }

            _count = (int)offset + 1;
            return !withinHorizon;
        }

        /// <summary>
        /// Whether <paramref name="sequence"/> is currently tracked as outstanding — sent, and not yet
        /// released by <see cref="ApplyAck"/>.
        /// </summary>
        public bool IsOutstanding(uint sequence) => TryGetSlot(sequence, out int slot) && !_released[slot];

        /// <summary>
        /// Whether any tracked sequence at or below <paramref name="sequence"/> is still outstanding, that is,
        /// sent and not yet released. A sequence below <see cref="_oldest"/> is already released.
        /// </summary>
        public bool HasOutstandingAtOrBelow(uint sequence)
        {
            for (int offset = 0; offset < _count; offset++)
            {
                uint tracked = unchecked(_oldest + (uint)offset);
                if (tracked != sequence && SequenceNumber.IsNewerThan(tracked, sequence))
                {
                    return false;
                }

                if (!_released[SlotFor((uint)offset)])
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Attempts to read <paramref name="sequence"/>'s deadline timestamp: fails when it is not
        /// tracked at all, when it has been released by <see cref="ApplyAck"/>, and when it is tracked
        /// but not currently armed.
        /// </summary>
        public bool TryGetDeadlineTimestamp(uint sequence, out long deadlineTimestamp)
        {
            if (TryGetSlot(sequence, out int slot) && !_released[slot] && _armed[slot])
            {
                deadlineTimestamp = _deadlineTimestamp[slot];
                return true;
            }

            deadlineTimestamp = default;
            return false;
        }

        /// <summary>
        /// Marks <paramref name="sequence"/> as retransmitted, if it is still tracked. A no-op for a
        /// sequence already released or never sent.
        /// </summary>
        public void MarkRetransmitted(uint sequence)
        {
            if (TryGetSlot(sequence, out int slot))
            {
                _retransmitted[slot] = true;
            }
        }

        /// <summary>
        /// Sweeps every sequence still tracked on this Space for a passed retransmit deadline (see the
        /// enclosing type's <c>CollectExpired</c> for the full contract): a deferred sequence has no
        /// armed deadline (<see cref="TryGetDeadlineTimestamp"/> fails for it) and so is never reported,
        /// matching the horizon rule. Every reported sequence is marked retransmitted
        /// (<see cref="MarkRetransmitted"/>) as it is found, and left un-armed:
        /// <see cref="ReArmExpired"/> arms them once the caller has decided this Peer's single backoff
        /// for the Pump (subtask 6.4.9, issue #199). What is found and the moment it was found at are
        /// stashed in <see cref="_pendingRearmCount"/> and <see cref="_pendingRearmTimestamp"/> for that
        /// call. Allocates nothing: the result is <see cref="_expiredSequences"/>, which never grows
        /// here (see its own doc) and is simply overwritten.
        /// </summary>
        public ReadOnlySpan<uint> CollectExpired(TimeProvider time)
        {
            int expiredCount = 0;
            if (_count > 0)
            {
                long now = time.GetTimestamp();
                for (int offset = 0; offset < _count; offset++)
                {
                    uint sequence = unchecked(_oldest + (uint)offset);
                    if (!TryGetDeadlineTimestamp(sequence, out long deadline) || time.GetElapsedTime(deadline, now) < TimeSpan.Zero)
                    {
                        continue;
                    }

                    _expiredSequences[expiredCount] = sequence;
                    expiredCount++;
                    MarkRetransmitted(sequence);
                }

                _pendingRearmCount = expiredCount;
                _pendingRearmTimestamp = now;
            }
            else
            {
                _pendingRearmCount = 0;
            }

            return _expiredSequences.AsSpan(0, expiredCount);
        }

        /// <summary>
        /// Arms whatever the most recent <see cref="CollectExpired"/> call found and left pending, at
        /// <paramref name="effectiveRto"/>, from that call's own moment (<see cref="_pendingRearmTimestamp"/>)
        /// rather than reading <paramref name="time"/> again — matching <see cref="ArmNewlyCovered"/>'s
        /// one-shared-moment shape. A no-op when nothing is pending, whether because
        /// <see cref="CollectExpired"/> found nothing or because a previous <see cref="ReArmExpired"/>
        /// call already cleared it. Clears the pending count either way, so a second call without an
        /// intervening <see cref="CollectExpired"/> re-arms nothing.
        /// </summary>
        public void ReArmExpired(TimeProvider time, TimeSpan effectiveRto)
        {
            for (int i = 0; i < _pendingRearmCount; i++)
            {
                if (TryGetSlot(_expiredSequences[i], out int slot))
                {
                    Arm(slot, _pendingRearmTimestamp, time, effectiveRto);
                }
            }

            _pendingRearmCount = 0;
        }

        /// <summary>
        /// Applies one received ack block (see the enclosing type's <c>ApplyAck</c> for the full
        /// contract): releases what it acknowledges (feeding <paramref name="timer"/>'s estimator one
        /// sample per armed sequence released), slides the horizon to <paramref name="ackBase"/>, and
        /// arms any deferred sequence the slide newly covers at <paramref name="effectiveRto"/>. A stale
        /// <paramref name="ackBase"/> — one <see cref="SequenceNumber.IsNewerThan"/> says is not newer
        /// than, and is not equal to, the base already applied — is rejected outright: nothing is
        /// released, sampled or armed, and the horizon does not move.
        /// </summary>
        /// <returns>
        /// Whether any sequence this call actually released was a Fragment, and whether any was an
        /// unfragmented record — <see langword="default"/> for a stale or empty block (see the enclosing
        /// type's <c>LastAckRelease</c>).
        /// </returns>
        public (bool ReleasedFragment, bool ReleasedUnfragmentedRecord) ApplyAck(
            uint ackBase, uint ackBits, TimeProvider time, TimeSpan effectiveRto, RetransmissionTimer timer)
        {
            _releasedSequenceCount = 0;

            bool stale = ackBase != _ackBase && !SequenceNumber.IsNewerThan(ackBase, _ackBase);
            if (stale)
            {
                return default;
            }

            long now = time.GetTimestamp();
            bool releasedFragment = false;
            bool releasedUnfragmentedRecord = false;

            ReleaseBelowBase(ackBase, time, now, timer, ref releasedFragment, ref releasedUnfragmentedRecord);

            for (int n = 0; n < (int)HorizonWidth; n++)
            {
                if ((ackBits & (1u << n)) == 0)
                {
                    continue;
                }

                uint sequence = unchecked(ackBase + (uint)n + 1);
                if (TryGetSlot(sequence, out int slot))
                {
                    Release(slot, sequence, time, now, timer, ref releasedFragment, ref releasedUnfragmentedRecord);
                }
            }

            _ackBase = ackBase;
            ArmNewlyCovered(time, now, effectiveRto);

            return (releasedFragment, releasedUnfragmentedRecord);
        }

        /// <summary>
        /// The sequence numbers this Space actually released during the most recent <see cref="ApplyAck"/>
        /// call (issue #345) — see the enclosing type's <c>LastAckReleasedSequences</c> for the full
        /// contract.
        /// </summary>
        public ReadOnlySpan<uint> LastReleasedSequences => _releasedSequences.AsSpan(0, _releasedSequenceCount);

        /// <summary>
        /// Releases every tracked sequence strictly below <paramref name="ackBase"/> in one step — each
        /// one sampled first if it was armed and not already released (see <see cref="Release"/>) — by
        /// moving the tracked window's start past them, whether or not any of them was ever named by a
        /// bit. A no-op when nothing is tracked, or when <paramref name="ackBase"/> does not reach past
        /// <see cref="_oldest"/> at all.
        /// </summary>
        private void ReleaseBelowBase(
            uint ackBase, TimeProvider time, long now, RetransmissionTimer timer,
            ref bool releasedFragment, ref bool releasedUnfragmentedRecord)
        {
            if (_count == 0 || _oldest == ackBase || SequenceNumber.IsNewerThan(_oldest, ackBase))
            {
                return;
            }

            uint advance = unchecked(ackBase - _oldest);
            int steps = advance >= (uint)_count ? _count : (int)advance;

            for (int i = 0; i < steps; i++)
            {
                Release(
                    SlotFor((uint)i), unchecked(_oldest + (uint)i), time, now, timer,
                    ref releasedFragment, ref releasedUnfragmentedRecord);
            }

            _oldest = unchecked(_oldest + (uint)steps);
            _headSlot = (int)(((uint)_headSlot + (uint)steps) % (uint)_capacity);
            _count -= steps;
        }

        /// <summary>
        /// Releases one slot: if it was armed and not already released, feeds
        /// <paramref name="timer"/>'s <see cref="RetransmissionTimer.Estimator"/> one sample — the
        /// elapsed time from when it armed to <paramref name="now"/>, and whether it was ever marked
        /// retransmitted (Karn's rule, issue #193) — and records the delivery against
        /// <paramref name="timer"/>'s backoff, then marks it released either way. A slot that was
        /// never armed (still deferred) or already released feeds no sample, matching the contract that
        /// a deferred release yields nothing and a sequence is never sampled twice.
        /// <para>
        /// The sample and the backoff reset are two separate things and are deliberately not gated
        /// alike (issue #194: "a successful delivery resets both the multiplier and the count in one
        /// step"). Karn's rule discards the <em>sample</em> from a retransmitted sequence because the
        /// round trip it measures is ambiguous, but the <em>delivery</em> is not ambiguous at all — the
        /// sequence arrived, so the path is working and the backoff that accumulated while it was
        /// failing must end. Resetting only on a clean sample would leave a path that recovered by
        /// retransmission backed off forever, which is exactly the case backoff exists to escape.
        /// </para>
        /// <para>
        /// A slot not already released also contributes to <paramref name="releasedFragment"/> or
        /// <paramref name="releasedUnfragmentedRecord"/> — whichever <see cref="_isFragment"/> says it
        /// is — regardless of whether it was armed, so a still-deferred sequence swept up by the base
        /// advancing past it is reported the same as an armed one (subtask 7.5.3.1, issue #294). An
        /// already-released slot contributes nothing, matching the no-double-sample rule above.
        /// </para>
        /// </summary>
        private void Release(
            int slot, uint sequence, TimeProvider time, long now, RetransmissionTimer timer,
            ref bool releasedFragment, ref bool releasedUnfragmentedRecord)
        {
            if (_released[slot])
            {
                return;
            }

            if (_armed[slot])
            {
                TimeSpan elapsed = time.GetElapsedTime(_armedAtTimestamp[slot], now);
                timer.Estimator.Sample(elapsed, _retransmitted[slot]);
                timer.RecordDelivery();
            }

            if (_isFragment[slot])
            {
                releasedFragment = true;
            }
            else
            {
                releasedUnfragmentedRecord = true;
            }

            _armed[slot] = false;
            _released[slot] = true;

            // _releasedSequences is sized to _capacity, which already bounds _count — the number of
            // slots this Space can ever have outstanding at once — so _releasedSequenceCount, bounded by
            // one entry per newly-released slot in one ApplyAck call, never exceeds it.
            _releasedSequences[_releasedSequenceCount] = sequence;
            _releasedSequenceCount++;
        }

        /// <summary>
        /// Arms every tracked sequence that is not released, not already armed, and now falls within
        /// <c>[_ackBase, _ackBase + <see cref="HorizonWidth"/>]</c> — the sequences a horizon slide just
        /// brought into view. Every sequence armed by one call shares the same arming moment
        /// (<paramref name="now"/>) and the same <paramref name="effectiveRto"/>, so a slide that covers
        /// several deferred sequences at once arms them all from that single moment rather than one
        /// clock read and one RTO read apiece.
        /// </summary>
        private void ArmNewlyCovered(TimeProvider time, long now, TimeSpan effectiveRto)
        {
            if (_count == 0)
            {
                return;
            }

            for (int offset = 0; offset < _count; offset++)
            {
                int slot = SlotFor((uint)offset);
                if (_released[slot] || _armed[slot])
                {
                    continue;
                }

                uint sequence = unchecked(_oldest + (uint)offset);
                if (unchecked(sequence - _ackBase) <= HorizonWidth)
                {
                    Arm(slot, now, time, effectiveRto);
                }
            }
        }

        /// <summary>
        /// Arms <paramref name="slot"/> at <paramref name="armedAt"/>, computing its deadline timestamp
        /// as <paramref name="armedAt"/> plus <paramref name="effectiveRto"/> converted into
        /// <paramref name="time"/>'s own provider-tick units.
        /// <para>
        /// The addition saturates rather than wrapping. <see cref="ToProviderTicks"/> already clamps an
        /// unbounded <paramref name="effectiveRto"/> to <see cref="long.MaxValue"/>, and
        /// <c>EffectiveTimeout</c> returns exactly that when Linger is <see cref="TimeSpan.MaxValue"/> —
        /// which <c>DatagramSessionOptions</c> permits, validating only that Linger is greater than zero.
        /// Adding an unclamped <see cref="long.MaxValue"/> to a positive <paramref name="armedAt"/>
        /// overflows into a negative timestamp, so the deadline would land in the past and the sequence
        /// would be swept as expired on its very first Pump and re-expire on every Pump after: a Linger
        /// of "effectively never abandon" would produce the most aggressive possible retransmission
        /// instead of the least. Saturating keeps the far-future deadline far in the future.
        /// </para>
        /// </summary>
        private void Arm(int slot, long armedAt, TimeProvider time, TimeSpan effectiveRto)
        {
            long providerTicks = ToProviderTicks(time, effectiveRto);
            long deadline = unchecked(armedAt + providerTicks);

            _armed[slot] = true;
            _armedAtTimestamp[slot] = armedAt;
            _deadlineTimestamp[slot] = providerTicks > 0 && deadline < armedAt ? long.MaxValue : deadline;
        }

        /// <summary>
        /// Resolves <paramref name="sequence"/> to its ring slot when it lies within
        /// <c>[_oldest, _oldest + _count)</c> — tracked and not yet released. A sequence below
        /// <see cref="_oldest"/> (already released) or at or beyond <c>_oldest + _count</c> (never sent)
        /// reports <see langword="false"/>, mirroring <see cref="ReceiveWindow.Classify(uint)"/>'s own bounds
        /// check.
        /// </summary>
        private bool TryGetSlot(uint sequence, out int slot)
        {
            slot = default;

            if (_count == 0)
            {
                return false;
            }

            bool atOrAboveOldest = sequence == _oldest || SequenceNumber.IsNewerThan(sequence, _oldest);
            if (!atOrAboveOldest)
            {
                return false;
            }

            uint offset = unchecked(sequence - _oldest);
            if (offset >= (uint)_count)
            {
                return false;
            }

            slot = SlotFor(offset);
            return true;
        }

        private int SlotFor(uint offset) => (int)((_headSlot + offset) % (uint)_capacity);

        /// <summary>
        /// Grows the ring, doubling its capacity until <paramref name="offset"/> fits, when the newly
        /// recorded sequence would not otherwise have a slot. Growth unwraps the ring into a fresh
        /// zero-based array — <see cref="_headSlot"/> resets to zero — since a resize already touches
        /// every tracked slot, unwrapping costs nothing further.
        /// </summary>
        private void EnsureCapacity(uint offset)
        {
            if (offset < (uint)_capacity)
            {
                return;
            }

            int newCapacity = _capacity;
            while (offset >= (uint)newCapacity)
            {
                newCapacity *= 2;
            }

            bool[] newArmed = new bool[newCapacity];
            bool[] newReleased = new bool[newCapacity];
            bool[] newRetransmitted = new bool[newCapacity];
            bool[] newIsFragment = new bool[newCapacity];
            long[] newArmedAtTimestamp = new long[newCapacity];
            long[] newDeadlineTimestamp = new long[newCapacity];
            for (int i = 0; i < _count; i++)
            {
                int oldSlot = (int)((_headSlot + i) % (uint)_capacity);
                newArmed[i] = _armed[oldSlot];
                newReleased[i] = _released[oldSlot];
                newRetransmitted[i] = _retransmitted[oldSlot];
                newIsFragment[i] = _isFragment[oldSlot];
                newArmedAtTimestamp[i] = _armedAtTimestamp[oldSlot];
                newDeadlineTimestamp[i] = _deadlineTimestamp[oldSlot];
            }

            _armed = newArmed;
            _released = newReleased;
            _retransmitted = newRetransmitted;
            _isFragment = newIsFragment;
            _armedAtTimestamp = newArmedAtTimestamp;
            _deadlineTimestamp = newDeadlineTimestamp;

            // Scratch buffers for CollectExpired and ApplyAck: neither needs its stale contents
            // preserved across a resize, only room for as many entries as the ring can ever hold at once.
            _expiredSequences = new uint[newCapacity];
            _releasedSequences = new uint[newCapacity];

            _capacity = newCapacity;
            _headSlot = 0;
        }
    }
}
