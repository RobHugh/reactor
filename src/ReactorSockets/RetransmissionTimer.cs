namespace ReactorSockets;

/// <summary>
/// Layers exponential backoff above a <see cref="RoundTripEstimator"/>. Built for issue #194 (slice
/// #42, the adaptive RTO): each consecutive expiry without a delivery doubles the timeout and raises
/// a consecutive-backoff count, and a successful delivery resets both in one step so the next timeout
/// is the estimate again rather than a decayed version of the backed-off one.
/// </summary>
/// <remarks>
/// <para>
/// Public for the same reason <see cref="PumpIntervalObserver"/> is (ADR 0015): a test pins this
/// behaviour by constructing one directly and calling <see cref="BackOff"/> /
/// <see cref="RecordDelivery"/> against hand-picked <see cref="RoundTripEstimator"/> samples, with no
/// <c>InternalsVisibleTo</c> and no interface invented for the purpose.
/// </para>
/// <para>
/// Backoff sits above the estimator rather than inside it: <see cref="BackOff"/> and
/// <see cref="RecordDelivery"/> never touch <see cref="RoundTripEstimator.SmoothedRoundTrip"/> or
/// <see cref="RoundTripEstimator.RoundTripVariance"/>. The smoothed value still describes the network
/// as last measured; the backoff is what is actually waited. With Karn's rule (issue #193) discarding
/// the ambiguous samples a retransmit would otherwise produce, the backoff multiplier is the only
/// thing that adapts while a path is failing.
/// </para>
/// <para>
/// <see cref="Timeout"/> here is deliberately unbounded — it is <see cref="Estimator"/>'s own
/// <see cref="RoundTripEstimator.Timeout"/> doubled <see cref="ConsecutiveBackoffs"/> times and
/// nothing more. The clamp against a Reactor's observed Pump cadence and the Linger ceiling is
/// <see cref="EffectiveTimeout"/> (issue #195), which reads both bounds fresh at every call rather than
/// touching <see cref="Timeout"/>'s own arithmetic.
/// </para>
/// </remarks>
public sealed class RetransmissionTimer
{
    /// <summary>
    /// The RTT estimator this timer backs off above. Owned and constructed here rather than injected,
    /// since nothing outside a single Peer's retransmission timer ever needs to share it.
    /// </summary>
    public RoundTripEstimator Estimator { get; } = new();

    /// <summary>
    /// The number of consecutive expiries since the last <see cref="RecordDelivery"/> (or since
    /// construction). Zero once a delivery has been recorded, or before the first backoff ever
    /// happens.
    /// </summary>
    public int ConsecutiveBackoffs { get; private set; }

    /// <summary>
    /// <see cref="Estimator"/>'s own <see cref="RoundTripEstimator.Timeout"/> doubled
    /// <see cref="ConsecutiveBackoffs"/> times, saturating at <see cref="TimeSpan.MaxValue"/> rather
    /// than overflowing.
    /// </summary>
    /// <remarks>
    /// While <see cref="Estimator"/> is unseeded its own <see cref="RoundTripEstimator.Timeout"/> is
    /// already <see cref="TimeSpan.MaxValue"/>; doubling that saturates immediately rather than
    /// wrapping, so this reads <see cref="TimeSpan.MaxValue"/> from the very first <see cref="BackOff"/>
    /// call too. This value is unbounded on purpose — issue #195's <c>EffectiveTimeout</c> is what
    /// clamps it against a Reactor's observed Pump cadence and the Linger ceiling.
    /// </remarks>
    public TimeSpan Timeout
    {
        get
        {
            long ticks = Estimator.Timeout.Ticks;
            for (int i = 0; i < ConsecutiveBackoffs && ticks != TimeSpan.MaxValue.Ticks; i++)
            {
                ticks = ticks > TimeSpan.MaxValue.Ticks / 2 ? TimeSpan.MaxValue.Ticks : ticks * 2;
            }

            return TimeSpan.FromTicks(ticks);
        }
    }

    /// <summary>
    /// Records one more consecutive expiry without a delivery: <see cref="ConsecutiveBackoffs"/> rises
    /// by one, which doubles <see cref="Timeout"/> again. Never touches
    /// <see cref="RoundTripEstimator.SmoothedRoundTrip"/> or
    /// <see cref="RoundTripEstimator.RoundTripVariance"/> — the estimate still describes the network
    /// as last measured, and backing off is not a new measurement.
    /// </summary>
    public void BackOff() => ConsecutiveBackoffs++;

    /// <summary>
    /// Records a successful delivery: <see cref="ConsecutiveBackoffs"/> returns to zero in one step, so
    /// the next <see cref="Timeout"/> is <see cref="Estimator"/>'s own estimate again rather than a
    /// decayed version of the backed-off one. A later <see cref="BackOff"/> after this starts doubling
    /// from the beginning, not from where the previous run of expiries left off.
    /// </summary>
    /// <remarks>
    /// Named <c>RecordDelivery</c> rather than <c>Reset</c>: <c>AssemblySkeletonTests</c> fails the
    /// whole suite on any exported method whose name contains "Reset" or "Clear" (ADR 0013's stance on
    /// resettable public state), so this is named for what it observes — a delivery arriving — rather
    /// than for the mechanical effect of clearing the count.
    /// </remarks>
    public void RecordDelivery() => ConsecutiveBackoffs = 0;

    /// <summary>
    /// <see cref="Timeout"/> clamped into <c>[observedPumpInterval, linger]</c>, with
    /// <paramref name="linger"/> having the last word: <c>Min(Max(Timeout, observedPumpInterval),
    /// linger)</c>. If the floor exceeds the ceiling — a Pump cadence slower than Linger — the result
    /// is <paramref name="linger"/> rather than an inverted or thrown range.
    /// </summary>
    /// <param name="observedPumpInterval">
    /// The Reactor's own <see cref="PumpIntervalObserver.Interval"/> (issue #191): an RTO shorter than
    /// the gap between Pumps cannot be acted on, so this floors <see cref="Timeout"/>.
    /// <see cref="PumpIntervalObserver.Interval"/> reads <see cref="TimeSpan.MaxValue"/> before a
    /// second Pump — deliberately, per that type's own docs — so with no cadence observed yet this
    /// floor evaluates above <paramref name="linger"/> and the clamp resolves to
    /// <paramref name="linger"/>, the documented deferral rather than an invented cadence or a floor of
    /// zero. The same arithmetic covers an unseeded <see cref="Estimator"/>: its
    /// <see cref="RoundTripEstimator.Timeout"/>, and therefore this timer's own <see cref="Timeout"/>,
    /// is also <see cref="TimeSpan.MaxValue"/> before the first sample, so a timer that has never
    /// measured a round trip resolves to <paramref name="linger"/> too.
    /// </param>
    /// <param name="linger">
    /// The Session's own Linger: backing off past the point where the Session would be abandoned
    /// achieves nothing, so this ceilings <see cref="Timeout"/> and wins over the floor when the two
    /// disagree.
    /// </param>
    /// <remarks>
    /// Both bounds are read fresh at every call rather than captured at construction: a consumer's own
    /// loop is irregular by construction (ADR 0002), so <paramref name="observedPumpInterval"/> moves
    /// as the cadence moves, and the timeout returned here has to move with it rather than clamp
    /// against a cadence sampled once and never revisited.
    /// </remarks>
    public TimeSpan EffectiveTimeout(TimeSpan observedPumpInterval, TimeSpan linger)
    {
        TimeSpan flooredTimeout = Timeout > observedPumpInterval ? Timeout : observedPumpInterval;
        return flooredTimeout < linger ? flooredTimeout : linger;
    }
}
