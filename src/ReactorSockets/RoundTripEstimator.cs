namespace ReactorSockets;

/// <summary>
/// Tracks a smoothed round-trip time and its variance using the RFC 6298 recurrence, and derives a
/// retransmission timeout from the two for the adaptive RTO: a fixed timeout constant is rejected
/// because no single number is right for both a LAN and a satellite link, so the timeout has to be
/// derived from what this link is actually doing.
/// </summary>
/// <remarks>
/// <para>
/// Public for the same reason <see cref="PumpIntervalObserver"/> is (ADR 0015): a test pins this
/// behaviour by constructing one directly and calling <see cref="Sample(TimeSpan)"/> with hand-picked
/// <see cref="TimeSpan"/> values, with no <c>InternalsVisibleTo</c> and no interface invented for
/// the purpose.
/// </para>
/// <para>
/// This type reads no clock of its own. A caller measures one round trip from monotonic
/// <see cref="TimeProvider.GetTimestamp"/> / <see cref="TimeProvider.GetElapsedTime(long,long)"/>
/// readings and passes the elapsed <see cref="TimeSpan"/> into <see cref="Sample(TimeSpan)"/> — the
/// monotonic-only discipline ADR 0002 states for every timer this library owns is satisfied simply
/// because there is no clock in here to read the wrong way. A wall clock adjustment can only ever
/// distort the sample the caller measured; it cannot make this estimator itself misbehave.
/// </para>
/// <para>
/// The recurrence is RFC 6298 §2.2–2.3. The first sample seeds the estimate rather than being
/// averaged into an estimate that does not yet exist: <c>SRTT = R</c>, <c>RTTVAR = R / 2</c>. Every
/// later sample updates <c>RTTVAR</c> from the smoothed value that was current before this sample
/// arrived, then updates <c>SRTT</c>:
/// <c>RTTVAR = 3/4 * RTTVAR + 1/4 * |SRTT - R|</c>, then <c>SRTT = 7/8 * SRTT + 1/8 * R</c>. Both
/// updates are computed in whole clock ticks (<see cref="TimeSpan.Ticks"/>) rather than through
/// <see cref="double"/> conversions, so repeated samples do not accumulate floating-point rounding
/// drift over the life of a link.
/// </para>
/// <para>
/// Karn's rule — a retransmitted sequence's ack is ambiguous, so its round trip is discarded rather
/// than folded in — is implemented on <see cref="Sample(TimeSpan, bool)"/>. Backoff is layered above
/// this type instead: this estimator produces an unbounded, unbacked-off timeout from the samples it is
/// given. The bound against a Reactor's observed Pump cadence and the Linger ceiling is
/// <see cref="RetransmissionTimer.EffectiveTimeout"/>.
/// </para>
/// </remarks>
public sealed class RoundTripEstimator
{
    private long _smoothedRoundTripTicks;
    private long _roundTripVarianceTicks;

    /// <summary>
    /// Whether at least one sample has been recorded. Every property below is meaningless — and
    /// documented with a deliberate placeholder value — until this is <see langword="true"/>.
    /// </summary>
    public bool HasEstimate { get; private set; }

    /// <summary>
    /// The RFC 6298 smoothed round-trip time (SRTT). <see cref="TimeSpan.Zero"/> before the first
    /// <see cref="Sample(TimeSpan)"/> call, since nothing has been measured yet.
    /// </summary>
    public TimeSpan SmoothedRoundTrip => TimeSpan.FromTicks(_smoothedRoundTripTicks);

    /// <summary>
    /// The RFC 6298 round-trip time variance (RTTVAR). <see cref="TimeSpan.Zero"/> before the first
    /// <see cref="Sample(TimeSpan)"/> call, since nothing has been measured yet.
    /// </summary>
    public TimeSpan RoundTripVariance => TimeSpan.FromTicks(_roundTripVarianceTicks);

    /// <summary>
    /// The derived retransmission timeout: <see cref="SmoothedRoundTrip"/> plus four times
    /// <see cref="RoundTripVariance"/>, saturating at <see cref="TimeSpan.MaxValue"/> rather than
    /// overflowing if the two ever sum past it.
    /// </summary>
    /// <remarks>
    /// While <see cref="HasEstimate"/> is <see langword="false"/> this reads
    /// <see cref="TimeSpan.MaxValue"/> — deliberately, for the same reason
    /// <see cref="PumpIntervalObserver.Interval"/> is <see cref="TimeSpan.MaxValue"/> before a
    /// cadence exists. With nothing measured yet there is no honest timeout to report, so this
    /// value defers entirely to whatever ceiling the caller clamps against rather than inventing a
    /// figure — the same claim about the consumer's network this whole estimator exists to avoid
    /// making.
    /// </remarks>
    public TimeSpan Timeout => HasEstimate
        ? TimeSpan.FromTicks(SaturatingAdd(_smoothedRoundTripTicks, SaturatingQuadruple(_roundTripVarianceTicks)))
        : TimeSpan.MaxValue;

    /// <summary>
    /// Records one round trip's elapsed time, measured by the caller from monotonic
    /// <see cref="TimeProvider"/> timestamps, for a sequence that was never retransmitted. Equivalent
    /// to <see cref="Sample(TimeSpan, bool)"/> with <c>sequenceWasRetransmitted: false</c>.
    /// </summary>
    /// <param name="roundTrip">The elapsed time this round trip took.</param>
    public void Sample(TimeSpan roundTrip) => Sample(roundTrip, sequenceWasRetransmitted: false);

    /// <summary>
    /// Records one round trip's elapsed time, measured by the caller from monotonic
    /// <see cref="TimeProvider"/> timestamps — unless <paramref name="sequenceWasRetransmitted"/> is
    /// <see langword="true"/>, in which case the sample is discarded (Karn's algorithm).
    /// </summary>
    /// <remarks>
    /// <para>
    /// An ack for a sequence sent more than once is ambiguous: it answers either the original
    /// transmission or the retransmit, so the round trip a caller measured is either the true one or
    /// the true one plus a whole retransmit interval. There is no way to tell which from the ack
    /// alone. Folding an ambiguous sample into <see cref="SmoothedRoundTrip"/> and
    /// <see cref="RoundTripVariance"/> corrupts the estimate in exactly the conditions — a link
    /// already losing or delaying datagrams — where the estimate matters most, so the sample is
    /// discarded instead.
    /// </para>
    /// <para>
    /// Discarding is silent by design (ADR 0013): it is the mechanism working as intended, not a
    /// fault, and a lossy path must not fill the Sink with volume. This type holds no Sink reference
    /// to emit a Diagnostic through even if it wanted to.
    /// </para>
    /// <para>
    /// A discarded sample never counts as the first sample. An estimator that has only ever seen
    /// retransmitted sequences is still unseeded (<see cref="HasEstimate"/> is
    /// <see langword="false"/>); the next sample with <paramref name="sequenceWasRetransmitted"/>
    /// <see langword="false"/> seeds it by the RFC 6298 first-sample rule, exactly as if it were the
    /// first call ever made.
    /// </para>
    /// </remarks>
    /// <param name="roundTrip">The elapsed time this round trip took.</param>
    /// <param name="sequenceWasRetransmitted">
    /// Whether the acked sequence was ever sent more than once. When <see langword="true"/> the
    /// sample is ambiguous and is discarded without updating any published property.
    /// </param>
    public void Sample(TimeSpan roundTrip, bool sequenceWasRetransmitted)
    {
        if (sequenceWasRetransmitted)
        {
            return;
        }

        long r = roundTrip.Ticks;

        if (!HasEstimate)
        {
            _smoothedRoundTripTicks = r;
            _roundTripVarianceTicks = r / 2;
            HasEstimate = true;
            return;
        }

        long deviation = Math.Abs(_smoothedRoundTripTicks - r);
        _roundTripVarianceTicks = ((3 * _roundTripVarianceTicks) + deviation) / 4;
        _smoothedRoundTripTicks = ((7 * _smoothedRoundTripTicks) + r) / 8;
    }

    /// <summary>Multiplies a non-negative tick count by four, saturating at <see cref="long.MaxValue"/>.</summary>
    private static long SaturatingQuadruple(long ticks) =>
        ticks > long.MaxValue / 4 ? long.MaxValue : ticks * 4;

    /// <summary>Adds two non-negative tick counts, saturating at <see cref="long.MaxValue"/>.</summary>
    private static long SaturatingAdd(long left, long right) =>
        long.MaxValue - left < right ? long.MaxValue : left + right;
}
