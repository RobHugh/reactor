using System.Reflection;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// <see cref="RetransmissionTimer"/> is pure arithmetic layered above a
/// <see cref="RoundTripEstimator"/>, so these tests construct it directly — the type is public
/// precisely so the backoff-and-reset behaviour, and issue #195's <see cref="RetransmissionTimer.EffectiveTimeout"/>
/// clamp, can be pinned without an <c>InternalsVisibleTo</c> (ADR 0015), the same reason
/// <see cref="PumpIntervalObserver"/> is public. The claim that pumping at one cadence
/// and then a different one under <c>FakeTimeProvider</c> moves the floor describes a
/// <see cref="DatagramSessionReactor"/> end-to-end, but <see cref="DatagramSessionReactor.ObservedPumpInterval"/>
/// is <c>internal</c> with no <c>InternalsVisibleTo</c> (ADR 0015), so it cannot be driven from here.
/// The cadence-change test below pins the equivalent at this type's own seam instead, by passing the
/// two cadence values straight in as the <c>observedPumpInterval</c> parameter; the end-to-end proof
/// over a real Peer belongs to #199.
/// </summary>
public class RetransmissionTimerTests
{
    [Fact]
    public void Timer_Fresh_HasZeroBackoffsAndReadsTheEstimatorsOwnTimeout()
    {
        // Arrange
        RetransmissionTimer timer = new();

        // Act, Assert — nothing has expired yet, so the timeout is exactly the estimator's own,
        // undoubled, and the estimator is unseeded so that is TimeSpan.MaxValue.
        timer.ConsecutiveBackoffs.Should().Be(0);
        timer.Timeout.Should().Be(timer.Estimator.Timeout);
        timer.Timeout.Should().Be(TimeSpan.MaxValue);
    }

    [Fact]
    public void BackOff_ConsecutiveExpiries_DoublesTheTimeoutEachTimeFromTheCurrentEstimate()
    {
        // Arrange
        RetransmissionTimer timer = new();
        timer.Estimator.Sample(TimeSpan.FromMilliseconds(100));
        TimeSpan estimatorTimeout = timer.Estimator.Timeout;

        // Act, Assert — each BackOff doubles Timeout again and raises the count by one.
        timer.BackOff();
        timer.ConsecutiveBackoffs.Should().Be(1);
        timer.Timeout.Should().Be(estimatorTimeout * 2);

        timer.BackOff();
        timer.ConsecutiveBackoffs.Should().Be(2);
        timer.Timeout.Should().Be(estimatorTimeout * 4);

        timer.BackOff();
        timer.ConsecutiveBackoffs.Should().Be(3);
        timer.Timeout.Should().Be(estimatorTimeout * 8);
    }

    [Fact]
    public void RecordDelivery_AfterSeveralBackoffs_ReturnsTimeoutToTheEstimateAndCountToZeroInOneStep()
    {
        // Arrange
        RetransmissionTimer timer = new();
        timer.Estimator.Sample(TimeSpan.FromMilliseconds(100));
        timer.BackOff();
        timer.BackOff();
        timer.BackOff();

        // Act
        timer.RecordDelivery();

        // Assert
        timer.ConsecutiveBackoffs.Should().Be(0);
        timer.Timeout.Should().Be(timer.Estimator.Timeout);
    }

    [Fact]
    public void RecordDelivery_AfterSeveralBackoffs_LeavesNoPartialMultiplierBehind()
    {
        // Arrange — a delivery must end the backoff cleanly, not decay it by one step at a time.
        RetransmissionTimer timer = new();
        timer.Estimator.Sample(TimeSpan.FromMilliseconds(100));
        for (int i = 0; i < 5; i++)
        {
            timer.BackOff();
        }

        TimeSpan estimatorTimeout = timer.Estimator.Timeout;

        // Act
        timer.RecordDelivery();

        // Assert — exactly the estimate, not half of the fifth doubling or any other intermediate.
        timer.Timeout.Should().Be(estimatorTimeout);
    }

    [Fact]
    public void BackOff_AfterARecordDelivery_StartsDoublingFromTheBeginningRatherThanWhereThePreviousRunLeftOff()
    {
        // Arrange — run up several backoffs, reset, then expire again.
        RetransmissionTimer timer = new();
        timer.Estimator.Sample(TimeSpan.FromMilliseconds(100));
        timer.BackOff();
        timer.BackOff();
        timer.BackOff();
        timer.RecordDelivery();
        TimeSpan estimatorTimeout = timer.Estimator.Timeout;

        // Act
        timer.BackOff();

        // Assert — one doubling, not a fourth doubling continuing from the previous run.
        timer.ConsecutiveBackoffs.Should().Be(1);
        timer.Timeout.Should().Be(estimatorTimeout * 2);
    }

    [Fact]
    public void BackOff_RepeatedCalls_LeavesTheEstimatorsSmoothedValueAndVarianceUntouched()
    {
        // Arrange — backoff sits above the estimator: the smoothed value still describes the network
        // as last measured, and backing off is not a new measurement.
        RetransmissionTimer timer = new();
        timer.Estimator.Sample(TimeSpan.FromMilliseconds(80));
        timer.Estimator.Sample(TimeSpan.FromMilliseconds(120));
        TimeSpan smoothedBefore = timer.Estimator.SmoothedRoundTrip;
        TimeSpan varianceBefore = timer.Estimator.RoundTripVariance;

        // Act
        timer.BackOff();
        timer.BackOff();
        timer.BackOff();

        // Assert
        timer.Estimator.SmoothedRoundTrip.Should().Be(smoothedBefore);
        timer.Estimator.RoundTripVariance.Should().Be(varianceBefore);
    }

    [Fact]
    public void RecordDelivery_AfterBackoffs_LeavesTheEstimatorsSmoothedValueAndVarianceUntouched()
    {
        // Arrange
        RetransmissionTimer timer = new();
        timer.Estimator.Sample(TimeSpan.FromMilliseconds(80));
        timer.Estimator.Sample(TimeSpan.FromMilliseconds(120));
        timer.BackOff();
        timer.BackOff();
        TimeSpan smoothedBefore = timer.Estimator.SmoothedRoundTrip;
        TimeSpan varianceBefore = timer.Estimator.RoundTripVariance;

        // Act
        timer.RecordDelivery();

        // Assert
        timer.Estimator.SmoothedRoundTrip.Should().Be(smoothedBefore);
        timer.Estimator.RoundTripVariance.Should().Be(varianceBefore);
    }

    [Fact]
    public void Timeout_ManyConsecutiveBackoffs_SaturatesAtMaxValueRatherThanOverflowing()
    {
        // Arrange — enough consecutive backoffs to blow well past any long tick count if doubling
        // wrapped instead of saturating.
        RetransmissionTimer timer = new();
        timer.Estimator.Sample(TimeSpan.FromMilliseconds(100));

        // Act
        for (int i = 0; i < 128; i++)
        {
            timer.BackOff();
        }

        // Assert
        timer.Timeout.Should().Be(TimeSpan.MaxValue);
    }

    [Fact]
    public void Timeout_BackOffOnAnUnseededEstimator_SaturatesFromTheFirstCallRatherThanOverflowing()
    {
        // Arrange — Estimator.Timeout is already TimeSpan.MaxValue while unseeded, so doubling has to
        // cope with that from the very first BackOff call, not just after many of them.
        RetransmissionTimer timer = new();

        // Act
        timer.BackOff();

        // Assert
        timer.Timeout.Should().Be(TimeSpan.MaxValue);
    }

    [Fact]
    public void BackOff_RepeatedCalls_AllocatesNothing()
    {
        // Arrange — a Peer backs off once per unacked expiry; this must cost nothing on the timer
        // sweep, matching the allocation-free discipline the ack path already holds itself to
        // (RoundTripEstimatorTests, RawReactorPumpTests).
        RetransmissionTimer timer = new();
        timer.Estimator.Sample(TimeSpan.FromMilliseconds(100));

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();

        // Act
        for (int i = 0; i < 1000; i++)
        {
            timer.BackOff();
        }

        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        (allocatedAfter - allocatedBefore).Should().Be(0);
    }

    [Fact]
    public void RecordDelivery_RepeatedCalls_AllocatesNothing()
    {
        // Arrange
        RetransmissionTimer timer = new();
        timer.Estimator.Sample(TimeSpan.FromMilliseconds(100));

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();

        // Act
        for (int i = 0; i < 1000; i++)
        {
            timer.BackOff();
            timer.RecordDelivery();
        }

        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        (allocatedAfter - allocatedBefore).Should().Be(0);
    }

    [Fact]
    public void RetransmissionTimer_PublicMembers_AreOnlyTheEstimatorCountTimeoutAndTheTwoTransitions()
    {
        // Arrange — no new knob: nothing configures the backoff, it only ever advances or resets in
        // response to what the caller observed on the wire.
        Type type = typeof(RetransmissionTimer);

        // Act
        IEnumerable<string> members = type
            .GetMembers(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Select(member => member.Name);

        // Assert
        members.Should().BeEquivalentTo(
            ".ctor",
            nameof(RetransmissionTimer.Estimator),
            $"get_{nameof(RetransmissionTimer.Estimator)}",
            nameof(RetransmissionTimer.ConsecutiveBackoffs),
            $"get_{nameof(RetransmissionTimer.ConsecutiveBackoffs)}",
            nameof(RetransmissionTimer.Timeout),
            $"get_{nameof(RetransmissionTimer.Timeout)}",
            nameof(RetransmissionTimer.BackOff),
            nameof(RetransmissionTimer.RecordDelivery),
            nameof(RetransmissionTimer.EffectiveTimeout));
    }

    [Fact]
    public void EffectiveTimeout_EstimateWithinBothBounds_ReturnsTheEstimateUnchanged()
    {
        // Arrange
        RetransmissionTimer timer = new();
        timer.Estimator.Sample(TimeSpan.FromMilliseconds(100));
        TimeSpan observedPumpInterval = TimeSpan.FromMilliseconds(10);
        TimeSpan linger = TimeSpan.FromSeconds(30);

        // Act
        TimeSpan effectiveTimeout = timer.EffectiveTimeout(observedPumpInterval, linger);

        // Assert
        effectiveTimeout.Should().Be(timer.Estimator.Timeout);
    }

    [Fact]
    public void EffectiveTimeout_BackedOffEstimateWithinBothBounds_ReturnsTheBackedOffTimeoutUnchanged()
    {
        // Arrange
        RetransmissionTimer timer = new();
        timer.Estimator.Sample(TimeSpan.FromMilliseconds(100));
        timer.BackOff();
        timer.BackOff();
        TimeSpan observedPumpInterval = TimeSpan.FromMilliseconds(10);
        TimeSpan linger = TimeSpan.FromSeconds(30);

        // Act
        TimeSpan effectiveTimeout = timer.EffectiveTimeout(observedPumpInterval, linger);

        // Assert
        effectiveTimeout.Should().Be(timer.Timeout);
    }

    [Fact]
    public void EffectiveTimeout_TimeoutBelowObservedPumpInterval_IsFlooredAtThePumpInterval()
    {
        // Arrange — a timeout shorter than the gap between Pumps cannot be acted on.
        RetransmissionTimer timer = new();
        timer.Estimator.Sample(TimeSpan.FromMilliseconds(1));
        TimeSpan observedPumpInterval = TimeSpan.FromSeconds(1);
        TimeSpan linger = TimeSpan.FromSeconds(30);

        // Act
        TimeSpan effectiveTimeout = timer.EffectiveTimeout(observedPumpInterval, linger);

        // Assert
        effectiveTimeout.Should().Be(observedPumpInterval);
    }

    [Fact]
    public void EffectiveTimeout_RepeatedBackoff_SaturatesAtLingerAndNeverExceedsItHoweverManyLossesOccur()
    {
        // Arrange — a starting estimate close to Linger so a handful of doublings crosses it, then many
        // more consecutive losses keep piling on top.
        RetransmissionTimer timer = new();
        timer.Estimator.Sample(TimeSpan.FromSeconds(1));
        TimeSpan observedPumpInterval = TimeSpan.FromMilliseconds(10);
        TimeSpan linger = TimeSpan.FromSeconds(5);

        // Act, Assert — every consecutive loss backs off further; the result never exceeds Linger, and
        // once the backed-off estimate has grown past it, the result is exactly Linger.
        for (int i = 0; i < 64; i++)
        {
            timer.BackOff();
            TimeSpan effectiveTimeout = timer.EffectiveTimeout(observedPumpInterval, linger);
            effectiveTimeout.Should().BeLessThanOrEqualTo(linger);
            if (timer.Timeout >= linger)
            {
                effectiveTimeout.Should().Be(linger);
            }
        }

        timer.EffectiveTimeout(observedPumpInterval, linger).Should().Be(linger);
    }

    [Fact]
    public void EffectiveTimeout_NoCadenceObservedYet_DefersToLingerRatherThanFlooringAtZero()
    {
        // Arrange — PumpIntervalObserver.Interval reads TimeSpan.MaxValue before a second Pump; that
        // is the "no cadence yet" reading this floor must defer on, per PumpIntervalObserver's own
        // documented rationale.
        RetransmissionTimer timer = new();
        timer.Estimator.Sample(TimeSpan.FromMilliseconds(100));
        TimeSpan linger = TimeSpan.FromSeconds(30);

        // Act
        TimeSpan effectiveTimeout = timer.EffectiveTimeout(TimeSpan.MaxValue, linger);

        // Assert
        effectiveTimeout.Should().Be(linger);
    }

    [Fact]
    public void EffectiveTimeout_UnseededEstimator_AlsoResolvesToLinger()
    {
        // Arrange — an unseeded RoundTripEstimator.Timeout is TimeSpan.MaxValue, so a timer that has
        // never sampled falls out of the same arithmetic as the no-cadence-yet case above.
        RetransmissionTimer timer = new();
        TimeSpan observedPumpInterval = TimeSpan.FromMilliseconds(10);
        TimeSpan linger = TimeSpan.FromSeconds(30);

        // Act
        TimeSpan effectiveTimeout = timer.EffectiveTimeout(observedPumpInterval, linger);

        // Assert
        effectiveTimeout.Should().Be(linger);
    }

    [Fact]
    public void EffectiveTimeout_ObservedPumpIntervalAboveLinger_ResolvesToLingerRatherThanInvertingOrThrowing()
    {
        // Arrange — a Pump cadence slower than Linger: the floor exceeds the ceiling.
        RetransmissionTimer timer = new();
        timer.Estimator.Sample(TimeSpan.FromMilliseconds(100));
        TimeSpan observedPumpInterval = TimeSpan.FromSeconds(60);
        TimeSpan linger = TimeSpan.FromSeconds(30);

        // Act
        Action act = () => timer.EffectiveTimeout(observedPumpInterval, linger).Should().Be(linger);

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void EffectiveTimeout_CadenceChangesBetweenCalls_MovesTheFloorWithTheNewCadence()
    {
        // Arrange — both bounds are read fresh at every call, not captured at construction; a
        // consumer's own loop is irregular by construction (ADR 0002), so the floor has to move as the
        // observed cadence moves. DatagramSessionReactor.ObservedPumpInterval is internal with no
        // InternalsVisibleTo, so this pins the equivalent at RetransmissionTimer's own seam by passing
        // the two cadence readings straight in; the end-to-end proof over a real Peer belongs to #199.
        RetransmissionTimer timer = new();
        timer.Estimator.Sample(TimeSpan.FromMilliseconds(1));
        TimeSpan linger = TimeSpan.FromSeconds(30);
        TimeSpan slowCadence = TimeSpan.FromSeconds(1);
        TimeSpan fastCadence = TimeSpan.FromMilliseconds(1);

        // Act
        TimeSpan effectiveAtSlowCadence = timer.EffectiveTimeout(slowCadence, linger);
        TimeSpan effectiveAtFastCadence = timer.EffectiveTimeout(fastCadence, linger);

        // Assert
        effectiveAtSlowCadence.Should().Be(slowCadence);
        effectiveAtFastCadence.Should().Be(timer.Estimator.Timeout);
        effectiveAtFastCadence.Should().BeLessThan(effectiveAtSlowCadence);
    }

    [Fact]
    public void EffectiveTimeout_RepeatedCalls_AllocatesNothing()
    {
        // Arrange
        RetransmissionTimer timer = new();
        timer.Estimator.Sample(TimeSpan.FromMilliseconds(100));
        TimeSpan observedPumpInterval = TimeSpan.FromMilliseconds(10);
        TimeSpan linger = TimeSpan.FromSeconds(30);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();

        // Act
        for (int i = 0; i < 1000; i++)
        {
            _ = timer.EffectiveTimeout(observedPumpInterval, linger);
        }

        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        (allocatedAfter - allocatedBefore).Should().Be(0);
    }
}
