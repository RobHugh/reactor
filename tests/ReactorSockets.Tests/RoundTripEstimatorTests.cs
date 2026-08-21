using System.Reflection;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// <see cref="RoundTripEstimator"/> is pure arithmetic over the <see cref="TimeSpan"/> values it is
/// handed, so these tests construct it directly — the type is public precisely so the RFC 6298
/// recurrence can be pinned without an <c>InternalsVisibleTo</c> (ADR 0015), the same reason
/// <see cref="PumpIntervalObserver"/> is public. Karn's rule (issue #193) is pinned here too, since it
/// lives on <see cref="RoundTripEstimator.Sample(TimeSpan, bool)"/>. Backoff (issue #194) is layered
/// above this type and is out of scope here.
/// </summary>
public class RoundTripEstimatorTests
{
    [Fact]
    public void Estimator_Fresh_HasNoEstimate()
    {
        // Arrange
        RoundTripEstimator estimator = new();

        // Act, Assert — no sample has arrived, so nothing is measured yet and Timeout defers
        // entirely to whatever ceiling the caller clamps against (issue #195) rather than inventing
        // a figure.
        estimator.HasEstimate.Should().BeFalse();
        estimator.SmoothedRoundTrip.Should().Be(TimeSpan.Zero);
        estimator.RoundTripVariance.Should().Be(TimeSpan.Zero);
        estimator.Timeout.Should().Be(TimeSpan.MaxValue);
    }

    [Fact]
    public void Sample_FirstSample_SeedsSmoothedAndVarianceByTheFirstSampleRuleRatherThanAveraging()
    {
        // Arrange — RFC 6298 §2.2: the first sample seeds the estimate, it is not folded into an
        // average that has nothing in it yet.
        RoundTripEstimator estimator = new();
        TimeSpan firstRoundTrip = TimeSpan.FromMilliseconds(100);

        // Act
        estimator.Sample(firstRoundTrip);

        // Assert
        estimator.HasEstimate.Should().BeTrue();
        estimator.SmoothedRoundTrip.Should().Be(firstRoundTrip);
        estimator.RoundTripVariance.Should().Be(TimeSpan.FromTicks(firstRoundTrip.Ticks / 2));
    }

    [Fact]
    public void Sample_ConstantDelayRepeated_ConvergesSmoothedOnThatDelayAndDrivesVarianceTowardsZero()
    {
        // Arrange — every later sample matches the running smoothed value exactly, so the deviation
        // RFC 6298 folds into RTTVAR is always zero and RTTVAR should decay towards zero while SRTT
        // never moves off the delay it already sits on.
        RoundTripEstimator estimator = new();
        TimeSpan delay = TimeSpan.FromMilliseconds(120);
        estimator.Sample(delay);

        // Act
        for (int i = 0; i < 50; i++)
        {
            estimator.Sample(delay);
        }

        // Assert
        estimator.SmoothedRoundTrip.Should().Be(delay);
        estimator.RoundTripVariance.Should().BeLessThan(TimeSpan.FromTicks(delay.Ticks / 1000));
    }

    [Fact]
    public void Sample_StepUpAfterConvergence_MovesVarianceUpFirstAndSmoothedTowardTheStepOverSeveralSamples()
    {
        // Arrange — converge on a low delay first, so the step's deviation is unambiguous.
        RoundTripEstimator estimator = new();
        TimeSpan lowDelay = TimeSpan.FromMilliseconds(100);
        TimeSpan highDelay = TimeSpan.FromMilliseconds(300);
        estimator.Sample(lowDelay);
        for (int i = 0; i < 10; i++)
        {
            estimator.Sample(lowDelay);
        }

        TimeSpan varianceBeforeStep = estimator.RoundTripVariance;
        TimeSpan smoothedBeforeStep = estimator.SmoothedRoundTrip;

        // Act — one sample at the higher delay.
        estimator.Sample(highDelay);
        TimeSpan varianceAfterOneStep = estimator.RoundTripVariance;
        TimeSpan smoothedAfterOneStep = estimator.SmoothedRoundTrip;

        // Assert — RTTVAR is computed from the OLD SRTT before SRTT itself moves, so the very first
        // sample of the step already shows up as a variance jump, while SRTT has only started moving
        // towards the new delay.
        varianceAfterOneStep.Should().BeGreaterThan(varianceBeforeStep);
        smoothedAfterOneStep.Should().BeGreaterThan(smoothedBeforeStep).And.BeLessThan(highDelay);

        // Act — feed the step several more times and track that SRTT keeps closing on it.
        TimeSpan previous = smoothedAfterOneStep;
        for (int i = 0; i < 20; i++)
        {
            estimator.Sample(highDelay);
            TimeSpan current = estimator.SmoothedRoundTrip;
            current.Should().BeGreaterThan(previous);
            current.Should().BeLessThanOrEqualTo(highDelay);
            previous = current;
        }
    }

    [Fact]
    public void Timeout_AfterSeveralSamples_IsSmoothedPlusFourVariancesRatherThanAMagicNumber()
    {
        // Arrange
        RoundTripEstimator estimator = new();
        estimator.Sample(TimeSpan.FromMilliseconds(80));
        estimator.Sample(TimeSpan.FromMilliseconds(150));
        estimator.Sample(TimeSpan.FromMilliseconds(95));

        // Act
        TimeSpan timeout = estimator.Timeout;

        // Assert — asserted as the relationship between the other two published properties, never a
        // literal.
        timeout.Should().Be(estimator.SmoothedRoundTrip + (estimator.RoundTripVariance * 4));
    }

    [Fact]
    public void Timeout_WhenSmoothedPlusFourVariancesWouldOverflow_SaturatesRatherThanOverflowing()
    {
        // Arrange — a single sample near TimeSpan's own ceiling seeds SRTT and RTTVAR near that
        // ceiling too (RTTVAR = R / 2), so SRTT + 4*RTTVAR would wrap past TimeSpan.MaxValue.Ticks if
        // it were computed without saturating.
        RoundTripEstimator estimator = new();
        estimator.Sample(TimeSpan.FromTicks(TimeSpan.MaxValue.Ticks - 10));

        // Act
        TimeSpan timeout = estimator.Timeout;

        // Assert
        timeout.Should().Be(TimeSpan.MaxValue);
    }

    [Fact]
    public void Sample_WallClockAdvancedBetweenSamples_ChangesNoEstimate()
    {
        // Arrange — the estimator reads no clock of its own; only the TimeSpan values it is handed
        // matter. Advancing a FakeTimeProvider's wall clock between samples must have no effect on
        // either published estimate, since nothing in RoundTripEstimator ever consults it.
        FakeTimeProvider timeProvider = new();
        RoundTripEstimator withoutClockAdvance = new();
        RoundTripEstimator withClockAdvance = new();

        // Act
        withoutClockAdvance.Sample(TimeSpan.FromMilliseconds(50));
        withoutClockAdvance.Sample(TimeSpan.FromMilliseconds(70));

        withClockAdvance.Sample(TimeSpan.FromMilliseconds(50));
        timeProvider.Advance(TimeSpan.FromHours(3));
        withClockAdvance.Sample(TimeSpan.FromMilliseconds(70));

        // Assert
        withClockAdvance.SmoothedRoundTrip.Should().Be(withoutClockAdvance.SmoothedRoundTrip);
        withClockAdvance.RoundTripVariance.Should().Be(withoutClockAdvance.RoundTripVariance);
    }

    [Fact]
    public void Sample_RepeatedCalls_AllocatesNothing()
    {
        // Arrange — a Peer samples one round trip per acked datagram; this must cost nothing on the
        // receive path, matching the allocation-free discipline the ack path already holds itself to
        // (DatagramSessionAckBlockTests, RawReactorPumpTests).
        RoundTripEstimator estimator = new();
        TimeSpan[] roundTrips =
        [
            TimeSpan.FromMilliseconds(80),
            TimeSpan.FromMilliseconds(120),
            TimeSpan.FromMilliseconds(95),
            TimeSpan.FromMilliseconds(110),
        ];

        foreach (TimeSpan roundTrip in roundTrips)
        {
            estimator.Sample(roundTrip);
        }

        // Act
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1000; i++)
        {
            estimator.Sample(roundTrips[i % roundTrips.Length]);
        }

        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        (allocatedAfter - allocatedBefore).Should().Be(0);
    }

    [Fact]
    public void RoundTripEstimator_PublicMembers_AreOnlyTheConstructorEstimatePropertiesAndSample()
    {
        // Arrange — no new knob: nothing configures the estimate, it is only ever measured.
        Type type = typeof(RoundTripEstimator);

        // Act
        IEnumerable<string> members = type
            .GetMembers(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Select(member => member.Name);

        // Assert
        members.Should().BeEquivalentTo(
            ".ctor",
            nameof(RoundTripEstimator.HasEstimate),
            $"get_{nameof(RoundTripEstimator.HasEstimate)}",
            nameof(RoundTripEstimator.SmoothedRoundTrip),
            $"get_{nameof(RoundTripEstimator.SmoothedRoundTrip)}",
            nameof(RoundTripEstimator.RoundTripVariance),
            $"get_{nameof(RoundTripEstimator.RoundTripVariance)}",
            nameof(RoundTripEstimator.Timeout),
            $"get_{nameof(RoundTripEstimator.Timeout)}",
            nameof(RoundTripEstimator.Sample),
            nameof(RoundTripEstimator.Sample));
    }

    [Fact]
    public void Sample_SequenceNeverRetransmitted_UpdatesTheEstimateAsBefore()
    {
        // Arrange — the two-arg overload with sequenceWasRetransmitted: false must behave identically
        // to the one-arg overload it delegates from.
        RoundTripEstimator estimator = new();
        TimeSpan roundTrip = TimeSpan.FromMilliseconds(100);

        // Act
        estimator.Sample(roundTrip, sequenceWasRetransmitted: false);

        // Assert
        estimator.HasEstimate.Should().BeTrue();
        estimator.SmoothedRoundTrip.Should().Be(roundTrip);
        estimator.RoundTripVariance.Should().Be(TimeSpan.FromTicks(roundTrip.Ticks / 2));
    }

    [Fact]
    public void Sample_SequenceWasRetransmitted_LeavesSmoothedVarianceAndTimeoutUnchanged()
    {
        // Arrange — seed the estimator with a normal sample first, so there is a live estimate that a
        // discarded sample could corrupt if Karn's rule were not applied.
        RoundTripEstimator estimator = new();
        estimator.Sample(TimeSpan.FromMilliseconds(100));
        estimator.Sample(TimeSpan.FromMilliseconds(120));

        TimeSpan smoothedBefore = estimator.SmoothedRoundTrip;
        TimeSpan varianceBefore = estimator.RoundTripVariance;
        TimeSpan timeoutBefore = estimator.Timeout;

        // Act — an ambiguous sample far outside the converged range, which would move every published
        // value if it were folded in.
        estimator.Sample(TimeSpan.FromMilliseconds(5000), sequenceWasRetransmitted: true);

        // Assert
        estimator.SmoothedRoundTrip.Should().Be(smoothedBefore);
        estimator.RoundTripVariance.Should().Be(varianceBefore);
        estimator.Timeout.Should().Be(timeoutBefore);
    }

    [Fact]
    public void Sample_OriginalLostRetransmitAcked_EstimatorDoesNotMove()
    {
        // Arrange — the ambiguous case Karn's algorithm exists for: the original transmission of a
        // sequence is lost, the retransmit is what actually gets acked, and the caller has no way to
        // know from the ack alone which transmission the measured round trip belongs to. The estimator
        // must treat this exactly like any other retransmitted-sequence sample: discard it.
        RoundTripEstimator estimator = new();
        estimator.Sample(TimeSpan.FromMilliseconds(90));

        TimeSpan smoothedBefore = estimator.SmoothedRoundTrip;
        TimeSpan varianceBefore = estimator.RoundTripVariance;

        // Act — original send is lost (no ack ever arrives for it), the sequence is retransmitted, and
        // the ack that eventually arrives is for that retransmitted sequence.
        TimeSpan measuredRoundTripForRetransmit = TimeSpan.FromMilliseconds(400);
        estimator.Sample(measuredRoundTripForRetransmit, sequenceWasRetransmitted: true);

        // Assert — the estimator did not move at all, not merely that delivery of the ack succeeded.
        estimator.SmoothedRoundTrip.Should().Be(smoothedBefore);
        estimator.RoundTripVariance.Should().Be(varianceBefore);
    }

    [Fact]
    public void Sample_OnlyRetransmittedSamplesSeen_RemainsUnseededUntilACleanSampleArrives()
    {
        // Arrange — a fresh estimator that has only ever seen retransmitted (discarded) samples must
        // not be seeded by them: discarding must not count as the first sample.
        RoundTripEstimator estimator = new();

        // Act
        estimator.Sample(TimeSpan.FromMilliseconds(50), sequenceWasRetransmitted: true);
        estimator.Sample(TimeSpan.FromMilliseconds(80), sequenceWasRetransmitted: true);

        // Assert — still unseeded.
        estimator.HasEstimate.Should().BeFalse();
        estimator.SmoothedRoundTrip.Should().Be(TimeSpan.Zero);
        estimator.RoundTripVariance.Should().Be(TimeSpan.Zero);

        // Act — the next clean sample seeds it by the first-sample rule, as if the discarded samples
        // never happened.
        TimeSpan firstCleanRoundTrip = TimeSpan.FromMilliseconds(100);
        estimator.Sample(firstCleanRoundTrip, sequenceWasRetransmitted: false);

        // Assert
        estimator.HasEstimate.Should().BeTrue();
        estimator.SmoothedRoundTrip.Should().Be(firstCleanRoundTrip);
        estimator.RoundTripVariance.Should().Be(TimeSpan.FromTicks(firstCleanRoundTrip.Ticks / 2));
    }
}
