using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// <see cref="ReceiveWindow"/> is pure arithmetic over a fixed-size array, so these tests construct
/// one directly and drive <see cref="ReceiveWindow.Classify"/> — the type is public precisely so this
/// behaviour can be pinned without an <c>InternalsVisibleTo</c> (ADR 0015), the same reason
/// <see cref="PumpIntervalObserver"/> is public. Nothing wires this structure into the data path in
/// this slice (#170); that is #172.
/// </summary>
public class ReceiveWindowTests
{
    [Fact]
    public void Classify_SequenceInsideWindowFirstTime_ReturnsAdmitted()
    {
        // Arrange
        ReceiveWindow window = new(capacityFragments: 4);

        // Act
        ReceiveWindowOutcome outcome = window.Classify(0);

        // Assert
        outcome.Should().Be(ReceiveWindowOutcome.Admitted);
    }

    [Fact]
    public void Classify_SameSequenceTwice_ReturnsDuplicateOnSecondCall()
    {
        // Arrange — the base does not advance past sequence 2, because 0 and 1 are still holes, so
        // 2 stays tracked as an admitted slot rather than being retired.
        ReceiveWindow window = new(capacityFragments: 4);
        window.Classify(2);

        // Act
        ReceiveWindowOutcome outcome = window.Classify(2);

        // Assert
        outcome.Should().Be(ReceiveWindowOutcome.Duplicate);
    }

    [Fact]
    public void Classify_SequenceFarAboveWindow_ReturnsOutOfWindowAndLeavesBaseUnmoved()
    {
        // Arrange
        ReceiveWindow window = new(capacityFragments: 4);

        // Act
        ReceiveWindowOutcome outcome = window.Classify(1_000);

        // Assert
        outcome.Should().Be(ReceiveWindowOutcome.OutOfWindow);
        window.Base.Should().Be(0);
    }

    [Fact]
    public void Classify_SequenceAtFarEdgeOfWindow_ReturnsOutOfWindow()
    {
        // Arrange — a window of capacity 4 based at 0 spans [0, 3]; sequence 4 is one past the edge.
        ReceiveWindow window = new(capacityFragments: 4);

        // Act
        ReceiveWindowOutcome outcome = window.Classify(4);

        // Assert
        outcome.Should().Be(ReceiveWindowOutcome.OutOfWindow);
    }

    [Fact]
    public void Classify_SequenceBelowBase_ReturnsOutOfWindowNotDuplicate()
    {
        // Arrange — admitting 0 through 3 in order advances the base to 4, retiring 0.
        ReceiveWindow window = new(capacityFragments: 4);
        window.Classify(0);
        window.Classify(1);
        window.Classify(2);
        window.Classify(3);

        // Act
        ReceiveWindowOutcome outcome = window.Classify(0);

        // Assert — not Duplicate: a duplicate is a sequence still tracked inside the window, and 0 no
        // longer is.
        outcome.Should().Be(ReceiveWindowOutcome.OutOfWindow);
    }

    [Fact]
    public void Classify_ContiguousSequencesInOrder_AdvancesBaseAfterEach()
    {
        // Arrange
        ReceiveWindow window = new(capacityFragments: 8);

        // Act, Assert
        window.Classify(0);
        window.Base.Should().Be(1);

        window.Classify(1);
        window.Base.Should().Be(2);

        window.Classify(2);
        window.Base.Should().Be(3);
    }

    [Fact]
    public void Classify_SequenceAboveAHole_LeavesBaseAtTheHole()
    {
        // Arrange
        ReceiveWindow window = new(capacityFragments: 8);

        // Act — sequence 1 arrives before sequence 0; the hole at 0 stops the base advancing.
        window.Classify(1);

        // Assert
        window.Base.Should().Be(0);
    }

    [Fact]
    public void Classify_ArrivalFillsAHole_AdvancesBaseAcrossEverythingAlreadyAdmittedAboveItInOneStep()
    {
        // Arrange — 1, 2 and 3 arrive first and sit behind the hole at 0.
        ReceiveWindow window = new(capacityFragments: 8);
        window.Classify(1);
        window.Classify(2);
        window.Classify(3);
        window.Base.Should().Be(0);

        // Act — filling the hole at 0 should carry the base past 0, 1, 2 and 3 in this one call.
        ReceiveWindowOutcome outcome = window.Classify(0);

        // Assert
        outcome.Should().Be(ReceiveWindowOutcome.Admitted);
        window.Base.Should().Be(4);
    }

    [Fact]
    public void Classify_SequenceAtTheBaseJustBelowTheU32Wrap_ReturnsAdmittedAndAdvancesBaseToTheWrap()
    {
        // Arrange — a window whose base already sits one below the wrap, built directly there rather
        // than by walking every sequence up to it.
        ReceiveWindow window = new(capacityFragments: 4, initialBase: uint.MaxValue - 1);

        // Act
        ReceiveWindowOutcome outcome = window.Classify(uint.MaxValue - 1);

        // Assert
        outcome.Should().Be(ReceiveWindowOutcome.Admitted);
        window.Base.Should().Be(uint.MaxValue);
    }

    [Fact]
    public void Classify_SequenceAtTheBaseWhenBaseIsMaxValue_AdvancesBaseAcrossTheWrapToZero()
    {
        // Arrange — a window whose base sits exactly at the wrap boundary: the last sequence before
        // the u32 space wraps back to 0.
        ReceiveWindow window = new(capacityFragments: 4, initialBase: uint.MaxValue);

        // Act
        ReceiveWindowOutcome outcome = window.Classify(uint.MaxValue);

        // Assert — admitting the sequence at the base carries it one step forward, and one step past
        // uint.MaxValue in serial order is 0, not an overflow.
        outcome.Should().Be(ReceiveWindowOutcome.Admitted);
        window.Base.Should().Be(0);
    }

    [Fact]
    public void Classify_SequenceJustAfterTheU32Wrap_IsAdmittedButDoesNotAdvanceTheBase()
    {
        // Arrange — a window whose base sits exactly at the wrap boundary; sequence 0 is one step
        // ahead of it, on the far side of the wrap.
        ReceiveWindow window = new(capacityFragments: 4, initialBase: uint.MaxValue);

        // Act
        ReceiveWindowOutcome outcome = window.Classify(0);

        // Assert — 0 is admitted into the window, but the base itself (uint.MaxValue) is still a
        // hole, so the base does not move.
        outcome.Should().Be(ReceiveWindowOutcome.Admitted);
        window.Base.Should().Be(uint.MaxValue);
    }

    [Fact]
    public void Classify_SequenceOneBelowBaseAcrossTheU32Wrap_ReturnsOutOfWindow()
    {
        // Arrange — a window whose base has already wrapped to sit just after uint.MaxValue.
        ReceiveWindow window = new(capacityFragments: 4, initialBase: 1);

        // Act — uint.MaxValue is below the base, on the far side of the wrap.
        ReceiveWindowOutcome outcome = window.Classify(uint.MaxValue);

        // Assert
        outcome.Should().Be(ReceiveWindowOutcome.OutOfWindow);
        window.Base.Should().Be(1);
    }

    [Fact]
    public void Classify_SequenceFarAboveBaseAcrossTheU32Wrap_ReturnsOutOfWindow()
    {
        // Arrange — a window based just below the wrap; the far edge of its span crosses the wrap
        // too, so this exercises the far-edge check on both sides of the boundary.
        ReceiveWindow window = new(capacityFragments: 4, initialBase: uint.MaxValue - 1);

        // Act — the window spans [uint.MaxValue - 1, 1]; sequence 2 is one past that span.
        ReceiveWindowOutcome outcome = window.Classify(2);

        // Assert
        outcome.Should().Be(ReceiveWindowOutcome.OutOfWindow);
        window.Base.Should().Be(uint.MaxValue - 1);
    }

    [Fact]
    public void Classify_ManyArrivalsFillingHolesRepeatedly_NeverAllocatesAfterConstruction()
    {
        // Arrange — a small window driven with an out-of-order pattern (odd sequences first, then the
        // even ones that fill the holes) repeated across many cycles, so any per-call allocation would
        // show up as growing managed memory.
        const int capacity = 16;
        const int cycles = 20_000;
        ReceiveWindow window = new(capacityFragments: capacity);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long before = GC.GetAllocatedBytesForCurrentThread();

        // Act
        uint sequence = 0;
        for (int cycle = 0; cycle < cycles; cycle++)
        {
            uint cycleBase = sequence;
            for (int i = 1; i < capacity; i += 2)
            {
                window.Classify(cycleBase + (uint)i);
            }

            for (int i = 0; i < capacity; i += 2)
            {
                window.Classify(cycleBase + (uint)i);
            }

            sequence = cycleBase + capacity;
        }

        long after = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        (after - before).Should().Be(0);
    }

    [Fact]
    public void Classify_NonSlidingWindowSequenceFarAboveWindow_LeavesLastSlideLossAtZero()
    {
        // Arrange — the default (slides: false) window, matching every existing caller.
        ReceiveWindow window = new(capacityFragments: 4);

        // Act
        ReceiveWindowOutcome outcome = window.Classify(1_000);

        // Assert — refused, not slid, and nothing reported lost.
        outcome.Should().Be(ReceiveWindowOutcome.OutOfWindow);
        window.Base.Should().Be(0);
        window.LastSlideLoss.Should().Be(0);
    }

    [Fact]
    public void Classify_SlidingWindowSequenceOnePastTheFarEdge_SlidesByOneAndCountsTheSkippedBaseAsLoss()
    {
        // Arrange — capacity 4 based at 0 spans [0, 3]; sequence 4 is one past the edge. The window's
        // own base slot (0) is a hole by construction — nothing has arrived yet — so admitting 4 must
        // slide past sequence 0 without it ever having been seen: that is the loss this ticket exists
        // to count, and it happens on the very first far-ahead arrival, not only after a visible hole.
        ReceiveWindow window = new(capacityFragments: 4, slides: true);

        // Act
        ReceiveWindowOutcome outcome = window.Classify(4);

        // Assert — the base slides forward exactly one, to keep 4 at the far edge (span [1, 4]).
        outcome.Should().Be(ReceiveWindowOutcome.Admitted);
        window.Base.Should().Be(1);
        window.LastSlideLoss.Should().Be(1);
    }

    [Fact]
    public void Classify_SlidingWindowFarAheadArrivalLeavesAnUnadmittedHoleBehind_ReportsOneLostPerHole()
    {
        // Arrange — capacity 4 based at 0 spans [0, 3]. 0, 1 and 2 admit in order, leaving 3 a hole
        // and the base stuck at 3 (span [3, 6]).
        ReceiveWindow window = new(capacityFragments: 4, slides: true);
        window.Classify(0);
        window.Classify(1);
        window.Classify(2);
        window.Base.Should().Be(3);

        // Act — 7 is one past the far edge (span [3, 6]); sliding to admit it must pass the hole at 3.
        ReceiveWindowOutcome outcome = window.Classify(7);

        // Assert — exactly one sequence (3) was retired un-admitted, and the base now sits past it.
        outcome.Should().Be(ReceiveWindowOutcome.Admitted);
        window.LastSlideLoss.Should().Be(1);
        window.Base.Should().BeGreaterThan(3);
    }

    [Fact]
    public void Classify_SlidingWindowJumpWiderThanCapacity_CountsSequencesNeverTrackedAsLossToo()
    {
        // Arrange — capacity 4 based at 0. A jump to sequence 100 retires the whole ring (4 slots,
        // none admitted) plus every sequence between the old far edge and the new one that never had a
        // slot to be tracked in at all.
        ReceiveWindow window = new(capacityFragments: 4, slides: true);

        // Act — the window's new base is 100 - 4 + 1 = 97, so 97 sequences (0 through 96) are retired.
        ReceiveWindowOutcome outcome = window.Classify(100);

        // Assert
        outcome.Should().Be(ReceiveWindowOutcome.Admitted);
        window.Base.Should().Be(97);
        window.LastSlideLoss.Should().Be(97);
    }

    [Fact]
    public void Classify_SlidingWindowLateArrivalStillInsideWindowBeforeAnySlide_AdmitsWithZeroLoss()
    {
        // Arrange — capacity 4 based at 0 spans [0, 3]. 0 and 2 admit, leaving a hole at 1; the base
        // stays at 1 (blocked by that hole), well inside the window's current span.
        ReceiveWindow window = new(capacityFragments: 4, slides: true);
        window.Classify(0);
        window.Classify(2);
        window.Base.Should().Be(1);

        // Act — 1 arrives late but the window never had to slide to make room for it.
        ReceiveWindowOutcome outcome = window.Classify(1);

        // Assert — admitted, base now past both, and nothing was ever counted as lost: a sequence
        // still inside the window when it arrives leaves no permanent gap.
        outcome.Should().Be(ReceiveWindowOutcome.Admitted);
        window.Base.Should().Be(3);
        window.LastSlideLoss.Should().Be(0);
    }

    [Fact]
    public void Classify_SlidingWindowDuplicateAfterASlide_ReportsZeroLoss()
    {
        // Arrange — sequence 4 arrives first, sliding a capacity-4 window based at 0 (span [0, 3]) one
        // step forward to base 1 (span [1, 4]) and counting the skipped sequence 0 as one loss.
        ReceiveWindow window = new(capacityFragments: 4, slides: true);
        window.Classify(4);
        window.LastSlideLoss.Should().Be(1);
        window.Base.Should().Be(1);

        // Act — resend the same sequence; it is still tracked inside the window, so nothing slides
        // and this call reports no further loss.
        ReceiveWindowOutcome outcome = window.Classify(4);

        // Assert
        outcome.Should().Be(ReceiveWindowOutcome.Duplicate);
        window.LastSlideLoss.Should().Be(0);
    }

    [Fact]
    public void Classify_SlidingWindowSequenceBelowBase_ReturnsOutOfWindowWithZeroLoss()
    {
        // Arrange — a slide that leaves the base at 1.
        ReceiveWindow window = new(capacityFragments: 4, slides: true);
        window.Classify(4);
        window.Base.Should().Be(1);

        // Act — 0 is below the base: already retired, not a new far-ahead arrival.
        ReceiveWindowOutcome outcome = window.Classify(0);

        // Assert — out of window, exactly as a non-sliding window reports a below-base sequence, and
        // no slide happens for a sequence behind the base.
        outcome.Should().Be(ReceiveWindowOutcome.OutOfWindow);
        window.LastSlideLoss.Should().Be(0);
    }

    [Fact]
    public void Constructor_CapacityBelowOne_ThrowsArgumentOutOfRangeException()
    {
        // Arrange, Act
        Action act = () => new ReceiveWindow(capacityFragments: 0);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Constructor_ValidCapacity_SetsCapacityAndStartsBaseAtZero()
    {
        // Arrange, Act
        ReceiveWindow window = new(capacityFragments: 7);

        // Assert
        window.Capacity.Should().Be(7);
        window.Base.Should().Be(0);
    }
}
