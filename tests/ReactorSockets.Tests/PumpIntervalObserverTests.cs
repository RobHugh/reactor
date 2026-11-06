using System.Reflection;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// <see cref="PumpIntervalObserver"/> is pure arithmetic over a supplied <see cref="TimeProvider"/>,
/// so these tests construct it directly — the type is public precisely so the behaviour can be pinned
/// without an <c>InternalsVisibleTo</c> (ADR 0015), the same reason <see cref="RedialSchedule"/> is
/// public. Its use by a Reactor's own Pump is issue #112 itself; its consumption by the re-dial
/// schedule is issue #125.
/// </summary>
public class PumpIntervalObserverTests
{
    [Fact]
    public void Interval_BeforeAnyObserveCall_IsMaxValueRatherThanZero()
    {
        // Arrange
        PumpIntervalObserver observer = new(new FakeTimeProvider());

        // Act, Assert — documented well-defined value, not an accidental zero: MaxValue defers to
        // RedialSchedule's own ceiling rather than inventing a cadence, and zero is the one value that
        // schedule's constructor refuses outright.
        observer.Interval.Should().Be(TimeSpan.MaxValue);
    }

    [Fact]
    public void Interval_AfterOnlyOneObserveCall_IsStillMaxValue()
    {
        // Arrange — one Pump alone establishes no interval between two Pumps.
        FakeTimeProvider timeProvider = new();
        PumpIntervalObserver observer = new(timeProvider);

        // Act
        observer.Observe();

        // Assert
        observer.Interval.Should().Be(TimeSpan.MaxValue);
    }

    [Fact]
    public void Interval_AfterTwoObserveCallsAFixedIntervalApart_ReadsThatInterval()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        PumpIntervalObserver observer = new(timeProvider);
        TimeSpan interval = TimeSpan.FromMilliseconds(50);

        // Act
        observer.Observe();
        timeProvider.Advance(interval);
        observer.Observe();

        // Assert
        observer.Interval.Should().Be(interval);
    }

    [Fact]
    public void Interval_UnderAnIrregularLoop_TracksTheMostRecentGapRatherThanAStaleFirstReading()
    {
        // Arrange — a consumer's loop is irregular by construction (ADR 0002: the library owns no
        // threads), so a later, different gap must overwrite the first rather than being averaged
        // with it or ignored.
        FakeTimeProvider timeProvider = new();
        PumpIntervalObserver observer = new(timeProvider);

        // Act
        observer.Observe();
        timeProvider.Advance(TimeSpan.FromMilliseconds(10));
        observer.Observe();
        TimeSpan afterFastGap = observer.Interval;

        timeProvider.Advance(TimeSpan.FromMilliseconds(200));
        observer.Observe();
        TimeSpan afterSlowGap = observer.Interval;

        // Assert
        afterFastGap.Should().Be(TimeSpan.FromMilliseconds(10));
        afterSlowGap.Should().Be(TimeSpan.FromMilliseconds(200));
    }

    [Fact]
    public void Interval_WallClockJumpedForwardAndBackBetweenTwoObserveCalls_ReadsTheMonotonicGapAndNothingElse()
    {
        // Arrange — the datagram Reactor's own cadence reading consults
        // only monotonic timestamps are consulted, so moving the wall clock changes nothing. A
        // FakeTimeProvider cannot prove it, because advancing it moves its timestamp and its UtcNow
        // together; this provider moves them apart, which is exactly the divergence a machine's clock
        // adjustment produces and the one that would otherwise let a single adjustment fire every
        // timer in the library at once.
        DivergentClockTimeProvider timeProvider = new();
        PumpIntervalObserver observer = new(timeProvider);

        // Act
        observer.Observe();
        timeProvider.AdvanceTimestamp(TimeSpan.FromMilliseconds(25));
        timeProvider.SetUtcNow(timeProvider.GetUtcNow() + TimeSpan.FromHours(3));
        timeProvider.SetUtcNow(timeProvider.GetUtcNow() - TimeSpan.FromDays(2));
        observer.Observe();

        // Assert
        observer.Interval.Should().Be(TimeSpan.FromMilliseconds(25));
    }

    [Fact]
    public void Interval_WallClockJumpedWithNoTimestampMovementAtAll_ReadsZeroRatherThanTheWallClockJump()
    {
        // Arrange — the other half of the same claim: a wall-clock jump on its own is not a cadence.
        // Two Pumps at the same monotonic instant are two Pumps with no gap between them, however far
        // the machine's clock moved in between.
        DivergentClockTimeProvider timeProvider = new();
        PumpIntervalObserver observer = new(timeProvider);

        // Act
        observer.Observe();
        timeProvider.SetUtcNow(timeProvider.GetUtcNow() + TimeSpan.FromHours(6));
        observer.Observe();

        // Assert
        observer.Interval.Should().Be(TimeSpan.Zero);
    }

    [Fact]
    public void Constructor_NullTimeProvider_ThrowsArgumentNullException()
    {
        // Act
        Action act = () => new PumpIntervalObserver(null!);

        // Assert
        act.Should().Throw<ArgumentNullException>().WithParameterName("timeProvider");
    }

    [Fact]
    public void PumpIntervalObserver_PublicMembers_AreOnlyTheConstructorIntervalAndObserve()
    {
        // Arrange — no new knob: nothing configures the interval, it is only ever measured.
        Type type = typeof(PumpIntervalObserver);

        // Act
        IEnumerable<string> members = type
            .GetMembers(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Select(member => member.Name);

        // Assert
        members.Should().BeEquivalentTo(
            ".ctor",
            nameof(PumpIntervalObserver.Interval),
            $"get_{nameof(PumpIntervalObserver.Interval)}",
            nameof(PumpIntervalObserver.Observe));
    }

    /// <summary>
    /// A <see cref="TimeProvider"/> whose monotonic timestamp and whose wall clock move
    /// independently, so a test can move one without the other. <see cref="FakeTimeProvider"/> ties
    /// the two together — <c>Advance</c> moves both — which makes it unable to distinguish code that
    /// reads <see cref="TimeProvider.GetTimestamp"/> from code that reads
    /// <see cref="TimeProvider.GetUtcNow"/>. This is a test double for a BCL abstraction the
    /// production code already takes, not an abstraction inserted for testability (ADR 0015).
    /// </summary>
    private sealed class DivergentClockTimeProvider : TimeProvider
    {
        private long _timestamp;
        private DateTimeOffset _utcNow = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp() => _timestamp;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        /// <summary>Moves the monotonic timestamp forward, leaving the wall clock where it is.</summary>
        public void AdvanceTimestamp(TimeSpan by) => _timestamp += by.Ticks;

        /// <summary>Moves the wall clock anywhere at all, leaving the monotonic timestamp where it is.</summary>
        public void SetUtcNow(DateTimeOffset utcNow) => _utcNow = utcNow;
    }
}
