using System.Reflection;
using System.Security.Cryptography;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// <see cref="RedialSchedule"/> is pure arithmetic over a supplied jitter source, so these tests
/// construct it directly — the type is public precisely so the schedule can be pinned without an
/// <c>InternalsVisibleTo</c> (ADR 0015). The schedule's use by a Reactor is issue #125.
/// </summary>
public class RedialScheduleTests
{
    /// <summary>
    /// A jitter source that returns the same byte for every draw, so a schedule built on it produces
    /// an exactly known sequence: <c>0xFF</c> throughout draws the top of every range and <c>0x00</c>
    /// throughout draws the bottom. Deriving from <see cref="RandomNumberGenerator"/> is the whole
    /// substitution route — nothing internal is reached, and no interface was invented for it.
    /// </summary>
    private sealed class ConstantByteJitter(byte value) : RandomNumberGenerator
    {
        public override void GetBytes(byte[] data) => GetBytes(data.AsSpan());

        public override void GetBytes(Span<byte> data) => data.Fill(value);
    }

    /// <summary>Draws the top of every range, so each delay equals the computed delay for that attempt.</summary>
    private static RandomNumberGenerator MaximumJitter => new ConstantByteJitter(0xFF);

    /// <summary>Draws the bottom of every range, so each delay is zero.</summary>
    private static RandomNumberGenerator MinimumJitter => new ConstantByteJitter(0x00);

    [Fact]
    public void NextDelay_UnderMaximumJitterAndAnUnreachableCeiling_DoublesFromTheObservedPumpInterval()
    {
        // Arrange — a Linger far beyond any delay drawn here, so the ceiling never bites and the
        // doubling itself is what the sequence shows.
        RedialSchedule schedule = new(
            pumpInterval: TimeSpan.FromMilliseconds(10),
            linger: TimeSpan.FromHours(1),
            jitter: MaximumJitter);

        // Act
        TimeSpan[] delays = [.. Enumerable.Range(0, 5).Select(_ => schedule.NextDelay())];

        // Assert
        delays.Should().Equal(
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMilliseconds(20),
            TimeSpan.FromMilliseconds(40),
            TimeSpan.FromMilliseconds(80),
            TimeSpan.FromMilliseconds(160));
    }

    [Fact]
    public void NextDelay_UnderMinimumJitter_ReturnsZeroBecauseTheRangeIsFull()
    {
        // Arrange — full jitter spans [0, computed], so the bottom of the range is zero itself and
        // not some fixed fraction of the computed delay.
        RedialSchedule schedule = new(
            pumpInterval: TimeSpan.FromMilliseconds(10),
            linger: TimeSpan.FromHours(1),
            jitter: MinimumJitter);

        // Act
        TimeSpan[] delays = [.. Enumerable.Range(0, 5).Select(_ => schedule.NextDelay())];

        // Assert
        delays.Should().AllSatisfy(delay => delay.Should().Be(TimeSpan.Zero));
    }

    [Fact]
    public void NextDelay_OverManyDrawsFromThePlatformGenerator_LiesWithinZeroAndTheComputedDelay()
    {
        // Arrange
        TimeSpan pumpInterval = TimeSpan.FromMilliseconds(10);
        RedialSchedule schedule = new(pumpInterval, linger: TimeSpan.FromHours(1));

        // Act
        TimeSpan[] delays = [.. Enumerable.Range(0, 8).Select(_ => schedule.NextDelay())];

        // Assert — bounds, never timestamps (ADR 0011): attempt n is capped by 2ⁿ × the interval.
        delays.Should().AllSatisfy(delay => delay.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero));
        for (int attempt = 0; attempt < delays.Length; attempt++)
        {
            delays[attempt].Should().BeLessThanOrEqualTo(pumpInterval * Math.Pow(2, attempt));
        }
    }

    [Fact]
    public void NextDelay_AskedForFarMoreAttemptsThanTheCeilingAllows_NeverExceedsTheEffectiveLinger()
    {
        // Arrange — 64 doublings from a millisecond overflow any naive multiplication, so this also
        // pins that the ceiling is applied before the arithmetic can run away.
        TimeSpan linger = TimeSpan.FromSeconds(2);
        RedialSchedule schedule = new(
            pumpInterval: TimeSpan.FromMilliseconds(1),
            linger: linger,
            jitter: MaximumJitter);

        // Act
        TimeSpan[] delays = [.. Enumerable.Range(0, 64).Select(_ => schedule.NextDelay())];

        // Assert
        delays.Should().AllSatisfy(delay => delay.Should().BeLessThanOrEqualTo(linger));
    }

    [Fact]
    public void NextDelay_OnceTheDoublingPassesTheCeiling_SettlesAtTheEffectiveLinger()
    {
        // Arrange
        TimeSpan linger = TimeSpan.FromMilliseconds(30);
        RedialSchedule schedule = new(
            pumpInterval: TimeSpan.FromMilliseconds(10),
            linger: linger,
            jitter: MaximumJitter);

        // Act
        TimeSpan[] delays = [.. Enumerable.Range(0, 4).Select(_ => schedule.NextDelay())];

        // Assert
        delays.Should().Equal(
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMilliseconds(20),
            linger,
            linger);
    }

    [Fact]
    public void NextDelay_WhenTheCeilingIsBelowTheObservedPumpInterval_IsTheCeilingFromTheFirstAttempt()
    {
        // Arrange — a Linger shorter than the consumer's own loop cadence: there is no room for even
        // one full-interval wait, and the ceiling is what remains.
        TimeSpan linger = TimeSpan.FromMilliseconds(4);
        RedialSchedule schedule = new(
            pumpInterval: TimeSpan.FromMilliseconds(10),
            linger: linger,
            jitter: MaximumJitter);

        // Act
        TimeSpan delay = schedule.NextDelay();

        // Assert
        delay.Should().Be(linger);
    }

    [Fact]
    public void NextDelay_WhenTheCeilingIsOdd_TakesTheLastFullDoublingBeforeSettlingOnIt()
    {
        // Arrange — an odd tick count is the case a halving test loses to truncation: 20 doubles to
        // 40, which is still under 41, so 40 is a delay the schedule owes before the ceiling applies.
        RedialSchedule schedule = new(
            pumpInterval: TimeSpan.FromTicks(10),
            linger: TimeSpan.FromTicks(41),
            jitter: MaximumJitter);

        // Act
        TimeSpan[] delays = [.. Enumerable.Range(0, 4).Select(_ => schedule.NextDelay())];

        // Assert
        delays.Should().Equal(
            TimeSpan.FromTicks(10),
            TimeSpan.FromTicks(20),
            TimeSpan.FromTicks(40),
            TimeSpan.FromTicks(41));
    }

    [Fact]
    public void NextDelay_UnderAJitterSourceDrawingMidRange_ScalesIntoTheRangeRatherThanPickingAnEnd()
    {
        // Arrange — 0x80 bytes are just over half of the 64-bit draw, so a source that scales its
        // draw across [0, computed] lands near the middle. An implementation that merely chose one
        // end or the other — a coin flip, the opposite of full jitter — passes the two constant-byte
        // tests above and fails here.
        TimeSpan pumpInterval = TimeSpan.FromSeconds(1);
        RedialSchedule schedule = new(pumpInterval, linger: TimeSpan.FromHours(1), jitter: new ConstantByteJitter(0x80));

        // Act
        TimeSpan delay = schedule.NextDelay();

        // Assert
        delay.Should().BeGreaterThan(pumpInterval * 0.4).And.BeLessThan(pumpInterval * 0.6);
    }

    [Fact]
    public void RedialSchedule_ConstructorParameters_AreTheTwoDerivedInputsAndTheJitterSource()
    {
        // Arrange — the knobs that must not exist would appear here if they existed anywhere: an
        // attempt limit, a separate initial delay, a separate maximum.
        ConstructorInfo constructor = typeof(RedialSchedule).GetConstructors().Single();

        // Act
        IEnumerable<string?> parameters = constructor.GetParameters().Select(parameter => parameter.Name);

        // Assert
        parameters.Should().BeEquivalentTo("pumpInterval", "linger", "jitter");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_PumpIntervalAtOrBelowZero_ThrowsArgumentOutOfRangeException(int milliseconds)
    {
        // Arrange
        TimeSpan pumpInterval = TimeSpan.FromMilliseconds(milliseconds);

        // Act
        Action act = () => new RedialSchedule(pumpInterval, linger: TimeSpan.FromSeconds(1));

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("pumpInterval");
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_LingerAtOrBelowZero_ThrowsArgumentOutOfRangeException(int milliseconds)
    {
        // Arrange
        TimeSpan linger = TimeSpan.FromMilliseconds(milliseconds);

        // Act
        Action act = () => new RedialSchedule(pumpInterval: TimeSpan.FromMilliseconds(10), linger);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("linger");
    }

    [Fact]
    public void Constructor_NoJitterSupplied_DrawsFromThePlatformGeneratorRatherThanThrowing()
    {
        // Arrange — the default is derived, not guessed: the platform CSPRNG is the citable
        // provenance, the same argument StreamSessionOptions.Entropy already makes.
        TimeSpan pumpInterval = TimeSpan.FromMilliseconds(10);
        RedialSchedule schedule = new(pumpInterval, linger: TimeSpan.FromSeconds(1));

        // Act
        TimeSpan delay = schedule.NextDelay();

        // Assert — the first computed delay is the Pump interval, so a defaulted source that was
        // never drawn from, or drawn from into the wrong range, shows up here.
        delay.Should().BeGreaterThanOrEqualTo(TimeSpan.Zero).And.BeLessThanOrEqualTo(pumpInterval);
    }

    [Fact]
    public void RedialSchedule_PublicMembers_AreOnlyTheConstructorAndTheNextDelayDraw()
    {
        // Arrange — Linger bounds the activity, so there is no attempt count and must not be one, and
        // initial, maximum and jitter are never three separate knobs (#111).
        Type type = typeof(RedialSchedule);

        // Act
        IEnumerable<string> members = type
            .GetMembers(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Select(member => member.Name);

        // Assert
        members.Should().BeEquivalentTo(".ctor", nameof(RedialSchedule.NextDelay));
    }
}
