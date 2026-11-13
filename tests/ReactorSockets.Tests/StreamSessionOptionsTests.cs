using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// <see cref="StreamSessionOptions"/> is validated at construction and nowhere else (see
/// tests/CLAUDE.md), so every throw test here builds a <see cref="StreamSessionReactor"/> — the
/// constructor is the only caller a consumer can reach.
/// </summary>
public class StreamSessionOptionsTests
{
    /// <summary>
    /// A valid baseline, with every knob overridable by name so each test reads as a statement about
    /// the one knob it varies.
    /// </summary>
    private static StreamSessionOptions CreateOptions(
        int capacity = 4,
        int? admissionCap = null,
        int maxMessageSize = 4096,
        int retentionBytes = 4100,
        int? maxPeersPerAddress = null,
        int? peerLimitIPv6PrefixLength = null,
        int? receiveBufferSize = null,
        int? sendBufferSize = null,
        int? missedHeartbeats = null,
        int? arenaSize = null,
        TimeSpan? sendIdleInterval = null,
        TimeSpan? linger = null,
        IReactorDiagnostics? sink = null,
        DiagnosticLevel? minimumLevel = null) => new()
        {
            Reactor = new ReactorOptions
            {
                Capacity = capacity,
                AdmissionCap = admissionCap,
                ArenaSize = arenaSize,
                Sink = sink,
                MinimumLevel = minimumLevel,
            },
            MaxMessageSize = maxMessageSize,
            RetentionBytes = retentionBytes,
            MaxPeersPerAddress = maxPeersPerAddress,
            PeerLimitIPv6PrefixLength = peerLimitIPv6PrefixLength,
            ReceiveBufferSize = receiveBufferSize,
            SendBufferSize = sendBufferSize,
            MissedHeartbeats = missedHeartbeats,
            SendIdleInterval = sendIdleInterval ?? TimeSpan.FromSeconds(1),
            Linger = linger ?? TimeSpan.FromSeconds(1),
        };

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_CapacityBelowOne_ThrowsArgumentOutOfRangeException(int capacity)
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(capacity: capacity);

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(ReactorOptions.Capacity));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_AdmissionCapBelowOne_ThrowsArgumentOutOfRangeException(int admissionCap)
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(admissionCap: admissionCap);

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(ReactorOptions.AdmissionCap));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_MaxMessageSizeBelowOne_ThrowsArgumentOutOfRangeException(int maxMessageSize)
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(maxMessageSize: maxMessageSize, retentionBytes: 100);

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(StreamSessionOptions.MaxMessageSize));
    }

    [Theory]
    [InlineData(4, 3)]
    [InlineData(2, 1)]
    [InlineData(1, 1)]
    public void ResolvedMaxPeersPerAddress_LeftUnset_IsOneBelowCapacityAndAtLeastOne(int capacity, int expected)
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(capacity: capacity);

        // Act
        int resolved = options.ResolvedMaxPeersPerAddress;

        // Assert
        resolved.Should().Be(expected);
    }

    [Fact]
    public void ResolvedMaxPeersPerAddress_Set_IsTheValueSet()
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(capacity: 4, maxPeersPerAddress: 4);

        // Act
        int resolved = options.ResolvedMaxPeersPerAddress;

        // Assert
        resolved.Should().Be(4);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_MaxPeersPerAddressBelowOne_ThrowsArgumentOutOfRangeException(int maxPeersPerAddress)
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(maxPeersPerAddress: maxPeersPerAddress);

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(StreamSessionOptions.MaxPeersPerAddress));
    }

    [Fact]
    public void ResolvedPeerLimitIPv6PrefixLength_LeftUnset_IsSixtyFour()
    {
        // Arrange
        StreamSessionOptions options = CreateOptions();

        // Act
        int resolved = options.ResolvedPeerLimitIPv6PrefixLength;

        // Assert
        resolved.Should().Be(64);
    }

    [Fact]
    public void ResolvedPeerLimitIPv6PrefixLength_Set_IsTheValueSet()
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(peerLimitIPv6PrefixLength: 128);

        // Act
        int resolved = options.ResolvedPeerLimitIPv6PrefixLength;

        // Assert
        resolved.Should().Be(128);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(129)]
    public void Constructor_PeerLimitIPv6PrefixLengthOutsideOneTo128_ThrowsArgumentOutOfRangeException(int prefixLength)
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(peerLimitIPv6PrefixLength: prefixLength);

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(StreamSessionOptions.PeerLimitIPv6PrefixLength));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(128)]
    public void Constructor_PeerLimitIPv6PrefixLengthAtEitherBound_Succeeds(int prefixLength)
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(peerLimitIPv6PrefixLength: prefixLength);

        // Act
        Action act = () => new StreamSessionReactor(options).Dispose();

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_RetentionBytesBelowMaxMessageSizePlusFour_ThrowsArgumentExceptionNamingArithmetic()
    {
        // Arrange — the floor is MaxMessageSize + 4 (one length prefix); one byte under must throw.
        StreamSessionOptions options = CreateOptions(maxMessageSize: 4096, retentionBytes: 4099);

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(StreamSessionOptions.RetentionBytes))
            .Which.Message.Should().Contain("4099").And.Contain("4096");
    }

    [Fact]
    public void Constructor_RetentionBytesAtMaxMessageSizePlusFour_DoesNotThrow()
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(maxMessageSize: 4096, retentionBytes: 4100);

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_ReceiveBufferSizeBelowMaxMessageSizePlusFour_ThrowsArgumentExceptionNamingArithmetic()
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(maxMessageSize: 4096, retentionBytes: 4100, receiveBufferSize: 4099);

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(StreamSessionOptions.ReceiveBufferSize))
            .Which.Message.Should().Contain("4099").And.Contain("4096");
    }

    [Fact]
    public void Constructor_ReceiveBufferSizeAtMaxMessageSizePlusFour_DoesNotThrow()
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(maxMessageSize: 4096, retentionBytes: 4100, receiveBufferSize: 4100);

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().NotThrow();
    }

    [Theory]
    [InlineData(int.MaxValue)]
    [InlineData(int.MaxValue - 1)]
    [InlineData(int.MaxValue - 3)]
    public void ReceiveBufferFloor_UnvalidatedMaxMessageSizeNearIntMaxValue_SaturatesAtIntMaxValue(int maxMessageSize)
    {
        // Arrange — never constructed, so Validate never rejects the out-of-limit MaxMessageSize.
        StreamSessionOptions options = CreateOptions(maxMessageSize: maxMessageSize);

        // Act
        int floor = options.ReceiveBufferFloor;

        // Assert
        floor.Should().Be(int.MaxValue);
    }

    [Theory]
    [InlineData(int.MaxValue)]
    [InlineData(int.MaxValue - 1)]
    [InlineData(int.MaxValue - 3)]
    public void ResolvedReceiveBufferSize_UnvalidatedMaxMessageSizeNearIntMaxValueAndUnset_SaturatesAtIntMaxValue(
        int maxMessageSize)
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(maxMessageSize: maxMessageSize);

        // Act
        int resolved = options.ResolvedReceiveBufferSize;

        // Assert
        resolved.Should().Be(int.MaxValue);
    }

    [Theory]
    [InlineData(2_147_483_587)] // The MaxMessageSize limit: Array.MaxLength minus the 4-byte prefix.
    [InlineData(4096)]
    public void ReceiveBufferFloor_MaxMessageSizeAtOrBelowTheLimit_IsMaxMessageSizePlusFour(int maxMessageSize)
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(maxMessageSize: maxMessageSize);

        // Act
        int floor = options.ReceiveBufferFloor;

        // Assert
        floor.Should().Be(maxMessageSize + 4);
    }

    [Fact]
    public void ResolvedReceiveBufferSize_ReceiveBufferSizeUnset_DefaultsToMaxMessageSizePlusFour()
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(maxMessageSize: 4096, retentionBytes: 4100);

        // Act
        int resolved = options.ResolvedReceiveBufferSize;

        // Assert
        resolved.Should().Be(4100);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_SendBufferSizeBelowOne_ThrowsArgumentOutOfRangeException(int sendBufferSize)
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(sendBufferSize: sendBufferSize);

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(StreamSessionOptions.SendBufferSize));
    }

    [Fact]
    public void Constructor_SendBufferSizeFarBelowMaxMessageSize_DoesNotThrow()
    {
        // Arrange — deliberately smaller than one Message, unlike the receive buffer, which has a
        // one-Message floor: a send buffer narrower than a Message is a slower connection, since a
        // flush hands over what the kernel takes and keeps the rest retained, not a broken one.
        StreamSessionOptions options = CreateOptions(maxMessageSize: 4096, retentionBytes: 4100, sendBufferSize: 1024);

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_ArenaSizeBelowMaxMessageSize_ThrowsArgumentExceptionNamingArithmetic()
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(maxMessageSize: 4096, retentionBytes: 4100, arenaSize: 4095);

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(ReactorOptions.ArenaSize))
            .Which.Message.Should().Contain("4095").And.Contain("4096");
    }

    [Fact]
    public void Constructor_ArenaSizeEqualToMaxMessageSize_DoesNotThrow()
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(maxMessageSize: 4096, retentionBytes: 4100, arenaSize: 4096);

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().NotThrow();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(0)]
    public void Constructor_MissedHeartbeatsBelowTwo_ThrowsArgumentOutOfRangeException(int missedHeartbeats)
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(missedHeartbeats: missedHeartbeats);

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(StreamSessionOptions.MissedHeartbeats));
    }

    [Fact]
    public void Constructor_MissedHeartbeatsAtTwo_DoesNotThrow()
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(missedHeartbeats: 2);

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void ResolvedMissedHeartbeats_MissedHeartbeatsUnset_DefaultsToThree()
    {
        // Arrange
        StreamSessionOptions options = CreateOptions();

        // Act
        int resolved = options.ResolvedMissedHeartbeats;

        // Assert
        resolved.Should().Be(3);
    }

    [Theory]
    [InlineData(0.0)]
    [InlineData(-1.0)]
    public void Constructor_SendIdleIntervalZeroOrNegative_ThrowsArgumentOutOfRangeException(double seconds)
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(sendIdleInterval: TimeSpan.FromSeconds(seconds));

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(StreamSessionOptions.SendIdleInterval));
    }

    [Theory]
    [InlineData(0.0)]
    [InlineData(-1.0)]
    public void Constructor_LingerZeroOrNegative_ThrowsArgumentOutOfRangeException(double seconds)
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(linger: TimeSpan.FromSeconds(seconds));

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(StreamSessionOptions.Linger));
    }

    [Theory]
    [InlineData(42_949_672_950_001L)] // one tick above uint.MaxValue milliseconds
    [InlineData(long.MaxValue)]
    public void Constructor_SendIdleIntervalAboveTheAnnounceableMaximum_ThrowsArgumentOutOfRangeException(long ticks)
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(sendIdleInterval: TimeSpan.FromTicks(ticks));

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(StreamSessionOptions.SendIdleInterval));
    }

    [Theory]
    [InlineData(42_949_672_950_001L)] // one tick above uint.MaxValue milliseconds
    [InlineData(long.MaxValue)]
    public void Constructor_LingerAboveTheAnnounceableMaximum_ThrowsArgumentOutOfRangeException(long ticks)
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(linger: TimeSpan.FromTicks(ticks));

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(StreamSessionOptions.Linger));
    }

    [Fact]
    public void Constructor_IntervalsAtTheAnnounceableMaximum_Constructs()
    {
        // Arrange
        TimeSpan maximum = TimeSpan.FromTicks(42_949_672_950_000L); // exactly uint.MaxValue milliseconds
        StreamSessionOptions options = CreateOptions(sendIdleInterval: maximum, linger: maximum);

        // Act
        Action act = () => new StreamSessionReactor(options).Dispose();

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_SinkWithoutMinimumLevel_ThrowsArgumentException()
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(sink: new RecordingDiagnosticsSink());

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(ReactorOptions.Sink));
    }

    [Fact]
    public void Constructor_MinimumLevelWithoutSink_ThrowsArgumentException()
    {
        // Arrange
        StreamSessionOptions options = CreateOptions(minimumLevel: DiagnosticLevel.Warn);

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(ReactorOptions.MinimumLevel));
    }

    [Fact]
    public void ReceiveBudgetBytes_WorkedExample_EqualsCapacityTimesReceiveBufferSize()
    {
        // Arrange — distinct numbers so a transposed factor in the formula is caught.
        StreamSessionOptions options = CreateOptions(capacity: 3, maxMessageSize: 4000, receiveBufferSize: 5000, retentionBytes: 7000);

        // Act
        long budget = options.ReceiveBudgetBytes;

        // Assert
        budget.Should().Be(3L * 5000);
    }

    [Fact]
    public void SendBudgetBytes_WorkedExample_EqualsCapacityTimesRetentionBytes()
    {
        // Arrange — distinct numbers so a transposed factor in the formula is caught.
        StreamSessionOptions options = CreateOptions(capacity: 3, retentionBytes: 7000);

        // Act
        long budget = options.SendBudgetBytes;

        // Assert
        budget.Should().Be(3L * 7000);
    }

    [Fact]
    public void BoundaryRingBudgetBytes_WorkedExample_EqualsCapacityTimesEntriesTimesFourBytes()
    {
        // Arrange — 7000 divides the four-byte length prefix evenly, so the ceiling division inside
        // the formula introduces no rounding and the expected figure is exact arithmetic (issue #110).
        StreamSessionOptions options = CreateOptions(capacity: 3, retentionBytes: 7000);

        // Act
        long budget = options.BoundaryRingBudgetBytes;

        // Assert
        budget.Should().Be(3L * (7000 / 4) * 4);
    }

    [Fact]
    public void Constructor_MemoryBillExceedsMaxAllocatable_ThrowsArgumentExceptionNamingTheTotal()
    {
        // Arrange — capacity 100 000 with a 1 000 000-byte retention adds to far more than can be
        // allocated, on the send line alone.
        StreamSessionOptions options = CreateOptions(capacity: 100_000, maxMessageSize: 4096, retentionBytes: 1_000_000);
        long expectedReceive = 100_000L * 4100;
        long expectedSend = 100_000L * 1_000_000;
        long expectedBoundaryRing = 100_000L * (1_000_000 / 4) * 4;
        long expectedTotal = expectedReceive + expectedSend + expectedBoundaryRing;

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(StreamSessionOptions.Reactor))
            .Which.Message.Should()
            .Contain(expectedTotal.ToString())
            .And.Contain(expectedReceive.ToString())
            .And.Contain(expectedSend.ToString())
            .And.Contain(expectedBoundaryRing.ToString());
    }

    [Fact]
    public void Constructor_MemoryBillExceedsMaxAllocatableOnlyBecauseOfTheBoundaryRing_ThrowsArgumentExceptionNamingTheTotal()
    {
        // Arrange — receive and send alone (capacity 1 against a 1.2-billion-byte retention) fit
        // comfortably under Array.MaxLength; the Message-boundary ring (issue #110) is roughly as
        // large as the send line itself — one four-byte entry per four retained bytes at this
        // profile's length-prefix floor — and it alone is what tips the total over.
        const int capacity = 1;
        const int maxMessageSize = 4096;
        const int retentionBytes = 1_200_000_000;
        StreamSessionOptions options = CreateOptions(capacity: capacity, maxMessageSize: maxMessageSize, retentionBytes: retentionBytes);
        long expectedReceive = capacity * 4100L;
        long expectedSend = capacity * (long)retentionBytes;
        long expectedBoundaryRing = capacity * (retentionBytes / 4L) * 4;
        long expectedTotal = expectedReceive + expectedSend + expectedBoundaryRing;

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert — receive + send alone would fit; only adding the ring pushes the total over.
        (expectedReceive + expectedSend).Should().BeLessThan(Array.MaxLength);
        expectedTotal.Should().BeGreaterThan(Array.MaxLength);
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(StreamSessionOptions.Reactor))
            .Which.Message.Should()
            .Contain(expectedTotal.ToString())
            .And.Contain(expectedBoundaryRing.ToString());
    }

    /// <summary>
    /// The largest <see cref="StreamSessionOptions.MaxMessageSize"/> whose framed form, the Message plus
    /// its 4-byte length prefix, still fits one .NET array: <see cref="Array.MaxLength"/> less 4.
    /// </summary>
    private const int MaxMessageSizeLimit = 2_147_483_587;

    [Theory]
    [InlineData(int.MaxValue)]
    [InlineData(int.MaxValue - 1)]
    [InlineData(MaxMessageSizeLimit + 1)]
    public void Constructor_MaxMessageSizeAboveTheLimit_ThrowsArgumentOutOfRangeExceptionNamingTheLimit(int maxMessageSize)
    {
        // Arrange — issue #488: MaxMessageSize + 4 wrapped negative above int.MaxValue - 4, the
        // retention check passed, and the arena allocation threw OutOfMemoryException.
        StreamSessionOptions options = CreateOptions(maxMessageSize: maxMessageSize, retentionBytes: 1024);

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName(nameof(StreamSessionOptions.MaxMessageSize))
            .Which.Message.Should()
            .Contain(MaxMessageSizeLimit.ToString())
            .And.NotMatchRegex("(?<![0-9])-[0-9]");
    }

    [Fact]
    public void Constructor_MaxMessageSizeAtTheLimitWithSmallRetention_ThrowsArgumentExceptionNamingRetention()
    {
        // Arrange — at the limit the framed floor is exactly Array.MaxLength, which an int holds.
        StreamSessionOptions options = CreateOptions(maxMessageSize: MaxMessageSizeLimit, retentionBytes: 1024);

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(StreamSessionOptions.RetentionBytes))
            .Which.Message.Should()
            .Contain(Array.MaxLength.ToString())
            .And.NotMatchRegex("(?<![0-9])-[0-9]");
    }

    [Fact]
    public void Constructor_MemoryBillLinesSumPastLongMaxValue_ThrowsArgumentExceptionNamingTheExactTotal()
    {
        // Arrange — issue #488: each line is about 2^62 bytes, and their sum overflowed long, so the
        // bill read negative and passed.
        StreamSessionOptions options = CreateOptions(
            capacity: int.MaxValue,
            maxMessageSize: 1000,
            retentionBytes: int.MaxValue,
            receiveBufferSize: int.MaxValue);
        Int128 expectedReceive = (Int128)int.MaxValue * int.MaxValue;
        Int128 expectedSend = (Int128)int.MaxValue * int.MaxValue;
        Int128 expectedBoundaryRing = (Int128)int.MaxValue * ((int.MaxValue / 4) + 1) * 4;
        Int128 expectedTotal = expectedReceive + expectedSend + expectedBoundaryRing;

        // Act
        Action act = () => new StreamSessionReactor(options);

        // Assert
        expectedTotal.Should().BeGreaterThan(long.MaxValue);
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(StreamSessionOptions.Reactor))
            .Which.Message.Should()
            .Contain(expectedTotal.ToString())
            .And.NotMatchRegex("(?<![0-9])-[0-9]");
    }
}
