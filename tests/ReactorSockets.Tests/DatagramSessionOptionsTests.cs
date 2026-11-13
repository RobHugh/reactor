using System.Net;
using System.Reflection;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// <see cref="DatagramSessionOptions"/> is validated at construction and nowhere else (see
/// tests/CLAUDE.md), so every throw test here builds a <see cref="DatagramSessionReactor"/> — the
/// constructor is the only caller a consumer can reach. Every baseline built by
/// <see cref="CreateOptions"/> declares no <see cref="DatagramSessionOptions.Endpoints"/>, so a
/// successful construction binds nothing and leaves no real socket behind.
/// </summary>
public class DatagramSessionOptionsTests
{
    /// <summary>
    /// A valid baseline, with every knob overridable by name so each test reads as a statement about
    /// the one knob it varies.
    /// </summary>
    private static DatagramSessionOptions CreateOptions(
        int capacity = 4,
        int? admissionCap = null,
        int maxMessageSize = 4096,
        int retentionBytes = 4096,
        int? maxPeersPerAddress = null,
        int? peerLimitIPv6PrefixLength = null,
        int? maxDatagramsPerPump = null,
        int? fragmentSize = null,
        int? receiveWindowFragments = null,
        int? missedHeartbeats = null,
        int? arenaSize = null,
        TimeSpan? sendIdleInterval = null,
        TimeSpan? linger = null,
        IReadOnlyList<IPEndPoint>? endpoints = null,
        IReactorDiagnostics? sink = null,
        DiagnosticLevel? minimumLevel = null,
        int? unreliableQueueBytes = null) => new()
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
            UnreliableQueueBytes = unreliableQueueBytes,
            MaxPeersPerAddress = maxPeersPerAddress,
            PeerLimitIPv6PrefixLength = peerLimitIPv6PrefixLength,
            MaxDatagramsPerPump = maxDatagramsPerPump,
            FragmentSize = fragmentSize,
            ReceiveWindowFragments = receiveWindowFragments,
            MissedHeartbeats = missedHeartbeats,
            SendIdleInterval = sendIdleInterval ?? TimeSpan.FromSeconds(1),
            Linger = linger ?? TimeSpan.FromSeconds(1),
            Endpoints = endpoints ?? [],
        };

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_CapacityBelowOne_ThrowsArgumentOutOfRangeException(int capacity)
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(capacity: capacity);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(ReactorOptions.Capacity));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_MaxDatagramsPerPumpBelowOne_ThrowsArgumentOutOfRangeException(int maxDatagramsPerPump)
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(maxDatagramsPerPump: maxDatagramsPerPump);

        // Act
        Action act = () => new DatagramSessionReactor(options).Dispose();

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(DatagramSessionOptions.MaxDatagramsPerPump));
    }

    [Fact]
    public void ResolvedMaxDatagramsPerPump_Unset_IsOneWindowForEverySpaceOfEveryPeerPlusTheAdmissionCap()
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(capacity: 8, admissionCap: 5, receiveWindowFragments: 10);

        // Act
        int resolved = options.ResolvedMaxDatagramsPerPump;

        // Assert
        resolved.Should().Be((8 * 10 * 4) + 5);
    }

    [Fact]
    public void ResolvedMaxDatagramsPerPump_Set_IsTheValueSet()
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(maxDatagramsPerPump: 7);

        // Act
        int resolved = options.ResolvedMaxDatagramsPerPump;

        // Assert
        resolved.Should().Be(7);
    }

    [Theory]
    [InlineData(4, 3)]
    [InlineData(2, 1)]
    [InlineData(1, 1)]
    public void ResolvedMaxPeersPerAddress_LeftUnset_IsOneBelowCapacityAndAtLeastOne(int capacity, int expected)
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(capacity: capacity);

        // Act
        int resolved = options.ResolvedMaxPeersPerAddress;

        // Assert
        resolved.Should().Be(expected);
    }

    [Fact]
    public void ResolvedMaxPeersPerAddress_Set_IsTheValueSet()
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(capacity: 4, maxPeersPerAddress: 4);

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
        DatagramSessionOptions options = CreateOptions(maxPeersPerAddress: maxPeersPerAddress);

        // Act
        Action act = () => new DatagramSessionReactor(options).Dispose();

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(DatagramSessionOptions.MaxPeersPerAddress));
    }

    [Fact]
    public void ResolvedPeerLimitIPv6PrefixLength_LeftUnset_IsSixtyFour()
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions();

        // Act
        int resolved = options.ResolvedPeerLimitIPv6PrefixLength;

        // Assert
        resolved.Should().Be(64);
    }

    [Fact]
    public void ResolvedPeerLimitIPv6PrefixLength_Set_IsTheValueSet()
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(peerLimitIPv6PrefixLength: 128);

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
        DatagramSessionOptions options = CreateOptions(peerLimitIPv6PrefixLength: prefixLength);

        // Act
        Action act = () => new DatagramSessionReactor(options).Dispose();

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(DatagramSessionOptions.PeerLimitIPv6PrefixLength));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(128)]
    public void Constructor_PeerLimitIPv6PrefixLengthAtEitherBound_Succeeds(int prefixLength)
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(peerLimitIPv6PrefixLength: prefixLength);

        // Act
        Action act = () => new DatagramSessionReactor(options).Dispose();

        // Assert
        act.Should().NotThrow();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_AdmissionCapBelowOne_ThrowsArgumentOutOfRangeException(int admissionCap)
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(admissionCap: admissionCap);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(ReactorOptions.AdmissionCap));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_MaxMessageSizeBelowOne_ThrowsArgumentOutOfRangeException(int maxMessageSize)
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(maxMessageSize: maxMessageSize);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(DatagramSessionOptions.MaxMessageSize));
    }

    [Fact]
    public void Constructor_FragmentSizeBelowMinimum_ThrowsArgumentOutOfRangeExceptionCitingRfc791()
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(fragmentSize: 575);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName(nameof(DatagramSessionOptions.FragmentSize))
            .Which.Message.Should().Contain("576").And.Contain("RFC 791");
    }

    [Fact]
    public void Constructor_FragmentSizeAtMinimum_DoesNotThrow()
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(fragmentSize: 576);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_FragmentSizeAboveMaximum_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(fragmentSize: 65_477, retentionBytes: 65_477);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(DatagramSessionOptions.FragmentSize));
    }

    [Fact]
    public void Constructor_FragmentSizeAtMaximum_DoesNotThrow()
    {
        // Arrange — retention must cover one fragment at the maximum fragment size, or the retention
        // check below would fire instead of exercising the fragment-size ceiling.
        DatagramSessionOptions options = CreateOptions(fragmentSize: 65_476, retentionBytes: 65_476);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_RetentionBytesBelowOneFragment_ThrowsArgumentException()
    {
        // Arrange — the resolved fragment size defaults to 1200; 1199 cannot hold one.
        DatagramSessionOptions options = CreateOptions(retentionBytes: 1199);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(DatagramSessionOptions.RetentionBytes));
    }

    [Fact]
    public void Constructor_UnreliableQueueBytesBelowOneFullRecord_ThrowsNamingTheOption()
    {
        // Arrange: one full record at the default fragment size is 1200 + 7 = 1207 bytes.
        DatagramSessionOptions options = CreateOptions(unreliableQueueBytes: 1206);

        // Act
        Action act = () => _ = new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(DatagramSessionOptions.UnreliableQueueBytes));
    }

    [Fact]
    public void Constructor_UnreliableQueueBytesAtOneFullRecord_DoesNotThrow()
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(unreliableQueueBytes: 1207);

        // Act
        Action act = () => new DatagramSessionReactor(options).Dispose();

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void ResolvedUnreliableQueueBytes_WhenUnset_IsRetentionSlotsFullRecords()
    {
        // Arrange: 4096 / 1200 = 3 slots.
        DatagramSessionOptions options = CreateOptions(retentionBytes: 4096);

        // Act
        int resolved = options.ResolvedUnreliableQueueBytes;

        // Assert
        resolved.Should().Be(3 * (1200 + 7));
    }


    [Fact]
    public void Constructor_ReceiveWindowNarrowerThanOneMaxMessage_ThrowsArgumentExceptionExplainingDeadlock()
    {
        // Arrange — MaxMessageSize 4096 over the default 1200-byte fragment occupies 4 fragments; 3 is
        // one short, which is the deadlock this cross-check exists to catch (ADR 0004).
        DatagramSessionOptions options = CreateOptions(maxMessageSize: 4096, receiveWindowFragments: 3);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(DatagramSessionOptions.ReceiveWindowFragments))
            .Which.Message.Should().Contain("deadlock").And.Contain("window never advances");
    }

    [Fact]
    public void Constructor_ArenaSizeBelowMaxMessageSize_ThrowsArgumentExceptionNamingArithmetic()
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(maxMessageSize: 4096, arenaSize: 4095);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(ReactorOptions.ArenaSize))
            .Which.Message.Should().Contain("4095").And.Contain("4096");
    }

    [Fact]
    public void Constructor_ArenaSizeEqualToMaxMessageSize_DoesNotThrow()
    {
        // Arrange — together with the "one byte less throws" case above, this shows the floor an unset
        // ArenaSize resolves to is exactly MaxMessageSize.
        DatagramSessionOptions options = CreateOptions(maxMessageSize: 4096, arenaSize: 4096);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().NotThrow();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(0)]
    public void Constructor_MissedHeartbeatsBelowTwo_ThrowsArgumentOutOfRangeException(int missedHeartbeats)
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(missedHeartbeats: missedHeartbeats);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(DatagramSessionOptions.MissedHeartbeats));
    }

    [Fact]
    public void Constructor_MissedHeartbeatsAtTwo_DoesNotThrow()
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(missedHeartbeats: 2);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void ResolvedMissedHeartbeats_MissedHeartbeatsUnset_DefaultsToThree()
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions();

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
        DatagramSessionOptions options = CreateOptions(sendIdleInterval: TimeSpan.FromSeconds(seconds));

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(DatagramSessionOptions.SendIdleInterval));
    }

    [Theory]
    [InlineData(0.0)]
    [InlineData(-1.0)]
    public void Constructor_LingerZeroOrNegative_ThrowsArgumentOutOfRangeException(double seconds)
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(linger: TimeSpan.FromSeconds(seconds));

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(DatagramSessionOptions.Linger));
    }

    [Theory]
    [InlineData(42_949_672_950_001L)] // one tick above uint.MaxValue milliseconds
    [InlineData(long.MaxValue)]
    public void Constructor_SendIdleIntervalAboveTheAnnounceableMaximum_ThrowsArgumentOutOfRangeException(long ticks)
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(sendIdleInterval: TimeSpan.FromTicks(ticks));

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(DatagramSessionOptions.SendIdleInterval));
    }

    [Theory]
    [InlineData(42_949_672_950_001L)] // one tick above uint.MaxValue milliseconds
    [InlineData(long.MaxValue)]
    public void Constructor_LingerAboveTheAnnounceableMaximum_ThrowsArgumentOutOfRangeException(long ticks)
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(linger: TimeSpan.FromTicks(ticks));

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(DatagramSessionOptions.Linger));
    }

    [Fact]
    public void Constructor_IntervalsAtTheAnnounceableMaximum_Constructs()
    {
        // Arrange
        TimeSpan maximum = TimeSpan.FromTicks(42_949_672_950_000L); // exactly uint.MaxValue milliseconds
        DatagramSessionOptions options = CreateOptions(sendIdleInterval: maximum, linger: maximum);

        // Act
        Action act = () => new DatagramSessionReactor(options).Dispose();

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_DuplicateEndpoints_ThrowsArgumentExceptionNamingTheEndpoint()
    {
        // Arrange
        IPEndPoint endpoint = new(IPAddress.Loopback, 5000);
        DatagramSessionOptions options = CreateOptions(endpoints: [endpoint, endpoint]);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(DatagramSessionOptions.Endpoints))
            .Which.Message.Should().Contain(endpoint.ToString());
    }

    [Fact]
    public void Constructor_NullEndpoint_ThrowsArgumentException()
    {
        // Arrange — intentionally null to exercise the runtime null check the compiler cannot catch
        // once an endpoint travels through a caller's own configuration data.
        IPEndPoint[] endpoints = [null!];
        DatagramSessionOptions options = CreateOptions(endpoints: endpoints);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(DatagramSessionOptions.Endpoints));
    }

    [Fact]
    public void Constructor_SinkWithoutMinimumLevel_ThrowsArgumentException()
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(sink: new RecordingDiagnosticsSink());

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(ReactorOptions.Sink));
    }

    [Fact]
    public void Constructor_MinimumLevelWithoutSink_ThrowsArgumentException()
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(minimumLevel: DiagnosticLevel.Warn);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(ReactorOptions.MinimumLevel));
    }

    [Fact]
    public void ResolvedFragmentSize_FragmentSizeUnset_DefaultsTo1200()
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions();

        // Act
        int resolved = options.ResolvedFragmentSize;

        // Assert
        resolved.Should().Be(1200);
    }

    [Fact]
    public void ResolvedReceiveWindowFragments_MaxMessageSizeDoesNotDivideEvenly_RoundsUp()
    {
        // Arrange — 2500 bytes over a 1200-byte fragment is 2 fragments and a remainder, so the floor
        // must round up to 3 rather than truncate to 2.
        DatagramSessionOptions options = CreateOptions(maxMessageSize: 2500, fragmentSize: 1200);

        // Act
        int resolved = options.ResolvedReceiveWindowFragments;

        // Assert
        resolved.Should().Be(3);
    }

    [Fact]
    public void RetentionSlots_BoundDividesEvenly_ReturnsExactQuotient()
    {
        // Arrange — 4 fragments of 1200 bytes each, no remainder.
        DatagramSessionOptions options = CreateOptions(retentionBytes: 4800, fragmentSize: 1200);

        // Act
        int slots = options.RetentionSlots;

        // Assert
        slots.Should().Be(4);
    }

    [Fact]
    public void RetentionSlots_BoundLeavesRemainder_Truncates()
    {
        // Arrange — 2 whole fragments and 500 bytes over, which is not a whole slot.
        DatagramSessionOptions options = CreateOptions(retentionBytes: 2900, fragmentSize: 1200);

        // Act
        int slots = options.RetentionSlots;

        // Assert
        slots.Should().Be(2);
    }

    [Fact]
    public void RetentionSlots_BoundEqualsOneFragment_ReturnsOne()
    {
        // Arrange — the minimum retention Validate allows: exactly one fragment.
        DatagramSessionOptions options = CreateOptions(retentionBytes: 1200, fragmentSize: 1200);

        // Act
        int slots = options.RetentionSlots;

        // Assert
        slots.Should().Be(1);
    }

    [Fact]
    public void ResolvedAdmissionCap_AdmissionCapUnset_DefaultsToCapacity()
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(capacity: 7);

        // Act
        int resolved = options.Reactor.ResolvedAdmissionCap;

        // Assert
        resolved.Should().Be(7);
    }

    [Fact]
    public void ReceiveBudgetBytes_WorkedExample_CountsPayloadForTwoSpacesAndAdmissionForFour()
    {
        // Arrange — distinct numbers so a transposed factor in the formula is caught.
        DatagramSessionOptions options = CreateOptions(
            capacity: 3,
            maxMessageSize: 4000,
            fragmentSize: 1000,
            receiveWindowFragments: 5);

        // Act
        long budget = options.ReceiveBudgetBytes;

        // Assert — payload slots for the two reliable Spaces, one admission byte per slot for all
        // four, a two-byte Fragment-length slot and a six-byte Fragment-group slot for the two reliable
        // Spaces (issue #257, #297), and a six-byte waiting-group entry per half-window for those same
        // two Spaces (issue #259), all scaled by Capacity and the window (issue #174).
        budget.Should().Be((3L * 2 * 5 * (1000 + 2 + 6)) + (3L * 4 * 5) + (3L * 2 * (5 / 2) * 6));
    }

    [Fact]
    public void HeldRecordBudgetBytes_WorkedExample_CountsTwoWindowsOfSlotsWithTheirBookkeeping()
    {
        // Arrange — distinct numbers so a transposed factor in the formula is caught.
        DatagramSessionOptions options = CreateOptions(
            capacity: 3,
            maxMessageSize: 4000,
            fragmentSize: 1000,
            receiveWindowFragments: 5);

        // Act
        long budget = options.HeldRecordBudgetBytes;

        // Assert — one ReliableOrdered Space per Peer, two windows of held records, each a whole fragment
        // slot plus 44 bytes: an 8-byte reference, a 4-byte length and a byte array's 32-byte ceiling of
        // object header and padding (issue #424).
        budget.Should().Be(3L * 2 * 5 * (1000 + 44));
    }

    [Fact]
    public void SendBudgetBytes_WorkedExample_EqualsCapacityTimesTwoTimesWholeRetentionSlotsWithTheirBookkeeping()
    {
        // Arrange — distinct numbers so a transposed factor in the formula is caught, and a retention
        // that is not a whole number of 1200-byte fragments, so the truncation to five slots is too.
        DatagramSessionOptions options = CreateOptions(capacity: 3, retentionBytes: 7000);

        // Act
        long budget = options.SendBudgetBytes;

        // Assert — each slot is its payload plus 15 bytes recording what it holds (issue #63).
        budget.Should().Be(3L * 2 * 5 * (1200 + 15));
    }

    [Fact]
    public void Constructor_MemoryBillExceedsMaxAllocatable_ThrowsArgumentExceptionNamingTheTotal()
    {
        // Arrange — capacity 100 000 with a 100 000-byte retention and a one-fragment receive window
        // adds to far more than can be allocated.
        DatagramSessionOptions options = CreateOptions(capacity: 100_000, maxMessageSize: 1200, retentionBytes: 100_000);
        // A one-fragment window still holds one waiting group per reliable Space — the floor of the
        // half-window bound, never fewer than one (issue #259). The payload term also carries a 2-byte
        // Fragment-length slot (issue #257) and a 6-byte Fragment-group slot (issue #297) per fragment.
        long expectedReceive = (100_000L * 2 * 1 * (1200 + 2 + 6)) + (100_000L * 4 * 1) + (100_000L * 2 * 1 * 6);
        // Send is 83 whole 1200-byte slots per reliable Space, each with 15 bytes of bookkeeping.
        long expectedSend = 100_000L * 2 * 83 * (1200 + 15);
        // The unreliable queue is 83 slots of a full record (1200 + 7 bytes) per Peer.
        long expectedUnreliableQueue = 100_000L * 83 * (1200 + 7);
        long expectedTotal = expectedReceive + expectedSend + expectedUnreliableQueue;

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(DatagramSessionOptions.Reactor))
            .Which.Message.Should()
            .Contain(expectedTotal.ToString())
            .And.Contain(expectedReceive.ToString())
            .And.Contain(expectedSend.ToString())
            .And.Contain(expectedUnreliableQueue.ToString());
    }

    [Fact]
    public void PublicSurface_NoMemberMentionsEpochOrCookie_BecauseTheCookieEpochIsAProtocolConstant()
    {
        // Arrange — a knob trades the consumer's own resources; the thirty-second cookie epoch fixes
        // what a datagram means, so exposing a way to configure it could only break interoperation.
        IEnumerable<string> memberNames = PublicPropertyAndMethodNames();

        // Act & Assert
        memberNames.Should().NotContain(name => name.Contains("Epoch", StringComparison.OrdinalIgnoreCase));
        memberNames.Should().NotContain(name => name.Contains("Cookie", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void PublicSurface_NoMemberMentionsAckOrSpace_BecauseTheAcknowledgementWidthAndSpacesAreProtocolConstants()
    {
        // Arrange — a knob trades the consumer's own resources; the 32-bit acknowledgement width and
        // the four sequence Spaces fix what a datagram means, so exposing a way to configure them could
        // only break interoperation.
        IEnumerable<string> memberNames = PublicPropertyAndMethodNames();

        // Act & Assert
        memberNames.Should().NotContain(name => name.Contains("Ack", StringComparison.OrdinalIgnoreCase));
        memberNames.Should().NotContain(name => name.Contains("Space", StringComparison.OrdinalIgnoreCase));
    }

    [Theory]
    [InlineData(int.MaxValue)]
    [InlineData(int.MaxValue - 1)]
    [InlineData(2_147_483_592)]
    public void Constructor_MaxMessageSizeAboveArrayMaxLength_ThrowsArgumentOutOfRangeExceptionNamingTheLimit(int maxMessageSize)
    {
        // Arrange — issue #488: MaxMessageSize + FragmentSize - 1 wrapped negative, and the caller was
        // told about MaxDatagramsPerPump, which they did not set. 2 147 483 592 is Array.MaxLength + 1.
        DatagramSessionOptions options = CreateOptions(maxMessageSize: maxMessageSize, retentionBytes: 1200);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>()
            .WithParameterName(nameof(DatagramSessionOptions.MaxMessageSize))
            .Which.Message.Should()
            .Contain(Array.MaxLength.ToString())
            .And.NotMatchRegex("(?<![0-9])-[0-9]");
    }

    [Fact]
    public void Constructor_MaxMessageSizeAtArrayMaxLength_ThrowsArgumentExceptionFromTheMemoryBill()
    {
        // Arrange — at the limit the window floor is ⌈2 147 483 591 / 1200⌉ = 1 789 570 fragments. The
        // sum MaxMessageSize + 1199 is above int.MaxValue, so the floor must be computed in long.
        DatagramSessionOptions options = CreateOptions(maxMessageSize: Array.MaxLength, retentionBytes: 1200);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(DatagramSessionOptions.Reactor))
            .Which.Message.Should()
            .Contain("1789570 fragments")
            .And.NotMatchRegex("(?<![0-9])-[0-9]");
    }

    [Fact]
    public void ReceiveWindowFloor_MaxMessageSizeAtArrayMaxLength_IsTheCeilingWithoutWrapping()
    {
        // Arrange
        DatagramSessionOptions options = CreateOptions(maxMessageSize: Array.MaxLength, retentionBytes: 1200);

        // Act
        int floor = options.ReceiveWindowFloor;

        // Assert
        floor.Should().Be(1_789_570);
    }

    [Fact]
    public void BudgetProperties_TrueFigureAboveLongMaxValue_SaturateInsteadOfWrapping()
    {
        // Arrange — issue #488: capacity and window at int.MaxValue overflowed long in every product.
        DatagramSessionOptions options = CreateOptions(capacity: int.MaxValue, maxMessageSize: 1000, retentionBytes: 1200, receiveWindowFragments: int.MaxValue);

        // Act
        long receive = options.ReceiveBudgetBytes;
        long held = options.HeldRecordBudgetBytes;

        // Assert
        receive.Should().Be(long.MaxValue);
        held.Should().Be(long.MaxValue);
    }

    [Fact]
    public void ResolvedMaxDatagramsPerPump_TrueFigureAboveIntMaxValue_SaturatesInsteadOfWrapping()
    {
        // Arrange — issue #488: capacity and window at int.MaxValue wrapped the default read limit negative.
        DatagramSessionOptions options = CreateOptions(capacity: int.MaxValue, maxMessageSize: 1000, retentionBytes: 1200, receiveWindowFragments: int.MaxValue);

        // Act
        int perPump = options.ResolvedMaxDatagramsPerPump;

        // Assert
        perPump.Should().Be(int.MaxValue);
    }

    [Fact]
    public void Constructor_EachLineBelowLongMaxValueButTotalAbove_ThrowsArgumentExceptionNamingTheExactTotal()
    {
        // Arrange — the receive line is about 4.05 × 10^18 and the send line about 5.58 × 10^18. Each fits
        // long, and their sum does not, so a long total would wrap negative and pass.
        DatagramSessionOptions options = CreateOptions(
            capacity: int.MaxValue, maxMessageSize: 1000, retentionBytes: 2_200_000 * 576, fragmentSize: 576, receiveWindowFragments: 1_600_000);
        long receive = options.ReceiveBudgetBytes;
        long send = options.SendBudgetBytes;
        Int128 expectedTotal = (Int128)receive + send + options.UnreliableQueueBudgetBytes;

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        receive.Should().BeLessThan(long.MaxValue);
        send.Should().BeLessThan(long.MaxValue);
        expectedTotal.Should().BeGreaterThan(long.MaxValue);
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(DatagramSessionOptions.Reactor))
            .Which.Message.Should()
            .Contain($"ask for {expectedTotal} bytes")
            .And.NotMatchRegex("(?<![0-9])-[0-9]");
    }

    [Fact]
    public void SendBudgetBytes_TrueFigureAboveLongMaxValue_Saturates()
    {
        // Arrange — 2 147 483 647 × 2 × 3 728 270 slots × (576 + 15) bytes is about 9.46 × 10^18,
        // just above long.MaxValue.
        DatagramSessionOptions options = CreateOptions(capacity: int.MaxValue, maxMessageSize: 1000, retentionBytes: int.MaxValue, fragmentSize: 576);

        // Act
        long send = options.SendBudgetBytes;

        // Assert
        send.Should().Be(long.MaxValue);
    }

    [Fact]
    public void Constructor_SendLineAboveLongMaxValue_ThrowsArgumentExceptionNamingTheExactLine()
    {
        // Arrange — issue #488: the send line wrapped negative and the memory bill passed.
        DatagramSessionOptions options = CreateOptions(capacity: int.MaxValue, maxMessageSize: 1000, retentionBytes: int.MaxValue, fragmentSize: 576);
        Int128 expectedSend = (Int128)int.MaxValue * 2 * (int.MaxValue / 576) * (576 + 15);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        expectedSend.Should().BeGreaterThan(long.MaxValue);
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(DatagramSessionOptions.Reactor))
            .Which.Message.Should()
            .Contain(expectedSend.ToString())
            .And.NotMatchRegex("(?<![0-9])-[0-9]");
    }

    [Fact]
    public void BudgetProperties_CapacityAndRetentionAtIntMaxValue_AreNotNegative()
    {
        // Arrange — issue #537: at the default 1200-byte fragment the send line is above
        // long.MaxValue and must saturate rather than wrap negative.
        DatagramSessionOptions options = CreateOptions(capacity: int.MaxValue, maxMessageSize: 1200, retentionBytes: int.MaxValue);

        // Act
        long[] budgets =
        [
            options.ReceiveBudgetBytes,
            options.HeldRecordBudgetBytes,
            options.SendBudgetBytes,
            options.UnreliableQueueBudgetBytes,
        ];

        // Assert
        budgets.Should().OnlyContain(budget => budget > 0);
    }

    [Fact]
    public void Constructor_CapacityAndRetentionAtIntMaxValue_ThrowsTheMemoryBill()
    {
        // Arrange — issue #537: the memory bill rejects this combination by name, before any allocation.
        DatagramSessionOptions options = CreateOptions(capacity: int.MaxValue, maxMessageSize: 1200, retentionBytes: int.MaxValue);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(DatagramSessionOptions.Reactor))
            .Which.Message.Should()
            .NotMatchRegex("(?<![0-9])-[0-9]");
    }

    [Fact]
    public void Constructor_ReceiveLineAboveLongMaxValue_ThrowsArgumentExceptionNamingTheExactLine()
    {
        // Arrange — issue #488: 2 000 000 Peers × a 2 000 000 000-fragment window wrapped the receive
        // line negative, and the memory bill passed.
        DatagramSessionOptions options = CreateOptions(capacity: 2_000_000, maxMessageSize: 1000, retentionBytes: 4096, receiveWindowFragments: 2_000_000_000);
        Int128 expectedReceive =
            ((Int128)2_000_000 * 2_000_000_000 * ((2 * (1200 + 2 + 6)) + 4))
            + ((Int128)2_000_000 * 2 * 1_000_000_000 * 6);

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        expectedReceive.Should().BeGreaterThan(long.MaxValue);
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(DatagramSessionOptions.Reactor))
            .Which.Message.Should()
            .Contain(expectedReceive.ToString())
            .And.NotMatchRegex("(?<![0-9])-[0-9]");
    }

    /// <summary>Every public instance property and ordinary method name declared directly on <see cref="DatagramSessionOptions"/>.</summary>
    private static IEnumerable<string> PublicPropertyAndMethodNames()
    {
        Type type = typeof(DatagramSessionOptions);

        IEnumerable<string> propertyNames = type
            .GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Select(property => property.Name);

        IEnumerable<string> methodNames = type
            .GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(method => !method.IsSpecialName)
            .Select(method => method.Name);

        return propertyNames.Concat(methodNames);
    }
}
