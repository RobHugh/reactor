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
        int? fragmentSize = null,
        int? receiveWindowFragments = null,
        int? missedHeartbeats = null,
        int? arenaSize = null,
        TimeSpan? sendIdleInterval = null,
        TimeSpan? linger = null,
        IReadOnlyList<IPEndPoint>? endpoints = null,
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
        DatagramSessionOptions options = CreateOptions(fragmentSize: 65_508, retentionBytes: 65_508);

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
        DatagramSessionOptions options = CreateOptions(fragmentSize: 65_507, retentionBytes: 65_507);

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
        // four, both scaled by Capacity and the window (issue #174).
        budget.Should().Be((3L * 2 * 5 * 1000) + (3L * 4 * 5));
    }

    [Fact]
    public void SendBudgetBytes_WorkedExample_EqualsCapacityTimesTwoTimesRetentionBytes()
    {
        // Arrange — distinct numbers so a transposed factor in the formula is caught.
        DatagramSessionOptions options = CreateOptions(capacity: 3, retentionBytes: 7000);

        // Act
        long budget = options.SendBudgetBytes;

        // Assert
        budget.Should().Be(3L * 2 * 7000);
    }

    [Fact]
    public void Constructor_MemoryBillExceedsMaxAllocatable_ThrowsArgumentExceptionNamingTheTotal()
    {
        // Arrange — capacity 100 000 with a 100 000-byte retention and a one-fragment receive window
        // adds to far more than can be allocated.
        DatagramSessionOptions options = CreateOptions(capacity: 100_000, maxMessageSize: 1200, retentionBytes: 100_000);
        long expectedReceive = (100_000L * 2 * 1 * 1200) + (100_000L * 4 * 1);
        long expectedSend = 100_000L * 2 * 100_000;
        long expectedTotal = expectedReceive + expectedSend;

        // Act
        Action act = () => new DatagramSessionReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(DatagramSessionOptions.Reactor))
            .Which.Message.Should()
            .Contain(expectedTotal.ToString())
            .And.Contain(expectedReceive.ToString())
            .And.Contain(expectedSend.ToString());
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
