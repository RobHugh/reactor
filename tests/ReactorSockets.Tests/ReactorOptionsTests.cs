using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The shared options value is validated at construction and nowhere else, so every test here builds a
/// <see cref="RawReactor"/> — the profile constructor is the only caller a consumer can reach.
/// </summary>
public class ReactorOptionsTests
{
    [Fact]
    public void Constructor_CapacityOmitted_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        ReactorOptions options = new();

        // Act
        Action act = () => new RawReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(ReactorOptions.Capacity));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_CapacityBelowOne_ThrowsArgumentOutOfRangeException(int capacity)
    {
        // Arrange
        ReactorOptions options = new() { Capacity = capacity };

        // Act
        Action act = () => new RawReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(ReactorOptions.Capacity));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Constructor_AdmissionCapBelowOne_ThrowsArgumentOutOfRangeException(int admissionCap)
    {
        // Arrange
        ReactorOptions options = new() { Capacity = 4, AdmissionCap = admissionCap };

        // Act
        Action act = () => new RawReactor(options);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(ReactorOptions.AdmissionCap));
    }

    [Fact]
    public void ResolvedAdmissionCap_AdmissionCapOmitted_ResolvesToCapacity()
    {
        // Arrange
        ReactorOptions options = new() { Capacity = 7 };

        // Act
        int resolved = options.ResolvedAdmissionCap;

        // Assert
        resolved.Should().Be(7);
    }

    [Fact]
    public void Constructor_SinkWithoutMinimumLevel_ThrowsArgumentException()
    {
        // Arrange
        ReactorOptions options = new() { Capacity = 4, Sink = new RecordingDiagnosticsSink() };

        // Act
        Action act = () => new RawReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(ReactorOptions.Sink));
    }

    [Fact]
    public void Constructor_MinimumLevelWithoutSink_ThrowsArgumentException()
    {
        // Arrange
        ReactorOptions options = new() { Capacity = 4, MinimumLevel = DiagnosticLevel.Warn };

        // Act
        Action act = () => new RawReactor(options);

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(ReactorOptions.MinimumLevel));
    }

    [Fact]
    public void Constructor_SinkAndMinimumLevelBothSupplied_DoesNotThrow()
    {
        // Arrange
        ReactorOptions options = new()
        {
            Capacity = 4,
            Sink = new RecordingDiagnosticsSink(),
            MinimumLevel = DiagnosticLevel.Error,
        };

        // Act
        Action act = () => new RawReactor(options);

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_SinkAndMinimumLevelBothAbsent_DoesNotThrow()
    {
        // Arrange
        ReactorOptions options = new() { Capacity = 4 };

        // Act
        Action act = () => new RawReactor(options);

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_ArenaSizeBelowTheProfileFloor_ThrowsArgumentExceptionStatingArithmetic()
    {
        // Arrange — the floor is one maximum-sized read, which for Raw is the scratch buffer.
        ReactorOptions options = new() { Capacity = 4, ArenaSize = 512 };

        // Act
        Action act = () => new RawReactor(options, scratchBufferSize: 1200);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(ReactorOptions.ArenaSize))
            .WithMessage("*512*1200*");
    }

    [Fact]
    public void Constructor_ArenaSizeAtOrAboveTheProfileFloor_DoesNotThrow()
    {
        // Arrange
        ReactorOptions options = new() { Capacity = 4, ArenaSize = 2000 };

        // Act
        Action act = () => new RawReactor(options, scratchBufferSize: 1200);

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Constructor_ScratchBufferSizeBelowOne_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        ReactorOptions options = new() { Capacity = 4 };

        // Act
        Action act = () => new RawReactor(options, scratchBufferSize: 0);

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName("scratchBufferSize");
    }

    [Fact]
    public void TimeProvider_Omitted_DefaultsToSystem()
    {
        // Arrange
        ReactorOptions options = new() { Capacity = 4 };

        // Act & Assert
        options.TimeProvider.Should().BeSameAs(TimeProvider.System);
    }
}
