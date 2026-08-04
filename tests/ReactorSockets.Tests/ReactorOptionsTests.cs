using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

public class ReactorOptionsTests
{
    [Fact]
    public void Validate_CapacityOmitted_ThrowsArgumentOutOfRangeException()
    {
        // Arrange
        ReactorOptions options = new();

        // Act
        Action act = options.Validate;

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(ReactorOptions.Capacity));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_CapacityBelowOne_ThrowsArgumentOutOfRangeException(int capacity)
    {
        // Arrange
        ReactorOptions options = new() { Capacity = capacity };

        // Act
        Action act = options.Validate;

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(ReactorOptions.Capacity));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_AdmissionCapBelowOne_ThrowsArgumentOutOfRangeException(int admissionCap)
    {
        // Arrange
        ReactorOptions options = new() { Capacity = 4, AdmissionCap = admissionCap };

        // Act
        Action act = options.Validate;

        // Assert
        act.Should().Throw<ArgumentOutOfRangeException>().WithParameterName(nameof(ReactorOptions.AdmissionCap));
    }

    [Fact]
    public void Validate_AdmissionCapOmitted_ResolvesToCapacity()
    {
        // Arrange
        ReactorOptions options = new() { Capacity = 7 };

        // Act
        options.Validate();

        // Assert
        options.ResolvedAdmissionCap.Should().Be(7);
    }

    [Fact]
    public void Validate_SinkWithoutMinimumLevel_ThrowsArgumentException()
    {
        // Arrange
        RecordingDiagnostics sink = new();
        ReactorOptions options = new() { Capacity = 4, Sink = sink };

        // Act
        Action act = options.Validate;

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(ReactorOptions.Sink));
    }

    [Fact]
    public void Validate_MinimumLevelWithoutSink_ThrowsArgumentException()
    {
        // Arrange
        ReactorOptions options = new() { Capacity = 4, MinimumLevel = DiagnosticLevel.Warn };

        // Act
        Action act = options.Validate;

        // Assert
        act.Should().Throw<ArgumentException>().WithParameterName(nameof(ReactorOptions.MinimumLevel));
    }

    [Fact]
    public void Validate_SinkAndMinimumLevelBothSupplied_DoesNotThrow()
    {
        // Arrange
        RecordingDiagnostics sink = new();
        ReactorOptions options = new() { Capacity = 4, Sink = sink, MinimumLevel = DiagnosticLevel.Error };

        // Act
        Action act = options.Validate;

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Validate_SinkAndMinimumLevelBothAbsent_DoesNotThrow()
    {
        // Arrange
        ReactorOptions options = new() { Capacity = 4 };

        // Act
        Action act = options.Validate;

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void ResolveArenaSize_ArenaSizeOmitted_ResolvesToFloor()
    {
        // Arrange
        ReactorOptions options = new() { Capacity = 4 };

        // Act
        int resolved = options.ResolveArenaSize(arenaFloor: 65_507, floorDescription: "one maximum UDP datagram");

        // Assert
        resolved.Should().Be(65_507);
    }

    [Fact]
    public void ResolveArenaSize_ArenaSizeBelowProfileFloor_ThrowsArgumentExceptionStatingArithmetic()
    {
        // Arrange
        ReactorOptions options = new() { Capacity = 4, ArenaSize = 512 };

        // Act
        Action act = () => options.ResolveArenaSize(arenaFloor: 1200, floorDescription: "one maximum UDP datagram of 1200 bytes");

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(ReactorOptions.ArenaSize))
            .WithMessage("*512*1200*");
    }

    [Fact]
    public void ResolveArenaSize_ArenaSizeAtOrAboveProfileFloor_ResolvesToSuppliedValue()
    {
        // Arrange
        ReactorOptions options = new() { Capacity = 4, ArenaSize = 2000 };

        // Act
        int resolved = options.ResolveArenaSize(arenaFloor: 1200, floorDescription: "one maximum UDP datagram of 1200 bytes");

        // Assert
        resolved.Should().Be(2000);
    }

    [Fact]
    public void TimeProvider_Omitted_DefaultsToSystem()
    {
        // Arrange
        ReactorOptions options = new() { Capacity = 4 };

        // Act & Assert
        options.TimeProvider.Should().BeSameAs(TimeProvider.System);
    }

    private sealed class RecordingDiagnostics : IReactorDiagnostics
    {
        public bool IsEnabled(DiagnosticLevel level) => true;

        public void Log(in DiagnosticRecord record)
        {
        }
    }
}
