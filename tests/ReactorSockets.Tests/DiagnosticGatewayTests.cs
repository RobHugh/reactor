using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

public class DiagnosticGatewayTests
{
    [Fact]
    public void IsEnabled_NoSinkInstalled_IsFalseForEveryLevel()
    {
        // Arrange
        DiagnosticGateway gateway = new(sink: null, minimumLevel: null, new ReactorCounterSet());

        // Act
        bool enabled = gateway.IsEnabled(DiagnosticLevel.Error);

        // Assert
        enabled.Should().BeFalse();
    }

    [Fact]
    public void Log_NoSinkInstalled_NoRecordIsConstructedAndLogIsANoOp()
    {
        // Arrange — the constructed record here stands in for "a record a caller built anyway"; the
        // acceptance criterion this protects is that production call sites never reach this call
        // because IsEnabled already answered false, costing one comparison and nothing else.
        ReactorCounterSet counters = new();
        DiagnosticGateway gateway = new(sink: null, minimumLevel: null, counters);
        DiagnosticRecord record = DiagnosticRecord.ForAdmissionCapReached(cap: 4, admissionsDeferred: 1);

        // Act
        Action act = () => gateway.Log(record);

        // Assert
        act.Should().NotThrow();
    }

    [Theory]
    [InlineData(DiagnosticLevel.Warn, DiagnosticLevel.Trace, false)]
    [InlineData(DiagnosticLevel.Warn, DiagnosticLevel.Warn, true)]
    [InlineData(DiagnosticLevel.Warn, DiagnosticLevel.Error, true)]
    public void IsEnabled_LevelAgainstConfiguredMinimum_ComparesAsAscendingSeverity(
        DiagnosticLevel minimumLevel, DiagnosticLevel level, bool expected)
    {
        // Arrange
        RecordingSink sink = new();
        DiagnosticGateway gateway = new(sink, minimumLevel, new ReactorCounterSet());

        // Act
        bool enabled = gateway.IsEnabled(level);

        // Assert
        enabled.Should().Be(expected);
    }

    [Fact]
    public void IsEnabled_SinkItselfDisablesTheLevel_IsFalse()
    {
        // Arrange
        RecordingSink sink = new() { Enabled = false };
        DiagnosticGateway gateway = new(sink, DiagnosticLevel.Trace, new ReactorCounterSet());

        // Act
        bool enabled = gateway.IsEnabled(DiagnosticLevel.Error);

        // Assert
        enabled.Should().BeFalse();
    }

    [Fact]
    public void Log_SinkInstalled_ReceivesTheExactRecordByValue()
    {
        // Arrange
        RecordingSink sink = new();
        DiagnosticGateway gateway = new(sink, DiagnosticLevel.Trace, new ReactorCounterSet());
        DiagnosticRecord record = DiagnosticRecord.ForAdmissionCapReached(cap: 4, admissionsDeferred: 1);

        // Act
        gateway.Log(record);

        // Assert
        sink.Received.Should().ContainSingle().Which.Should().Be(record);
    }

    [Fact]
    public void Log_ThrowingSink_IsInvokedExactlyOnceAndSinkFaultedReadsOne()
    {
        // Arrange
        ThrowingSink sink = new();
        ReactorCounterSet counters = new();
        DiagnosticGateway gateway = new(sink, DiagnosticLevel.Trace, counters);
        DiagnosticRecord record = DiagnosticRecord.ForAdmissionCapReached(cap: 4, admissionsDeferred: 1);

        // Act
        gateway.Log(record);
        gateway.Log(record);
        gateway.Log(record);

        // Assert
        sink.CallCount.Should().Be(1);
        counters.Snapshot().SinkFaulted.Should().Be(1);
    }

    [Fact]
    public void IsEnabled_AfterSinkFaulted_IsFalseForEveryLevel()
    {
        // Arrange
        ThrowingSink sink = new();
        DiagnosticGateway gateway = new(sink, DiagnosticLevel.Trace, new ReactorCounterSet());
        gateway.Log(DiagnosticRecord.ForAdmissionCapReached(cap: 4, admissionsDeferred: 1));

        // Act
        bool enabled = gateway.IsEnabled(DiagnosticLevel.Error);

        // Assert
        enabled.Should().BeFalse();
    }

    [Fact]
    public void Log_AfterSinkFaulted_NoSubsequentRecordReachesIt()
    {
        // Arrange
        ThrowingSink sink = new();
        DiagnosticGateway gateway = new(sink, DiagnosticLevel.Trace, new ReactorCounterSet());
        DiagnosticRecord record = DiagnosticRecord.ForAdmissionCapReached(cap: 4, admissionsDeferred: 1);
        gateway.Log(record);

        // Act
        Action act = () => gateway.Log(record);

        // Assert
        act.Should().NotThrow();
        sink.CallCount.Should().Be(1);
    }

    private sealed class RecordingSink : IReactorDiagnostics
    {
        public List<DiagnosticRecord> Received { get; } = [];

        public bool Enabled { get; init; } = true;

        public bool IsEnabled(DiagnosticLevel level) => Enabled;

        public void Log(in DiagnosticRecord record) => Received.Add(record);
    }

    private sealed class ThrowingSink : IReactorDiagnostics
    {
        public int CallCount { get; private set; }

        public bool IsEnabled(DiagnosticLevel level) => true;

        public void Log(in DiagnosticRecord record)
        {
            CallCount++;
            throw new InvalidOperationException("Sink deliberately throws for the fault-and-disable test.");
        }
    }
}
