using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The diagnostics facade observed through the surface a consumer has. Every test provokes a real
/// record — a datagram larger than a deliberately small scratch buffer, which is <c>Warn</c> — rather
/// than constructing one, because the gate that matters is the one the Reactor applies before it
/// builds a record at all.
/// </summary>
public class RawReactorDiagnosticsTests
{
    [Fact]
    public void Log_RecordBelowTheConfiguredMinimumLevel_NeverReachesTheSink()
    {
        // Arrange
        using RawReactorHarness harness = new(scratchBufferSize: 16, minimumLevel: DiagnosticLevel.Error);

        // Act
        ProvokeTruncation(harness);

        // Assert
        harness.Sink.Received.Should().BeEmpty("DatagramTruncated is Warn, below the configured Error floor");
    }

    [Fact]
    public void Log_RecordAtTheConfiguredMinimumLevel_ReachesTheSink()
    {
        // Arrange
        using RawReactorHarness harness = new(scratchBufferSize: 16, minimumLevel: DiagnosticLevel.Warn);

        // Act
        ProvokeTruncation(harness);

        // Assert
        harness.Sink.Received.Should().ContainSingle()
            .Which.Id.Should().Be(DiagnosticId.DatagramTruncated);
    }

    [Fact]
    public void Log_SinkThatDisablesTheLevelItself_ReceivesNothing()
    {
        // Arrange — the Sink's own IsEnabled is consulted as well as the configured minimum.
        RecordingDiagnosticsSink disabledSink = new() { Enabled = false };
        using RawReactorHarness harness = new(scratchBufferSize: 16, sinkOverride: disabledSink);

        // Act
        ProvokeTruncation(harness);

        // Assert
        disabledSink.Received.Should().BeEmpty();
    }

    [Fact]
    public void Log_RecordReachingTheSink_CarriesItsIdLevelAndTaggedSubject()
    {
        // Arrange
        using RawReactorHarness harness = new(scratchBufferSize: 16);

        // Act
        ProvokeTruncation(harness);

        // Assert
        DiagnosticRecord record = harness.Sink.Received.Should().ContainSingle().Subject;
        record.Id.Should().Be(DiagnosticId.DatagramTruncated);
        record.Level.Should().Be(DiagnosticLevel.Warn);
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
        record.Value0.Should().Be(16, "the record carries the scratch buffer size the datagram was truncated to");
    }

    [Fact]
    public void Log_ThrowingSink_IsInvokedExactlyOnceAndTheFrameStillCompletes()
    {
        // Arrange
        ThrowingDiagnosticsSink throwingSink = new();
        using RawReactorHarness harness = new(scratchBufferSize: 16, sinkOverride: throwingSink);

        // Act
        Action act = () =>
        {
            ProvokeTruncation(harness);
            ProvokeTruncation(harness);
            ProvokeTruncation(harness);
        };

        // Assert
        act.Should().NotThrow("a faulting Sink is a deliberate exception to bugs-always-throw");
        throwingSink.CallCount.Should().Be(1, "the Sink is disabled for the life of the Reactor after it throws");
    }

    [Fact]
    public void Counters_AfterTheSinkThrows_ReportSinkFaultedExactlyOnce()
    {
        // Arrange
        ThrowingDiagnosticsSink throwingSink = new();
        using RawReactorHarness harness = new(scratchBufferSize: 16, sinkOverride: throwingSink);

        // Act
        ProvokeTruncation(harness);
        ProvokeTruncation(harness);

        // Assert
        harness.Reactor.Counters.SinkFaulted.Should().Be(1);
    }

    [Fact]
    public void Pump_WithNoSinkInstalled_CompletesWithoutBuildingARecord()
    {
        // Arrange — with no Sink there is nowhere for a record to go, so the gate answers false and no
        // record is ever constructed. Observable only as the Frame completing normally.
        using RawReactorHarness harness = new(scratchBufferSize: 16, withoutDiagnostics: true);

        // Act
        Action act = () => ProvokeTruncation(harness);

        // Assert
        act.Should().NotThrow();
    }

    /// <summary>Sends a datagram larger than the Reactor's scratch buffer and Pumps once.</summary>
    private static void ProvokeTruncation(RawReactorHarness harness)
    {
        (_, IPEndPoint localEndPoint) = harness.BindLoopbackLink();
        harness.SendFromCounterparty(localEndPoint, new byte[100]);
        harness.Reactor.Pump();
    }
}
