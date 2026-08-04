using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

public class ReactorCounterSetTests
{
    [Fact]
    public void Snapshot_NewCounterSet_IsAllZero()
    {
        // Arrange
        ReactorCounterSet counters = new();

        // Act
        ReactorCounters snapshot = counters.Snapshot();

        // Assert
        snapshot.Should().Be(default(ReactorCounters));
    }

    [Fact]
    public void Snapshot_TwoSnapshotsAroundKnownTraffic_DifferByExactlyTheExpectedAmounts()
    {
        // Arrange
        ReactorCounterSet counters = new();
        ReactorCounters before = counters.Snapshot();

        // Act
        counters.RecordDatagramSent();
        counters.RecordDatagramSent();
        counters.RecordDatagramReceived();
        counters.RecordAdmissionDeferred();
        ReactorCounters after = counters.Snapshot();

        // Assert
        (after.DatagramsSent - before.DatagramsSent).Should().Be(2);
        (after.DatagramsReceived - before.DatagramsReceived).Should().Be(1);
        (after.AdmissionsDeferred - before.AdmissionsDeferred).Should().Be(1);
    }

    [Fact]
    public void RecordSinkFaulted_Called_IncrementsSinkFaulted()
    {
        // Arrange
        ReactorCounterSet counters = new();

        // Act
        counters.RecordSinkFaulted();

        // Assert
        counters.Snapshot().SinkFaulted.Should().Be(1);
    }

    [Fact]
    public void ObserveArenaUsage_IncreasingThenLowerUsage_KeepsTheHighWaterMark()
    {
        // Arrange
        ReactorCounterSet counters = new();

        // Act
        counters.ObserveArenaUsage(500);
        counters.ObserveArenaUsage(200);

        // Assert
        counters.Snapshot().ArenaHighWaterBytes.Should().Be(500);
    }
}
