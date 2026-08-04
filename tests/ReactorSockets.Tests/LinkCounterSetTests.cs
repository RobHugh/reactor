using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

public class LinkCounterSetTests
{
    [Fact]
    public void Snapshot_NewCounterSet_IsAllZero()
    {
        // Arrange
        LinkCounterSet counters = new();

        // Act
        LinkCounters snapshot = counters.Snapshot();

        // Assert
        snapshot.Should().Be(default(LinkCounters));
    }

    [Fact]
    public void Snapshot_TwoSnapshotsAroundKnownTraffic_DifferByExactlyTheExpectedAmounts()
    {
        // Arrange
        LinkCounterSet counters = new();
        LinkCounters before = counters.Snapshot();

        // Act
        counters.RecordBytesSent(100);
        counters.RecordBytesReceived(40);
        counters.RecordReadShareExhaustion();
        LinkCounters after = counters.Snapshot();

        // Assert
        (after.BytesSent - before.BytesSent).Should().Be(100);
        (after.BytesReceived - before.BytesReceived).Should().Be(40);
        (after.ReadShareExhaustions - before.ReadShareExhaustions).Should().Be(1);
    }
}
