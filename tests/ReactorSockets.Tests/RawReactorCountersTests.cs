using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Counters are pull-only and monotonic (ADR 0013): a consumer takes two snapshots and subtracts. The
/// mutable storage behind them is internal, so these tests do what a consumer does.
/// </summary>
public class RawReactorCountersTests
{
    [Fact]
    public void Counters_TwoSnapshotsAroundKnownTraffic_DifferByExactlyTheExpectedAmounts()
    {
        // Arrange
        using RawReactorHarness harness = new();
        (LinkId sender, _) = harness.BindLoopbackLink();
        (_, IPEndPoint receiverEndPoint) = harness.BindLoopbackLink();
        ReactorCounters before = harness.Reactor.Counters;

        // Act
        harness.Reactor.SendTo(sender, Source.FromIPEndPoint(receiverEndPoint), new byte[64])
            .IsSuccess.Should().BeTrue();
        harness.Reactor.Pump();
        harness.DrainToList().Should().ContainSingle();

        // Assert
        ReactorCounters after = harness.Reactor.Counters;
        (after.DatagramsSent - before.DatagramsSent).Should().Be(1);
        (after.DatagramsReceived - before.DatagramsReceived).Should().Be(1);
    }

    [Fact]
    public void CountersOf_TwoSnapshotsAroundKnownTraffic_DifferByExactlyTheBytesMoved()
    {
        // Arrange
        using RawReactorHarness harness = new();
        (LinkId sender, _) = harness.BindLoopbackLink();
        (LinkId receiver, IPEndPoint receiverEndPoint) = harness.BindLoopbackLink();
        LinkCounters sentBefore = harness.Reactor.CountersOf(sender);
        LinkCounters receivedBefore = harness.Reactor.CountersOf(receiver);

        // Act
        harness.Reactor.SendTo(sender, Source.FromIPEndPoint(receiverEndPoint), new byte[64]);
        harness.Reactor.Pump();
        harness.DrainToList().Should().ContainSingle();

        // Assert
        (harness.Reactor.CountersOf(sender).BytesSent - sentBefore.BytesSent).Should().Be(64);
        (harness.Reactor.CountersOf(receiver).BytesReceived - receivedBefore.BytesReceived).Should().Be(64);
    }

    [Fact]
    public void Counters_AfterADatagramIsCommitted_RecordTheArenaHighWaterMark()
    {
        // Arrange
        using RawReactorHarness harness = new();
        (LinkId sender, _) = harness.BindLoopbackLink();
        (_, IPEndPoint receiverEndPoint) = harness.BindLoopbackLink();

        // Act
        harness.Reactor.SendTo(sender, Source.FromIPEndPoint(receiverEndPoint), new byte[64]);
        harness.Reactor.Pump();
        harness.DrainToList().Should().ContainSingle();

        // Assert
        harness.Reactor.Counters.ArenaHighWaterBytes.Should().Be(64);
    }

    [Fact]
    public void Counters_AcrossAWipedFrame_DoNotResetWithTheArena()
    {
        // Arrange — the arena is wiped every Pump; the counters are not. Two readers sampling
        // independently must both see a monotonic figure.
        using RawReactorHarness harness = new();
        (LinkId sender, _) = harness.BindLoopbackLink();
        (_, IPEndPoint receiverEndPoint) = harness.BindLoopbackLink();
        harness.Reactor.SendTo(sender, Source.FromIPEndPoint(receiverEndPoint), new byte[64]);
        harness.Reactor.Pump();

        // Act
        harness.Reactor.Pump();
        harness.Reactor.Pump();

        // Assert
        harness.Reactor.Counters.DatagramsReceived.Should().Be(1);
    }

    [Fact]
    public void CountersOf_ClosedLink_ThrowsBecauseTheLinkIsGone()
    {
        // Arrange
        using RawReactorHarness harness = new();
        (LinkId link, _) = harness.BindLoopbackLink();
        harness.Reactor.Close(link);

        // Act
        Action act = () => harness.Reactor.CountersOf(link);

        // Assert
        act.Should().Throw<ArgumentException>();
    }
}
