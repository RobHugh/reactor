using System.Net;
using System.Net.Sockets;
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
    public void Pump_DatagramExchange_MovesTheDatagramFiguresTheArenaHighWaterAndEachLinksOwnBytesOnly()
    {
        // Arrange — issue #61: every field rises where expected and nowhere else.
        using RawReactorHarness harness = new();
        (LinkId sender, _) = harness.BindLoopbackLink();
        (LinkId receiver, IPEndPoint receiverEndPoint) = harness.BindLoopbackLink();
        ReactorCounters reactorBefore = harness.Reactor.Counters;
        LinkCounters senderBefore = harness.Reactor.CountersOf(sender);
        LinkCounters receiverBefore = harness.Reactor.CountersOf(receiver);

        // Act
        harness.Reactor.SendTo(sender, Source.FromIPEndPoint(receiverEndPoint), new byte[64])
            .IsSuccess.Should().BeTrue();
        harness.Reactor.Pump();
        harness.DrainToList().Should().ContainSingle();

        // Assert
        CounterFields.Moved(reactorBefore, harness.Reactor.Counters).Should().BeEquivalentTo(
            nameof(ReactorCounters.DatagramsSent),
            nameof(ReactorCounters.DatagramsReceived),
            nameof(ReactorCounters.ArenaHighWaterBytes));
        CounterFields.Moved(senderBefore, harness.Reactor.CountersOf(sender))
            .Should().BeEquivalentTo(nameof(LinkCounters.BytesSent));
        CounterFields.Moved(receiverBefore, harness.Reactor.CountersOf(receiver))
            .Should().BeEquivalentTo(nameof(LinkCounters.BytesReceived));
    }

    [Fact]
    public void Pump_StreamExchange_MovesTheArenaHighWaterAndTheLinksBytesButNoDatagramFigure()
    {
        // Arrange
        using RawReactorHarness harness = new();
        (_, IPEndPoint listenEndPoint) = harness.ListenOnLoopback();
        using Socket counterparty = harness.ConnectCounterpartyTo(listenEndPoint);
        harness.Reactor.Pump();
        LinkId accepted = harness.FirstAcceptedLink();
        ReactorCounters reactorBefore = harness.Reactor.Counters;
        LinkCounters linkBefore = harness.Reactor.CountersOf(accepted);

        // Act
        harness.Reactor.Send(accepted, new byte[32]).IsSuccess.Should().BeTrue();
        counterparty.Send(new byte[48]);
        harness.PumpUntil(() => harness.Reactor.CountersOf(accepted).BytesReceived - linkBefore.BytesReceived == 48);

        // Assert
        CounterFields.Moved(reactorBefore, harness.Reactor.Counters)
            .Should().BeEquivalentTo(nameof(ReactorCounters.ArenaHighWaterBytes));
        LinkCounters linkAfter = harness.Reactor.CountersOf(accepted);
        (linkAfter.BytesSent - linkBefore.BytesSent).Should().Be(32);
        (linkAfter.BytesReceived - linkBefore.BytesReceived).Should().Be(48);
    }

    [Fact]
    public void Pump_ThreeConnectionsWaitingBehindOneListeningLink_CountsOneDeferralAndNothingElse()
    {
        // Arrange — the unit is declinings, not connections: one listening Link declines its whole
        // backlog with one declining, so three waiting connections must still read one (issue #61).
        using RawReactorHarness harness = new(capacity: 8, admissionCap: 1);
        (_, IPEndPoint listenEndPoint) = harness.ListenOnLoopback();
        using Socket first = harness.ConnectCounterpartyTo(listenEndPoint);
        using Socket second = harness.ConnectCounterpartyTo(listenEndPoint);
        using Socket third = harness.ConnectCounterpartyTo(listenEndPoint);
        ReactorCounters before = harness.Reactor.Counters;

        // Act
        harness.PumpUntil(() => harness.Reactor.Counters.AdmissionDeferrals > before.AdmissionDeferrals);

        // Assert
        ReactorCounters after = harness.Reactor.Counters;
        (after.AdmissionDeferrals - before.AdmissionDeferrals).Should().Be(1);
        CounterFields.Moved(before, after).Should().BeEquivalentTo(nameof(ReactorCounters.AdmissionDeferrals));
    }

    [Fact]
    public void Counters_SnapshotsTakenBeforeMoreTraffic_AreValuesTheReactorDoesNotChange()
    {
        // Arrange
        using RawReactorHarness harness = new();
        (LinkId sender, _) = harness.BindLoopbackLink();
        (_, IPEndPoint receiverEndPoint) = harness.BindLoopbackLink();
        ReactorCounters reactorSnapshot = harness.Reactor.Counters;
        LinkCounters linkSnapshot = harness.Reactor.CountersOf(sender);
        IReadOnlyDictionary<string, ulong> reactorValues = CounterFields.Read(reactorSnapshot);
        IReadOnlyDictionary<string, ulong> linkValues = CounterFields.Read(linkSnapshot);

        // Act
        harness.Reactor.SendTo(sender, Source.FromIPEndPoint(receiverEndPoint), new byte[64]);
        harness.Reactor.Pump();

        // Assert — the Reactor moved, and the snapshots taken before it did not.
        harness.Reactor.Counters.Should().NotBe(reactorSnapshot);
        harness.Reactor.CountersOf(sender).Should().NotBe(linkSnapshot);
        CounterFields.Read(reactorSnapshot).Should().Equal(reactorValues);
        CounterFields.Read(linkSnapshot).Should().Equal(linkValues);
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
