using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.2.1 (issue #168): the datagram profile's per-Peer counter reader. Nothing increments
/// <see cref="PeerCounters.OutOfWindowPackets"/> or <see cref="PeerCounters.DuplicatePackets"/> yet —
/// that lands with the per-Space receive window (issue #170) — so these tests assert only that the
/// reader exists, that a freshly connected Peer reads zero for both, and that it fails the same way
/// the stream profile's <c>CountersOf</c> does for a Peer that is not open.
/// </summary>
public class DatagramSessionCounterTests
{
    [Fact]
    public void CountersOf_AFreshlyConnectedPeer_ReadsZeroForOutOfWindowAndDuplicatePackets()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);

        PeerId diallerPeer = dialler.Reactor.Connect(accepter.Endpoint);
        (var diallerEvents, var accepterEvents) =
            DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);

        PeerId accepterPeer = accepterEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

        // Act
        PeerCounters diallerCounters = dialler.Reactor.CountersOf(diallerPeer);
        PeerCounters accepterCounters = accepter.Reactor.CountersOf(accepterPeer);

        // Assert
        diallerCounters.OutOfWindowPackets.Should().Be(0);
        diallerCounters.DuplicatePackets.Should().Be(0);
        accepterCounters.OutOfWindowPackets.Should().Be(0);
        accepterCounters.DuplicatePackets.Should().Be(0);
    }

    [Fact]
    public void CountersOf_AFreshlyConnectedPeer_ReadsZeroForHorizonDeferrals()
    {
        // Arrange — subtask 6.3.2 (issue #184). Nothing increments HorizonDeferrals yet; that lands
        // with the horizon gate (issue #185), so this asserts only that a freshly connected Peer's
        // figure starts at zero.
        using DatagramSessionReactorHarness accepter = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);

        PeerId diallerPeer = dialler.Reactor.Connect(accepter.Endpoint);
        (var diallerEvents, var accepterEvents) =
            DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);

        PeerId accepterPeer = accepterEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

        // Act
        PeerCounters diallerCounters = dialler.Reactor.CountersOf(diallerPeer);
        PeerCounters accepterCounters = accepter.Reactor.CountersOf(accepterPeer);

        // Assert
        diallerCounters.HorizonDeferrals.Should().Be(0);
        accepterCounters.HorizonDeferrals.Should().Be(0);
    }

    [Fact]
    public void CountersOf_AFreshlyConnectedPeer_ReadsZeroForPacketsRetransmitted()
    {
        // Arrange — subtask 6.4.6 (issue #196). Nothing increments PacketsRetransmitted yet; that
        // lands with the expiry path (issue #198), so this asserts only that a freshly connected
        // Peer's figure starts at zero and rides the same snapshot as the other window counters.
        using DatagramSessionReactorHarness accepter = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);

        PeerId diallerPeer = dialler.Reactor.Connect(accepter.Endpoint);
        (var diallerEvents, var accepterEvents) =
            DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);

        PeerId accepterPeer = accepterEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

        // Act
        PeerCounters diallerCounters = dialler.Reactor.CountersOf(diallerPeer);
        PeerCounters accepterCounters = accepter.Reactor.CountersOf(accepterPeer);

        // Assert
        diallerCounters.PacketsRetransmitted.Should().Be(0);
        accepterCounters.PacketsRetransmitted.Should().Be(0);
    }

    [Fact]
    public void CountersOf_UnknownPeerId_ThrowsMatchingStreamProfilesCountersOfConvention()
    {
        // Arrange — a PeerId this Reactor never issued, mirroring
        // StreamSessionSendRetentionTests.Send_UnknownPeerId_ThrowsMatchingCountersOfsConvention.
        using DatagramSessionReactorHarness harness = new();
        PeerId unknown = new(Origin: 1, Index: 0, Generation: 1);

        // Act
        Action act = () => harness.Reactor.CountersOf(unknown);

        // Assert
        act.Should().Throw<ArgumentException>().WithMessage("*is not open*");
    }
}
