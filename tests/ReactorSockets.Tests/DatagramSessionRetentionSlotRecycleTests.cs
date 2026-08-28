using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.5.7 (issue #206) and subtask 6.5.6 (issue #205): the retention pool belongs to the
/// capacity Slot, not to the Peer occupying it, so it outlives whichever Peer is admitted. A Peer
/// admitted into a Slot a previous Peer vacated must therefore find that Slot's retention empty and its
/// <see cref="PeerCounters.RetentionHighWaterBytes"/> at zero, exactly as every other per-Peer counter
/// on a freshly connected Peer reads zero.
/// </summary>
/// <remarks>
/// Capacity is pinned to one throughout, which is what forces the second Peer into the first Peer's
/// Slot rather than leaving it to chance. Without the Slot's retention being reset on admission the
/// second Peer inherits the first's occupied slots, and a Peer that has sent nothing is refused with
/// <see cref="SendOutcome.RetentionExhausted"/> on its very first reliable Send.
/// </remarks>
public class DatagramSessionRetentionSlotRecycleTests
{
    [Fact]
    public void Send_ByAPeerDialledIntoASlotAPreviousPeerFilled_FindsTheSlotsRetentionEmpty()
    {
        // Arrange — one slot per reliable Space, so the first Peer exhausts ReliableOrdered with a
        // single Send and any inherited occupancy is immediately visible on the second Peer's first one.
        const int fragmentSize = 600;
        using DatagramSessionReactorHarness harness = new(
            capacity: 1,
            bindLoopback: false,
            fragmentSize: fragmentSize,
            retentionBytes: fragmentSize,
            maxMessageSize: fragmentSize);
        using DatagramSessionReactorHarness firstRemote = new();
        using DatagramSessionReactorHarness secondRemote = new();

        // A reliable Send is refused unless its Peer has learned the remote's announced MaxMessageSize
        // (issue #284), so each Peer here must complete a real handshake before it can Send.
        PeerId first = harness.Reactor.Connect(firstRemote.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(harness, firstRemote);
        harness.Reactor.Send(first, DeliveryMode.ReliableOrdered, [1, 2, 3]).Should().Be(SendOutcome.Sent);
        harness.Reactor.Send(first, DeliveryMode.ReliableOrdered, [1, 2, 3]).Should().Be(
            SendOutcome.RetentionExhausted, "the first Peer genuinely filled its Space's one slot");
        harness.Reactor.Close(first);
        harness.Reactor.Pump();
        harness.DrainToList();

        // Act — capacity is one, so this dial reuses the Slot the first Peer just vacated.
        PeerId second = harness.Reactor.Connect(secondRemote.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(harness, secondRemote);
        SendOutcome outcome = harness.Reactor.Send(second, DeliveryMode.ReliableOrdered, [4, 5, 6]);

        // Assert
        outcome.Should().Be(
            SendOutcome.Sent, "the Slot's retention is reset on admission, so a new Peer does not inherit its predecessor's slots");
    }

    [Fact]
    public void CountersOf_APeerDialledIntoASlotAPreviousPeerFilled_ReadsRetentionHighWaterBytesAtZero()
    {
        // Arrange
        const int fragmentSize = 600;
        using DatagramSessionReactorHarness harness = new(
            capacity: 1,
            bindLoopback: false,
            fragmentSize: fragmentSize,
            retentionBytes: fragmentSize,
            maxMessageSize: fragmentSize);
        using DatagramSessionReactorHarness firstRemote = new();
        using DatagramSessionReactorHarness secondRemote = new();

        // A reliable Send is refused unless its Peer has learned the remote's announced MaxMessageSize
        // (issue #284), so the first Peer must complete a real handshake before it can Send.
        PeerId first = harness.Reactor.Connect(firstRemote.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(harness, firstRemote);
        harness.Reactor.Send(first, DeliveryMode.ReliableOrdered, [1, 2, 3]).Should().Be(SendOutcome.Sent);
        harness.Reactor.CountersOf(first).RetentionHighWaterBytes.Should().Be(
            3UL, "the first Peer's high-water is the three bytes it retained");
        harness.Reactor.Close(first);
        harness.Reactor.Pump();
        harness.DrainToList();

        // Act
        PeerId second = harness.Reactor.Connect(secondRemote.Endpoint);

        // Assert — monotonic within one Peer's life (#205), but a new occupant of the Slot is a new
        // Peer, and every other per-Peer counter starts it at zero.
        harness.Reactor.CountersOf(second).RetentionHighWaterBytes.Should().Be(
            0UL, "a freshly connected Peer reads zero, whatever the previous occupant of its Slot retained");
    }

    [Fact]
    public void Send_ByAPeerAcceptedIntoASlotAPreviousPeerFilled_FindsTheSlotsRetentionEmpty()
    {
        // Arrange — the accept side of the same claim: a Peer admitted by an inbound handshake gets the
        // same reset as a dialled one, so the two admission paths cannot drift apart.
        const int fragmentSize = 600;
        using DatagramSessionReactorHarness accepter = new(
            capacity: 1, fragmentSize: fragmentSize, retentionBytes: fragmentSize, maxMessageSize: fragmentSize);
        using DatagramSessionReactorHarness firstDialler = new(
            bindLoopback: false, fragmentSize: fragmentSize, retentionBytes: fragmentSize, maxMessageSize: fragmentSize);

        firstDialler.Reactor.Connect(accepter.Endpoint);
        PeerId firstAccepted = AcceptedPeer(firstDialler, accepter);
        accepter.Reactor.Send(firstAccepted, DeliveryMode.ReliableOrdered, [1, 2, 3]).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(firstAccepted, DeliveryMode.ReliableOrdered, [1, 2, 3]).Should().Be(
            SendOutcome.RetentionExhausted, "the first accepted Peer genuinely filled its Space's one slot");

        accepter.Reactor.Close(firstAccepted);
        accepter.Reactor.Pump();
        accepter.DrainToList();
        firstDialler.Reactor.Pump();
        firstDialler.DrainToList();

        // Act — a second dialler is admitted into the Slot the first accepted Peer vacated.
        using DatagramSessionReactorHarness secondDialler = new(
            bindLoopback: false, fragmentSize: fragmentSize, retentionBytes: fragmentSize, maxMessageSize: fragmentSize);
        secondDialler.Reactor.Connect(accepter.Endpoint);
        PeerId secondAccepted = AcceptedPeer(secondDialler, accepter);

        // Assert
        accepter.Reactor.CountersOf(secondAccepted).RetentionHighWaterBytes.Should().Be(
            0UL, "an accepted Peer reads zero too, whatever the previous occupant of its Slot retained");
        accepter.Reactor.Send(secondAccepted, DeliveryMode.ReliableOrdered, [4, 5, 6]).Should().Be(
            SendOutcome.Sent, "the accept path resets the Slot's retention exactly as the dial path does");
    }

    /// <summary>
    /// Completes the handshake between a freshly dialling <paramref name="dialler"/> and
    /// <paramref name="accepter"/>, and returns the <see cref="PeerId"/> the accepter admitted the
    /// dialler under — the id the accept-side assertions are made against.
    /// </summary>
    private static PeerId AcceptedPeer(DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter)
    {
        (var _, var accepterEvents) = DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);

        return accepterEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerConnected).Which.Peer;
    }
}
