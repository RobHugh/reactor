using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtasks 6.6.9 and 6.6.10 (issues #214 and #220): the ordered delivery queue, and its wiring into
/// the <c>ReliableOrdered</c> Space. A record whose predecessor has not arrived is held rather than
/// drained, and the whole run behind the hole drains in send order the moment it fills. The queue
/// itself is internal with no <c>InternalsVisibleTo</c> (ADR 0015), so every promise it makes is
/// asserted here at the one seam a consumer has: the Messages a Pump drains, and the counters it moves.
/// </summary>
/// <remarks>
/// Loss and reordering are scripted through <see cref="LossyProxy"/>, never probabilistic, so a failure
/// is reproducible. The receiver's window is widened past the default four fragments because these
/// tests deliberately park several already-admitted sequences above an open hole, which a window only
/// four wide has no room for — the hole, not the window, is what these tests are about.
/// </remarks>
public class DatagramSessionOrderedDeliveryTests
{
    /// <summary>
    /// Comfortably past the RTO, whose ceiling is the harness's one-second Linger, so the deadline has
    /// certainly passed however the estimator has moved (matches
    /// <see cref="DatagramSessionRetransmitResendTests"/>).
    /// </summary>
    private static readonly TimeSpan _pastTheRetransmitDeadline = TimeSpan.FromSeconds(3);

    /// <summary>Wide enough to hold a run of already-admitted sequences above an open hole.</summary>
    private const int WideReceiveWindow = 16;

    /// <summary>Room for the handful of reliable records these tests keep in flight at once (1200 bytes is the default fragment size).</summary>
    private const int RetentionForSeveralInFlight = 1200 * 8;

    [Fact]
    public void Pump_ReliableOrderedMessagesArrivingInOrder_DrainInSendOrderOneForOne()
    {
        // Arrange — nothing is dropped or reordered, so this is the steady in-order path: every record
        // is the one the queue was waiting for and nothing is ever held.
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, retentionBytes: RetentionForSeveralInFlight);
        using DatagramSessionReactorHarness receiver = new(receiveWindowFragments: WideReceiveWindow);
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        byte[][] messages = [[1], [2], [3], [4]];

        // Act — one Send per round, so each Message gets a Datagram of its own and arrives on its own.
        List<byte[]> delivered = [];
        foreach (byte[] message in messages)
        {
            sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, message).Should().Be(SendOutcome.Sent);
            DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);
            delivered.AddRange(receiver.DrainPayloads());
        }

        // Assert
        delivered.Should().Equal(messages, (actual, expected) => actual.SequenceEqual(expected),
            "an in-order arrival is yielded immediately, one for one, in send order");
        receiver.Reactor.CountersOf(receiverPeer).MessagesReceived.Should().Be((ulong)messages.Length);
    }

    [Fact]
    public void Pump_AReliableOrderedMessageArrivingWhileAnEarlierOneIsMissing_IsHeldAndDrainsNothing()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, retentionBytes: RetentionForSeveralInFlight);
        using DatagramSessionReactorHarness receiver = new(receiveWindowFragments: WideReceiveWindow);
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        byte[] first = [1, 1];
        byte[] second = [2, 2];

        // Act — the first Message is dropped, so the second arrives with a hole beneath it.
        proxy.DropNext(RelayDirection.ClientToUpstream);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, first).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);

        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, second).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);

        // Assert — the second Message reached the receiver and its window admitted it, and still nothing
        // drains, because the Space delivers in send order and the sequence beneath it is missing.
        receiver.DrainPayloads().Should().BeEmpty("a record whose predecessor has not arrived is held, not drained");
        receiver.Reactor.CountersOf(receiverPeer).MessagesReceived.Should().Be(
            0UL, "a Message held for a gap is not counted as received until it drains");
        receiver.Reactor.CountersOf(receiverPeer).DuplicatePackets.Should().Be(
            0UL, "holding a record is not a duplicate — its sequence was admitted, only its turn has not come");
    }

    [Fact]
    public void Pump_TheMissingReliableOrderedMessageArriving_ReleasesTheWholeRunBehindItInSendOrder()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, retentionBytes: RetentionForSeveralInFlight);
        using DatagramSessionReactorHarness receiver = new(receiveWindowFragments: WideReceiveWindow);
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        byte[] missing = [0, 0];
        byte[][] behindTheHole = [[1], [2], [3]];

        // Act — the first Message is dropped and three more are Sent above the hole it left.
        proxy.DropNext(RelayDirection.ClientToUpstream);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, missing).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);

        foreach (byte[] message in behindTheHole)
        {
            sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, message).Should().Be(SendOutcome.Sent);
            DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);
        }

        receiver.DrainPayloads().Should().BeEmpty("all three are held behind the hole the drop left");

        // The deadline passes, so the dropped sequence is resent and the hole fills.
        sender.TimeProvider.Advance(_pastTheRetransmitDeadline);
        List<byte[]> delivered = PumpRoundsCollectingPayloads(sender, proxy, receiver, rounds: 12);

        // Assert — the filled hole released itself and everything held above it, in send order, and each
        // Message exactly once.
        delivered.Should().Equal(
            [missing, .. behindTheHole], (actual, expected) => actual.SequenceEqual(expected),
            "the missing sequence and the whole contiguous run held behind it drain together, in send order");
        receiver.Reactor.CountersOf(receiverPeer).MessagesReceived.Should().Be(
            4UL, "each held Message is counted as received exactly when it drains, and only then");
    }

    [Fact]
    public void Pump_AReliableOrderedMessageDuplicatedOnTheWire_IsDeliveredOnceNotTwice()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, retentionBytes: RetentionForSeveralInFlight);
        using DatagramSessionReactorHarness receiver = new(receiveWindowFragments: WideReceiveWindow);
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        byte[] message = [8, 8, 8];

        // Act — the Datagram carrying the Message is relayed twice, so the same sequence arrives twice.
        proxy.DuplicateNext(RelayDirection.ClientToUpstream);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, message).Should().Be(SendOutcome.Sent);
        List<byte[]> delivered = PumpRoundsCollectingPayloads(sender, proxy, receiver, rounds: 6);

        // Assert
        delivered.Should().ContainSingle("a sequence already yielded is never yielded a second time")
            .Which.Should().Equal(message);
        PeerCounters counters = receiver.Reactor.CountersOf(receiverPeer);
        counters.MessagesReceived.Should().Be(1UL);

        // The repeat was recognised and refused rather than silently ignored. Which of the two refusal
        // counters it lands on is the receive window's business, not this Space's: a sequence admitted
        // at the window's own base is retired as the base advances past it, so its repeat arrives below
        // the base and reads as out of window rather than as a duplicate.
        (counters.DuplicatePackets + counters.OutOfWindowPackets).Should().Be(
            1UL, "the second copy was counted once as a refusal");
    }

    [Fact]
    public void Pump_AReliableOrderedGap_HoldsNothingInTheOtherThreeSpaces()
    {
        // Arrange — the head-of-line blocking a hole causes is confined to its own Space by
        // construction, because only that Space has an ordered queue at all.
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, retentionBytes: RetentionForSeveralInFlight);
        using DatagramSessionReactorHarness receiver = new(receiveWindowFragments: WideReceiveWindow);
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        byte[] stalled = [9, 9];
        byte[] unordered = [1];
        byte[] sequenced = [2];
        byte[] unreliable = [3];

        // Act — open a hole in the ReliableOrdered Space, then Send one Message in each of the other three.
        proxy.DropNext(RelayDirection.ClientToUpstream);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, stalled).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);

        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [9, 9, 9]).Should().Be(SendOutcome.Sent);
        sender.Reactor.Send(peer, DeliveryMode.ReliableUnordered, unordered).Should().Be(SendOutcome.Sent);
        sender.Reactor.Send(peer, DeliveryMode.UnreliableSequenced, sequenced).Should().Be(SendOutcome.Sent);
        sender.Reactor.Send(peer, DeliveryMode.Unreliable, unreliable).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);

        // Assert
        receiver.DrainPayloads().Should().BeEquivalentTo(
            new[] { unordered, sequenced, unreliable },
            "the other three Spaces have no ordered queue, so the ReliableOrdered hole holds nothing of theirs");
    }

    /// <summary>
    /// Pumps the three parties in a round <paramref name="rounds"/> times, collecting every payload the
    /// receiver drains along the way. A fixed round count rather than a wait-for-condition loop: a test
    /// asserting a Message arrives exactly once must keep Pumping past the arrival to catch a second one,
    /// which a loop stopping at the first would miss.
    /// </summary>
    private static List<byte[]> PumpRoundsCollectingPayloads(
        DatagramSessionReactorHarness sender,
        LossyProxy proxy,
        DatagramSessionReactorHarness receiver,
        int rounds)
    {
        List<byte[]> payloads = [];
        for (int round = 0; round < rounds; round++)
        {
            DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);
            payloads.AddRange(receiver.DrainPayloads());
            sender.DrainAndDiscard();
            PumpDeadline.Yield();
        }

        return payloads;
    }
}
