using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.6.8 (issue #218): a reliable sequence whose retransmit deadline has passed is resent —
/// its bytes read back out of retention and repacked into the next Pump's Datagram — so a dropped
/// reliable Message arrives after all. Driven by <see cref="LossyProxy"/> with the loss scripted, never
/// probabilistic, so a failure here is reproducible.
/// </summary>
public class DatagramSessionRetransmitResendTests
{
    /// <summary>
    /// Comfortably past the RTO, whose ceiling is the harness's one-second Linger, so the deadline has
    /// certainly passed however the estimator has moved.
    /// </summary>
    private static readonly TimeSpan _pastTheRetransmitDeadline = TimeSpan.FromSeconds(3);

    [Fact]
    public void Pump_AReliableOrderedMessageWhoseOnlyTransmissionWasDropped_ArrivesOnTheRetransmission()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = new();
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        byte[] message = [7, 7, 7, 7];
        ulong retransmittedBefore = sender.Reactor.CountersOf(peer).PacketsRetransmitted;

        // Act — the one transmission of this Message is dropped, so only a resend can deliver it.
        proxy.DropNext(RelayDirection.ClientToUpstream);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, message);
        DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);
        receiver.DrainPayloads().Should().BeEmpty("the only transmission was dropped");

        sender.TimeProvider.Advance(_pastTheRetransmitDeadline);
        List<byte[]> delivered = PumpRoundsCollectingPayloads(sender, proxy, receiver, rounds: 12);

        // Assert
        delivered.Should().ContainSingle("the dropped Message is delivered once, by the resend")
            .Which.Should().Equal(message, "the resent record carries the bytes the original did");
        (sender.Reactor.CountersOf(peer).PacketsRetransmitted - retransmittedBefore).Should().BeGreaterThan(
            0UL, "the expired sequence was resent, not merely noticed");
        receiver.Reactor.CountersOf(receiverPeer).MessagesReceived.Should().Be(
            1UL, "a resend that arrives once delivers once");
    }

    [Fact]
    public void Pump_AResentRecord_TravelsInTheSameDatagramAsRecordsSentThatPump()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = new();
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        byte[] dropped = [1, 1, 1];
        byte[] fresh = [2, 2, 2];

        proxy.DropNext(RelayDirection.ClientToUpstream);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, dropped);
        DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);

        ulong datagramsSentBefore = sender.Reactor.Counters.DatagramsSent;
        ulong receivedBefore = receiver.Reactor.Counters.DatagramsReceived;

        // Act — the deadline has passed and a new Message is Sent in the same Frame, so the resend and
        // the new record have one Datagram to share.
        sender.TimeProvider.Advance(_pastTheRetransmitDeadline);
        sender.Reactor.Send(peer, DeliveryMode.ReliableUnordered, fresh);
        sender.Reactor.Pump();

        // Assert — one Datagram left carrying both records, which is what "repacked into whatever
        // container goes out next" means: the resend does not get a Datagram of its own.
        (sender.Reactor.Counters.DatagramsSent - datagramsSentBefore).Should().Be(
            1UL, "a resend rides the Pump's single Datagram rather than one of its own");

        proxy.Pump();
        receiver.PumpUntil(() => receiver.Reactor.Counters.DatagramsReceived > receivedBefore);
        receiver.DrainPayloads().Should().BeEquivalentTo(
            new[] { dropped, fresh }, "both records travelled in the one Datagram and both delivered");
        receiver.Reactor.CountersOf(receiverPeer).MessagesReceived.Should().Be(2UL);
    }

    [Fact]
    public void Pump_ASequenceAcknowledgedBeforeItsDeadline_IsNeverResent()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = new();
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        ulong retransmittedBefore = sender.Reactor.CountersOf(peer).PacketsRetransmitted;

        // Act — nothing is dropped, so the Message arrives on its first transmission.
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [5, 5, 5]);
        PumpRoundsCollectingPayloads(sender, proxy, receiver, rounds: 12)
            .Should().ContainSingle("the Message arrived on its first transmission");

        // An ack block rides a Datagram and a Datagram is only sent for a Peer with records queued, so
        // the acknowledgement needs traffic in the reverse direction to travel on. A reply is the
        // ordinary way a Session has some; until slice 8's Ping, a wholly silent receiver has none.
        receiver.Reactor.Send(receiverPeer, DeliveryMode.Unreliable, [1]);
        PumpRoundsCollectingPayloads(sender, proxy, receiver, rounds: 4);

        sender.TimeProvider.Advance(_pastTheRetransmitDeadline);
        PumpRoundsCollectingPayloads(sender, proxy, receiver, rounds: 12);

        // Assert
        sender.Reactor.CountersOf(peer).PacketsRetransmitted.Should().Be(
            retransmittedBefore, "an acknowledged sequence has nothing left to resend");
    }

    /// <summary>
    /// Pumps the three parties in a round <paramref name="rounds"/> times, collecting every payload the
    /// receiver drains along the way. A fixed round count rather than a wait-for-condition loop: a test
    /// asserting that exactly one Message arrives must keep Pumping past the arrival to catch a second
    /// one, which a loop stopping at the first would miss.
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
