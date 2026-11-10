using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The two reliable promises, each phrased as the
/// promise <c>docs/delivery-modes.md</c> makes and driven by <see cref="LossyProxy"/> under a scripted
/// plan of drops, duplicates and reorders. <c>ReliableUnordered</c> promises every Message arrives
/// exactly once and nothing about order; <c>ReliableOrdered</c> promises exactly once <em>and</em> in
/// send order.
/// </summary>
/// <remarks>
/// <para>
/// Every fault here is scripted rather than probabilistic, so a failure is reproducible from the test
/// alone: the proxy is told which relay to drop, duplicate or hold back, and the clock is a
/// <c>FakeTimeProvider</c> the test advances by hand to make a retransmit deadline pass.
/// </para>
/// <para>
/// The <c>ReliableUnordered</c> test asserts the multiset of delivered payloads and never their order.
/// Asserting order there would assert something the mode does not promise, and would pass only by
/// accident on whatever path the proxy happened to produce.
/// </para>
/// </remarks>
public class DatagramSessionReliableDeliveryPromiseTests
{
    /// <summary>
    /// Comfortably past the RTO, whose ceiling is the harness's one-second Linger, so the deadline has
    /// certainly passed however the estimator has moved.
    /// </summary>
    private static readonly TimeSpan _pastTheRetransmitDeadline = TimeSpan.FromSeconds(3);

    /// <summary>Wide enough to hold the run these tests park above an open hole; the default four fragments is not.</summary>
    private const int WideReceiveWindow = 16;

    /// <summary>Room for every reliable record these tests keep in flight at once (1200 bytes is the default fragment size).</summary>
    private const int RetentionForSeveralInFlight = 1200 * 12;

    [Fact]
    public void Pump_ReliableUnorderedUnderDropsDuplicatesAndReorders_DeliversEveryMessageExactlyOnce()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, retentionBytes: RetentionForSeveralInFlight);
        using DatagramSessionReactorHarness receiver = new(receiveWindowFragments: WideReceiveWindow);
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        byte[][] messages = [[0], [1], [2], [3], [4], [5]];

        // Act — one Send per round so each Message gets a Datagram of its own to be faulted
        // individually: the first is dropped outright, the third is relayed twice, and the fifth is held
        // back so the sixth overtakes it. Each round drains as it goes, because a Pump resets the frame
        // arena and a Message left undrained across one is gone.
        List<byte[]> delivered = [];
        for (int index = 0; index < messages.Length; index++)
        {
            switch (index)
            {
                case 0:
                    proxy.DropNext(RelayDirection.ClientToUpstream);
                    break;
                case 2:
                    proxy.DuplicateNext(RelayDirection.ClientToUpstream);
                    break;
                case 4:
                    proxy.ReorderNextTwo(RelayDirection.ClientToUpstream);
                    break;
            }

            sender.Reactor.Send(peer, DeliveryMode.ReliableUnordered, messages[index]).Should().Be(SendOutcome.Sent);
            DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);
            delivered.AddRange(receiver.DrainPayloads());
        }

        // The dropped Message is recovered only by a retransmission, which needs its deadline to pass.
        sender.TimeProvider.Advance(_pastTheRetransmitDeadline);
        delivered.AddRange(CollectDeliveries(sender, proxy, receiver, rounds: 20));

        // Assert — the multiset, never the order: this mode promises everything arrives exactly once and
        // promises nothing at all about the sequence it arrives in.
        delivered.Should().BeEquivalentTo(
            messages,
            options => options.WithoutStrictOrdering(),
            "every Message Sent is delivered, and none is delivered twice");
        receiver.Reactor.CountersOf(receiverPeer).MessagesReceived.Should().Be(
            (ulong)messages.Length, "exactly one delivery per Message Sent, whatever the wire did");
    }

    [Fact]
    public void Pump_ReliableOrderedUnderDropsAndReorders_DeliversEveryMessageExactlyOnceInSendOrder()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, retentionBytes: RetentionForSeveralInFlight);
        using DatagramSessionReactorHarness receiver = new(receiveWindowFragments: WideReceiveWindow);
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        byte[][] messages = [[0], [1], [2], [3], [4]];
        ulong retransmittedBefore = sender.Reactor.CountersOf(peer).PacketsRetransmitted;

        // Act — the second Message is dropped, opening a hole every later one must wait behind, and the
        // fourth is held back so the fifth overtakes it on the wire.
        List<byte[]> deliveredWhileTheHoleWasOpen = [];
        for (int index = 0; index < messages.Length; index++)
        {
            switch (index)
            {
                case 1:
                    proxy.DropNext(RelayDirection.ClientToUpstream);
                    break;
                case 3:
                    proxy.ReorderNextTwo(RelayDirection.ClientToUpstream);
                    break;
            }

            sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, messages[index]).Should().Be(SendOutcome.Sent);
            DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);
            deliveredWhileTheHoleWasOpen.AddRange(receiver.DrainPayloads());
        }

        // Assert — only the Message beneath the hole reached the consumer; everything behind it is held
        // rather than delivered early, however it arrived on the wire.
        deliveredWhileTheHoleWasOpen.Should().Equal(
            [messages[0]], (actual, expected) => actual.SequenceEqual(expected),
            "the Messages behind the hole are held, not delivered out of order");

        // Act — the deadline passes, the dropped sequence is resent, and the hole fills.
        sender.TimeProvider.Advance(_pastTheRetransmitDeadline);
        List<byte[]> deliveredAfterTheHoleFilled = CollectDeliveries(sender, proxy, receiver, rounds: 20);

        // Assert
        List<byte[]> delivered = [.. deliveredWhileTheHoleWasOpen, .. deliveredAfterTheHoleFilled];
        delivered.Should().Equal(
            messages, (actual, expected) => actual.SequenceEqual(expected),
            "every Message arrives exactly once, in send order");
        receiver.Reactor.CountersOf(receiverPeer).MessagesReceived.Should().Be((ulong)messages.Length);
        (sender.Reactor.CountersOf(peer).PacketsRetransmitted - retransmittedBefore).Should().BeGreaterThan(
            0UL, "the dropped Message arrived on a retransmission rather than being skipped");
    }

    /// <summary>
    /// Pumps the three parties in a round <paramref name="rounds"/> times, collecting every payload the
    /// receiver drains along the way. A fixed round count rather than a wait-for-condition loop: a test
    /// asserting each Message arrives exactly once must keep Pumping past the last arrival to catch a
    /// second copy, which a loop stopping at the first would miss.
    /// </summary>
    private static List<byte[]> CollectDeliveries(
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
