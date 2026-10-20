using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A <c>ReliableOrdered</c> Message held behind a missing earlier one, then refused by the frame arena when
/// the gap fills, is still delivered. A <see cref="LossyProxy"/> drops M1's datagram, so the receiver admits,
/// and holds, M2. When M1's retransmission arrives, M1 takes the Peer's first reservation of the Frame and
/// M2 no longer fits the read share. M2 was admitted when it first arrived, so no retransmission will bring
/// it back: only the receiver's own retry on a later Pump can deliver it.
/// </summary>
public class DatagramSessionHeldOrderedMessageRefusedThroughProxyTests
{
    private const int MaxMessageSize = 512;
    private const int MessageLength = MaxMessageSize - 64;
    private const int WideReceiveWindow = 16;

    private static readonly TimeSpan _linger = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan _step = TimeSpan.FromMilliseconds(100);

    [Fact]
    public void Pump_HeldOrderedMessageTheArenaRefused_IsDeliveredOnALaterPumpInSendOrder()
    {
        // Arrange — two Peers' worth of arena gives a 512-byte read share, so M1 escapes the share as the
        // Peer's first reservation of the Frame and M2 is refused for the share, not for the whole arena. The
        // receive window must be wide enough to admit M2 ahead of the gap.
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, maxMessageSize: MaxMessageSize, linger: _linger);
        using DatagramSessionReactorHarness receiver = new(
            capacity: 2,
            maxMessageSize: MaxMessageSize,
            receiveWindowFragments: WideReceiveWindow,
            arenaSize: 2 * MaxMessageSize,
            linger: _linger,
            timeProvider: sender.TimeProvider);
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        // One clean round trip seeds the estimator. Unseeded, the RTO resolves to the Linger itself.
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Sent);
        sender.Reactor.Pump();
        proxy.Pump();
        receiver.Reactor.Pump();
        receiver.DrainPayloads();
        sender.TimeProvider.Advance(TimeSpan.FromMilliseconds(20));
        receiver.Reactor.Send(receiverPeer, DeliveryMode.Unreliable, [9]).Should().Be(SendOutcome.Sent);
        receiver.Reactor.Pump();
        proxy.Pump();
        sender.Reactor.Pump();
        sender.DrainPayloads();

        byte[] first = Enumerable.Repeat((byte)0xA0, MessageLength).ToArray();
        byte[] second = Enumerable.Repeat((byte)0xA1, MessageLength).ToArray();
        List<byte[]> delivered = [];

        proxy.DropNext(RelayDirection.ClientToUpstream);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, first).Should().Be(SendOutcome.Sent);
        sender.Reactor.Pump();
        proxy.Pump();
        receiver.Reactor.Pump();
        delivered.AddRange(receiver.DrainPayloads());
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, second).Should().Be(SendOutcome.Sent);
        sender.Reactor.Pump();
        proxy.Pump();
        receiver.Reactor.Pump();
        delivered.AddRange(receiver.DrainPayloads());

        delivered.Should().BeEmpty("M2 is held behind M1, whose datagram was dropped");
        ulong exhaustionsBefore = receiver.Reactor.CountersOf(receiverPeer).ReadShareExhaustions;
        ulong retransmittedBefore = sender.Reactor.CountersOf(peer).PacketsRetransmitted;

        TimeSpan elapsed = TimeSpan.Zero;
        while (elapsed < _linger / 2 && sender.Reactor.CountersOf(peer).PacketsRetransmitted == retransmittedBefore)
        {
            sender.TimeProvider.Advance(_step);
            elapsed += _step;
            sender.Reactor.Pump();
            sender.DrainPayloads();
        }

        proxy.Pump();
        receiver.Reactor.Pump();
        delivered.AddRange(receiver.DrainPayloads());

        delivered.Should().ContainSingle("M1 filled the gap, and M2 behind it did not fit the read share in the same Frame");
        receiver.Reactor.CountersOf(receiverPeer).ReadShareExhaustions.Should().BeGreaterThan(
            exhaustionsBefore, "the arena refused M2 for the read share");

        // Act — the receiver alone, with no new datagram from the sender, so M2 can only come from its queue.
        receiver.Reactor.Pump();
        delivered.AddRange(receiver.DrainPayloads());

        // Assert
        byte[][] expectedPayloads = [first, second];
        delivered.Should().Equal(
            expectedPayloads,
            (actual, expected) => actual.SequenceEqual(expected),
            "a held ReliableOrdered Message the arena refused stays first in line and is delivered on the next Pump");
    }
}
