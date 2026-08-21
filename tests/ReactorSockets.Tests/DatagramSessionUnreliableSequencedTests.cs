using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.6.11 (issue #221): <see cref="DeliveryMode.UnreliableSequenced"/> delivers a Message only
/// if it is strictly newer than the highest already delivered in its Space. The promise is a sentence
/// with no exceptions — you never receive an older update after a newer one — so these tests assert
/// that sentence rather than the mechanism behind it.
/// </summary>
public class DatagramSessionUnreliableSequencedTests
{
    [Fact]
    public void Pump_AMessageReorderedToArriveAfterANewerOne_IsNotDelivered()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = new();
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        byte[] older = [1];
        byte[] newer = [2];

        // Act — the two Datagrams swap places on the wire, so the newer Message arrives first and the
        // older one arrives already superseded.
        proxy.ReorderNextTwo(RelayDirection.ClientToUpstream);
        sender.Reactor.Send(peer, DeliveryMode.UnreliableSequenced, older);
        sender.Reactor.Pump();
        sender.Reactor.Send(peer, DeliveryMode.UnreliableSequenced, newer);
        sender.Reactor.Pump();

        List<byte[]> delivered = PumpRoundsCollectingPayloads(sender, proxy, receiver, rounds: 8);

        // Assert
        delivered.Should().ContainSingle("the superseded Message is discarded, not delivered late")
            .Which.Should().Equal(newer, "only the newer Message is delivered");
        receiver.Reactor.CountersOf(receiverPeer).MessagesReceived.Should().Be(
            1UL, "a discarded stale Message is not counted as received");
    }

    [Fact]
    public void Pump_MessagesArrivingInOrder_AreAllDelivered()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = new();
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        byte[][] messages = [[1], [2], [3]];

        // Act — nothing is reordered, so each Message is strictly newer than the last delivered.
        foreach (byte[] message in messages)
        {
            sender.Reactor.Send(peer, DeliveryMode.UnreliableSequenced, message);
            sender.Reactor.Pump();
        }

        List<byte[]> delivered = PumpRoundsCollectingPayloads(sender, proxy, receiver, rounds: 8);

        // Assert
        delivered.Should().BeEquivalentTo(
            messages,
            options => options.WithStrictOrdering(),
            "every Message was newer than the one before it");
    }

    [Fact]
    public void Pump_ALongRunOfReorderedArrivals_DeliversAStrictlyIncreasingSequenceWithNoException()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = new();
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        // Act — every other pair swaps, so roughly half the arrivals are stale on arrival. Each round
        // drains as it goes: a Session Event lives for its Frame only, so a Pump that is not drained
        // before the next one loses what it delivered.
        const int messageCount = 12;
        List<byte[]> delivered = [];
        for (int i = 0; i < messageCount; i++)
        {
            if (i % 2 == 0)
            {
                proxy.ReorderNextTwo(RelayDirection.ClientToUpstream);
            }

            sender.Reactor.Send(peer, DeliveryMode.UnreliableSequenced, [(byte)i]);
            sender.Reactor.Pump();
            proxy.Pump();
            receiver.Reactor.Pump();
            delivered.AddRange(receiver.DrainPayloads());
            PumpDeadline.Yield();
        }

        delivered.AddRange(PumpRoundsCollectingPayloads(sender, proxy, receiver, rounds: 8));

        // Assert — the sentence with no exceptions: what a consumer sees only ever moves forward.
        List<byte> order = [.. delivered.Select(payload => payload.Single())];
        order.Should().NotBeEmpty("a reordered run still delivers whatever was newest on arrival");
        order.Should().BeInAscendingOrder("no older update is ever received after a newer one")
            .And.OnlyHaveUniqueItems("a Message equal to the last delivered is not strictly newer");
    }

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
