using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Review R3 on issue #57: <see cref="DatagramSessionReactor.Close"/> on a Peer whose <c>ReliableOrdered</c>
/// retention is full cannot retain the Goodbye at once. The Peer is Closing, so <see cref="DatagramSessionReactor.Send"/>
/// returns <see cref="SendOutcome.Closed"/>, and the Reactor retries the Goodbye every Pump until an ack
/// frees room. The remote's acks are withheld with a payload drop rule until the test lets them through, so
/// retention stays full for as long as the test wants.
/// </summary>
public class DatagramSessionCloseWithFullRetentionThroughProxyTests
{
    private const int MessageSize = 1000;
    private const int Rounds = 12;
    private const int WideReceiveWindow = 16;
    private const int RoomyArena = 65536;
    private const byte ControlBit = 0b0000_1000;

    /// <summary>One receiver-side Event in the order it was drained.</summary>
    private readonly record struct Observed(SessionEventKind Kind, DisconnectReason Reason, byte[] Payload);

    private static void Round(
        DatagramSessionReactorHarness sender,
        LossyProxy proxy,
        DatagramSessionReactorHarness receiver,
        List<(SessionEventKind Kind, DisconnectReason Reason)> senderEvents,
        List<Observed> receiverEvents)
    {
        sender.Reactor.Pump();
        foreach ((SessionEventKind kind, PeerId _, DisconnectReason reason) in sender.DrainToList())
        {
            if (kind != SessionEventKind.MessageReceived)
            {
                senderEvents.Add((kind, reason));
            }
        }

        proxy.Pump();
        receiver.Reactor.Pump();
        foreach (SessionEvent evt in receiver.Reactor.Drain())
        {
            receiverEvents.Add(new Observed(evt.Kind, evt.Reason, evt.Kind == SessionEventKind.MessageReceived ? evt.Payload.ToArray() : []));
        }

        proxy.Pump();
        PumpDeadline.Yield();
    }

    private static bool CarriesAGoodbye(byte[] datagram)
    {
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(datagram);
        return decoded.IsSuccess
            && decoded.Datagram!.Records.Any(r => r.Mode == DeliveryMode.ReliableOrdered && (r.Flags & ControlBit) != 0);
    }

    private static int GoodbyesForwarded(LossyProxy proxy) =>
        proxy.Forwarded.Count(f => f.Direction == RelayDirection.ClientToUpstream && CarriesAGoodbye(f.Payload));

    [Fact]
    public void Close_WhenReliableOrderedRetentionIsFull_SendsTheGoodbyeAfterTheQueuedMessagesOnceAnAckFreesRoom()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = new(receiveWindowFragments: WideReceiveWindow, arenaSize: RoomyArena, timeProvider: sender.TimeProvider);
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId senderPeer = sender.Reactor.Connect(proxy.PublicEndPoint);
        PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);
        List<(SessionEventKind Kind, DisconnectReason Reason)> senderEvents = [];
        List<Observed> receiverEvents = [];

        // Act — with every ack withheld, fill retention until a Send is refused, then Close.
        proxy.SetPayloadDropRule(RelayDirection.UpstreamToClient, _ => true);
        List<byte[]> accepted = [];
        while (true)
        {
            byte[] message = [.. Enumerable.Repeat((byte)accepted.Count, MessageSize)];
            SendOutcome outcome = sender.Reactor.Send(senderPeer, DeliveryMode.ReliableOrdered, message);
            if (outcome == SendOutcome.RetentionExhausted)
            {
                break;
            }

            outcome.Should().Be(SendOutcome.Sent);
            accepted.Add(message);
        }

        sender.Reactor.Close(senderPeer);
        SendOutcome afterClose = sender.Reactor.Send(senderPeer, DeliveryMode.ReliableOrdered, [0xEE]);
        for (int round = 0; round < Rounds; round++)
        {
            Round(sender, proxy, receiver, senderEvents, receiverEvents);
        }

        // Assert (precondition) — the Messages went out, no Goodbye could be retained, and the sender is not closed.
        accepted.Should().NotBeEmpty();
        afterClose.Should().Be(SendOutcome.Closed);
        GoodbyesForwarded(proxy).Should().Be(0, "retention is full, so the Goodbye cannot be queued yet");
        receiverEvents.Select(o => o.Kind).Should().OnlyContain(k => k == SessionEventKind.MessageReceived);
        receiverEvents.Should().HaveCount(accepted.Count);
        senderEvents.Should().BeEmpty("the Peer stays Closing until its Goodbye is acked");

        // Act — the receiver's next datagram carries the acks home, which frees retention.
        proxy.SetPayloadDropRule(RelayDirection.UpstreamToClient, null);
        receiver.Reactor.Send(receiverPeer, DeliveryMode.Unreliable, [0x01]).Should().Be(SendOutcome.Sent);
        for (int round = 0; round < Rounds; round++)
        {
            Round(sender, proxy, receiver, senderEvents, receiverEvents);
        }

        // Assert
        GoodbyesForwarded(proxy).Should().Be(1, "the deferred Goodbye is sent once room is free");
        receiverEvents.Take(accepted.Count).Select(o => o.Payload).Should().Equal(
            accepted, (actual, expected) => actual.SequenceEqual(expected), "the Messages arrive first, in send order");
        receiverEvents.Skip(accepted.Count).Select(o => (o.Kind, o.Reason)).Should().Equal(
            [(SessionEventKind.PeerClosed, DisconnectReason.PeerGoodbye)]);
        senderEvents.Should().Equal([(SessionEventKind.PeerClosed, DisconnectReason.LocalClose)]);
    }
}
