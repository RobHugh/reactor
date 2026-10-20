using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtasks 6.6.5 and 6.6.6 (issues #217, #219): an admitted record's payload is copied into the frame
/// arena, raises the receiving Peer's <see cref="PeerCounters.MessagesReceived"/>, and drains as a
/// <see cref="SessionEventKind.MessageReceived"/> carrying the bytes that were Sent. The mode used
/// throughout is <see cref="DeliveryMode.ReliableUnordered"/>, whose rule — deliver everything, in
/// whatever order it arrives — needs nothing this ticket does not build; the three other modes' own
/// rules are #220's, #221's and #222's.
/// </summary>
public class DatagramSessionMessageDeliveryTests
{
    [Fact]
    public void Pump_AReliableUnorderedMessageSentToAnEstablishedPeer_DrainsAtTheFarEndWithItsBytesIntact()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = new();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);

        byte[] message = [9, 8, 7, 6, 5];
        sender.Reactor.Send(peer, DeliveryMode.ReliableUnordered, message);

        // Act
        sender.Reactor.Pump();
        receiver.PumpUntil(() => receiver.Reactor.Counters.DatagramsReceived > 0);
        List<byte[]> payloads = receiver.DrainPayloads();

        // Assert
        payloads.Should().ContainSingle().Which.Should().Equal(
            message, "the bytes a consumer drains are the bytes the far end Sent");
    }

    [Fact]
    public void Pump_SeveralMessagesSentInOneFrame_AreAllDrainedExactlyOnce()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = new();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        (var _, var receiverEvents) = DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);
        PeerId receiverPeer = receiverEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

        byte[][] messages = [[1], [2, 2], [3, 3, 3]];
        foreach (byte[] message in messages)
        {
            sender.Reactor.Send(peer, DeliveryMode.ReliableUnordered, message);
        }

        // Act
        sender.Reactor.Pump();
        receiver.PumpUntil(() => receiver.Reactor.Counters.DatagramsReceived > 0);
        List<byte[]> payloads = receiver.DrainPayloads();

        // Assert
        payloads.Should().BeEquivalentTo(messages, "every Message Sent in the Frame is delivered, once each");
        receiver.Reactor.CountersOf(receiverPeer).MessagesReceived.Should().Be(
            (ulong)messages.Length, "the counter matches what the consumer drained");
    }

    [Fact]
    public void Pump_ARecordTheArenaHasNoRoomFor_IsNotCountedAsReceivedAndReportsArenaExhausted()
    {
        // Arrange — an arena that holds one maximum Message and no more, so the second Message to
        // arrive in the same Frame finds no room. Capacity 1 keeps the per-share fair split out of it:
        // the shortfall under test is the arena's own, not a share's.
        const int maxMessageSize = 512;
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, capacity: 1, maxMessageSize: maxMessageSize);
        using DatagramSessionReactorHarness receiver = new(capacity: 1, maxMessageSize: maxMessageSize, arenaSize: maxMessageSize);
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        (var _, var receiverEvents) = DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);
        PeerId receiverPeer = receiverEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;
        receiver.Sink.Received.Clear();

        byte[] message = new byte[maxMessageSize - 64];
        sender.Reactor.Send(peer, DeliveryMode.ReliableUnordered, message);
        sender.Reactor.Send(peer, DeliveryMode.ReliableUnordered, message);

        // Act — both records ride one Datagram, so both are classified in the same Frame and the
        // second one meets an arena the first has all but filled.
        sender.Reactor.Pump();
        receiver.PumpUntil(() => receiver.Reactor.Counters.DatagramsReceived > 0);
        List<byte[]> payloads = receiver.DrainPayloads();

        // Assert
        payloads.Should().ContainSingle("the arena had room for the first Message only");
        receiver.Reactor.CountersOf(receiverPeer).MessagesReceived.Should().Be(
            1UL, "a Message the arena refused is not counted as received");
        receiver.Sink.Received.Should().Contain(
            record => record.Id == DiagnosticId.ArenaExhausted,
            "an arena shortfall names itself, unlike a share's fair-split refusal");
    }

    [Fact]
    public void Pump_AReliableMessageTheArenaRefused_IsDeliveredOnALaterPump()
    {
        // Arrange — the same arena shortfall as the test above, in a reliable mode. The receiver leaves
        // the refused record unacknowledged, so the sender's retransmission must deliver it on a later
        // Pump.
        const int maxMessageSize = 512;
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, capacity: 1, maxMessageSize: maxMessageSize);
        using DatagramSessionReactorHarness receiver = new(capacity: 1, maxMessageSize: maxMessageSize, arenaSize: maxMessageSize);
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        (var _, var receiverEvents) = DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);
        PeerId receiverPeer = receiverEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

        byte[] message = new byte[maxMessageSize - 64];
        sender.Reactor.Send(peer, DeliveryMode.ReliableUnordered, message);
        sender.Reactor.Send(peer, DeliveryMode.ReliableUnordered, message);
        sender.Reactor.Pump();
        receiver.PumpUntil(() => receiver.Reactor.CountersOf(receiverPeer).MessagesReceived > 0);

        // Act — move only the sender's clock a whole Linger at a time, the ceiling on its retransmission
        // timeout, so every round resends whatever is still unacknowledged.
        for (int round = 0; round < 10 && receiver.Reactor.CountersOf(receiverPeer).MessagesReceived < 2; round++)
        {
            DatagramSessionReactorHarness.SettleThenAdvance(sender, sender.Options.Linger);
            DatagramSessionReactorHarness.PumpBothUntil(
                sender,
                receiver,
                () => receiver.Reactor.CountersOf(receiverPeer).MessagesReceived == 2,
                TimeSpan.FromMilliseconds(200));
        }

        // Assert
        receiver.Reactor.CountersOf(receiverPeer).MessagesReceived.Should().Be(
            2UL, "a reliable Message is always delivered, even one the arena once had no room for");
    }
}
