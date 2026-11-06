using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A Closing sender closes with <see cref="DisconnectReason.LocalClose"/>
/// only once every <c>ReliableOrdered</c> record up to and including the Goodbye is acknowledged. Two
/// Messages then <see cref="DatagramSessionReactor.Close"/> cross a <see cref="LossyProxy"/> that drops the
/// first Message's datagram, so the receiver holds the second Message and the Goodbye behind the hole. The
/// receiver then sends the sender a datagram whose ack block selectively acknowledges the Goodbye with the
/// first Message still missing. The sender must stay Closing and resend the first Message, so the receiver
/// delivers both Messages and then raises <see cref="DisconnectReason.PeerGoodbye"/>.
/// </summary>
public class DatagramSessionGoodbyeAckedBehindAHoleThroughProxyTests
{
    private const int RetentionForSeveralInFlight = 16384;
    private const int WideReceiveWindow = 16;

    private static readonly TimeSpan _linger = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan _step = TimeSpan.FromMilliseconds(100);

    /// <summary>One Event in the order it was drained.</summary>
    private readonly record struct Observed(SessionEventKind Kind, DisconnectReason Reason, byte[] Payload);

    private static void Collect(DatagramSessionReactorHarness harness, List<Observed> observed)
    {
        foreach (SessionEvent evt in harness.Reactor.Drain())
        {
            observed.Add(new Observed(evt.Kind, evt.Reason, evt.Kind == SessionEventKind.MessageReceived ? evt.Payload.ToArray() : []));
        }
    }

    private static void RelayToReceiver(
        DatagramSessionReactorHarness sender, LossyProxy proxy, DatagramSessionReactorHarness receiver,
        List<Observed> senderObserved, List<Observed> receiverObserved)
    {
        sender.Reactor.Pump();
        Collect(sender, senderObserved);
        proxy.Pump();
        receiver.Reactor.Pump();
        Collect(receiver, receiverObserved);
    }

    [Fact]
    public void Pump_GoodbyeSelectivelyAckedWhileAnEarlierMessageIsMissing_StaysClosingUntilTheMessageIsAcked()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, retentionBytes: RetentionForSeveralInFlight, linger: _linger);
        using DatagramSessionReactorHarness receiver = new(receiveWindowFragments: WideReceiveWindow, linger: _linger, timeProvider: sender.TimeProvider);
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

        List<Observed> senderObserved = [];
        List<Observed> receiverObserved = [];
        ulong retransmittedBefore = sender.Reactor.CountersOf(peer).PacketsRetransmitted;

        // Act — M1's datagram is dropped, then M2 and the Goodbye each cross in a datagram of their own.
        proxy.DropNext(RelayDirection.ClientToUpstream);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [0xA0]).Should().Be(SendOutcome.Sent);
        RelayToReceiver(sender, proxy, receiver, senderObserved, receiverObserved);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [0xA1]).Should().Be(SendOutcome.Sent);
        RelayToReceiver(sender, proxy, receiver, senderObserved, receiverObserved);
        sender.Reactor.Close(peer);
        RelayToReceiver(sender, proxy, receiver, senderObserved, receiverObserved);

        // The receiver sends the sender a datagram whose ack block selectively acks M2 and the Goodbye, M1 still missing.
        receiver.Reactor.Send(receiverPeer, DeliveryMode.Unreliable, [9]).Should().Be(SendOutcome.Sent);
        receiver.Reactor.Pump();
        Collect(receiver, receiverObserved);
        proxy.Pump();
        sender.Reactor.Pump();
        Collect(sender, senderObserved);
        sender.Reactor.Pump();
        Collect(sender, senderObserved);

        // Assert (precondition) — the sender has not closed, and the receiver holds everything behind M1.
        senderObserved.Should().NotContain(o => o.Kind == SessionEventKind.PeerClosed, "M1 is still unacknowledged");
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [0xFF]).Should().Be(SendOutcome.Closed);
        receiverObserved.Should().BeEmpty("nothing is deliverable while M1 is missing");
        sender.Reactor.CountersOf(peer).PacketsRetransmitted.Should().Be(retransmittedBefore, "M1's deadline has not passed");

        // Time passes until M1 is resent, then the exchange runs to its end.
        ulong retransmitted = retransmittedBefore;
        TimeSpan elapsed = TimeSpan.Zero;
        while (elapsed < _linger / 2 && retransmitted == retransmittedBefore)
        {
            sender.TimeProvider.Advance(_step);
            elapsed += _step;
            sender.Reactor.Pump();
            Collect(sender, senderObserved);
            retransmitted = sender.Reactor.CountersOf(peer).PacketsRetransmitted;
        }

        for (int round = 0; round < 6; round++)
        {
            proxy.Pump();
            receiver.Reactor.Pump();
            Collect(receiver, receiverObserved);
            proxy.Pump();
            sender.Reactor.Pump();
            Collect(sender, senderObserved);
        }

        // Assert
        retransmitted.Should().BeGreaterThan(retransmittedBefore, "the sender kept retransmitting M1");
        receiverObserved.Take(2).Select(o => o.Kind).Should().OnlyContain(k => k == SessionEventKind.MessageReceived);
        byte[][] expectedPayloads = [[0xA0], [0xA1]];
        receiverObserved.Take(2).Select(o => o.Payload).Should().Equal(
            expectedPayloads, (actual, expected) => actual.SequenceEqual(expected), "in send order");
        receiverObserved.Skip(2).Select(o => o.Kind).Should().OnlyContain(
            k => k == SessionEventKind.PeerClosing || k == SessionEventKind.PeerClosed);
        receiverObserved.Single(o => o.Kind == SessionEventKind.PeerClosed).Reason.Should().Be(DisconnectReason.PeerGoodbye);
        senderObserved.Where(o => o.Kind == SessionEventKind.PeerClosed).Should().ContainSingle()
            .Which.Reason.Should().Be(DisconnectReason.LocalClose);
    }
}
