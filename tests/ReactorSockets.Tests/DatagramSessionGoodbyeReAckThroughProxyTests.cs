using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Review R2 on PR #409 (parent #57, ADR 0017 second amendment): when the receiver's final ack of a
/// Goodbye is lost, the sender's resent Goodbye names a Session the receiver has already freed. The
/// receiver remembers the closed Session for one effective Linger and answers such a datagram with one
/// header-only re-ack, so the sender ends <see cref="DisconnectReason.LocalClose"/> instead of
/// <see cref="DisconnectReason.LingerExpired"/>.
/// </summary>
/// <remarks>Every Pump is drained.</remarks>
public class DatagramSessionGoodbyeReAckThroughProxyTests
{
    private const byte ControlBit = 0b0000_1000;
    private const int HeaderSize = 20;

    private static readonly TimeSpan _linger = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan _step = TimeSpan.FromMilliseconds(100);

    private static bool CarriesAGoodbye(byte[] datagram)
    {
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(datagram);
        return decoded.IsSuccess
            && decoded.Datagram!.Records.Any(r => r.Mode == DeliveryMode.ReliableOrdered && (r.Flags & ControlBit) != 0);
    }

    /// <summary>One connected pair through a proxy, with the sender's Goodbye delivered and the receiver's final ack dropped.</summary>
    private sealed class Rig : IDisposable
    {
        public DatagramSessionReactorHarness Sender { get; } = new(bindLoopback: false, linger: _linger);

        public DatagramSessionReactorHarness Receiver { get; }

        public LossyProxy Proxy { get; }

        public PeerId SenderPeer { get; }

        public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> SenderEvents { get; } = [];

        public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> ReceiverEvents { get; } = [];

        public Rig()
        {
            Receiver = new(linger: _linger, timeProvider: Sender.TimeProvider);
            Proxy = new(Receiver.Endpoint, Sender.TimeProvider);
            SenderPeer = Sender.Reactor.Connect(Proxy.PublicEndPoint);
            PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(Sender, Proxy, Receiver);

            // One clean round trip seeds the estimator. Unseeded, the RTO resolves to the Linger itself.
            Sender.Reactor.Send(SenderPeer, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Sent);
            Sender.Reactor.Pump();
            Proxy.Pump();
            Receiver.Reactor.Pump();
            Receiver.DrainPayloads();
            Sender.TimeProvider.Advance(TimeSpan.FromMilliseconds(20));
            Receiver.Reactor.Send(receiverPeer, DeliveryMode.Unreliable, [9]).Should().Be(SendOutcome.Sent);
            Receiver.Reactor.Pump();
            Proxy.Pump();
            Sender.Reactor.Pump();
            Sender.DrainPayloads();
        }

        /// <summary>Closes the sender and delivers its Goodbye, dropping the receiver's final ack datagram.</summary>
        public void CloseSenderAndDropTheFinalAck()
        {
            Sender.Reactor.Close(SenderPeer);
            Sender.Reactor.Pump();
            SenderEvents.AddRange(Sender.DrainToList());
            Proxy.Pump();
            Proxy.DropNext(RelayDirection.UpstreamToClient);
            Receiver.Reactor.Pump();
            ReceiverEvents.AddRange(Receiver.DrainToList());
            Proxy.Pump();
            Sender.Reactor.Pump();
            SenderEvents.AddRange(Sender.DrainToList());
        }

        /// <summary>One relayed round: sender, proxy, receiver, proxy, sender, each drained.</summary>
        public void Round()
        {
            Sender.Reactor.Pump();
            SenderEvents.AddRange(Sender.DrainToList());
            Proxy.Pump();
            Receiver.Reactor.Pump();
            ReceiverEvents.AddRange(Receiver.DrainToList());
            Proxy.Pump();
            Sender.Reactor.Pump();
            SenderEvents.AddRange(Sender.DrainToList());
        }

        public void Dispose()
        {
            Proxy.Dispose();
            Receiver.Dispose();
            Sender.Dispose();
        }
    }

    [Fact]
    public void Pump_ReceiversFinalAckDropped_ResentGoodbyeIsReAckedAndBothEndsCloseCleanlyInsideLinger()
    {
        // Arrange
        using Rig rig = new();
        rig.CloseSenderAndDropTheFinalAck();
        rig.ReceiverEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.PeerGoodbye);
        rig.SenderEvents.Should().BeEmpty("the ack that would close the sender was dropped");

        // Act — the sender's RTO resends the Goodbye to the freed Session.
        TimeSpan elapsed = TimeSpan.Zero;
        while (elapsed < _linger / 2 && !rig.SenderEvents.Any(e => e.Kind == SessionEventKind.PeerClosed))
        {
            rig.Sender.TimeProvider.Advance(_step);
            elapsed += _step;
            rig.Round();
        }

        rig.Round();

        // Assert
        rig.SenderEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.LocalClose);
        elapsed.Should().BeLessThan(_linger, "the re-ack arrived well before Linger");
        rig.ReceiverEvents.Should().ContainSingle().Which.Should().Match<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)>(
            e => e.Kind == SessionEventKind.PeerClosed && e.Reason == DisconnectReason.PeerGoodbye,
            "the receiver raised nothing but its one PeerClosed(PeerGoodbye)");
    }

    [Fact]
    public void Pump_ReAckAnswersAResentGoodbye_WithOneHeaderOnlyDatagramNoLargerThanTheTrigger()
    {
        // Arrange
        using Rig rig = new();
        rig.CloseSenderAndDropTheFinalAck();
        int acksBefore = rig.Proxy.Captures.Count(c => c.Direction == RelayDirection.UpstreamToClient);

        // Act
        TimeSpan elapsed = TimeSpan.Zero;
        while (elapsed < _linger / 2 && !rig.SenderEvents.Any(e => e.Kind == SessionEventKind.PeerClosed))
        {
            rig.Sender.TimeProvider.Advance(_step);
            elapsed += _step;
            rig.Round();
        }

        // Assert
        byte[] resent = rig.Proxy.Captures
            .Last(c => c.Direction == RelayDirection.ClientToUpstream && CarriesAGoodbye(c.Payload)).Payload;
        List<RelayCapture> replies = rig.Proxy.Captures
            .Where(c => c.Direction == RelayDirection.UpstreamToClient).Skip(acksBefore).ToList();
        replies.Should().ContainSingle("one reply per triggering datagram");
        replies[0].Payload.Length.Should().Be(HeaderSize, "the reply is the header alone");
        replies[0].Payload.Length.Should().BeLessThanOrEqualTo(resent.Length);
    }

    [Fact]
    public void Pump_ClosedSessionNamedFromADifferentAddress_GetsNoReply()
    {
        // Arrange
        using Rig rig = new();
        rig.CloseSenderAndDropTheFinalAck();
        byte[] goodbye = rig.Proxy.Captures.First(c => c.Direction == RelayDirection.ClientToUpstream && CarriesAGoodbye(c.Payload)).Payload;
        int acksBefore = rig.Proxy.Captures.Count(c => c.Direction == RelayDirection.UpstreamToClient);
        using UdpClient stranger = new(new IPEndPoint(IPAddress.Loopback, 0));

        // Act
        stranger.Send(goodbye, rig.Receiver.Endpoint);
        for (int i = 0; i < 20; i++)
        {
            rig.Receiver.Reactor.Pump();
            rig.ReceiverEvents.AddRange(rig.Receiver.DrainToList());
        }

        bool strangerReplied = stranger.Client.Poll(200_000, SelectMode.SelectRead);
        rig.Proxy.Pump();
        int acksAfter = rig.Proxy.Captures.Count(c => c.Direction == RelayDirection.UpstreamToClient);

        // Assert — and the same Session still re-acks the address it was established from.
        strangerReplied.Should().BeFalse("a datagram from another address must not draw a reply");
        acksAfter.Should().Be(acksBefore, "nor may it draw one toward the address the Session was established from");
        TimeSpan elapsed = TimeSpan.Zero;
        while (elapsed < _linger / 2 && !rig.SenderEvents.Any(e => e.Kind == SessionEventKind.PeerClosed))
        {
            rig.Sender.TimeProvider.Advance(_step);
            elapsed += _step;
            rig.Round();
        }

        rig.SenderEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.LocalClose);
    }

    [Fact]
    public void Pump_ResentGoodbyeArrivesAfterLinger_GetsNoReply()
    {
        // Arrange
        using Rig rig = new();
        rig.CloseSenderAndDropTheFinalAck();
        int acksBefore = rig.Proxy.Captures.Count(c => c.Direction == RelayDirection.UpstreamToClient);
        ulong retransmittedBefore = rig.Sender.Reactor.CountersOf(rig.SenderPeer).PacketsRetransmitted;

        // Act — hold the resend in the proxy until well past the receiver's Linger.
        rig.Proxy.DelayNext(RelayDirection.ClientToUpstream, _linger * 3);
        while (rig.Sender.Reactor.CountersOf(rig.SenderPeer).PacketsRetransmitted == retransmittedBefore)
        {
            rig.Sender.TimeProvider.Advance(_step);
            rig.Sender.Reactor.Pump();
            rig.SenderEvents.AddRange(rig.Sender.DrainToList());
            rig.Proxy.Pump();
        }

        rig.Sender.TimeProvider.Advance(_linger * 4);
        rig.Round();
        rig.Round();

        // Assert
        rig.Proxy.Captures.Count(c => c.Direction == RelayDirection.UpstreamToClient).Should().Be(acksBefore, "the record expired with the Linger");
        rig.SenderEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.LingerExpired);
        rig.ReceiverEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed);
    }
}
