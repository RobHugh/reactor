using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #403 (parent #57): a Goodbye lost in transit is resent by the ordinary retransmission-timeout
/// path and delivered. The datagram carrying the first Goodbye transmission is dropped by a
/// <see cref="LossyProxy"/>, time then advances in steps that stay inside the Linger, and the receiver still
/// raises <see cref="DisconnectReason.PeerGoodbye"/> while the sender raises
/// <see cref="DisconnectReason.LocalClose"/>. The Goodbye is the only reliable record in flight, and the
/// send-idle interval is far out, so nothing else can move the retransmission counter.
/// </summary>
public class DatagramSessionGoodbyeRetransmitThroughProxyTests
{
    private const byte ControlBit = 0b0000_1000;

    private static readonly TimeSpan _linger = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan _step = TimeSpan.FromMilliseconds(100);

    private static bool CarriesAGoodbye(byte[] datagram)
    {
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(datagram);
        return decoded.IsSuccess
            && decoded.Datagram!.Records.Any(r => r.Mode == DeliveryMode.ReliableOrdered && (r.Flags & ControlBit) != 0);
    }

    [Fact]
    public void Pump_TheFirstGoodbyeDatagramDropped_IsResentByRtoAndClosesBothEnds()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, linger: _linger);
        using DatagramSessionReactorHarness receiver = new(linger: _linger, timeProvider: sender.TimeProvider);
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        // One clean round trip seeds the estimator. Unseeded, the RTO resolves to the Linger itself, which
        // would fire in the same Pump that expires the Linger since Close.
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

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> senderEvents = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> receiverEvents = [];
        ulong retransmittedBefore = sender.Reactor.CountersOf(peer).PacketsRetransmitted;

        // Act — Close with nothing else outstanding, and drop the datagram that carries the first Goodbye.
        sender.Reactor.Close(peer);
        proxy.DropNext(RelayDirection.ClientToUpstream);
        sender.Reactor.Pump();
        senderEvents.AddRange(sender.DrainToList());
        proxy.Pump();
        receiver.Reactor.Pump();
        receiverEvents.AddRange(receiver.DrainToList());

        int arrivedGoodbyes = proxy.Captures.Count(c => c.Direction == RelayDirection.ClientToUpstream && CarriesAGoodbye(c.Payload));
        int forwardedGoodbyes = proxy.Forwarded.Count(f => f.Direction == RelayDirection.ClientToUpstream && CarriesAGoodbye(f.Payload));
        receiverEvents.Should().BeEmpty("the only Goodbye transmission so far was dropped");

        // Advance in steps inside the Linger until the sender resends. The sender is pumped alone first so the
        // counter is read before the relayed resend can be acked and close the Peer.
        ulong retransmittedAfterResend = retransmittedBefore;
        TimeSpan elapsed = TimeSpan.Zero;
        while (elapsed < _linger / 2 && retransmittedAfterResend == retransmittedBefore)
        {
            sender.TimeProvider.Advance(_step);
            elapsed += _step;
            sender.Reactor.Pump();
            senderEvents.AddRange(sender.DrainToList());
            retransmittedAfterResend = sender.Reactor.CountersOf(peer).PacketsRetransmitted;
        }

        proxy.Pump();
        receiver.Reactor.Pump();
        receiverEvents.AddRange(receiver.DrainToList());
        proxy.Pump();
        sender.Reactor.Pump();
        senderEvents.AddRange(sender.DrainToList());
        senderEvents.AddRange(sender.PumpAndDrainAll(2));
        receiverEvents.AddRange(receiver.PumpAndDrainAll(2));

        // Assert
        arrivedGoodbyes.Should().Be(1, "the first Goodbye transmission reached the proxy");
        forwardedGoodbyes.Should().Be(0, "and the proxy dropped it");
        elapsed.Should().BeLessThan(_linger, "the resend fired well inside the Linger");
        (retransmittedAfterResend - retransmittedBefore).Should().Be(1UL, "the Goodbye was resent exactly once");
        proxy.Forwarded.Count(f => f.Direction == RelayDirection.ClientToUpstream && CarriesAGoodbye(f.Payload))
            .Should().Be(1, "the resend was the one Goodbye relayed to the receiver");
        receiverEvents.Where(e => e.Kind == SessionEventKind.PeerClosed).Should().ContainSingle()
            .Which.Reason.Should().Be(DisconnectReason.PeerGoodbye);
        senderEvents.Where(e => e.Kind == SessionEventKind.PeerClosed).Should().ContainSingle()
            .Which.Reason.Should().Be(DisconnectReason.LocalClose);
    }
}
