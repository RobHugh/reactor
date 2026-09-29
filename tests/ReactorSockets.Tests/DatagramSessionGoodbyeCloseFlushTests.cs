using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Review R4 on issue #57: a Peer closing on a received Goodbye sends its final ack-bearing datagram
/// through the ordinary once-per-Peer-per-Pump flush (ADR 0009), not as a second datagram beside it. The
/// tests read the accepter's datagrams off a <see cref="WireTap"/> in the Pump the Goodbye arrives.
/// </summary>
public class DatagramSessionGoodbyeCloseFlushTests
{
    private static (DatagramSessionReactorHarness Dialler, DatagramSessionReactorHarness Accepter, WireTap Tap, PeerId DiallerPeer, PeerId AccepterPeer) Connect()
    {
        DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        DatagramSessionReactorHarness accepter = new();
        WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);
        return (dialler, accepter, tap, diallerPeer, accepterPeer);
    }

    /// <summary>
    /// Pumps the tap several times with a yield between, so a second datagram sent in the same Pump has
    /// time to reach it. A single tap Pump can read only the first of two datagrams sent back to back.
    /// </summary>
    private static void Relay(WireTap tap)
    {
        for (int round = 0; round < 5; round++)
        {
            tap.Pump();
            PumpDeadline.Yield();
        }
    }

    [Fact]
    public void Pump_GoodbyeArrivesWhileARecordIsQueuedForThePeer_SendsExactlyOneDatagramToItAndClosesInThatPump()
    {
        // Arrange
        (DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, WireTap tap, PeerId diallerPeer, PeerId accepterPeer) = Connect();
        using (dialler)
        using (accepter)
        using (tap)
        {
            accepter.Reactor.Send(accepterPeer, DeliveryMode.Unreliable, [0xB0]).Should().Be(SendOutcome.Sent);
            dialler.Reactor.Close(diallerPeer);
            dialler.Reactor.Pump();
            Relay(tap);
            int sentBefore = tap.CountInDirection(TapDirection.AccepterToDialler);

            // Act
            accepter.Reactor.Pump();
            Relay(tap);
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = accepter.DrainToList();

            // Assert
            tap.CountInDirection(TapDirection.AccepterToDialler).Should().Be(sentBefore + 1, "the queued record and the final ack share the one flush datagram");
            DatagramDecodeResult decoded = DatagramDecoder.TryDecode(tap.PayloadsInDirection(TapDirection.AccepterToDialler).Last());
            decoded.IsSuccess.Should().BeTrue();
            decoded.Datagram!.Records.Should().ContainSingle().Which.Payload.Should().Equal(0xB0);
            events.Select(e => e.Kind).Should().Equal(SessionEventKind.PeerClosed);
            events[0].Reason.Should().Be(DisconnectReason.PeerGoodbye);
        }
    }

    [Fact]
    public void Pump_GoodbyeArrivesWithNothingQueuedForThePeer_SendsOneHeaderOnlyDatagramThatLetsTheSenderClose()
    {
        // Arrange
        (DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, WireTap tap, PeerId diallerPeer, PeerId accepterPeer) = Connect();
        using (dialler)
        using (accepter)
        using (tap)
        {
            dialler.Reactor.Close(diallerPeer);
            dialler.Reactor.Pump();
            Relay(tap);
            int sentBefore = tap.CountInDirection(TapDirection.AccepterToDialler);

            // Act
            accepter.Reactor.Pump();
            Relay(tap);
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> accepterEvents = accepter.DrainToList();
            dialler.Reactor.Pump();
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents = dialler.DrainToList();

            // Assert
            tap.CountInDirection(TapDirection.AccepterToDialler).Should().Be(sentBefore + 1);
            DatagramDecodeResult decoded = DatagramDecoder.TryDecode(tap.PayloadsInDirection(TapDirection.AccepterToDialler).Last());
            decoded.IsSuccess.Should().BeTrue();
            decoded.Datagram!.Records.Should().BeEmpty("nothing else was queued, so only the ack rides the datagram");
            accepterEvents.Select(e => e.Kind).Should().Equal(SessionEventKind.PeerClosed);
            diallerEvents.Select(e => (e.Kind, e.Reason)).Should().Equal((SessionEventKind.PeerClosed, DisconnectReason.LocalClose));
        }
    }
}
