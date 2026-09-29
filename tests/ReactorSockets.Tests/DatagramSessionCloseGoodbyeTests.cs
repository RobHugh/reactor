using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #397 (parent #57): <see cref="DatagramSessionReactor.Close"/> on an Established Peer queues a
/// Goodbye, a zero-length control record in the Peer's <c>ReliableOrdered</c> Space, and puts the Peer in
/// Closing. The tests decode the bytes a <see cref="WireTap"/> captured off the dialler, so they read the
/// wire itself and not the far end's behaviour. They also cover the wire shape of the Goodbye record
/// (issues #395 and #396), which nothing could observe before a Reactor sent one.
/// </summary>
public class DatagramSessionCloseGoodbyeTests
{
    private const byte ControlBit = 0b0000_1000;

    private static (DatagramSessionReactorHarness Dialler, DatagramSessionReactorHarness Accepter, WireTap Tap, PeerId Peer) Connect()
    {
        DatagramSessionReactorHarness dialler = new(bindLoopback: false, retentionBytes: 16384);
        DatagramSessionReactorHarness accepter = new();
        WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId peer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);
        return (dialler, accepter, tap, peer);
    }

    private static List<DecodedPacketRecord> RecordsSince(WireTap tap, int capturesBefore)
    {
        List<DecodedPacketRecord> records = [];
        foreach (byte[] datagram in tap.PayloadsInDirection(TapDirection.DiallerToAccepter).Skip(capturesBefore))
        {
            DatagramDecodeResult decoded = DatagramDecoder.TryDecode(datagram);
            decoded.IsSuccess.Should().BeTrue();
            records.AddRange(decoded.Datagram!.Records);
        }

        return records;
    }

    [Fact]
    public void Close_AfterAReliableOrderedSend_PutsTheMessageThenAGoodbyeOnTheWireInOneDatagram()
    {
        // Arrange
        (DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, WireTap tap, PeerId peer) = Connect();
        using (dialler)
        using (accepter)
        using (tap)
        {
            int capturesBefore = tap.CountInDirection(TapDirection.DiallerToAccepter);
            dialler.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [1, 2, 3]).Should().Be(SendOutcome.Sent);

            // Act
            dialler.Reactor.Close(peer);
            dialler.Reactor.Pump();
            tap.Pump();

            // Assert
            tap.CountInDirection(TapDirection.DiallerToAccepter).Should().Be(capturesBefore + 1, "the Message and the Goodbye share one datagram");
            List<DecodedPacketRecord> records = RecordsSince(tap, capturesBefore);
            records.Should().HaveCount(2);

            records[0].Mode.Should().Be(DeliveryMode.ReliableOrdered);
            (records[0].Flags & ControlBit).Should().Be(0);
            records[0].Payload.Should().Equal(1, 2, 3);

            records[1].Mode.Should().Be(DeliveryMode.ReliableOrdered, "a Goodbye is in the ReliableOrdered Space");
            (records[1].Flags & ControlBit).Should().Be(ControlBit, "the control bit marks the record a Goodbye, not an empty Message");
            records[1].Length.Should().Be(0);
            records[1].Sequence.Should().Be(records[0].Sequence + 1, "the Goodbye takes the next ReliableOrdered sequence");
        }
    }

    [Fact]
    public void Send_ToAPeerAfterClose_ReturnsClosed()
    {
        // Arrange
        (DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, WireTap tap, PeerId peer) = Connect();
        using (dialler)
        using (accepter)
        using (tap)
        {
            dialler.Reactor.Close(peer);

            // Act
            SendOutcome reliable = dialler.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [1]);
            SendOutcome unreliable = dialler.Reactor.Send(peer, DeliveryMode.Unreliable, [1]);

            // Assert
            reliable.Should().Be(SendOutcome.Closed);
            unreliable.Should().Be(SendOutcome.Closed);
        }
    }

    [Fact]
    public void Pump_TheOneThatSendsTheGoodbye_RaisesNoPeerClosed()
    {
        // Arrange
        (DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, WireTap tap, PeerId peer) = Connect();
        using (dialler)
        using (accepter)
        using (tap)
        {
            dialler.Reactor.Close(peer);
            dialler.DrainToList().Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed, "Close itself raises no PeerClosed");

            // Act
            dialler.Reactor.Pump();
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = dialler.DrainToList();
            tap.Pump();

            // Assert
            events.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
            tap.PayloadsInDirection(TapDirection.DiallerToAccepter).Should().NotBeEmpty();
        }
    }

    [Fact]
    public void Close_CalledTwice_QueuesOnlyOneGoodbye()
    {
        // Arrange
        (DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, WireTap tap, PeerId peer) = Connect();
        using (dialler)
        using (accepter)
        using (tap)
        {
            int capturesBefore = tap.CountInDirection(TapDirection.DiallerToAccepter);

            // Act
            dialler.Reactor.Close(peer);
            dialler.Reactor.Close(peer);
            dialler.Reactor.Pump();
            tap.Pump();

            // Assert
            RecordsSince(tap, capturesBefore).Where(r => (r.Flags & ControlBit) != 0).Should().ContainSingle();
        }
    }
}
