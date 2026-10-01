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
    public void Pump_ClosingPeerWhoseGoodbyeIsAcked_RaisesExactlyOneLocalCloseOnlyAfterTheAck()
    {
        // Arrange
        (DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, WireTap tap, PeerId peer) = Connect();
        using (dialler)
        using (accepter)
        using (tap)
        {
            dialler.Reactor.Close(peer);
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents = [];

            // Act: the Goodbye leaves the dialler and reaches the accepter's socket, but nothing acks it yet
            dialler.Reactor.Pump();
            diallerEvents.AddRange(dialler.DrainToList());
            tap.Pump();
            dialler.Reactor.Pump();
            diallerEvents.AddRange(dialler.DrainToList());
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> beforeAck = [.. diallerEvents];

            accepter.Reactor.Pump();
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> accepterEvents = accepter.DrainToList();
            tap.Pump();
            dialler.Reactor.Pump();
            diallerEvents.AddRange(dialler.DrainToList());
            for (int i = 0; i < 3; i++)
            {
                dialler.Reactor.Pump();
                diallerEvents.AddRange(dialler.DrainToList());
            }

            // Assert
            beforeAck.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed, "the Goodbye is not acked yet");
            accepterEvents.Where(e => e.Kind == SessionEventKind.PeerClosed).Should().ContainSingle()
                .Which.Reason.Should().Be(DisconnectReason.PeerGoodbye);
            diallerEvents.Where(e => e.Kind == SessionEventKind.PeerClosed).Should().ContainSingle()
                .Which.Should().Be((SessionEventKind.PeerClosed, peer, DisconnectReason.LocalClose));
            Action counters = () => dialler.Reactor.CountersOf(peer);
            counters.Should().Throw<Exception>("the PeerId is dead");
            dialler.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Closed);
        }
    }

    [Fact]
    public void Pump_ReceiverThatWentThroughPeerClosing_AcksTheGoodbyeSoTheSenderClosesWithLocalClose()
    {
        // Arrange
        (DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, WireTap tap, PeerId peer) = Connect();
        using (dialler)
        using (accepter)
        using (tap)
        {
            dialler.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [1, 2, 3]).Should().Be(SendOutcome.Sent);
            dialler.Reactor.Close(peer);
            dialler.Reactor.Pump();
            tap.Pump();

            // Act
            accepter.Reactor.Pump();
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> firstAccepterPump = accepter.DrainToList();
            accepter.Reactor.Pump();
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> secondAccepterPump = accepter.DrainToList();
            tap.Pump();
            dialler.Reactor.Pump();
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents = dialler.DrainToList();

            // Assert
            firstAccepterPump.Select(e => e.Kind).Should().Contain(SessionEventKind.PeerClosing)
                .And.NotContain(SessionEventKind.PeerClosed);
            secondAccepterPump.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed && e.Reason == DisconnectReason.PeerGoodbye);
            diallerEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed && e.Reason == DisconnectReason.LocalClose);
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

    /// <summary>The datagram header (wire format, ADR 0009) and one unfragmented record header, in bytes.</summary>
    private const int DatagramHeaderBytes = 20;

    private const int RecordHeaderBytes = 7;

    /// <summary>
    /// The Goodbye writer refuses a destination shorter than its seven-byte record (issue #395), and the
    /// flush carries the Goodbye in the next datagram. The Message is sized so its datagram leaves only
    /// <paramref name="leftover"/> bytes, fewer than a Goodbye needs. The Message's near-maximum datagram
    /// is proven by the dialler's own byte and write counts, because whether a <see cref="WireTap"/>
    /// relays a datagram that close to the UDP maximum depends on the platform's loopback. The Goodbye's
    /// datagram is picked from the captures by its size and decoded.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(6)]
    public void Close_WhenTheGoodbyeDoesNotFitTheMessagesDatagram_CarriesTheGoodbyeAloneInTheNextOne(int leftover)
    {
        // Arrange: a Message that leaves fewer bytes than a Goodbye record needs in its datagram
        int payloadSize = RawReactor.MaxUdpDatagramSize - DatagramHeaderBytes - RecordHeaderBytes - leftover;
        int messageDatagramBytes = RawReactor.MaxUdpDatagramSize - leftover;
        const int goodbyeDatagramBytes = DatagramHeaderBytes + RecordHeaderBytes;
        DatagramSessionReactorHarness dialler = new(bindLoopback: false, maxMessageSize: payloadSize, fragmentSize: payloadSize, retentionBytes: payloadSize * 2, arenaSize: payloadSize * 4);
        DatagramSessionReactorHarness accepter = new(maxMessageSize: payloadSize, fragmentSize: payloadSize, retentionBytes: payloadSize * 2, arenaSize: payloadSize * 4);
        WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        using (dialler)
        using (accepter)
        using (tap)
        {
            PeerId peer = dialler.Reactor.Connect(tap.PublicEndPoint);
            DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);
            int capturesBefore = tap.CountInDirection(TapDirection.DiallerToAccepter);
            dialler.Reactor.Send(peer, DeliveryMode.ReliableOrdered, new byte[payloadSize]).Should().Be(SendOutcome.Sent);
            PeerCounters before = dialler.Reactor.CountersOf(peer);

            // Act
            dialler.Reactor.Close(peer);
            dialler.Reactor.Pump();
            dialler.Reactor.Pump();
            PumpDeadline deadline = PumpDeadline.Start();
            tap.Pump();
            while (!CapturesOfSize(tap, capturesBefore, goodbyeDatagramBytes).Any() && !deadline.Expired)
            {
                PumpDeadline.Yield();
                tap.Pump();
            }

            // Assert
            PeerCounters after = dialler.Reactor.CountersOf(peer);
            (after.SocketWrites - before.SocketWrites).Should().Be(2UL, "the Goodbye does not fit the Message's datagram, so it goes in the next one");
            (after.BytesSent - before.BytesSent).Should().Be(
                (ulong)(messageDatagramBytes + goodbyeDatagramBytes),
                "the first datagram holds only the Message and the second only the Goodbye");

            byte[] goodbyeDatagram = CapturesOfSize(tap, capturesBefore, goodbyeDatagramBytes).Should().ContainSingle().Subject;
            DatagramDecodeResult decoded = DatagramDecoder.TryDecode(goodbyeDatagram);
            decoded.IsSuccess.Should().BeTrue();
            DecodedPacketRecord goodbye = decoded.Datagram!.Records.Should().ContainSingle().Subject;
            goodbye.Mode.Should().Be(DeliveryMode.ReliableOrdered);
            (goodbye.Flags & ControlBit).Should().Be(ControlBit);
            goodbye.Length.Should().Be(0);
            goodbye.Sequence.Should().Be(1, "the Message took ReliableOrdered sequence 0 and the Goodbye the next");
        }
    }

    /// <summary>The dialler-to-accepter datagrams captured after the first <paramref name="skip"/> that are <paramref name="size"/> bytes long.</summary>
    private static IEnumerable<byte[]> CapturesOfSize(WireTap tap, int skip, int size) =>
        tap.PayloadsInDirection(TapDirection.DiallerToAccepter).Skip(skip).Where(payload => payload.Length == size);
}
