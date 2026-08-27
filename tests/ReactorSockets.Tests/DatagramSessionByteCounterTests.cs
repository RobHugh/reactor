using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.7.4 (issue #231): the datagram profile's per-Peer <see cref="PeerCounters.BytesSent"/>
/// and <see cref="PeerCounters.BytesReceived"/>. Every assertion below reads only what the public
/// seam allows — the value <see cref="DatagramSessionReactor.Send"/> and
/// <see cref="DatagramSessionReactor.Pump"/> leave observable through
/// <see cref="DatagramSessionReactor.CountersOf"/> and <see cref="DatagramSessionReactor.Counters"/> —
/// never a byte count computed from the protocol's own internal, untested-through constants (ADR
/// 0015). Where an exact figure is asserted, it comes from comparing two flushes against each other or
/// the sender's own figure against the receiver's, not from hard-coded header arithmetic.
/// </summary>
public class DatagramSessionByteCounterTests
{
    [Fact]
    public void CountersOf_AFreshlyConnectedPeer_ReadsZeroForBytesSentAndBytesReceived()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);

        PeerId diallerPeer = dialler.Reactor.Connect(accepter.Endpoint);
        (var _, var accepterEvents) =
            DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);

        PeerId accepterPeer = accepterEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

        // Act
        PeerCounters diallerCounters = dialler.Reactor.CountersOf(diallerPeer);
        PeerCounters accepterCounters = accepter.Reactor.CountersOf(accepterPeer);

        // Assert
        diallerCounters.BytesSent.Should().Be(0);
        diallerCounters.BytesReceived.Should().Be(0);
        accepterCounters.BytesSent.Should().Be(0);
        accepterCounters.BytesReceived.Should().Be(0);
    }

    [Fact]
    public void Pump_AfterSendingAnEmptyMessage_RaisesThatPeersBytesSentEvenThoughThePayloadIsEmpty()
    {
        // Arrange — an empty payload contributes nothing itself, so any rise in BytesSent can only be
        // the Datagram's own header: proof the header is counted, not just a record's payload.
        using DatagramSessionReactorHarness sender = new(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = new();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);

        ulong bytesSentBefore = sender.Reactor.CountersOf(peer).BytesSent;

        // Act
        sender.Reactor.Send(peer, DeliveryMode.Unreliable, []);
        sender.Reactor.Pump();

        // Assert
        (sender.Reactor.CountersOf(peer).BytesSent - bytesSentBefore).Should().BeGreaterThan(
            0UL, "the Datagram's header is counted even though the record it carries has no payload");
    }

    [Fact]
    public void Pump_WithALargerPayloadThanAPriorFlush_RaisesBytesSentByExactlyTheExtraPayloadBytes()
    {
        // Arrange — the fixed session header and each record's own fixed header are identical between
        // these two flushes, so the whole difference between them has to be the extra payload bytes:
        // proof the header amortises rather than being miscounted per byte sent.
        using DatagramSessionReactorHarness sender = new(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = new();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);

        ulong bytesSentBeforeEmptyFlush = sender.Reactor.CountersOf(peer).BytesSent;
        sender.Reactor.Send(peer, DeliveryMode.Unreliable, []);
        sender.Reactor.Pump();
        ulong emptyFlushBytes = sender.Reactor.CountersOf(peer).BytesSent - bytesSentBeforeEmptyFlush;

        // Act
        ulong bytesSentBeforeFiveByteFlush = sender.Reactor.CountersOf(peer).BytesSent;
        sender.Reactor.Send(peer, DeliveryMode.Unreliable, [1, 2, 3, 4, 5]);
        sender.Reactor.Pump();
        ulong fiveByteFlushBytes = sender.Reactor.CountersOf(peer).BytesSent - bytesSentBeforeFiveByteFlush;

        // Assert
        (fiveByteFlushBytes - emptyFlushBytes).Should().Be(
            5UL, "the two flushes' fixed overhead is identical, so only the five payload bytes differ");
    }

    [Fact]
    public void Pump_ADatagramFromAnEstablishedPeer_RaisesThatPeersBytesReceivedByExactlyWhatTheSenderSent()
    {
        // Arrange — UDP loopback delivers a Datagram whole, so what the receiver attributes to this
        // Peer has to equal exactly what the sender's own flush handed the transport for it.
        using DatagramSessionReactorHarness sender = new(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = new();
        PeerId senderPeer = sender.Reactor.Connect(receiver.Endpoint);
        (var _, var receiverEvents) =
            DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);
        PeerId receiverPeer = receiverEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

        ulong bytesSentBefore = sender.Reactor.CountersOf(senderPeer).BytesSent;
        ulong bytesReceivedBefore = receiver.Reactor.CountersOf(receiverPeer).BytesReceived;

        // Act
        sender.Reactor.Send(senderPeer, DeliveryMode.ReliableOrdered, [1, 2, 3, 4]);
        sender.Reactor.Pump();
        receiver.PumpUntil(() => receiver.Reactor.CountersOf(receiverPeer).BytesReceived > bytesReceivedBefore);

        // Assert
        ulong bytesSentDelta = sender.Reactor.CountersOf(senderPeer).BytesSent - bytesSentBefore;
        ulong bytesReceivedDelta = receiver.Reactor.CountersOf(receiverPeer).BytesReceived - bytesReceivedBefore;

        bytesReceivedDelta.Should().Be(
            bytesSentDelta, "the whole Datagram the sender handed the transport is what the receiver attributes to this Peer");
        bytesReceivedDelta.Should().BeGreaterThan(4UL, "the header is counted, not just the four payload bytes");
    }

    [Fact]
    public void Pump_ADatagramNamingAnUnknownSession_RaisesNoPeersBytesReceived()
    {
        // Arrange — a data Datagram naming a session id no Peer holds is discarded as malformed
        // (DatagramSessionReactor.ProcessDataDatagram) and lands on the Reactor-scope
        // UnknownSourceDiscards figure. It names no Peer, so it must charge no Peer's BytesReceived.
        using DatagramSessionReactorHarness accepter = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using HostilePeer stray = new();

        dialler.Reactor.Connect(accepter.Endpoint);
        (var _, var accepterEvents) =
            DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);
        PeerId accepterPeer = accepterEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

        ulong bytesReceivedBefore = accepter.Reactor.CountersOf(accepterPeer).BytesReceived;
        ulong unknownSourceDiscardsBefore = accepter.Reactor.Counters.UnknownSourceDiscards;

        // Act — a session id that names no Session (0 is reserved for control; every assigned id here
        // starts from 1), followed by an arbitrary header-shaped body.
        byte[] strayDatagram = new byte[24];
        BinaryPrimitives.WriteUInt32BigEndian(strayDatagram, 0xFFFF_FFFEU);
        stray.SendDatagram(accepter.Endpoint, strayDatagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDiscards > unknownSourceDiscardsBefore);

        // Assert
        accepter.Reactor.Counters.UnknownSourceDiscards.Should().BeGreaterThan(unknownSourceDiscardsBefore);
        accepter.Reactor.CountersOf(accepterPeer).BytesReceived.Should().Be(
            bytesReceivedBefore, "a Datagram naming no Peer charges no Peer's bytes received");
    }

    [Fact]
    public void Pump_AFlushObservedOnTheWire_RaisesBytesSentByTheWholeDatagramLengthHeaderIncluded()
    {
        // Arrange — a WireTap (#228) holds the exact bytes the flush handed the transport, so the
        // whole Datagram's length is an observed figure rather than header arithmetic this test
        // would otherwise have to mirror from the protocol's internal constants (ADR 0015).
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        ulong bytesSentBefore = dialler.Reactor.CountersOf(diallerPeer).BytesSent;

        // Act
        dialler.Reactor.Send(diallerPeer, DeliveryMode.Unreliable, [1, 2, 3, 4]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Assert
        byte[] flushed = tap.PayloadsInDirection(TapDirection.DiallerToAccepter).Last();
        ulong bytesSentDelta = dialler.Reactor.CountersOf(diallerPeer).BytesSent - bytesSentBefore;

        bytesSentDelta.Should().Be(
            (ulong)flushed.Length,
            "bytes sent counts the whole Datagram the flush handed the transport, header included");
        bytesSentDelta.Should().BeGreaterThan(
            4UL + 7UL,
            "the figure exceeds the record's own 7-byte header plus its four payload bytes, so the 20-byte session header is inside it");
    }

    [Fact]
    public void Pump_AnArrivalObservedOnTheWire_RaisesBytesReceivedByTheWholeDatagramLengthHeaderIncluded()
    {
        // Arrange — the same observed-length proof for the receive side: what the tap relayed is
        // exactly what the far end must attribute to the Peer it came from.
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        ulong bytesReceivedBefore = accepter.Reactor.CountersOf(accepterPeer).BytesReceived;

        // Act
        dialler.Reactor.Send(diallerPeer, DeliveryMode.Unreliable, [1, 2, 3, 4]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Assert
        byte[] relayed = tap.PayloadsInDirection(TapDirection.DiallerToAccepter).Last();
        ulong bytesReceivedDelta = accepter.Reactor.CountersOf(accepterPeer).BytesReceived - bytesReceivedBefore;

        bytesReceivedDelta.Should().Be(
            (ulong)relayed.Length,
            "bytes received counts the whole arriving Datagram, header included");
    }
}
