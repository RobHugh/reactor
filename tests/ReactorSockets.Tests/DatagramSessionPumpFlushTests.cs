using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A <see cref="DatagramSessionReactor.Pump"/> puts everything Sent to an
/// Established Peer since the last Pump on the wire as exactly one data Datagram, and the far end's
/// own parse loop accepts it. Coalescing proper — splitting a Pump's queue across more than one
/// Datagram once it stops fitting in one, the multi-Peer breadth cases, and wire-observer byte
/// assertions — stays #45's; every assertion below reads only the four things the public seam allows:
/// the value <see cref="DatagramSessionReactor.Send"/> and <see cref="DatagramSessionReactor.Pump"/>
/// leave observable through <see cref="DatagramSessionReactor.Counters"/> and
/// <see cref="DatagramSessionReactor.CountersOf"/>.
/// </summary>
public class DatagramSessionPumpFlushTests
{
    [Fact]
    public void Pump_AfterSendsToAnEstablishedPeer_SendsExactlyOneDataDatagram()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = new();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);

        ulong datagramsSentBeforeFlush = sender.Reactor.Counters.DatagramsSent;
        sender.Reactor.Send(peer, DeliveryMode.Unreliable, [1, 2, 3]);
        sender.Reactor.Send(peer, DeliveryMode.UnreliableSequenced, [4, 5, 6]);
        sender.Reactor.Send(peer, DeliveryMode.ReliableUnordered, [7, 8, 9]);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [10, 11, 12]);

        // Act
        sender.Reactor.Pump();

        // Assert
        (sender.Reactor.Counters.DatagramsSent - datagramsSentBeforeFlush).Should().Be(
            1UL, "every record queued since the last Pump leaves in exactly one data Datagram");
    }

    [Fact]
    public void Pump_WithRecordsSentInEveryMode_TheFarEndAcceptsWithNoMalformedDiscardAndClassifiesIntoItsSpaces()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = new();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        (var _, var receiverEvents) = DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);
        PeerId receiverPeer = receiverEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

        sender.Reactor.Send(peer, DeliveryMode.Unreliable, [1, 2, 3]);
        sender.Reactor.Send(peer, DeliveryMode.UnreliableSequenced, [4, 5, 6]);
        sender.Reactor.Send(peer, DeliveryMode.ReliableUnordered, [7, 8, 9]);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [10, 11, 12]);

        // Act
        sender.Reactor.Pump();
        receiver.PumpUntil(() => receiver.Reactor.Counters.DatagramsReceived > 0);

        // Assert
        receiver.Reactor.Counters.UnknownSourceDatagrams.Should().Be(0UL, "a well-formed Datagram is never discarded as malformed");
        PeerCounters receiverCounters = receiver.Reactor.CountersOf(receiverPeer);
        receiverCounters.DuplicatePackets.Should().Be(0UL, "every record above is a first arrival in its Space");
        receiverCounters.OutOfWindowPackets.Should().Be(0UL, "every record above is a first arrival in its Space");
    }

    [Fact]
    public void Send_WithNoFollowingPump_SendsNothing()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = new();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);

        ulong datagramsSentBeforeSend = sender.Reactor.Counters.DatagramsSent;

        // Act
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [1, 2, 3]);

        // Assert
        sender.Reactor.Counters.DatagramsSent.Should().Be(
            datagramsSentBeforeSend, "nothing reaches the wire before the Pump that flushes it");
    }

    [Fact]
    public void Pump_WithNothingSentSinceTheLastOne_SendsNoDataDatagram()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = new();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);

        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [1, 2, 3]);
        sender.Reactor.Pump();
        ulong datagramsSentAfterFirstFlush = sender.Reactor.Counters.DatagramsSent;

        // Act — a second Pump with nothing Sent in between.
        sender.Reactor.Pump();

        // Assert
        sender.Reactor.Counters.DatagramsSent.Should().Be(
            datagramsSentAfterFirstFlush, "nothing was Sent since the last Pump, so this one flushes nothing");
    }
}
