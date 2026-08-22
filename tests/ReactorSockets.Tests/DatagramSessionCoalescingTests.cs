using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.7.5 (issue #232): the wire-observed proof that coalescing to one Peer per Pump is real,
/// not merely what <see cref="DatagramSessionReactor.Counters"/> reports about itself. A
/// <see cref="WireTap"/> (#228) counts arrivals the way a consumer's syscalls actually would, and
/// <see cref="DatagramDecoder"/> (#229) reads the records inside the one arrival independently of the
/// production codec that wrote them. <see cref="DatagramSessionPumpFlushTests"/> already proved the
/// same facts from the sender's own counters; this ticket proves them from outside.
/// </summary>
public class DatagramSessionCoalescingTests
{
    [Fact]
    public void Pump_AfterFourSendsToOnePeer_ArrivesAtTheTapAsOneDatagramDecodingToFourRecordsInSendOrder()
    {
        // Arrange — retentionBytes: 4800 gives the ReliableOrdered Space room for all four records
        // (1200 bytes is the default fragment size, so 4 × 1200 covers four in-flight records rather
        // than the default 4096, which only fits three and would silently retire the fourth Send).
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, retentionBytes: 1200 * 4);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        int arrivalsBeforeSends = tap.CountInDirection(TapDirection.DiallerToAccepter);
        byte[][] messages = [[1], [2, 2], [3, 3, 3], [4, 4, 4, 4]];
        foreach (byte[] message in messages)
        {
            dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, message).Should().Be(
                SendOutcome.Sent, "the retention pool is sized for all four records");
        }

        // Act — one round: the dialler's single Pump flushes all four Sends onto the wire in one Datagram.
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Assert
        int arrivalsAfterSends = tap.CountInDirection(TapDirection.DiallerToAccepter);
        (arrivalsAfterSends - arrivalsBeforeSends).Should().Be(
            1, "four Sends between two Pumps must coalesce into exactly one arrival at the tap");

        byte[] onlyArrival = tap.PayloadsInDirection(TapDirection.DiallerToAccepter).Last();
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(onlyArrival);
        decoded.IsSuccess.Should().BeTrue(decoded.Error);
        decoded.Datagram!.Records.Select(r => r.Payload).Should().Equal(
            messages, (actual, expected) => actual.SequenceEqual(expected),
            "the one arrival must decode to all four records, in the order they were Sent");
    }

    [Fact]
    public void Send_WithNoFollowingPump_ProducesNoArrivalAtTheTap()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        int arrivalsBeforeSend = tap.CountInDirection(TapDirection.DiallerToAccepter);

        // Act — Send queues the record, but nothing pumps the dialler's Reactor to flush it.
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [1, 2, 3]);
        tap.Pump();
        accepter.Reactor.Pump();
        tap.Pump();

        // Assert
        tap.CountInDirection(TapDirection.DiallerToAccepter).Should().Be(
            arrivalsBeforeSend, "nothing reaches the wire before the Pump that flushes it");
    }

    [Fact]
    public void Pump_WithNothingSentSinceTheLastOne_ProducesNoArrivalAtTheTap()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [1, 2, 3]);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);
        int arrivalsAfterFirstFlush = tap.CountInDirection(TapDirection.DiallerToAccepter);

        // Act — a second round with nothing Sent in between.
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Assert
        tap.CountInDirection(TapDirection.DiallerToAccepter).Should().Be(
            arrivalsAfterFirstFlush, "nothing was Sent since the last Pump, so this one flushes nothing");
    }

    [Fact]
    public void Reactor_PumpedOnceAfterTheSingleArrival_DrainsOneMessagePerRecordInSendOrder()
    {
        // Arrange — see the retentionBytes note above: four ReliableOrdered records need four slots.
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, retentionBytes: 1200 * 4);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        byte[][] messages = [[1], [2, 2], [3, 3, 3], [4, 4, 4, 4]];
        foreach (byte[] message in messages)
        {
            dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, message).Should().Be(SendOutcome.Sent);
        }

        // Act — the dialler flushes the four Sends as one Datagram, the tap relays it, and the accepter
        // drains it with the single Pump inside this one round.
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Assert
        List<byte[]> received = accepter.DrainPayloads();
        received.Should().Equal(
            messages, (actual, expected) => actual.SequenceEqual(expected),
            "the far end drains exactly one Message per record, in the order the records arrived");
    }
}
