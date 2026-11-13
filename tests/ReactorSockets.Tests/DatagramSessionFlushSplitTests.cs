using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A record queued past what one data Datagram can hold waits for the next Pump instead of
/// being dropped, and the split preserves Send order within a Space and the per-Peer Datagram count of one
/// Pump. Every assertion below reads only the four things the public seam allows (tests/CLAUDE.md):
/// the value <see cref="DatagramSessionReactor.Send"/> and <see cref="DatagramSessionReactor.Pump"/>
/// leave observable through Events, <see cref="DatagramSessionReactor.Counters"/> and
/// <see cref="DatagramSessionReactor.CountersOf"/>.
/// </summary>
public class DatagramSessionFlushSplitTests
{
    /// <summary>
    /// <see cref="DatagramSessionOptions.FragmentSize"/> and <see cref="DatagramSessionOptions.MaxMessageSize"/>
    /// set so one Message occupies exactly this many bytes on the wire (7-byte record header +
    /// payload). A data Datagram holds one of them (30 000 + 31 bytes), and one Pump writes at most
    /// 65 507 / 30 031 = 2 such Datagrams per Peer, so three Sends reach the boundary.
    /// </summary>
    private const int PayloadSize = 30_000;

    /// <summary>
    /// A single-Peer Reactor, so <see cref="Core.FrameArena.ReadShare"/> is the whole
    /// <see cref="ReactorOptions.ArenaSize"/> rather than a capacity-divided slice of it: with
    /// <paramref name="bindLoopback"/>'s default capacity of more than one Peer, the frame arena's
    /// escape for a share-holder's very first reservation of a Frame would let only one 30 000-byte
    /// Message through per Pump even though the receive window admitted more, which is an arena-sizing
    /// artefact these tests are not about. <see cref="ReactorOptions.ArenaSize"/> is set to hold every
    /// Message these tests deliver inside a single Frame.
    /// </summary>
    private static DatagramSessionReactorHarness NewHarness(bool bindLoopback = true) => new(
        bindLoopback: bindLoopback,
        capacity: 1,
        maxMessageSize: PayloadSize,
        fragmentSize: PayloadSize,
        retentionBytes: PayloadSize * 4,
        arenaSize: PayloadSize * 4);

    private static byte[] Payload(byte marker)
    {
        byte[] payload = new byte[PayloadSize];
        payload[0] = marker;
        return payload;
    }

    [Fact]
    public void Pump_WithMoreReliableSendsThanFitInOneDatagram_DeliversAllOfThemAcrossPumpsInSendOrder()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = NewHarness(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = NewHarness();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);

        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, Payload(1)).Should().Be(SendOutcome.Sent);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, Payload(2)).Should().Be(SendOutcome.Sent);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, Payload(3)).Should().Be(SendOutcome.Sent);

        // Act — the first Pump can only fit two of the three 30 007-byte records in one Datagram.
        sender.Reactor.Pump();
        List<byte[]> firstFlush = [];
        receiver.PumpUntil(() =>
        {
            firstFlush.AddRange(receiver.DrainPayloads());
            return firstFlush.Count > 0;
        });

        // Assert — the third record was not dropped: a second Pump on the sender carries it across.
        sender.Reactor.Pump();
        List<byte[]> secondFlush = [];
        receiver.PumpUntil(() =>
        {
            secondFlush.AddRange(receiver.DrainPayloads());
            return secondFlush.Count > 0;
        });

        firstFlush.Should().HaveCount(2, "only two 30 007-byte records fit in one Datagram");
        secondFlush.Should().HaveCount(1, "the third record was carried to the next Pump rather than dropped");
        List<byte[]> all = [.. firstFlush, .. secondFlush];
        all.Select(p => p[0]).Should().Equal([1, 2, 3], "the far end admits this Space's sequences in the order they were Sent");
    }

    [Fact]
    public void Pump_WhileAPeersQueueDrainsAcrossPumps_SendsAtMostTheUdpMaximumsWorthOfDatagramsEachPump()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = NewHarness(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = NewHarness();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);

        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, Payload(1)).Should().Be(SendOutcome.Sent);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, Payload(2)).Should().Be(SendOutcome.Sent);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, Payload(3)).Should().Be(SendOutcome.Sent);

        // Act
        ulong beforeFirstPump = sender.Reactor.Counters.DatagramsSent;
        sender.Reactor.Pump();
        ulong afterFirstPump = sender.Reactor.Counters.DatagramsSent;

        ulong beforeSecondPump = afterFirstPump;
        sender.Reactor.Pump();
        ulong afterSecondPump = sender.Reactor.Counters.DatagramsSent;

        ulong beforeThirdPump = afterSecondPump;
        sender.Reactor.Pump();
        ulong afterThirdPump = sender.Reactor.Counters.DatagramsSent;

        // Assert
        (afterFirstPump - beforeFirstPump).Should().Be(2UL, "one Pump sends at most 65 507 / 30 031 = 2 data Datagrams for this Peer, one record each");
        (afterSecondPump - beforeSecondPump).Should().Be(1UL, "the second Pump sends the remainder as exactly one more data Datagram");
        (afterThirdPump - beforeThirdPump).Should().Be(0UL, "the queue drained on the second Pump, so the third sends nothing");
    }

    [Fact]
    public void Pump_WhenAPeersQueueEmptiesCompletelyOnAFlush_SendsNoHeaderOnlyDatagramNextPump()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = NewHarness(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = NewHarness();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);

        // Two 30 007-byte records fit in one Datagram exactly (60 014 of 65 487 available bytes) —
        // this Peer's queue empties completely on the flush that packs them.
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, Payload(1)).Should().Be(SendOutcome.Sent);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, Payload(2)).Should().Be(SendOutcome.Sent);

        sender.Reactor.Pump();
        ulong datagramsSentAfterDraining = sender.Reactor.Counters.DatagramsSent;

        // Act — a second Pump with nothing left queued.
        sender.Reactor.Pump();

        // Assert
        sender.Reactor.Counters.DatagramsSent.Should().Be(
            datagramsSentAfterDraining, "the queue emptied exactly on the previous flush, so this Pump sends no header-only Datagram");
    }

    [Fact]
    public void Pump_WhenARecordDoesNotFit_ItIsNotArmedForRetransmitUntilTheEndThatActuallySendsIt()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = NewHarness(bindLoopback: false);
        using DatagramSessionReactorHarness receiver = NewHarness();
        PeerId peer = sender.Reactor.Connect(receiver.Endpoint);
        (var _, var receiverEvents) = DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);
        PeerId receiverPeer = receiverEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

        // One record fits and is Sent this Pump; the third does not fit and stays queued, unsent.
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, Payload(1)).Should().Be(SendOutcome.Sent);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, Payload(2)).Should().Be(SendOutcome.Sent);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, Payload(3)).Should().Be(SendOutcome.Sent);
        sender.Reactor.Pump();

        List<byte[]> received = [];
        receiver.Reactor.Pump();
        received.AddRange(receiver.DrainPayloads());

        // Act — advance the clock well past the Linger (the effective RTO before any round trip has
        // been observed, per RetransmissionTimer.EffectiveTimeout) and Pump the sender several more
        // times, without ever letting the receiver's ack reach it (the receiver never Sends, so it
        // never flushes one back). If the third record had been armed in the first Pump — before it
        // was ever actually sent — its retransmit deadline would also have expired here, and it would
        // be queued for retransmission a second time alongside its own ordinary send, reaching the far
        // end twice under the same sequence.
        sender.TimeProvider.Advance(TimeSpan.FromSeconds(5));
        for (int i = 0; i < 6; i++)
        {
            sender.Reactor.Pump();
            receiver.Reactor.Pump();
            received.AddRange(receiver.DrainPayloads());
        }

        // Assert
        received.Select(p => p[0]).Should().Equal([1, 2, 3], "every record arrives exactly once, in order, never duplicated");
        receiver.Reactor.CountersOf(receiverPeer).DuplicatePackets.Should().Be(
            0UL, "the record that did not fit was armed only once it was actually sent, so it is never queued for retransmission twice");
    }
}
