using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The evidence behind the head-of-line amplification paragraph in
/// <c>docs/delivery-modes.md</c> — the one beside the delivery-mode table, which says that a large
/// fragmented <see cref="DeliveryMode.ReliableOrdered"/> Message blocks every later Message in its
/// ordering domain until all of its Fragments arrive, and names
/// <see cref="DeliveryMode.ReliableUnordered"/> as the answer for bulk transfer. The single test below
/// is that paragraph's demonstration: while a large <c>ReliableOrdered</c> Message is one Fragment
/// short, a small <c>ReliableOrdered</c> Message sent behind it is received but not delivered, a
/// <c>ReliableUnordered</c> Message sent in the same stall is delivered straight away, and the held
/// Message is released only when the large one finally completes. If this test changes meaning, the
/// paragraph is what has to change with it.
/// </summary>
/// <remarks>
/// Real <c>Send</c> calls, not injected Fragment records: the paragraph is a claim about what a
/// consumer sees from <c>Send</c>, so the demonstration has to cut the Message the way <c>Send</c>
/// cuts it. The stall is opened by dropping the large Message's first Fragment through a
/// <see cref="LossyProxy"/> — scripted, never probabilistic — and closed by advancing the clock past
/// the retransmit deadline, so the stall lasts exactly as long as the whole large Message takes to
/// complete, which is the amplification the paragraph is about.
/// </remarks>
public class DatagramSessionHeadOfLineAmplificationTests
{
    private const int MaxUdpDatagramSize = 65_507;
    private const int DatagramHeaderSize = 20;
    private const int FragmentRecordSize = 11;

    /// <summary>
    /// One full Fragment record leaves under 12 spare bytes in a Datagram — less than even the smallest
    /// possible second record could need — so every Fragment rides a Datagram of its own and the drop
    /// below removes exactly one Fragment rather than a whole coalesced run of them (matches
    /// <see cref="DatagramSessionFragmentSurvivesInterruptionTests"/> and its own sizing note).
    /// </summary>
    private const int FragmentSize = MaxUdpDatagramSize - DatagramHeaderSize - FragmentRecordSize - 11;

    /// <summary>Enough pieces that the stall is plainly the whole Message completing rather than one Datagram arriving.</summary>
    private const int FragmentCount = 4;

    /// <summary>Far short of <see cref="FragmentSize"/>, so the last Fragment is a genuine partial remainder.</summary>
    private const int LastFragmentRemainder = 1_000;

    /// <summary>An exact multiple of <see cref="FragmentSize"/> plus one short remainder, cutting into exactly <see cref="FragmentCount"/> Fragments.</summary>
    private const int LargeMessageSize = (FragmentSize * (FragmentCount - 1)) + LastFragmentRemainder;

    /// <summary>
    /// One reliable Space's send-side retention: room for the large Message's Fragments and the small
    /// Message sent behind them. Retention is per Space per Peer, so the <c>ReliableUnordered</c> Space
    /// gets a budget of this size all to itself.
    /// </summary>
    private const int RetentionBytes = FragmentSize * (FragmentCount + 1);

    /// <summary>Wide enough to admit every Fragment of the large Message plus the sequences sent behind it while its first piece is missing.</summary>
    private const int ReceiveWindowFragments = FragmentCount * 2;

    /// <summary>Room for the assembled large Message and the small one released behind it in the same Frame.</summary>
    private const int ArenaSize = LargeMessageSize + FragmentSize;

    /// <summary>Comfortably past the RTO, whose ceiling is the harness's one-second Linger.</summary>
    private static readonly TimeSpan _pastTheRetransmitDeadline = TimeSpan.FromSeconds(3);

    /// <summary>
    /// The Message held behind the stalled one. Unfragmented — far below <see cref="FragmentSize"/> —
    /// but deliberately some kilobytes rather than a few bytes, so the rise in the accepter's
    /// <see cref="PeerCounters.BytesReceived"/> asserted below cannot be accounted for by the stall's
    /// own header-sized ack traffic and can only mean this Message itself arrived.
    /// </summary>
    private static readonly byte[] _smallOrderedMessage = Fill(new byte[4_000], 7);

    private static readonly byte[] _unorderedMessage = Fill(new byte[2], 5);

    [Fact]
    public void Pump_WhileALargeReliableOrderedMessageIsOneFragmentShort_ItBlocksTheOrderedMessageBehindItAndNoUnorderedTrafficAtAll()
    {
        // Arrange — a large ReliableOrdered Message whose first Fragment is dropped, so its Space's line
        // has a missing head, and every other Fragment of it has already arrived.
        using DatagramSessionReactorHarness dialler = BuildDialler();
        using DatagramSessionReactorHarness accepter = BuildAccepter(dialler);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        byte[] large = BuildLargeMessage();
        proxy.DropNext(RelayDirection.ClientToUpstream);
        Send(dialler, diallerPeer, DeliveryMode.ReliableOrdered, large);
        PumpRoundsCollectingPayloads(dialler, proxy, accepter, rounds: FragmentCount + 2).Should().BeEmpty(
            "the set is one Fragment short, so nothing of the large Message can be delivered");
        ulong bytesBeforeTheSmallMessages = accepter.Reactor.CountersOf(accepterPeer).BytesReceived;

        // Act — two small Messages into the middle of that stall, one under each reliable mode, then the
        // retransmit deadline passes so the missing Fragment is resent and the large Message completes.
        Send(dialler, diallerPeer, DeliveryMode.ReliableOrdered, _smallOrderedMessage);
        Send(dialler, diallerPeer, DeliveryMode.ReliableUnordered, _unorderedMessage);
        List<byte[]> duringTheStall = PumpRoundsCollectingPayloads(dialler, proxy, accepter, rounds: 6);
        PeerCounters atTheEndOfTheStall = accepter.Reactor.CountersOf(accepterPeer);

        dialler.TimeProvider.Advance(_pastTheRetransmitDeadline);
        List<byte[]> afterTheLargeMessageCompletes = PumpRoundsCollectingPayloads(dialler, proxy, accepter, rounds: 16);

        // Assert — during the stall only the ReliableUnordered Message is delivered, which is why the
        // paragraph names that mode as the answer for bulk transfer.
        duringTheStall.Should().ContainSingle("nothing in the ReliableOrdered Space can be delivered while the head of its line is missing")
            .Which.Should().Equal(_unorderedMessage);

        // The small ReliableOrdered Message was received during the stall and held, rather than lost: its
        // bytes reached the accepter, and its sequence was admitted rather than refused, yet it was not
        // among the Messages delivered.
        atTheEndOfTheStall.BytesReceived.Should().BeGreaterThanOrEqualTo(
            bytesBeforeTheSmallMessages + (ulong)_smallOrderedMessage.Length,
            "the held Message's own bytes reached the accepter during the stall — more of them than the stall's ack traffic could account for");
        atTheEndOfTheStall.OutOfWindowPackets.Should().Be(
            0UL, "the small Message's sequence fits the window; only its turn has not come");

        // And once the large Message completes, both drain in send order — the small Message waited out
        // the whole large Message, not one Datagram of it. That is the amplification.
        afterTheLargeMessageCompletes.Should().HaveCount(2);
        afterTheLargeMessageCompletes[0].Should().Equal(large, "the stalled Message completes first, being the head of the line");
        afterTheLargeMessageCompletes[1].Should().Equal(
            _smallOrderedMessage, "the Message behind it is released only once the whole large Message is delivered");
    }

    private static DatagramSessionReactorHarness BuildDialler() => new(
        capacity: 1,
        bindLoopback: false,
        maxMessageSize: LargeMessageSize,
        retentionBytes: RetentionBytes,
        fragmentSize: FragmentSize,
        arenaSize: ArenaSize,
        receiveWindowFragments: ReceiveWindowFragments);

    /// <summary>Built on <paramref name="dialler"/>'s own clock, so one Advance moves both ends of the scenario together.</summary>
    private static DatagramSessionReactorHarness BuildAccepter(DatagramSessionReactorHarness dialler) => new(
        capacity: 1,
        maxMessageSize: LargeMessageSize,
        retentionBytes: RetentionBytes,
        fragmentSize: FragmentSize,
        arenaSize: ArenaSize,
        receiveWindowFragments: ReceiveWindowFragments,
        timeProvider: dialler.TimeProvider);

    /// <summary>Fills <paramref name="message"/> with <paramref name="value"/> and returns it, so a field can be built inline.</summary>
    private static byte[] Fill(byte[] message, byte value)
    {
        Array.Fill(message, value);
        return message;
    }

    private static byte[] BuildLargeMessage()
    {
        byte[] message = new byte[LargeMessageSize];
        for (int i = 0; i < message.Length; i++)
        {
            message[i] = (byte)i;
        }

        return message;
    }

    private static void Send(DatagramSessionReactorHarness dialler, PeerId peer, DeliveryMode mode, byte[] payload) =>
        dialler.Reactor.Send(peer, mode, payload).Should().Be(
            SendOutcome.Sent, "retention was sized to hold everything this test Sends");

    private static List<byte[]> PumpRoundsCollectingPayloads(
        DatagramSessionReactorHarness dialler, LossyProxy proxy, DatagramSessionReactorHarness accepter, int rounds)
    {
        List<byte[]> delivered = [];
        for (int i = 0; i < rounds; i++)
        {
            DatagramSessionReactorHarness.PumpRoundThroughProxy(dialler, proxy, accepter);
            delivered.AddRange(accepter.DrainPayloads());
        }

        return delivered;
    }
}
