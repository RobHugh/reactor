using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.4.9 (issue #199), the headline deliverable of issue #42: the whole adaptive timer proved
/// over a real Peer rather than over a <see cref="RetransmissionTimer"/> in isolation, through the
/// <see cref="DiagnosticId.RetransmissionTimerBackedOff"/> record each backoff emits. The parts of
/// subtask 6.4.7 (issue #197) and 6.4.8 (issue #198) that are only observable once a Peer's estimator
/// has actually been fed — that releasing an armed sequence samples it, that releasing a deferred one
/// does not, and that an expired sequence re-arms at the backed-off RTO — are asserted here too, since
/// the reported RTO is the one instrument that shows them.
/// </summary>
/// <remarks>
/// <para>
/// <b>Assert the mechanism, not the outcome.</b> ADR 0009 names the adaptive RTO as invisible in normal
/// operation and therefore the profile of code that rots: every assertion below is on the reported RTO
/// in milliseconds and the consecutive count, never on delivery eventually succeeding.
/// </para>
/// <para>
/// <b>The arithmetic these tests are counted against.</b> A single clean round-trip sample of
/// <see cref="_roundTrip"/> (20ms) seeds the estimator by RFC 6298's first-sample rule — smoothed 20ms,
/// variance 10ms — so its timeout is <c>20 + 4 x 10 = 60ms</c>, well inside the harness's one-second
/// Linger. Backing off doubles that and nothing else: 120, 240, 480, 960, then 1000 and 1000 as the
/// Linger ceiling saturates it. Those six figures are what the run below reads back, and they are the
/// mechanism — a backoff folded into the estimate, a missing ceiling, or a reset that decayed rather
/// than cleared would each produce a different sequence.
/// </para>
/// <para>
/// <b>The instrument.</b> A live <see cref="DatagramSessionReactor"/> whose one Peer is a hand-built
/// client over a bare <see cref="RawReactor"/>, the same instrument
/// <see cref="DatagramSessionHorizonTests"/> uses: the scenario needs acks that never arrive, and then
/// one ack naming a base of the test's own choosing, neither of which an ordinary Session produces.
/// </para>
/// </remarks>
public class DatagramSessionAdaptiveRtoTests
{
    private const int ControlDatagramSize = 256;
    private const uint ControlSession = 0;

    private const byte HelloType = 1;
    private const byte HelloWithCookieType = 3;
    private const int ControlBodyOffset = 5;
    private const int CookieLength = 32;
    private const int ResumeSecretLength = 16;

    private const uint ProtocolVersion = 1;
    private const uint DialerSessionId = 0xCAFE_BABE;
    private const uint DialerMaxMessageSize = 4096;
    private const ulong DialerSendIdleIntervalMilliseconds = 60_000;

    private const int DatagramHeaderSize = 20;
    private const int ReliableOrderedAckBlockOffset = 4;

    private const byte ModeUnreliable = 0b0000_0000;

    /// <summary>Retention sized for 64 records per reliable Space, so nothing is dropped for want of it.</summary>
    private const int RetentionForSixtyFourRecords = 1200 * 64;

    /// <summary>The span one ack block describes: its base plus the 32 sequences its bitfield covers.</summary>
    private const int SequencesInsideTheInitialHorizon = 33;

    /// <summary>Five more than the horizon covers, so exactly five sequences are held back unarmed.</summary>
    private const int SequencesSent = 38;

    /// <summary>
    /// The round trip every seeding ack in this file measures. Chosen so the seeded timeout — three
    /// times this, by the first-sample rule — sits well below the harness's one-second Linger, leaving
    /// room for four doublings before the ceiling takes over.
    /// </summary>
    private static readonly TimeSpan _roundTrip = TimeSpan.FromMilliseconds(20);

    /// <summary>The timeout <see cref="_roundTrip"/> seeds: smoothed 20ms plus four variances of 10ms.</summary>
    private static readonly TimeSpan _seededTimeout = TimeSpan.FromMilliseconds(60);

    /// <summary>The harness's Linger, which is the RTO's ceiling.</summary>
    private static readonly TimeSpan _linger = TimeSpan.FromSeconds(1);

    [Fact]
    public void Pump_ConsecutiveLossesWithNothingAcknowledged_EmitOneRecordEachWithARisingCountAndADoublingRtoThatSaturatesAtLinger()
    {
        // Arrange — one clean round trip seeds the estimator at a 60ms timeout, then one further
        // reliable sequence is armed and nothing ever acknowledges it.
        using DatagramSessionReactorHarness node = new(retentionBytes: RetentionForSixtyFourRecords);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);

        SeedTheEstimator(node, client, clientLink, sessionId, nodePeer);
        ArmOneReliableSequence(node, nodePeer);
        node.Sink.Received.Clear();

        // Act — each advance is exactly the deadline currently armed, so every Pump below expires the
        // one outstanding sequence and backs off once more.
        TimeSpan[] deadlines =
        [
            _seededTimeout,      // 60ms  -> backs off to 120
            _seededTimeout * 2,  // 120ms -> 240
            _seededTimeout * 4,  // 240ms -> 480
            _seededTimeout * 8,  // 480ms -> 960
            _seededTimeout * 16, // 960ms -> 1920, clamped to the one-second Linger
            _linger,             // 1000ms -> 3840, clamped again
        ];

        foreach (TimeSpan deadline in deadlines)
        {
            node.TimeProvider.Advance(deadline);
            node.Reactor.Pump();
        }

        // Assert — the doubling, the rising count, and the ceiling, read back as literal figures.
        List<(long Rto, long Count)> reported = BackedOffRecords(node, nodePeer);
        reported.Should().Equal(
            [(120, 1), (240, 2), (480, 3), (960, 4), (1000, 5), (1000, 6)],
            "each expiry doubles the 60ms estimate and raises the count by one, until the Linger ceiling saturates it");
        reported.Should().OnlyContain(
            entry => entry.Rto <= (long)_linger.TotalMilliseconds,
            "the RTO never exceeds Linger however many consecutive losses occur");
        node.Sink.Received
            .Where(record => record.Id == DiagnosticId.RetransmissionTimerBackedOff)
            .Should().OnlyContain(record => record.Peer == nodePeer && record.Level == DiagnosticLevel.Trace);
    }

    [Fact]
    public void Pump_AnAckAfterSeveralBackoffs_ResetsTheCountSoTheNextLossReportsOneAndTheRtoIsBackAtTheEstimate()
    {
        // Arrange — seed the estimator, then back a sequence off twice so there is a run to reset.
        using DatagramSessionReactorHarness node = new(retentionBytes: RetentionForSixtyFourRecords);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);

        SeedTheEstimator(node, client, clientLink, sessionId, nodePeer);
        ArmOneReliableSequence(node, nodePeer);

        node.TimeProvider.Advance(_seededTimeout);
        node.Reactor.Pump();
        node.TimeProvider.Advance(_seededTimeout * 2);
        node.Reactor.Pump();
        BackedOffRecords(node, nodePeer).Should().Equal(
            [(120, 1), (240, 2)], "two consecutive losses precede the reset");

        // Act — the twice-retransmitted sequence is acknowledged 200ms after its last arming, inside the
        // 240ms deadline now armed. Karn's rule must discard that 200ms as ambiguous, while the delivery
        // itself still resets the backoff.
        node.TimeProvider.Advance(TimeSpan.FromMilliseconds(200));
        SendCraftedArrival(
            node,
            client,
            clientLink,
            BuildDatagram(sessionId, reliableOrderedAckBase: 2, reliableOrderedAckBits: 0, sequence: 10));
        SettleCadence(node);

        node.Sink.Received.Clear();
        ArmOneReliableSequence(node, nodePeer);
        node.TimeProvider.Advance(_seededTimeout);
        node.Reactor.Pump();

        // Assert — count back at one, and the RTO back at twice the untouched 60ms estimate. Had the
        // ambiguous 200ms sample been folded in, the estimate would have moved to roughly 250ms and this
        // would read 505 rather than 120; had the reset decayed rather than cleared, the count would not
        // be one.
        BackedOffRecords(node, nodePeer).Should().Equal(
            [(120, 1)],
            "the delivery reset the count in one step, and Karn's rule left the estimate where the clean sample put it");
    }

    [Fact]
    public void Pump_AnExpiredSequenceReArmedAtTheBackedOffRto_IsNotReportedAgainBeforeThatDeadlinePasses()
    {
        // Arrange — one expiry, which backs the RTO off to 120ms and re-arms the sequence at it.
        using DatagramSessionReactorHarness node = new(retentionBytes: RetentionForSixtyFourRecords);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);

        SeedTheEstimator(node, client, clientLink, sessionId, nodePeer);
        ArmOneReliableSequence(node, nodePeer);
        node.TimeProvider.Advance(_seededTimeout);
        node.Reactor.Pump();

        ulong retransmittedAfterTheFirstExpiry = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted;
        retransmittedAfterTheFirstExpiry.Should().Be(1, "exactly the one armed sequence expired");
        node.Sink.Received.Clear();

        // Act — three more Pumps with the clock standing still, then one 119ms short of the 120ms the
        // re-arm gave it.
        node.Reactor.Pump();
        node.Reactor.Pump();
        node.Reactor.Pump();
        node.TimeProvider.Advance(TimeSpan.FromMilliseconds(119));
        node.Reactor.Pump();

        ulong beforeTheDeadlinePasses = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted;
        List<(long Rto, long Count)> beforeTheDeadlinePassesRecords = BackedOffRecords(node, nodePeer);

        node.TimeProvider.Advance(TimeSpan.FromMilliseconds(2));
        node.Reactor.Pump();

        // Assert — nothing was reported a second time until the backed-off deadline actually passed, and
        // when it did the count went to two rather than starting again at one.
        beforeTheDeadlinePasses.Should().Be(
            retransmittedAfterTheFirstExpiry,
            "a re-armed sequence is not reported again before its new deadline passes");
        beforeTheDeadlinePassesRecords.Should().BeEmpty("no backoff happened, so no record was emitted");
        node.Reactor.CountersOf(nodePeer).PacketsRetransmitted.Should().Be(
            retransmittedAfterTheFirstExpiry + 1, "the sequence expired once more, and only once");
        BackedOffRecords(node, nodePeer).Should().Equal(
            [(240, 2)],
            "the re-armed deadline was the backed-off 120ms, and passing it backed the timer off again");
    }

    [Fact]
    public void Pump_AnAckReleasingArmedAndDeferredSequencesTogether_SamplesOnlyTheArmedOnes()
    {
        // Arrange — 38 outstanding ReliableOrdered sequences: 0 through 32 armed inside the initial
        // [0, 32] horizon, 33 through 37 outstanding with no deadline at all.
        using DatagramSessionReactorHarness node = new(retentionBytes: RetentionForSixtyFourRecords);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);

        SequencesSent.Should().BeGreaterThan(
            SequencesInsideTheInitialHorizon,
            "the scenario is only meaningful while some of the released sequences were deferred");

        for (int i = 0; i < SequencesSent; i++)
        {
            node.Reactor.Send(nodePeer, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Sent);
        }

        node.Reactor.Pump();

        // Act — one ack whose base is past every one of them releases all 38 at once, 20ms after the 33
        // armed ones were armed. The five deferred ones were never armed and so have no arming moment to
        // measure from; they must contribute nothing.
        node.TimeProvider.Advance(_roundTrip);
        SendCraftedArrival(
            node,
            client,
            clientLink,
            BuildDatagram(sessionId, reliableOrderedAckBase: SequencesSent, reliableOrderedAckBits: 0, sequence: 10));
        SettleCadence(node);

        node.Sink.Received.Clear();
        ArmOneReliableSequence(node, nodePeer);
        node.TimeProvider.Advance(TimeSpan.FromMilliseconds(21));
        node.Reactor.Pump();

        // Assert — 33 identical 20ms samples leave the smoothed value at 20ms and drive the variance to
        // almost nothing, so the timeout is 20ms and the first backoff reports 40. A deferred sequence
        // sampled from an arming that never happened would measure the whole life of the clock and push
        // this to the 1000ms ceiling instead.
        BackedOffRecords(node, nodePeer).Should().Equal(
            [(40, 1)],
            "only the 33 armed sequences fed the estimator, each with the same 20ms round trip");
    }

    [Fact]
    public void Pump_TheRtoChangingBetweenTwoArms_GivesThemDifferentDeadlinesAndLeavesTheEarlierOneWhereItWas()
    {
        // Arrange — two ReliableOrdered sequences armed in the same flush, while the estimator is still
        // unseeded and so the effective RTO is the one-second Linger. Both deadlines are a second away.
        using DatagramSessionReactorHarness node = new(retentionBytes: RetentionForSixtyFourRecords);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);

        node.Reactor.Send(nodePeer, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Sent);
        node.Reactor.Send(nodePeer, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Sent);
        node.Reactor.Pump();

        // Act — acknowledging only sequence 0, 20ms after it armed, drops the effective RTO from the
        // 1000ms Linger to the 60ms the sample seeds. Sequence 1 keeps the deadline it was armed with;
        // sequence 2, armed after the change, takes the new one.
        node.TimeProvider.Advance(_roundTrip);
        SendCraftedArrival(
            node,
            client,
            clientLink,
            BuildDatagram(sessionId, reliableOrderedAckBase: 1, reliableOrderedAckBits: 0, sequence: 9));
        SettleCadence(node);

        ArmOneReliableSequence(node, nodePeer);
        ulong retransmittedBefore = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted;

        node.TimeProvider.Advance(_seededTimeout);
        node.Reactor.Pump();
        ulong afterTheNewerDeadline = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted - retransmittedBefore;

        node.TimeProvider.Advance(TimeSpan.FromMilliseconds(920));
        node.Reactor.Pump();
        ulong afterTheOlderDeadline = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted - retransmittedBefore;

        // Assert — only the sequence armed at 60ms expired at 60ms. Had the earlier sequence's deadline
        // been moved to the RTO current now, it would have expired in the same sweep and this would read
        // two. It expires on its own 1000ms deadline instead, in the later sweep, alongside the newer
        // sequence's second, backed-off deadline.
        afterTheNewerDeadline.Should().Be(
            1, "the sequence armed after the RTO changed has its own deadline, and the earlier one is not retroactively moved");
        afterTheOlderDeadline.Should().Be(
            3, "past a second the sequence armed at the Linger-wide RTO expires too, with the newer one expiring a second time");
    }

    /// <summary>
    /// Sends one ReliableOrdered record, flushes it, and acknowledges it exactly
    /// <see cref="_roundTrip"/> later, so the Peer's estimator takes its one clean first sample and its
    /// timeout becomes <see cref="_seededTimeout"/>.
    /// </summary>
    private static void SeedTheEstimator(
        DatagramSessionReactorHarness node,
        RawReactor client,
        LinkId clientLink,
        uint sessionId,
        PeerId peer)
    {
        ArmOneReliableSequence(node, peer);
        node.TimeProvider.Advance(_roundTrip);
        SendCraftedArrival(
            node,
            client,
            clientLink,
            BuildDatagram(sessionId, reliableOrderedAckBase: 1, reliableOrderedAckBits: 0, sequence: 9));
        SettleCadence(node);
    }

    /// <summary>Sends one ReliableOrdered record and flushes it, which is what arms its deadline.</summary>
    private static void ArmOneReliableSequence(DatagramSessionReactorHarness node, PeerId peer)
    {
        node.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Sent);
        node.Reactor.Pump();
    }

    /// <summary>
    /// One short Pump interval, so the RTO's floor is a known small value rather than whatever the last
    /// advance happened to be. <see cref="SendCraftedArrival"/> Pumps an unspecified number of times at
    /// one instant, so without this the observed cadence after a large advance is not deterministic.
    /// </summary>
    private static void SettleCadence(DatagramSessionReactorHarness node)
    {
        node.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
        node.Reactor.Pump();
    }

    /// <summary>
    /// Every <see cref="DiagnosticId.RetransmissionTimerBackedOff"/> record recorded for
    /// <paramref name="peer"/>, as the pair of figures it carries: the new RTO in milliseconds and the
    /// consecutive backoff count.
    /// </summary>
    private static List<(long Rto, long Count)> BackedOffRecords(DatagramSessionReactorHarness node, PeerId peer) =>
        node.Sink.Received
            .Where(record => record.Id == DiagnosticId.RetransmissionTimerBackedOff && record.Peer == peer)
            .Select(record => (record.Value0, record.Value1))
            .ToList();

    private static void SendCraftedArrival(
        DatagramSessionReactorHarness node,
        RawReactor client,
        LinkId clientLink,
        byte[] datagram)
    {
        ulong before = node.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(node.Endpoint), datagram);
        node.PumpUntil(() => node.Reactor.Counters.DatagramsReceived > before);
    }

    /// <summary>
    /// Builds a live-Session datagram carrying one empty Unreliable record: a 20-byte header naming
    /// <paramref name="session"/>, whose <c>ReliableOrdered</c> ack block is the caller's and whose
    /// <c>ReliableUnordered</c> block is left zeroed. The record exists only to give the datagram a
    /// recognisable length — an Unreliable one, so it disturbs neither reliable Space.
    /// </summary>
    private static byte[] BuildDatagram(
        uint session,
        uint reliableOrderedAckBase,
        uint reliableOrderedAckBits,
        uint sequence)
    {
        const int unfragmentedRecordSize = 7;
        byte[] datagram = new byte[DatagramHeaderSize + unfragmentedRecordSize];
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(0, 4), session);
        BinaryPrimitives.WriteUInt32BigEndian(
            datagram.AsSpan(ReliableOrderedAckBlockOffset, 4), reliableOrderedAckBase);
        BinaryPrimitives.WriteUInt32BigEndian(
            datagram.AsSpan(ReliableOrderedAckBlockOffset + 4, 4), reliableOrderedAckBits);

        Span<byte> record = datagram.AsSpan(DatagramHeaderSize);
        record[0] = ModeUnreliable;
        BinaryPrimitives.WriteUInt32BigEndian(record.Slice(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.Slice(5, 2), 0);
        return datagram;
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> — a bare
    /// <see cref="RawReactor"/> — and returns the wire session id <paramref name="node"/> assigned this
    /// Peer, mirroring <see cref="DatagramSessionHorizonTests"/>'s helper of the same name.
    /// </summary>
    private static uint EstablishLiveSessionId(DatagramSessionReactorHarness node, RawReactor client, LinkId clientLink)
    {
        const ulong clientNonce = 0x0102_0304_0506_0708UL;
        Source destination = Source.FromIPEndPoint(node.Endpoint);

        byte[] hello = BuildHelloDatagram(clientNonce);
        client.SendTo(clientLink, destination, hello);
        byte[] cookieDatagram = ExchangeUntilReceived(client, node);

        ulong echoedNonce = BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset, 8));
        long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset + 8, 8)));
        byte[] cookie = cookieDatagram.AsSpan(ControlBodyOffset + 16, CookieLength).ToArray();
        echoedNonce.Should().Be(clientNonce);

        byte[] helloWithCookie = BuildHelloWithCookieDatagram(clientNonce, epoch, cookie);
        client.SendTo(clientLink, destination, helloWithCookie);
        byte[] acceptDatagram = ExchangeUntilReceived(client, node);

        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
    }

    /// <summary>The <see cref="PeerId"/> the node assigned the one Peer this test's handshake produced.</summary>
    private static PeerId FindNodePeer(DatagramSessionReactorHarness node)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = node.DrainToList();
        return drained.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;
    }

    private static byte[] ExchangeUntilReceived(RawReactor client, DatagramSessionReactorHarness node)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            node.Reactor.Pump();
            client.Pump();
            foreach (RawEvent evt in client.Drain())
            {
                if (evt.Kind == RawEventKind.DatagramReceived)
                {
                    return evt.Payload.ToArray();
                }
            }

            if (deadline.Expired)
            {
                throw new InvalidOperationException("The handshake reply did not arrive within the Pump budget.");
            }

            PumpDeadline.Yield();
        }
    }

    private static byte[] BuildControlDatagram(byte controlType, ReadOnlySpan<byte> body)
    {
        byte[] datagram = new byte[ControlDatagramSize];
        BinaryPrimitives.WriteUInt32BigEndian(datagram, ControlSession);
        datagram[4] = controlType;
        body.CopyTo(datagram.AsSpan(ControlBodyOffset));
        return datagram;
    }

    private static byte[] BuildHelloDatagram(ulong clientNonce)
    {
        byte[] body = new byte[4 + 8];
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(0, 4), ProtocolVersion);
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(4, 8), clientNonce);
        return BuildControlDatagram(HelloType, body);
    }

    private static byte[] BuildHelloWithCookieDatagram(ulong clientNonce, long epoch, byte[] cookie)
    {
        byte[] body = new byte[4 + 8 + 8 + CookieLength + 4 + 4 + 8 + ResumeSecretLength];
        int offset = 0;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), ProtocolVersion);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), clientNonce);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), unchecked((ulong)epoch));
        offset += 8;
        cookie.CopyTo(body.AsSpan(offset, CookieLength));
        offset += CookieLength;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), DialerSessionId);
        offset += 4;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), DialerMaxMessageSize);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), DialerSendIdleIntervalMilliseconds);
        offset += 8;
        return BuildControlDatagram(HelloWithCookieType, body);
    }
}
