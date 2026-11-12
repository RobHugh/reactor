using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// What a consumer actually sees once <c>FragmentStallSignature</c>
/// fires and <see cref="DatagramSessionReactor"/> acts on it — the Peer that stalled on Fragments is
/// closed with <see cref="DisconnectReason.FragmentSizeUnviable"/> named on the drained Event, nothing
/// else about the wire or the Session's other Peers changes because of it.
/// </summary>
/// <remarks>
/// <para>
/// Three properties of the close are non-obvious enough to need their own proof rather than trusting
/// the signature's own conditions, pinned further down this file, and <c>ReportExpiredRetransmissions</c>'s
/// one call site to speak for them. <b>Once-only:</b> closing removes the Peer's Slot outright, so a
/// consumer draining several more times after the close must never see a second
/// <see cref="SessionEventKind.PeerClosed"/> for the same Peer — a duplicate would tell a consumer to
/// expect a second close that will never come. <b>Nothing recovered:</b> the diagnosis is silent on the
/// wire — no probe, no announcement, nothing beyond the ordinary retransmissions already in flight — the
/// diagnosed Peer's own Fragments never change size across every one of those retransmissions, and the
/// fragment size that caused it is unchanged for every other Peer, so a consumer's own configuration
/// is exactly where it left it. <b>One path, not the Reactor:</b> a second Peer that never matches the
/// signature keeps working — sending and receiving — while the first is being diagnosed and after it is
/// gone, proving the close is scoped to the one Peer whose path could not carry a Fragment.
/// </para>
/// <para>
/// <b>Driving the stall without ever acking a Fragment.</b> The predicate
/// (<c>FragmentStallSignature.IsFragmentSizeStall</c>) needs an unfragmented reliable record acked
/// at or after the Fragment window opened, while no Fragment is ever acked. The two records are sent in
/// different reliable Spaces — the large, fragmented Message as <see cref="DeliveryMode.ReliableOrdered"/>,
/// the small, unfragmented one as <see cref="DeliveryMode.ReliableUnordered"/> — so one crafted ack
/// naming only the <c>ReliableUnordered</c> block, with the <c>ReliableOrdered</c> block left at its
/// initial zeroed state, releases the small record without ever touching a Fragment sequence. That ack
/// also seeds the Peer's round-trip estimator (matching <see cref="DatagramSessionAdaptiveRtoTests"/>'s
/// own seeding), but only for sequences armed after it: the Fragments were armed before it, at the
/// unseeded RTO that resolves to the harness's Linger (<c>RetransmissionTimer.EffectiveTimeout</c>),
/// so the first of the four rounds this test needs waits out that full second.
/// <c>RetransmissionTimer.BackOff</c> runs unconditionally on every round before the re-arm, so
/// even that first re-arm already reflects one doubling of the seeded 60ms timeout — 120ms — and each
/// wait after it doubles again, to 240 and then 480. <c>FragmentStallSignature.StallRoundCount</c>
/// is referenced rather than hardcoded so a change to that constant cannot silently desynchronise this
/// file's clock advances from what the signature actually requires.
/// </para>
/// <para>
/// <b>The instrument.</b> A live <see cref="DatagramSessionReactor"/> node with two Peers, each a
/// hand-built client over a bare <see cref="RawReactor"/> — the same instrument
/// <see cref="DatagramSessionAdaptiveRtoTests"/> and <see cref="DatagramSessionFragmentSetAbandonedTests"/>
/// use: the scenario needs an ack the test chooses — one Space released, the other left untouched — which
/// no ordinary Session produces.
/// </para>
/// </remarks>
public class DatagramSessionFragmentSizeUnviableTests
{
    private const int ControlDatagramSize = 256;
    private const uint ControlSession = 0;

    private const byte HelloType = 1;
    private const int ControlBodyOffset = 5;
    private const int CookieLength = 32;

    private const uint ProtocolVersion = 1;
    private const uint DialerSessionId = 0xCAFE_BABE;

    private const int DatagramHeaderSize = 20;
    private const int ReliableOrderedAckBlockOffset = 4;
    private const int AckBlockSize = 8;
    private const int ReliableUnorderedAckBlockOffset = ReliableOrderedAckBlockOffset + AckBlockSize;

    private const int UnfragmentedRecordSize = 7;
    private const int FragmentRecordSize = 11;
    private const byte ModeUnreliable = 0b0000_0000;
    private const byte FragmentBit = 0b0000_0100;

    /// <summary>
    /// Small enough that a Message several times its size is cut into more than one Fragment, and no
    /// smaller: <see cref="DatagramSessionOptions.Validate"/> enforces the RFC 791 reassembly floor of
    /// 576 bytes.
    /// </summary>
    private const int FragmentSize = 600;

    private const int MaxMessageSize = 4096;

    /// <summary>
    /// The expired retransmission rounds the fragment-size diagnosis waits for: four, chosen and argued
    /// on <c>FragmentStallSignature.StallRoundCount</c>.
    /// </summary>
    private const int StallRoundCount = 4;

    /// <summary>Sized so both the fragmented Message and its retransmissions, on both Peers, stay retained for the whole run.</summary>
    private const int RetentionBytes = 16384;

    /// <summary>Wider than <see cref="FragmentSize"/> by more than double, so the cut Message is always three Fragments.</summary>
    private const int BigMessageLength = 1500;

    /// <summary>The highest Fragment sequence the big Message is cut into: three records, sequence 0 through 2.</summary>
    private const uint HighestFragmentSequence = 2;

    /// <summary>The one unfragmented record the small Message is sent as, on the other reliable Space.</summary>
    private const uint UnfragmentedRecordSequence = 0;

    /// <summary>
    /// The round trip the ack releasing the unfragmented record measures, seeding the estimator exactly
    /// as <see cref="DatagramSessionAdaptiveRtoTests"/>'s own <c>_roundTrip</c> does.
    /// </summary>
    private static readonly TimeSpan _roundTrip = TimeSpan.FromMilliseconds(20);

    /// <summary>The timeout <see cref="_roundTrip"/> seeds: smoothed 20ms plus four variances of 10ms.</summary>
    private static readonly TimeSpan _seededTimeout = TimeSpan.FromMilliseconds(60);

    /// <summary>The harness's Linger, which is also the unseeded RTO the Fragments are first armed at.</summary>
    private static readonly TimeSpan _linger = TimeSpan.FromSeconds(1);

    /// <summary>
    /// The increment <see cref="AdvanceInSteps"/> advances by. Divides evenly into every wait this file
    /// steps through (1000, 120, 240 and 480ms), so each wait lands exactly on its deadline rather than
    /// overshooting into the next round.
    /// </summary>
    private static readonly TimeSpan _pumpStep = TimeSpan.FromMilliseconds(10);

    [Fact]
    public void Pump_TheFourthExpiredRoundWithNoFragmentEverAckedAndAnUnfragmentedRecordAckedSince_ClosesThePeerWithFragmentSizeUnviable()
    {
        // Arrange
        using DatagramSessionReactorHarness node = NewNode();
        using RawReactor clientA = NewClient();
        LinkId clientALink = clientA.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionIdA = EstablishLiveSessionId(node, clientA, clientALink);
        PeerId peerA = FindNodePeer(node);

        List<byte[]> peerADatagrams = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];

        // Act
        DriveFragmentSizeStall(node, clientA, clientALink, sessionIdA, peerA, peerADatagrams, events);

        // Assert — the Event names the Peer and the reason explicitly, not merely "some non-generic reason".
        events.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed && e.Peer.Equals(peerA))
            .Which.Reason.Should().Be(
                DisconnectReason.FragmentSizeUnviable,
                "no Fragment was ever acked while an unfragmented record acked after the window opened, across four retransmission rounds");
    }

    [Fact]
    public void Pump_SeveralPumpsAfterTheClose_ProducesNoSecondPeerClosedForThatPeer()
    {
        // Arrange
        using DatagramSessionReactorHarness node = NewNode();
        using RawReactor clientA = NewClient();
        LinkId clientALink = clientA.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionIdA = EstablishLiveSessionId(node, clientA, clientALink);
        PeerId peerA = FindNodePeer(node);

        List<byte[]> peerADatagrams = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];
        DriveFragmentSizeStall(node, clientA, clientALink, sessionIdA, peerA, peerADatagrams, events);

        events.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed && e.Peer.Equals(peerA));

        // Act — several more Pumps once the Peer's Slot is already gone.
        for (int i = 0; i < 5; i++)
        {
            PumpAndCollect(node, clientA, peerADatagrams, events);
        }

        // Assert — the close happened exactly once: removing the Slot means there is nothing left for a
        // later Pump to close a second time.
        events.Count(e => e.Kind == SessionEventKind.PeerClosed && e.Peer.Equals(peerA)).Should().Be(
            1, "the Peer's Slot was removed by the first close, so no later Pump can produce a second PeerClosed for it");
    }

    [Fact]
    public void Pump_AcrossTheWholeRun_NeverPutsAnythingOnTheWireBeyondTheKnownRecordsAndNothingNewAfterTheDiagnosisPump()
    {
        // Arrange
        using DatagramSessionReactorHarness node = NewNode();
        using RawReactor clientA = NewClient();
        LinkId clientALink = clientA.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionIdA = EstablishLiveSessionId(node, clientA, clientALink);
        PeerId peerA = FindNodePeer(node);

        List<byte[]> peerADatagrams = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];

        // Act
        DriveFragmentSizeStall(node, clientA, clientALink, sessionIdA, peerA, peerADatagrams, events);
        int datagramCountAtDiagnosis = peerADatagrams.Count;

        // A "probe" would be some datagram this scenario's own sends and retransmissions cannot account
        // for — an extra control-shaped message, a record naming a sequence or Space outside the two the
        // test sent, or anything at all arriving after the close. ReportExpiredRetransmissions closes the
        // Peer in place of re-arming it (ReactorSockets/DatagramSessionReactor.cs, ~line 1457) and never
        // queues anything of its own, so nothing here should exist to find.
        for (int i = 0; i < 5; i++)
        {
            PumpAndCollect(node, clientA, peerADatagrams, events);
        }

        // Assert — every datagram received decodes as an ordinary data datagram for the established
        // session, carrying only records the test itself sent. Decoded by hand rather than through
        // DatagramDecoder: that decoder assumes every record uses the unfragmented, 7-byte header shape
        // (ReactorSockets.Tests/DatagramDecoder.cs), so it cannot walk a datagram carrying a Fragment
        // record's 11-byte header without misreading everything after it — DecodeKnownRecords below is
        // this file's own reader, the same fragment-aware walk CountRecords already uses.
        peerADatagrams.Should().NotBeEmpty("the fragmented and unfragmented Messages must have reached the client at least once");
        foreach (byte[] datagram in peerADatagrams)
        {
            uint session = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(0, 4));
            session.Should().Be(
                DialerSessionId,
                "every datagram the node sends to this client is addressed with the session id the client announced in its own Hello — the opposite direction from sessionIdA, which is what this client addresses the node with");

            foreach ((byte mode, uint sequence, ushort _) in DecodeKnownRecords(datagram))
            {
                bool isKnownFragment = mode == (byte)DeliveryMode.ReliableOrdered && sequence <= HighestFragmentSequence;
                bool isKnownUnfragmentedRecord = mode == (byte)DeliveryMode.ReliableUnordered && sequence == UnfragmentedRecordSequence;
                (isKnownFragment || isKnownUnfragmentedRecord).Should().BeTrue(
                    $"record mode {mode} sequence {sequence} is not one of the two Messages this test sent — a probe would show up as exactly this kind of unaccounted-for record");
            }
        }

        // Assert — nothing new appeared once the Peer was diagnosed and closed.
        peerADatagrams.Count.Should().Be(
            datagramCountAtDiagnosis,
            "the closed Peer's Slot was removed, so no further flush can reach the client — a probe sent from outside the ordinary flush path would have shown up here instead");
    }

    [Fact]
    public void Pump_ThreeExpiredRoundsThenAFourth_LeavesThePeerOpenUntilTheFourthClosesIt()
    {
        // Arrange — every condition of the signature holds except the round count, which stops one short.
        using DatagramSessionReactorHarness node = NewNode();
        using RawReactor clientA = NewClient();
        LinkId clientALink = clientA.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionIdA = EstablishLiveSessionId(node, clientA, clientALink);
        PeerId peerA = FindNodePeer(node);

        List<byte[]> peerADatagrams = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];
        DriveFragmentSizeStall(node, clientA, clientALink, sessionIdA, peerA, peerADatagrams, events, rounds: StallRoundCount - 1);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> afterThreeRounds = [.. events];

        // Act — the fourth round's 480ms wait.
        AdvanceInSteps(node, clientA, peerADatagrams, events, _seededTimeout * 8, _pumpStep);

        // Assert
        afterThreeRounds.Should().NotContain(
            e => e.Kind == SessionEventKind.PeerClosed, "three rounds are one short of the count the diagnosis needs");
        events.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed && e.Peer.Equals(peerA))
            .Which.Reason.Should().Be(DisconnectReason.FragmentSizeUnviable);
    }

    [Fact]
    public void Pump_APeerThatOnceDeliveredAFragmentedMessage_IsNeverDiagnosedWhenALaterOneStalls()
    {
        // Arrange — the first fragmented Message is acked whole, so this path has carried a Fragment.
        using DatagramSessionReactorHarness node = NewNode();
        using RawReactor clientA = NewClient();
        LinkId clientALink = clientA.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionIdA = EstablishLiveSessionId(node, clientA, clientALink);
        PeerId peerA = FindNodePeer(node);

        List<byte[]> datagrams = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];
        byte[] bigMessage = Enumerable.Range(0, BigMessageLength).Select(i => (byte)i).ToArray();

        node.Reactor.Send(peerA, DeliveryMode.ReliableOrdered, bigMessage).Should().Be(SendOutcome.Sent);
        PumpAndCollect(node, clientA, datagrams, events);
        node.TimeProvider.Advance(_roundTrip);
        SendCraftedArrivalAndCollect(
            node,
            clientA,
            clientALink,
            BuildAckDatagram(sessionIdA, HighestFragmentSequence + 1, 0, 0, 0, sequence: 50),
            datagrams,
            events);

        // A second fragmented Message whose Fragments are never acked, then an unfragmented record acked
        // after it, so every other condition of the signature holds for the new window.
        node.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
        node.Reactor.Send(peerA, DeliveryMode.ReliableOrdered, bigMessage).Should().Be(SendOutcome.Sent);
        PumpAndCollect(node, clientA, datagrams, events);
        node.Reactor.Send(peerA, DeliveryMode.ReliableUnordered, [0x5A]).Should().Be(SendOutcome.Sent);
        PumpAndCollect(node, clientA, datagrams, events);
        node.TimeProvider.Advance(_roundTrip);
        SendCraftedArrivalAndCollect(
            node,
            clientA,
            clientALink,
            BuildAckDatagram(sessionIdA, HighestFragmentSequence + 1, 0, reliableUnorderedAckBase: 1, 0, sequence: 51),
            datagrams,
            events);

        // Act — well past the four rounds the seeded 60ms RTO reaches in 900ms.
        AdvanceInSteps(node, clientA, datagrams, events, TimeSpan.FromMilliseconds(950), _pumpStep);

        // Assert
        events.Should().NotContain(
            e => e.Reason == DisconnectReason.FragmentSizeUnviable,
            "a path that has carried a Fragment cannot be one that never carries a Fragment");
    }

    [Fact]
    public void Pump_TheOnlyUnfragmentedAckArrivedBeforeTheFragmentsWereSent_IsNeverDiagnosed()
    {
        // Arrange — the unfragmented record is acked first, then the fragmented Message is sent, and
        // nothing is acked after that.
        using DatagramSessionReactorHarness node = NewNode();
        using RawReactor clientA = NewClient();
        LinkId clientALink = clientA.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionIdA = EstablishLiveSessionId(node, clientA, clientALink);
        PeerId peerA = FindNodePeer(node);

        List<byte[]> datagrams = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];

        node.Reactor.Send(peerA, DeliveryMode.ReliableUnordered, [0x5A]).Should().Be(SendOutcome.Sent);
        PumpAndCollect(node, clientA, datagrams, events);
        node.TimeProvider.Advance(_roundTrip);
        SendCraftedArrivalAndCollect(
            node,
            clientA,
            clientALink,
            BuildAckDatagram(sessionIdA, 0, 0, reliableUnorderedAckBase: 1, 0, sequence: 50),
            datagrams,
            events);

        node.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
        byte[] bigMessage = Enumerable.Range(0, BigMessageLength).Select(i => (byte)i).ToArray();
        node.Reactor.Send(peerA, DeliveryMode.ReliableOrdered, bigMessage).Should().Be(SendOutcome.Sent);
        PumpAndCollect(node, clientA, datagrams, events);

        // Act — well past the four rounds the seeded 60ms RTO reaches in 900ms.
        AdvanceInSteps(node, clientA, datagrams, events, TimeSpan.FromMilliseconds(950), _pumpStep);

        // Assert
        events.Should().NotContain(
            e => e.Reason == DisconnectReason.FragmentSizeUnviable,
            "an unfragmented ack from before the Fragments were sent says nothing about whether they can arrive");
    }

    [Fact]
    public void Pump_ASecondPeerNeverMeetingTheSignature_KeepsItsConfiguredFragmentSizeAndStaysUsable()
    {
        // Arrange — a second Peer over its own client, established alongside the one that will be driven
        // to the stall, sharing the same Reactor and so the same configured fragment size.
        using DatagramSessionReactorHarness node = NewNode();
        using RawReactor clientA = NewClient();
        LinkId clientALink = clientA.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionIdA = EstablishLiveSessionId(node, clientA, clientALink);
        PeerId peerA = FindNodePeer(node);

        using RawReactor clientB = NewClient();
        LinkId clientBLink = clientB.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionIdB = EstablishLiveSessionId(node, clientB, clientBLink);
        PeerId peerB = FindNodePeer(node);

        List<byte[]> peerADatagrams = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];

        // Act — drive the first Peer to its close, leaving the second untouched throughout.
        DriveFragmentSizeStall(node, clientA, clientALink, sessionIdA, peerA, peerADatagrams, events);
        events.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed && e.Peer.Equals(peerA));

        // Act — after the close, a Message wider than the fragment size, sent to the second Peer.
        byte[] messageToPeerB = Enumerable.Range(0, BigMessageLength).Select(i => (byte)(i % 256)).ToArray();
        node.Reactor.Send(peerB, DeliveryMode.ReliableOrdered, messageToPeerB).Should().Be(SendOutcome.Sent);
        List<byte[]> peerBDatagrams = [];
        PumpAndCollect(node, clientB, peerBDatagrams, events);
        byte[] datagramToPeerB = peerBDatagrams.Last();

        // Assert — proven observably rather than against the test's own init-only options object: the
        // Message reaching the second Peer is still cut at the configured fragment size, unaffected by
        // the first Peer's close.
        CountRecords(datagramToPeerB).Should().Be(
            (BigMessageLength + FragmentSize - 1) / FragmentSize, "the fragment size configured at construction is unchanged after the first Peer closed");
        FirstRecordLength(datagramToPeerB).Should().Be(
            (ushort)FragmentSize, "the second Peer's Message is cut to the same fragment size the first Peer's was, proving the setting itself never moved");

        // Act — the second Peer sends its own record back, unreliable so no ack machinery is needed to
        // prove receipt.
        byte[] payloadFromPeerB = [0x11, 0x22, 0x33];
        byte[] recordFromPeerB = BuildUnreliableRecordDatagram(sessionIdB, sequence: 0, payloadFromPeerB);
        ulong before = node.Reactor.Counters.DatagramsReceived;
        clientB.SendTo(clientBLink, Source.FromIPEndPoint(node.Endpoint), recordFromPeerB);
        node.PumpUntil(() => node.Reactor.Counters.DatagramsReceived > before);
        List<byte[]> received = node.DrainPayloads();

        // Assert — the second Peer completes both directions: a send the client received, cut correctly,
        // and a receive the node delivered whole.
        received.Should().ContainSingle().Which.Should().Equal(payloadFromPeerB, "the second Peer's own record reached the node and was delivered");

        // Assert — the second Peer was never touched by the first Peer's diagnosis.
        events.Should().NotContain(
            e => e.Kind == SessionEventKind.PeerClosed && e.Peer.Equals(peerB),
            "the second Peer never matched the stall signature and must stay connected");
        Action stillUsable = () => node.Reactor.CountersOf(peerB);
        stillUsable.Should().NotThrow("the second Peer's Slot is still live");
    }

    [Fact]
    public void Pump_AcrossEverySendAndRetransmissionRoundUpToTheDiagnosis_KeepsEveryFragmentAtItsOriginallyCutSize()
    {
        // Arrange
        using DatagramSessionReactorHarness node = NewNode();
        using RawReactor clientA = NewClient();
        LinkId clientALink = clientA.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionIdA = EstablishLiveSessionId(node, clientA, clientALink);
        PeerId peerA = FindNodePeer(node);

        List<byte[]> peerADatagrams = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];

        // Act — the whole run: the original cut and every retransmission round the stall signature
        // needs, up to and including the round that diagnoses and closes the Peer.
        DriveFragmentSizeStall(node, clientA, clientALink, sessionIdA, peerA, peerADatagrams, events);
        events.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed && e.Peer.Equals(peerA));

        // A recovering implementation would have re-cut the still-undelivered Message smaller, hoping a
        // narrower Fragment would get through where the wider one stalled: on the wire that would show up
        // as one of this Message's Fragment sequences (0 through HighestFragmentSequence) carrying a
        // shorter declared length on a later retransmission than it carried on an earlier one, for the
        // two Fragments (sequence 0 and 1) that are not the set's short remainder. This test's own
        // remarks are explicit that ReportExpiredRetransmissions never re-cuts anything — it only closes
        // the Peer in place of re-arming it — so no such shrink should exist to find; this proves that by
        // reading every Fragment this Peer's Message was ever sent as, across every round, rather than
        // trusting the absence of a re-cut path in the source.
        const ushort finalFragmentLength = (ushort)(BigMessageLength - (int)HighestFragmentSequence * FragmentSize);

        // Act — flatten every Fragment record belonging to the big Message, across every datagram sent
        // to Peer A during the whole drive, in the order the wire carried them.
        List<(uint Sequence, ushort Length)> fragmentRecords = [];
        foreach (byte[] datagram in peerADatagrams)
        {
            foreach ((byte mode, uint sequence, ushort length) in DecodeKnownRecords(datagram))
            {
                if (mode == (byte)DeliveryMode.ReliableOrdered && sequence <= HighestFragmentSequence)
                {
                    fragmentRecords.Add((sequence, length));
                }
            }
        }

        // Assert — the set was actually retransmitted more than once, so this proves size across
        // repeated re-sends rather than only the original cut.
        fragmentRecords.Should().HaveCountGreaterThan(
            (int)HighestFragmentSequence + 1, "the four stall rounds each retransmit the whole unacked Fragment set at least once beyond the original send");

        // Assert — every occurrence of the two full-size Fragments, original send and every
        // retransmission alike, carries exactly the configured fragment size.
        fragmentRecords.Where(r => r.Sequence < HighestFragmentSequence).Should().AllSatisfy(
            r => r.Length.Should().Be(
                (ushort)FragmentSize,
                "a recovering implementation would have shrunk this Fragment on a later round; this Peer's own Fragments never move from the size they were originally cut at"));

        // Assert — the set's short remainder likewise never changes size across every round it was resent.
        fragmentRecords.Where(r => r.Sequence == HighestFragmentSequence).Should().AllSatisfy(
            r => r.Length.Should().Be(
                finalFragmentLength,
                "the remainder Fragment is shorter than the others by construction, not because anything re-cut it — it stays at that same remainder size on every retransmission"));
    }

    /// <summary>
    /// Drives one Peer through the whole fragment-size stall shape: a fragmented Message opens the
    /// window, a small unfragmented Message on the other reliable Space is acked to satisfy the
    /// "acked since the window opened" half of the signature, and then exactly
    /// <c>FragmentStallSignature.StallRoundCount</c> retransmission rounds elapse with no Fragment
    /// ever acked. Every datagram the client receives and every Event the node drains, across the whole
    /// drive, is appended to <paramref name="datagrams"/> and <paramref name="events"/> respectively.
    /// </summary>
    private static void DriveFragmentSizeStall(
        DatagramSessionReactorHarness node,
        RawReactor client,
        LinkId clientLink,
        uint sessionId,
        PeerId peer,
        List<byte[]> datagrams,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events,
        int rounds = StallRoundCount)
    {
        // A Message several times FragmentSize, cut into three Fragments; flushing opens the window.
        byte[] bigMessage = Enumerable.Range(0, BigMessageLength).Select(i => (byte)i).ToArray();
        node.Reactor.Send(peer, DeliveryMode.ReliableOrdered, bigMessage).Should().Be(SendOutcome.Sent);
        PumpAndCollect(node, client, datagrams, events);

        // A single-record Message on the other reliable Space, which the crafted ack below releases
        // without ever touching a Fragment sequence.
        node.Reactor.Send(peer, DeliveryMode.ReliableUnordered, [0x5A]).Should().Be(SendOutcome.Sent);
        PumpAndCollect(node, client, datagrams, events);

        // The ack: ReliableOrdered's block stays zeroed (no Fragment acked), ReliableUnordered's names a
        // base of 1 (its one sequence, 0, fully acked). This is _roundTrip after the unfragmented record
        // armed, seeding the estimator the same way DatagramSessionAdaptiveRtoTests does.
        node.TimeProvider.Advance(_roundTrip);
        SendCraftedArrivalAndCollect(
            node,
            client,
            clientLink,
            BuildAckDatagram(
                sessionId,
                reliableOrderedAckBase: 0,
                reliableOrderedAckBits: 0,
                reliableUnorderedAckBase: 1,
                reliableUnorderedAckBits: 0,
                sequence: 50),
            datagrams,
            events);
        node.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
        PumpAndCollect(node, client, datagrams, events);

        // Four rounds: the Fragments were armed before the ack above, at the unseeded RTO that resolves
        // to the Linger ceiling, so the first wait is a full second. RetransmissionTimer.BackOff() runs
        // unconditionally before every re-arm, so even the very first re-arm already reflects one
        // doubling of the seeded 60ms timeout — 120ms, not 60ms — and each wait after it doubles again:
        // 240, then 480. Each wait is stepped in small increments rather than one large Advance, because
        // EffectiveTimeout floors the RTO against PumpIntervalObserver's own most recently observed gap
        // (issue #191): a single one-second jump right before a round fires would re-floor that round's
        // own re-arm back up to the Linger ceiling, defeating the seeded timeout the ack above
        // established. Stepping keeps the observed cadence small right up to the moment each round
        // actually fires, so the re-arm it computes reflects the seeded, doubling RTO instead.
        TimeSpan[] deadlines = [_linger, _seededTimeout * 2, _seededTimeout * 4, _seededTimeout * 8];
        deadlines.Should().HaveCount(
            StallRoundCount,
            "one deadline per retransmission round the signature counts before the Peer is diagnosed");

        foreach (TimeSpan deadline in deadlines.Take(rounds))
        {
            AdvanceInSteps(node, client, datagrams, events, deadline, _pumpStep);
        }
    }

    /// <summary>Advances the clock to cover <paramref name="total"/> in <paramref name="step"/>-sized increments, Pumping and collecting after each one.</summary>
    private static void AdvanceInSteps(
        DatagramSessionReactorHarness node,
        RawReactor client,
        List<byte[]> datagrams,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events,
        TimeSpan total,
        TimeSpan step)
    {
        TimeSpan elapsed = TimeSpan.Zero;
        while (elapsed < total)
        {
            TimeSpan thisStep = step <= total - elapsed ? step : total - elapsed;
            node.TimeProvider.Advance(thisStep);
            elapsed += thisStep;
            PumpAndCollect(node, client, datagrams, events);
        }
    }

    private static DatagramSessionReactorHarness NewNode() =>
        new(fragmentSize: FragmentSize, maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes);

    private static RawReactor NewClient() => new(new ReactorOptions { Capacity = 4 });

    /// <summary>Pumps the node once, appending every Event it drains, then pumps the client and appends every datagram it received.</summary>
    private static void PumpAndCollect(
        DatagramSessionReactorHarness node,
        RawReactor client,
        List<byte[]> datagrams,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events)
    {
        node.Reactor.Pump();
        events.AddRange(node.DrainToList());
        client.Pump();
        foreach (RawEvent evt in client.Drain())
        {
            if (evt.Kind == RawEventKind.DatagramReceived)
            {
                datagrams.Add(evt.Payload.ToArray());
            }
        }
    }

    /// <summary>Sends a crafted arrival and pumps/collects until the node has received it.</summary>
    private static void SendCraftedArrivalAndCollect(
        DatagramSessionReactorHarness node,
        RawReactor client,
        LinkId clientLink,
        byte[] datagram,
        List<byte[]> datagrams,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events)
    {
        ulong before = node.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(node.Endpoint), datagram);
        PumpDeadline deadline = PumpDeadline.Start();
        while (node.Reactor.Counters.DatagramsReceived <= before)
        {
            PumpAndCollect(node, client, datagrams, events);
            if (deadline.Expired)
            {
                throw new InvalidOperationException("The crafted arrival did not reach the node within the Pump budget.");
            }

            PumpDeadline.Yield();
        }
    }

    /// <summary>The first record's declared length — the size the first Fragment of a data datagram was cut to.</summary>
    private static ushort FirstRecordLength(byte[] datagram) =>
        BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(DatagramHeaderSize + 5, 2));

    /// <summary>Walks every packet record in <paramref name="datagram"/> by hand, returning each one's mode bits, sequence, and declared payload length.</summary>
    private static List<(byte Mode, uint Sequence, ushort Length)> DecodeKnownRecords(byte[] datagram)
    {
        const byte modeMask = 0b0000_0011;
        List<(byte Mode, uint Sequence, ushort Length)> records = [];
        int offset = DatagramHeaderSize;
        while (offset < datagram.Length)
        {
            byte flags = datagram[offset];
            uint sequence = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(offset + 1, 4));
            ushort length = BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + 5, 2));
            bool isFragment = (flags & FragmentBit) != 0;
            int headerSize = isFragment ? FragmentRecordSize : UnfragmentedRecordSize;
            records.Add(((byte)(flags & modeMask), sequence, length));
            offset += headerSize + length;
        }

        return records;
    }

    /// <summary>How many packet records one datagram carries, walking the wire by hand.</summary>
    private static int CountRecords(byte[] datagram)
    {
        int count = 0;
        int offset = DatagramHeaderSize;
        while (offset < datagram.Length)
        {
            byte flags = datagram[offset];
            ushort length = BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + 5, 2));
            bool isFragment = (flags & FragmentBit) != 0;
            int headerSize = isFragment ? FragmentRecordSize : UnfragmentedRecordSize;
            offset += headerSize + length;
            count++;
        }

        return count;
    }

    /// <summary>Builds a 20-byte header naming <paramref name="session"/> with both ack blocks set from the caller's values.</summary>
    private static byte[] BuildAckDatagram(
        uint session,
        uint reliableOrderedAckBase,
        uint reliableOrderedAckBits,
        uint reliableUnorderedAckBase,
        uint reliableUnorderedAckBits,
        uint sequence)
    {
        byte[] datagram = new byte[DatagramHeaderSize + UnfragmentedRecordSize];
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(0, 4), session);
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(ReliableOrderedAckBlockOffset, 4), reliableOrderedAckBase);
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(ReliableOrderedAckBlockOffset + 4, 4), reliableOrderedAckBits);
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(ReliableUnorderedAckBlockOffset, 4), reliableUnorderedAckBase);
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(ReliableUnorderedAckBlockOffset + 4, 4), reliableUnorderedAckBits);

        Span<byte> record = datagram.AsSpan(DatagramHeaderSize);
        record[0] = ModeUnreliable;
        BinaryPrimitives.WriteUInt32BigEndian(record.Slice(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.Slice(5, 2), 0);
        return datagram;
    }

    /// <summary>Builds one live-Session datagram carrying a single unfragmented <c>Unreliable</c> record with a real payload.</summary>
    private static byte[] BuildUnreliableRecordDatagram(uint session, uint sequence, ReadOnlySpan<byte> payload)
    {
        byte[] datagram = new byte[DatagramHeaderSize + UnfragmentedRecordSize + payload.Length];
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(0, 4), session);

        Span<byte> record = datagram.AsSpan(DatagramHeaderSize);
        record[0] = ModeUnreliable;
        BinaryPrimitives.WriteUInt32BigEndian(record.Slice(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.Slice(5, 2), (ushort)payload.Length);
        payload.CopyTo(record.Slice(UnfragmentedRecordSize));
        return datagram;
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> — a bare
    /// <see cref="RawReactor"/> — and returns the wire session id <paramref name="node"/> assigned this
    /// Peer, mirroring <see cref="DatagramSessionAdaptiveRtoTests"/>'s helper of the same name.
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

        byte[] helloWithCookie = DatagramSessionWire.BuildHelloWithCookie(clientNonce, epoch, cookie);
        client.SendTo(clientLink, destination, helloWithCookie);
        byte[] acceptDatagram = ExchangeUntilReceived(client, node);

        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
    }

    /// <summary>The <see cref="PeerId"/> the node assigned the most recently completed handshake's Peer.</summary>
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
}
