using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.2.5 (issue #172): a packet record arriving on a live Session picks its Space from the
/// mode bits in its flags byte and is classified through that Space's receive window — what the
/// window rejects is counted, never logged, and delivery is out of scope (that is #173 and 6.6's).
/// The instrument is the one <see cref="DatagramSessionDataDatagramParseTests"/> already uses: a raw
/// dialler completing an ordinary handshake by hand, then hand-built data datagrams sent at the live
/// session id the accepter assigned.
/// </summary>
/// <remarks>
/// Most windows here are 4 fragments wide (<c>ReceiveWindowFragments: 4</c>), the smallest this
/// profile's floor allows for the harness's default 4096-byte <c>MaxMessageSize</c> and 1200-byte
/// fragment size, so a sequence of 100 is unreachably far ahead without needing a huge send. One test
/// widens to 8 (with <c>MaxMessageSize</c> raised to keep it above that floor) so a hole opened at 0
/// has three already-admitted sequences sitting above it to advance the base across in one step.
/// </remarks>
public class DatagramSessionSpaceRoutingTests
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
    private const ulong DialerLingerMilliseconds = 30_000;

    private const int DatagramHeaderSize = 20;

    private const byte ModeUnreliable = 0b0000_0000;
    private const byte ModeUnreliableSequenced = 0b0000_0001;
    private const byte ModeReliableUnordered = 0b0000_0010;
    private const byte ModeReliableOrdered = 0b0000_0011;

    [Fact]
    public void Pump_OneSequenceSentUnderAllFourModes_IsAdmittedOncePerSpace()
    {
        // Arrange — sequence 1 sent once under each of the four modes, deliberately leaving sequence 0
        // a hole: an admission that fills the base's own slot immediately retires that sequence rather
        // than leaving it classifiable as a duplicate, so a gap is what keeps the evidence in place for
        // the resend below. If a Space were shared, the second, third and fourth arrivals here would
        // already classify as duplicates.
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 4);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] firstArrival =
        [
            .. BuildHeader(sessionId),
            .. BuildRecord(ModeUnreliable, sequence: 1, payload: []),
            .. BuildRecord(ModeUnreliableSequenced, sequence: 1, payload: []),
            .. BuildRecord(ModeReliableUnordered, sequence: 1, payload: []),
            .. BuildRecord(ModeReliableOrdered, sequence: 1, payload: []),
        ];

        // Act
        ulong datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), firstArrival);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();

        // Assert — each of the four Spaces admitted sequence 1 independently: no duplicate and no
        // out-of-window counted, and nothing logged. Three of the four deliver, since an admitted
        // record now delivers (#217, #219); the ReliableOrdered one is held behind the hole this test
        // deliberately left at sequence 0, because that Space delivers in send order (#220). Admission
        // is what this test is about and all four Spaces admitted — what each Space then does with an
        // admitted record is its own mode's rule.
        PeerCounters counters = accepter.Reactor.CountersOf(accepterPeer);
        counters.DuplicatePackets.Should().Be(0);
        counters.OutOfWindowPackets.Should().Be(0);
        accepter.Sink.Received.Should().BeEmpty();
        drained.Should().HaveCount(3).And.OnlyContain(e => e.Kind == SessionEventKind.MessageReceived);

        // Act again — the same four records, resent. If each mode kept its own window, all four are
        // now duplicates; if any two modes shared a window, fewer than four would show as new.
        byte[] secondArrival = firstArrival;
        datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), secondArrival);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);

        // Assert — all four Spaces independently flagged their own repeat, and a duplicate reaches
        // neither the arena nor the received count (#217): suppressing it is the window's whole
        // purpose, so delivering one would make the suppression invisible in the figure a consumer
        // reads to check it.
        PeerCounters afterResend = accepter.Reactor.CountersOf(accepterPeer);
        afterResend.DuplicatePackets.Should().Be(4);
        afterResend.OutOfWindowPackets.Should().Be(0);
        afterResend.MessagesReceived.Should().Be(
            3, "three of the four first arrivals delivered, the ReliableOrdered one is still held behind its hole, and no duplicate delivered");
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().BeEmpty("a duplicate delivers nothing");
    }

    [Fact]
    public void Pump_RecordFarAheadOfItsSpacesWindow_IsDiscardedCountedOnceAndLeavesTheBaseUnmoved()
    {
        // Arrange — a 4-fragment window based at 0 spans [0, 4); 100 is unreachably far ahead. Uses
        // ReliableOrdered rather than Unreliable: since #173, Unreliable's window slides a far-ahead
        // arrival in rather than discarding it (retransmission fills a reliable Space's holes, so only
        // a reliable Space keeps the discard-and-leave-the-base-unmoved behaviour this test pins).
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 4);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] farAhead = [.. BuildHeader(sessionId), .. BuildRecord(ModeReliableOrdered, sequence: 100, payload: [])];

        // Act
        ulong datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), farAhead);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);

        // Assert — discarded and counted once, nothing logged, nothing delivered: an out-of-window
        // record reaches neither the arena nor the received count (#217).
        PeerCounters afterDiscard = accepter.Reactor.CountersOf(accepterPeer);
        afterDiscard.OutOfWindowPackets.Should().Be(1);
        afterDiscard.DuplicatePackets.Should().Be(0);
        afterDiscard.MessagesReceived.Should().Be(0);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().BeEmpty("an out-of-window record delivers nothing");

        // Act — sequence 3, the far edge still inside the untouched window. Left as a gap below it
        // (0, 1 and 2 never arrive), this stays classifiable below rather than being immediately
        // retired the instant it admits, which is what lets the resend below tell admitted-and-held
        // apart from already-out-of-window: were the far-ahead arrival to have dragged the base
        // forward, 3 would now fall below it and classify as out-of-window instead of admitting.
        byte[] atTheOldBasesFarEdge = [.. BuildHeader(sessionId), .. BuildRecord(ModeReliableOrdered, sequence: 3, payload: [])];
        datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), atTheOldBasesFarEdge);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);

        // Assert — admitted: neither counter moved from the discard above.
        PeerCounters afterAdmit = accepter.Reactor.CountersOf(accepterPeer);
        afterAdmit.OutOfWindowPackets.Should().Be(1);
        afterAdmit.DuplicatePackets.Should().Be(0);

        // Act — resend sequence 3. Only a sequence the window still tracks as admitted reports a
        // duplicate, which is what confirms the base genuinely never moved off zero.
        datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), atTheOldBasesFarEdge);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);

        // Assert
        accepter.Reactor.CountersOf(accepterPeer).DuplicatePackets.Should().Be(1);
        accepter.Reactor.CountersOf(accepterPeer).OutOfWindowPackets.Should().Be(1);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    [Fact]
    public void Pump_ARepeatedSequenceWithinTheWindow_RaisesDuplicateByOneAndIsNotAdmittedTwice()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 4);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] datagram = [.. BuildHeader(sessionId), .. BuildRecord(ModeReliableOrdered, sequence: 1, payload: [0xAB])];

        // Act — first arrival admits.
        ulong datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);

        // Act — a retransmission of the exact same record.
        datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);

        // Act — a second retransmission, to prove the counter keeps counting rather than latching.
        datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);

        // Assert
        PeerCounters counters = accepter.Reactor.CountersOf(accepterPeer);
        counters.DuplicatePackets.Should().Be(2);
        counters.OutOfWindowPackets.Should().Be(0);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    [Fact]
    public void Pump_OutOfWindowRecordInOneSpace_LeavesTheOtherThreeSpacesUnaffected()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 4);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        // Act — sequence 100 is unreachably far ahead of the ReliableUnordered Space alone. Uses
        // ReliableUnordered rather than Unreliable: since #173, Unreliable's window slides a far-ahead
        // arrival in rather than discarding it, so only a reliable Space still produces an
        // OutOfWindow discard here.
        byte[] farAheadInOneSpace = [.. BuildHeader(sessionId), .. BuildRecord(ModeReliableUnordered, sequence: 100, payload: [])];
        ulong datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), farAheadInOneSpace);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);

        // Assert — the one discard registered.
        accepter.Reactor.CountersOf(accepterPeer).OutOfWindowPackets.Should().Be(1);

        // Act — sequence 0, still reachable in each of the other three Spaces, sent in one datagram.
        byte[] admittedInTheOtherThree =
        [
            .. BuildHeader(sessionId),
            .. BuildRecord(ModeUnreliable, sequence: 0, payload: []),
            .. BuildRecord(ModeUnreliableSequenced, sequence: 0, payload: []),
            .. BuildRecord(ModeReliableOrdered, sequence: 0, payload: []),
        ];
        datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), admittedInTheOtherThree);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);

        // Assert — every one of the three admitted cleanly: neither counter moved past the single
        // discard the first Space alone produced, proving the other three Spaces' bases and counters
        // were untouched by it.
        PeerCounters counters = accepter.Reactor.CountersOf(accepterPeer);
        counters.OutOfWindowPackets.Should().Be(1);
        counters.DuplicatePackets.Should().Be(0);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    [Fact]
    public void Pump_SequenceExactlyAtTheFarEdgeOfANonSlidingWindow_ReturnsOutOfWindowAndLeavesTheBaseUnmoved()
    {
        // Arrange — a 4-fragment window based at 0 spans [0, 3]; sequence 4 sits exactly one past the
        // last reachable sequence — the off-by-one boundary, as distinct from the unreachably-far
        // sequence 1_000 the far-above-window seam test already covers. ReliableOrdered, because since
        // #173 only a non-sliding Space still refuses a far-edge arrival instead of sliding to admit it.
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 4);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] atTheFarEdge = [.. BuildHeader(sessionId), .. BuildRecord(ModeReliableOrdered, sequence: 4, payload: [])];

        // Act
        ulong datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), atTheFarEdge);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);

        // Assert — discarded, not admitted.
        PeerCounters afterDiscard = accepter.Reactor.CountersOf(accepterPeer);
        afterDiscard.OutOfWindowPackets.Should().Be(1);
        afterDiscard.DuplicatePackets.Should().Be(0);

        // Act — sequence 3, the base's own far edge, still inside the untouched window: if the base had
        // moved, admitting 3 would be impossible and this send would itself count as another discard.
        byte[] stillInsideTheWindow = [.. BuildHeader(sessionId), .. BuildRecord(ModeReliableOrdered, sequence: 3, payload: [])];
        datagramsReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), stillInsideTheWindow);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > datagramsReceivedBefore);

        // Assert — admitted: the discard count is still exactly the one from the far-edge arrival.
        PeerCounters afterAdmit = accepter.Reactor.CountersOf(accepterPeer);
        afterAdmit.OutOfWindowPackets.Should().Be(1);
        afterAdmit.DuplicatePackets.Should().Be(0);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    [Fact]
    public void Pump_EachContiguousSequenceAdmittedInOrder_RetiresItBelowTheBaseImmediately()
    {
        // Arrange — a 4-fragment window based at 0. Each sequence, sent in order with no hole ever
        // opened, should advance the base past itself the instant it admits rather than waiting for a
        // run to finish: resending the same sequence right after it first admits reports OutOfWindow,
        // never Duplicate, which is only true if the base already passed it. ReliableUnordered, so
        // ordering within the Space plays no part in what is being pinned here.
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 4);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        // Act, Assert — 0, 1 and 2 each admit and are immediately retired below the base.
        foreach (uint sequence in new uint[] { 0, 1, 2 })
        {
            byte[] firstArrival = [.. BuildHeader(sessionId), .. BuildRecord(ModeReliableUnordered, sequence, payload: [])];
            ulong before = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), firstArrival);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);

            byte[] resend = firstArrival;
            before = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), resend);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);
        }

        // Assert — every resend classified OutOfWindow, none as Duplicate: the base moved past each
        // sequence before its resend ever arrived.
        PeerCounters counters = accepter.Reactor.CountersOf(accepterPeer);
        counters.OutOfWindowPackets.Should().Be(3);
        counters.DuplicatePackets.Should().Be(0);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    [Fact]
    public void Pump_SequenceArrivingAboveAHole_LeavesTheHoleStillAdmittableAfterwards()
    {
        // Arrange — sequence 1 arrives before sequence 0. If the base had advanced past the hole at 0,
        // sequence 0 arriving next would be OutOfWindow rather than Admitted; it stays Admitted here,
        // which is what proves the base is still sitting at the hole.
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 4);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] one = [.. BuildHeader(sessionId), .. BuildRecord(ModeReliableOrdered, sequence: 1, payload: [])];
        byte[] zero = [.. BuildHeader(sessionId), .. BuildRecord(ModeReliableOrdered, sequence: 0, payload: [])];

        // Act
        ulong before = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), one);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);

        before = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), zero);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);

        // Assert — both admitted, neither discarded nor flagged a duplicate.
        PeerCounters counters = accepter.Reactor.CountersOf(accepterPeer);
        counters.OutOfWindowPackets.Should().Be(0);
        counters.DuplicatePackets.Should().Be(0);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    [Fact]
    public void Pump_ArrivalFillingAHole_AdvancesTheBaseAcrossEveryAlreadyAdmittedSequenceAboveItInOneStep()
    {
        // Arrange — an 8-fragment window. 1, 2 and 3 arrive first and sit behind the hole at 0, each
        // admitting on its own but leaving the base at 0.
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 8, maxMessageSize: 8192);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        foreach (uint sequence in new uint[] { 1, 2, 3 })
        {
            byte[] datagram = [.. BuildHeader(sessionId), .. BuildRecord(ModeReliableOrdered, sequence, payload: [])];
            ulong before = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);
        }

        accepter.Reactor.CountersOf(accepterPeer).OutOfWindowPackets.Should().Be(0);
        accepter.Reactor.CountersOf(accepterPeer).DuplicatePackets.Should().Be(0);

        // Act — filling the hole at 0 should carry the base past 0, 1, 2 and 3 in this one arrival.
        byte[] zero = [.. BuildHeader(sessionId), .. BuildRecord(ModeReliableOrdered, sequence: 0, payload: [])];
        ulong beforeFill = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), zero);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > beforeFill);

        accepter.Reactor.CountersOf(accepterPeer).OutOfWindowPackets.Should().Be(0);
        accepter.Reactor.CountersOf(accepterPeer).DuplicatePackets.Should().Be(0);

        // Act — resending any of 0, 1, 2 or 3 now reports OutOfWindow, not Duplicate: the whole run is
        // below the base in one step, not merely admitted-and-held.
        foreach (uint sequence in new uint[] { 0, 1, 2, 3 })
        {
            byte[] resend = [.. BuildHeader(sessionId), .. BuildRecord(ModeReliableOrdered, sequence, payload: [])];
            ulong before = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), resend);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);
        }

        // Assert
        PeerCounters counters = accepter.Reactor.CountersOf(accepterPeer);
        counters.OutOfWindowPackets.Should().Be(4);
        counters.DuplicatePackets.Should().Be(0);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    [Fact]
    public void Pump_APeerReusingASlotAfterAnEarlierPeerClosed_ClassifiesFromACleanWindow()
    {
        // Arrange — one Slot total, so the second Peer's handshake can only complete by reusing the
        // first Peer's freed Slot (issue #180): the arena's admission bytes for that Slot outlive the
        // first Peer, and this is what proves a reused Slot does not inherit what the first Peer had
        // already admitted there.
        using DatagramSessionReactorHarness accepter = new(capacity: 1, receiveWindowFragments: 4);
        using RawReactor firstClient = new(new ReactorOptions { Capacity = 4 });
        LinkId firstClientLink = firstClient.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint firstSessionId = EstablishLiveSessionId(accepter, firstClient, firstClientLink);
        PeerId firstPeer = FindAccepterPeer(accepter);

        byte[] datagram = [.. BuildHeader(firstSessionId), .. BuildRecord(ModeReliableOrdered, sequence: 0, payload: [])];
        ulong before = accepter.Reactor.Counters.DatagramsReceived;
        firstClient.SendTo(firstClientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);
        accepter.Reactor.CountersOf(firstPeer).OutOfWindowPackets.Should().Be(0);
        accepter.Reactor.CountersOf(firstPeer).DuplicatePackets.Should().Be(0);

        // Act — the first Peer closes, freeing the one Slot, and a second Peer's handshake takes it.
        accepter.Reactor.Close(firstPeer);
        accepter.Reactor.Pump();
        accepter.DrainToList();

        using RawReactor secondClient = new(new ReactorOptions { Capacity = 4 });
        LinkId secondClientLink = secondClient.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint secondSessionId = EstablishLiveSessionId(accepter, secondClient, secondClientLink);
        PeerId secondPeer = FindAccepterPeer(accepter);

        // Act — sequence 0 again, this time from the second Peer. If the reused Slot's window still
        // carried the first Peer's admission, this would classify as a Duplicate rather than Admitted.
        byte[] sameSequenceFromTheSecondPeer =
            [.. BuildHeader(secondSessionId), .. BuildRecord(ModeReliableOrdered, sequence: 0, payload: [])];
        before = accepter.Reactor.Counters.DatagramsReceived;
        secondClient.SendTo(secondClientLink, Source.FromIPEndPoint(accepter.Endpoint), sameSequenceFromTheSecondPeer);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);

        // Assert — admitted cleanly: neither a duplicate nor an out-of-window discard.
        PeerCounters secondPeerCounters = accepter.Reactor.CountersOf(secondPeer);
        secondPeerCounters.OutOfWindowPackets.Should().Be(0);
        secondPeerCounters.DuplicatePackets.Should().Be(0);
        accepter.Sink.Received.Should().BeEmpty();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind != SessionEventKind.MessageReceived,
            "an admitted record now delivers a Message (#217, #219); no other event is justified here");
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> — a bare
    /// <see cref="RawReactor"/> — and returns the wire session id <paramref name="accepter"/> assigned
    /// this Peer, mirroring <see cref="DatagramSessionDataDatagramParseTests"/>'s helper of the same
    /// name.
    /// </summary>
    private static uint EstablishLiveSessionId(DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink)
    {
        const ulong clientNonce = 0x0102_0304_0506_0708UL;
        Source destination = Source.FromIPEndPoint(accepter.Endpoint);

        byte[] hello = BuildHelloDatagram(clientNonce);
        client.SendTo(clientLink, destination, hello);
        byte[] cookieDatagram = ExchangeUntilReceived(client, accepter);

        ulong echoedNonce = BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset, 8));
        long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset + 8, 8)));
        byte[] cookie = cookieDatagram.AsSpan(ControlBodyOffset + 16, CookieLength).ToArray();
        echoedNonce.Should().Be(clientNonce);

        byte[] helloWithCookie = BuildHelloWithCookieDatagram(clientNonce, epoch, cookie);
        client.SendTo(clientLink, destination, helloWithCookie);
        byte[] acceptDatagram = ExchangeUntilReceived(client, accepter);

        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
    }

    /// <summary>
    /// The <see cref="PeerId"/> the accepter assigned the one Peer this test's handshake produced,
    /// read off the drain so a test can call <see cref="DatagramSessionReactor.CountersOf"/> on it.
    /// </summary>
    private static PeerId FindAccepterPeer(DatagramSessionReactorHarness accepter)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();
        PeerId peer = drained.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

        // Establishment itself logs DiagnosticId.LingerNegotiated (issue #364); every test in this
        // file wants a clean Sink for the arrivals it is actually testing.
        accepter.Sink.Received.Clear();
        return peer;
    }

    private static byte[] ExchangeUntilReceived(RawReactor client, DatagramSessionReactorHarness accepter)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            accepter.Reactor.Pump();
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
        byte[] body = new byte[4 + 8 + 8 + CookieLength + 4 + 4 + 8 + 8 + ResumeSecretLength];
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
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), DialerLingerMilliseconds);
        offset += 8;
        return BuildControlDatagram(HelloWithCookieType, body);
    }

    /// <summary>Builds a 20-byte live-Session datagram header naming <paramref name="session"/> with both ack blocks zeroed.</summary>
    private static byte[] BuildHeader(uint session)
    {
        byte[] header = new byte[DatagramHeaderSize];
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0, 4), session);
        return header;
    }

    /// <summary>Builds one unfragmented packet record: flags, a four-byte sequence, a two-byte length, then the payload.</summary>
    private static byte[] BuildRecord(byte modeBits, uint sequence, ReadOnlySpan<byte> payload)
    {
        const int unfragmentedRecordSize = 7;
        byte[] record = new byte[unfragmentedRecordSize + payload.Length];
        record[0] = modeBits;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        payload.CopyTo(record.AsSpan(unfragmentedRecordSize));
        return record;
    }
}
