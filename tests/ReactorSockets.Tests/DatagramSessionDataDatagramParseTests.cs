using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The datagram parse loop on the Reactor's data path. A datagram naming a live Session is
/// parsed header-first, then packet records until the datagram is exhausted, exactly as ADR 0009
/// fixes the wire. Every malformed shape here — a short header, a spare bit set, a
/// record whose length runs past the end, or truncation inside a record — must discard the
/// <em>whole</em> datagram and log <see cref="DiagnosticId.DatagramMalformed"/>, the same treatment
/// <see cref="DatagramSessionMalformedInputTests"/> already proves for the control path. Most of this
/// file ends at "the datagram parsed, or it did not", asserting the drain stays empty whatever the
/// input. A record can also legally be a Fragment, so the tests for that shape also drain and check
/// that an unfragmented record's payload still reaches the consumer alongside a well-formed Fragment,
/// which is held rather than delivered.
/// </summary>
/// <remarks>
/// A live Session id is not exposed by the public surface — <see cref="PeerId"/> is an opaque handle,
/// not the wire session id a remote stamps on datagrams. So each test first completes an ordinary
/// handshake by hand, using nothing but <see cref="RawReactor"/> (the same instrument
/// <see cref="HostilePeer"/> is built from) to play the dialling end, and reads the wire session id the
/// accepter assigned straight out of its Accept reply — the same bytes any real dialler would read. From
/// that point every data datagram sent is a hand-built byte array, as every other malformed-input test in
/// this suite builds one.
/// </remarks>
public class DatagramSessionDataDatagramParseTests
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
    private const int UnfragmentedRecordSize = 7;
    private const int FragmentRecordSize = 11;

    private const byte ModeUnreliable = 0b0000_0000;
    private const byte ModeUnreliableSequenced = 0b0000_0001;
    private const byte ModeReliableUnordered = 0b0000_0010;
    private const byte ModeReliableOrdered = 0b0000_0011;
    private const byte FragmentBit = 0b0000_0100;

    // 0b0000_1000 is now DatagramSessionProtocol.ControlBit (issue #339), not a spare bit — moved to
    // the next bit up so this still proves the general "unknown bit discards" rule.
    private const byte OneSpareBit = 0b0001_0000;

    [Fact]
    public void Pump_WellFormedHeaderAndSeveralRecords_ParsesToExhaustionAndIsNotReportedMalformed()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        ulong receivedBefore = accepter.Reactor.Counters.DatagramsReceived;

        byte[] datagram =
        [
            .. BuildHeader(sessionId),
            .. BuildRecord(ModeUnreliable, sequence: 1, payload: [0xAA, 0xBB, 0xCC, 0xDD]),
            .. BuildRecord(ModeUnreliableSequenced, sequence: 2, payload: []),
            .. BuildRecord(ModeReliableOrdered, sequence: 3, payload: [0x01, 0x02]),
            .. BuildRecord(ModeReliableUnordered, sequence: 4, payload: [0xFF]),
        ];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > receivedBefore);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> further = accepter.PumpAndDrainAll(3);

        // Assert — the datagram was taken from the transport, never logged as malformed, and no event
        // ever appears: this test ends at "the datagram parsed", not at admission.
        accepter.Reactor.Counters.DatagramsReceived.Should().BeGreaterThan(receivedBefore);
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        further.Should().BeEmpty();
    }

    [Fact]
    public void Pump_HeaderOnlyDatagramWithNoRecords_ParsesToExhaustionAndIsNotReportedMalformed()
    {
        // Arrange — zero records is not a violation: a datagram is a container of "as many records as
        // fit", and fitting zero is legal.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        ulong receivedBefore = accepter.Reactor.Counters.DatagramsReceived;

        byte[] datagram = BuildHeader(sessionId);

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > receivedBefore);

        // Assert
        accepter.Reactor.Counters.DatagramsReceived.Should().BeGreaterThan(receivedBefore);
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
    }

    [Fact]
    public void Pump_DatagramTruncatedInsideTheHeader_IsDiscardedWithDatagramMalformed()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        // A header is 20 bytes; this stops mid-header at 12, past the session id but short of both ack
        // blocks.
        byte[] datagram = BuildHeader(sessionId)[..12];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.MalformedSessionDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: datagram.Length);
    }

    [Fact]
    public void Pump_DatagramTruncatedInsideARecord_DiscardsTheWholeDatagramAndEarlierRecordsTakeNoEffect()
    {
        // Arrange — one complete, well-formed record ahead of a second record cut off mid-flags-plus-
        // sequence: if the first record were admitted independently there would be evidence of it, and
        // there must be none.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] wellFormedRecord = BuildRecord(ModeReliableOrdered, sequence: 1, payload: [0x11, 0x22, 0x33]);
        byte[] truncatedSecondRecord = [ModeUnreliable, 0x00, 0x00]; // flags + 2 of the 4 sequence bytes

        byte[] datagram = [.. BuildHeader(sessionId), .. wellFormedRecord, .. truncatedSecondRecord];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.MalformedSessionDatagrams > 0);

        // Assert — exactly one discard for the whole datagram, not one per record.
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: datagram.Length);
    }

    [Fact]
    public void Pump_LastRecordsLengthRunsPastTheEndOfTheDatagram_DiscardsTheWholeDatagram()
    {
        // Arrange — a well-formed record followed by one whose declared length claims more bytes than
        // the datagram actually carries.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] wellFormedRecord = BuildRecord(ModeUnreliable, sequence: 7, payload: [0xAA]);
        byte[] overrunRecordHeader = new byte[UnfragmentedRecordSize];
        overrunRecordHeader[0] = ModeReliableUnordered;
        BinaryPrimitives.WriteUInt32BigEndian(overrunRecordHeader.AsSpan(1, 4), 8u);
        BinaryPrimitives.WriteUInt16BigEndian(overrunRecordHeader.AsSpan(5, 2), 100); // claims 100 bytes, none follow

        byte[] datagram = [.. BuildHeader(sessionId), .. wellFormedRecord, .. overrunRecordHeader];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.MalformedSessionDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: datagram.Length);
    }

    [Fact]
    public void Pump_RecordWithASpareBitSet_DiscardsTheWholeDatagramAndEarlierRecordsTakeNoEffect()
    {
        // Arrange — a well-formed record ahead of one with a spare bit set. Unknown flag bits are a
        // protocol violation, never something to ignore.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] wellFormedRecord = BuildRecord(ModeReliableOrdered, sequence: 1, payload: [0x01]);
        byte[] spareBitRecord = BuildRecord(ModeUnreliable, sequence: 2, payload: [], extraFlags: OneSpareBit);

        byte[] datagram = [.. BuildHeader(sessionId), .. wellFormedRecord, .. spareBitRecord];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.MalformedSessionDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: datagram.Length);
    }

    [Fact]
    public void Pump_RecordWithAWellFormedFragmentShape_ParsesSuccessfullyAndDeliversOnlyTheUnfragmentedRecord()
    {
        // Arrange — the fragment shape reads (issue #252): a record with the bit set and a complete
        // 11-byte header no longer discards the datagram, and an earlier well-formed record still takes
        // effect. Since issue #257, an admitted Fragment is held rather than delivered — it is a piece
        // of a Message, not a Message — so only the unfragmented record reaches the consumer here.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] wellFormedRecord = BuildRecord(ModeUnreliableSequenced, sequence: 5, payload: [0x9A, 0x9B]);
        byte[] fragmentRecord = BuildFragmentRecord(
            ModeReliableUnordered, sequence: 0, fragmentIndex: 0, fragmentCount: 2, payload: [0x01]);

        byte[] datagram = [.. BuildHeader(sessionId), .. wellFormedRecord, .. fragmentRecord];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > 0);
        List<byte[]> payloads = accepter.DrainPayloads();

        // Assert — neither record is discarded, but the Fragment is held rather than delivered (#257).
        // Whether the Fragment's index and count describe a legal set is #253's question, not this one's.
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        payloads.Should().BeEquivalentTo(
            new[] { new byte[] { 0x9A, 0x9B } },
            "the well-formed record parses and delivers, while the admitted Fragment is held (#257)");
    }

    [Fact]
    public void Pump_DatagramOfOnlyFragmentRecords_ParsesAndDeliversTheAssembledMessage()
    {
        // Arrange — one datagram carrying nothing but Fragments, the case between "only
        // unfragmented" and "mixed". A completed set is delivered as one assembled Message, concatenated in fragment-index order.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] datagram =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 0, fragmentIndex: 0, fragmentCount: 3, payload: [0x01]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 1, fragmentIndex: 1, fragmentCount: 3, payload: [0x02, 0x02]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 2, fragmentIndex: 2, fragmentCount: 3, payload: [0x03, 0x03, 0x03]),
        ];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > 0);
        List<byte[]> payloads = accepter.DrainPayloads();

        // Assert — parses cleanly, and the completed set delivers as one assembled Message: the three
        // Fragments' payloads concatenated in fragment-index order.
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        payloads.Should().BeEquivalentTo(
            new[] { new byte[] { 0x01, 0x02, 0x02, 0x03, 0x03, 0x03 } },
            "the three-Fragment set completes and is delivered whole");
    }

    [Fact]
    public void Pump_DatagramMixingUnfragmentedAndFragmentRecords_ParsesEveryRecordAndDeliversOnlyTheUnfragmentedOnes()
    {
        // Arrange — the mixed case, the one a naive parser gets
        // wrong: unfragmented records and Fragments interleaved, each carrying a payload distinct enough
        // that a record misattributed to the wrong shape's byte count would show up as a missing,
        // duplicated or corrupted payload rather than a passing test. Every Fragment here is reliable
        // (ADR 0004: fragmentation is reliable-only); the two unfragmented records at sequences 2 and 5
        // still exercise a second mode, including plain Unreliable, since nothing about ADR 0004 restricts
        // an unfragmented record's mode. The two Fragments' groups (firstSeq 1 and firstSeq 3) are
        // deliberately far enough apart that their declared ranges — [1,2] and [3,4] — do not overlap:
        // #256's declared-range check would otherwise (rightly) reject this datagram. Since issue #257,
        // an admitted Fragment is held rather than delivered, so only the three unfragmented records'
        // payloads reach the consumer.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] datagram =
        [
            .. BuildHeader(sessionId),
            .. BuildRecord(ModeReliableUnordered, sequence: 0, payload: [0xA0]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 1, fragmentIndex: 0, fragmentCount: 2, payload: [0xB1]),
            .. BuildRecord(ModeReliableUnordered, sequence: 2, payload: [0xA2]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 3, fragmentIndex: 0, fragmentCount: 2, payload: [0xB3]),
            .. BuildRecord(ModeUnreliable, sequence: 5, payload: [0xE5]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 6, fragmentIndex: 0, fragmentCount: 2, payload: [0xF6]),
        ];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > 0);
        List<byte[]> payloads = accepter.DrainPayloads();

        // Assert — every record parsed; only the unfragmented records' payloads reached the consumer,
        // each exactly once, while the admitted Fragments are held (#257).
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        payloads.Should().BeEquivalentTo(
            new[] { new byte[] { 0xA0 }, new byte[] { 0xA2 }, new byte[] { 0xE5 } });
    }

    [Fact]
    public void Pump_FragmentRecordWithTheFullElevenByteHeaderAndNoPayload_ParsesSuccessfully()
    {
        // Arrange — pins the Fragment shape's header at exactly 11 bytes: flags(1) + seq(4) + length(2)
        // + fragIndex(2) + fragCount(2), against the unfragmented shape's 7 the sibling test below
        // pins.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] fragmentHeader = BuildFragmentRecord(
            ModeReliableOrdered, sequence: 0, fragmentIndex: 0, fragmentCount: 2, payload: []);
        fragmentHeader.Length.Should().Be(FragmentRecordSize);

        byte[] datagram = [.. BuildHeader(sessionId), .. fragmentHeader];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > 0);

        // Assert
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
    }

    [Fact]
    public void Pump_FragmentRecordOneByteShortOfItsElevenByteHeader_DiscardsTheWholeDatagram()
    {
        // Arrange — one byte short of the Fragment shape's 11-byte header (the fragment count's low
        // byte is missing). An unfragmented record this short would still be four bytes over its own
        // 7-byte requirement, so this only fails because the Fragment shape, not the unfragmented one,
        // is what the flags byte claims.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] wellFormedRecord = BuildRecord(ModeUnreliable, sequence: 1, payload: [0x11]);
        byte[] truncatedFragmentHeader = BuildFragmentRecord(
            ModeReliableOrdered, sequence: 0, fragmentIndex: 0, fragmentCount: 2, payload: [])[..(FragmentRecordSize - 1)];

        byte[] datagram = [.. BuildHeader(sessionId), .. wellFormedRecord, .. truncatedFragmentHeader];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.MalformedSessionDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: datagram.Length);
    }

    [Fact]
    public void Pump_FragmentRecordWithASpareBitSet_DiscardsTheWholeDatagramAndEarlierRecordsTakeNoEffect()
    {
        // Arrange — a spare bit still fails the read on the Fragment shape exactly as it does on the
        // unfragmented one; only the fragment bit itself stopped being a violation.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] wellFormedRecord = BuildRecord(ModeReliableOrdered, sequence: 1, payload: [0x01]);
        byte[] spareBitFragmentRecord = BuildFragmentRecord(
            ModeUnreliable, sequence: 2, fragmentIndex: 0, fragmentCount: 2, payload: [], extraFlags: OneSpareBit);

        byte[] datagram = [.. BuildHeader(sessionId), .. wellFormedRecord, .. spareBitFragmentRecord];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.MalformedSessionDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: datagram.Length);
    }

    [Fact]
    public void Pump_FragmentRecordWithIndexAtOrBeyondItsCount_DiscardsTheWholeDatagram()
    {
        // Arrange — issue #253: an index equal to its count names a piece one past the end of the set
        // it claims, which is impossible for any Fragment we would ever send.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] wellFormedRecord = BuildRecord(ModeUnreliable, sequence: 1, payload: [0x11]);
        byte[] outOfRangeFragmentRecord = BuildFragmentRecord(
            ModeReliableOrdered, sequence: 0, fragmentIndex: 2, fragmentCount: 2, payload: [0x22]);

        byte[] datagram = [.. BuildHeader(sessionId), .. wellFormedRecord, .. outOfRangeFragmentRecord];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.MalformedSessionDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: datagram.Length);
    }

    [Fact]
    public void Pump_FragmentRecordWithCountOfZero_DiscardsTheWholeDatagram()
    {
        // Arrange — issue #253: a count of zero has no index that could describe a piece of it.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] wellFormedRecord = BuildRecord(ModeUnreliable, sequence: 1, payload: [0x11]);
        byte[] zeroCountFragmentRecord = BuildFragmentRecord(
            ModeReliableOrdered, sequence: 0, fragmentIndex: 0, fragmentCount: 0, payload: [0x22]);

        byte[] datagram = [.. BuildHeader(sessionId), .. wellFormedRecord, .. zeroCountFragmentRecord];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.MalformedSessionDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: datagram.Length);
    }

    [Fact]
    public void Pump_FragmentRecordWithCountOfOne_DiscardsTheWholeDatagram()
    {
        // Arrange — issue #253: a one-piece Message is unfragmented and should never have carried the
        // fragment shape at all, so a count of one is refused rather than treated as a degenerate but
        // legal set.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] wellFormedRecord = BuildRecord(ModeUnreliable, sequence: 1, payload: [0x11]);
        byte[] oneCountFragmentRecord = BuildFragmentRecord(
            ModeReliableOrdered, sequence: 0, fragmentIndex: 0, fragmentCount: 1, payload: [0x22]);

        byte[] datagram = [.. BuildHeader(sessionId), .. wellFormedRecord, .. oneCountFragmentRecord];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.MalformedSessionDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: datagram.Length);
    }

    [Fact]
    public void Pump_FragmentRecordsAtTheValidBoundaries_AllParseSuccessfully()
    {
        // Arrange — issue #253's boundaries either side of the refused combinations: a count of two at
        // both its legal indexes, an index one below its count, and a count at this Reactor's own legal
        // maximum. Each is asserted rather than assumed, so a future change to the refusal cannot
        // silently start rejecting a legal Fragment. Each boundary case travels in its own datagram
        // (issue #256): the second and third cases' declared ranges genuinely overlap each other and the
        // two-Fragment group above when packed into one datagram together — a real declared-range
        // collision #256's overlap check is right to reject, not a reason to weaken it.
        //
        // The former boundary values here — index 999 of count 1000, and a lone Fragment of count 65535
        // — predate issue #282: a declared count is now also refused, with a Peer close rather than a
        // parse failure, once it exceeds this Reactor's own ReceiveWindowFloor (4, from the default
        // MaxMessageSize 4096 over FragmentSize 1200). Both former values sit far past that floor and
        // would now close the Peer before reaching this test's own boundary, so they are replaced with
        // values that keep the same shape — "an index one below its count" and "a count at the maximum
        // this Reactor allows" — while staying legal under #282 too.
        //
        // Issue #264 reworked the bound from the floor to DatagramSessionOptions.ResolvedReceiveWindowFragments,
        // which a harness may widen independently of MaxMessageSize. The index-999-of-count-1000 case is
        // restored under that wider bound in Pump_FragmentIndex999OfDeclaredCount1000WithinAWidenedReceiveWindow_ParsesSuccessfully
        // below, at a receive window and memory bill affordable enough to run in this suite. The count-65535
        // case is not proved anywhere: PacketRecordCodec is internal and ADR 0015 forbids reaching it
        // directly via InternalsVisibleTo, and #264's own bound closes the Peer at any receive window this
        // suite can afford long before a count of 65535 could ever be legal, so there is no route back to
        // it through the live parse path either.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        // Act
        List<byte[]> payloads = [];
        payloads.AddRange(SendOneDatagramAndDrain(
            accepter, client, clientLink, sessionId,
            BuildFragmentRecord(ModeReliableUnordered, sequence: 0, fragmentIndex: 0, fragmentCount: 2, payload: [0x01]),
            BuildFragmentRecord(ModeReliableUnordered, sequence: 1, fragmentIndex: 1, fragmentCount: 2, payload: [0x02])));
        payloads.AddRange(SendOneDatagramAndDrain(
            accepter, client, clientLink, sessionId,
            BuildFragmentRecord(ModeReliableUnordered, sequence: 2, fragmentIndex: 2, fragmentCount: 3, payload: [0x03])));
        payloads.AddRange(SendOneDatagramAndDrain(
            accepter, client, clientLink, sessionId,
            BuildFragmentRecord(ModeReliableUnordered, sequence: 3, fragmentIndex: 0, fragmentCount: 4, payload: [0x04])));

        // Assert — parses cleanly at every boundary; only the two-Fragment set actually completes (the
        // 3- and 4-piece sets each send only one of their pieces, so neither ever completes and neither
        // delivers).
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        payloads.Should().BeEquivalentTo(
            new[] { new byte[] { 0x01, 0x02 } },
            "only the two-Fragment set at sequences 0-1 ever completes");
    }

    [Fact]
    public void Pump_FragmentIndex999OfDeclaredCount1000WithinAWidenedReceiveWindow_ParsesSuccessfully()
    {
        // Arrange — issue #253's original count-1000 boundary, retired from the test above when #282
        // landed because the default ReceiveWindowFloor (4) could never admit it. Restored here under
        // #264's rework of the bound from the floor to DatagramSessionOptions.ResolvedReceiveWindowFragments,
        // which a harness may widen independently of MaxMessageSize: at Capacity 1, FragmentSize 576 (the
        // RFC 791 minimum) and ReceiveWindowFragments 1000, the memory bill
        // (DatagramSessionOptions.ReceiveBudgetBytes) is Capacity(1) x 1000 x (2 reliable Spaces x
        // (576 + 2 length bytes) + 4 admission bytes) + Capacity(1) x 2 x 500 pending-group entries x 6
        // bytes = 1,166,000 bytes — well under a megabyte and a quarter, so the boundary is affordable
        // again through the live parse path (PacketRecordCodec stays internal per ADR 0015).
        using DatagramSessionReactorHarness accepter = new(capacity: 1, fragmentSize: 576, receiveWindowFragments: 1000);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        // Act — a lone Fragment at index 999 of a declared count of 1000: individually legal (#253:
        // index 999 < count 1000) and exactly at the widened #264 bound (1000 <= ResolvedReceiveWindowFragments),
        // so it is expected to parse and be admitted into its receive-window slot rather than close the
        // Peer or fail to parse. Only this one piece of the declared 1000 is ever sent, so the set never
        // completes and nothing is delivered.
        List<byte[]> payloads = SendOneDatagramAndDrain(
            accepter, client, clientLink, sessionId,
            BuildFragmentRecord(ModeReliableOrdered, sequence: 0, fragmentIndex: 999, fragmentCount: 1000, payload: [0x01]));

        // Assert — parses cleanly, no violation, and delivers nothing (only one of 1000 declared pieces
        // was ever sent).
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.OversizedFragmentSet);
        payloads.Should().BeEmpty("only one of the declared 1000 pieces was ever sent");
    }

    /// <summary>
    /// Builds one datagram from <paramref name="records"/> behind <paramref name="sessionId"/>'s header,
    /// sends it, pumps until the accepter has received it, and returns the payloads drained. Lets a test
    /// drive several boundary cases each in their own datagram without repeating the send-pump-drain
    /// dance for every one (issue #256's split of what used to be one datagram).
    /// </summary>
    private static List<byte[]> SendOneDatagramAndDrain(
        DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink, uint sessionId, params byte[][] records)
    {
        List<byte> datagram = [.. BuildHeader(sessionId)];
        foreach (byte[] record in records)
        {
            datagram.AddRange(record);
        }

        ulong receivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), [.. datagram]);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > receivedBefore);
        return accepter.DrainPayloads();
    }

    [Fact]
    public void Pump_WellFormedFragmentSetsCompletePartialAndNonOverlapping_CompleteSetsAndTheUnfragmentedRecordDeliver()
    {
        // Arrange — issue #256's positive cases in one datagram: a complete contiguous 3-Fragment set
        // (sequences 100-102), an unfragmented record between the groups, a partial set contiguous only
        // as far as it goes (2 of a declared 4, sequences 200-201, indices 2 and 3 simply absent from
        // this datagram), and a second, wholly separate 2-Fragment set (sequences 300-301) whose range
        // does not come near either other group's. None of this is a violation: #256 only refuses a gap
        // within one claimed group's own indices or two groups whose declared ranges overlap, and none
        // of these three groups' declared ranges — [100,102], [200,203], [300,301] — come near each
        // other. Every Fragment is ReliableUnordered (ADR 0004: fragmentation is reliable-only), so the
        // receive window is sized generously above the highest sequence used here — the window's own
        // verdict is not what this test is about. Since issue #259, each completed set is delivered as
        // one assembled Message, so the two complete sets and the one unfragmented record all reach the
        // consumer; the still-partial set (200-201 of a declared 4) delivers nothing.
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 400);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] datagram =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 100, fragmentIndex: 0, fragmentCount: 3, payload: [0x01]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 101, fragmentIndex: 1, fragmentCount: 3, payload: [0x02]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 102, fragmentIndex: 2, fragmentCount: 3, payload: [0x03]),
            .. BuildRecord(ModeUnreliable, sequence: 103, payload: [0x04]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 200, fragmentIndex: 0, fragmentCount: 4, payload: [0x05]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 201, fragmentIndex: 1, fragmentCount: 4, payload: [0x06]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 300, fragmentIndex: 0, fragmentCount: 2, payload: [0x07]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 301, fragmentIndex: 1, fragmentCount: 2, payload: [0x08]),
        ];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > 0);
        List<byte[]> payloads = accepter.DrainPayloads();

        // Assert — every group parses; the two complete sets assemble and the unfragmented record
        // delivers, while the still-partial set (200-201) delivers nothing.
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        payloads.Should().BeEquivalentTo(
            new[] { new byte[] { 0x01, 0x02, 0x03 }, new byte[] { 0x04 }, new byte[] { 0x07, 0x08 } },
            "the 100-102 and 300-301 sets complete and assemble, the unfragmented record at 103 delivers " +
            "as itself, and the still-partial 200-201 set delivers nothing");
    }

    [Fact]
    public void Pump_ReliableOrderedAndReliableUnorderedFragmentGroupsStartingAtTheSameSequence_ParsesFullyForBothGroups()
    {
        // Arrange — issue #47/#256 false-positive fix: ReceiveWindowSpaces keeps one independent Space
        // per PacketRecordMode (ADR 0009), so a ReliableOrdered group and a ReliableUnordered group both
        // starting at firstSeq 0 are not the same group at all — they live in different Spaces and can
        // never collide. FragmentSetsAreWellFormed must key group identity on (mode, firstSequence), not
        // firstSequence alone, or it folds these two legitimate groups into one and sees index 0 claimed
        // twice, discarding the whole datagram. Since issue #259, each group is its own complete set and
        // is delivered as one assembled Message, so the proof this is not a group conflict is that both
        // groups reach the consumer independently, not merely that the datagram parses.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] datagram =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 0, fragmentIndex: 0, fragmentCount: 2, payload: [0x01]),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 1, fragmentIndex: 1, fragmentCount: 2, payload: [0x02]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 0, fragmentIndex: 0, fragmentCount: 2, payload: [0x03]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 1, fragmentIndex: 1, fragmentCount: 2, payload: [0x04]),
        ];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > 0);
        List<byte[]> payloads = accepter.DrainPayloads();

        // Assert — this is not a group conflict: both groups parse and both deliver, independently.
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        payloads.Should().BeEquivalentTo(
            new[] { new byte[] { 0x01, 0x02 }, new byte[] { 0x03, 0x04 } },
            "the ReliableOrdered group assembles to 0x01,0x02 and the ReliableUnordered group to 0x03,0x04, " +
            "each delivered from its own independent Space");
    }

    [Fact]
    public void Pump_FragmentGroupsInDifferentModesWithDeclaredRangesThatWouldOverlapIgnoringMode_ParsesFullyForBothGroups()
    {
        // Arrange — issue #47/#256 false-positive fix: a ReliableOrdered group declaring range [0,4]
        // and a ReliableUnordered group declaring range [2,6] cannot overlap — they are different
        // Spaces (ReceiveWindowSpaces, ADR 0009) — even though the raw sequence numbers collide.
        // NoTwoDeclaredRangesOverlap must only ever compare two groups in the same mode. Only two of
        // each group's five declared Fragments are actually sent (issue #256: a partial set contiguous
        // only as far as it goes is not itself a violation). Since issue #257, neither group's Fragments
        // deliver — they are held — so the proof this is not a violation is that the datagram parses
        // cleanly at all, not what reaches the consumer.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] datagram =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 0, fragmentIndex: 0, fragmentCount: 5, payload: [0x01]),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 1, fragmentIndex: 1, fragmentCount: 5, payload: [0x02]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 2, fragmentIndex: 0, fragmentCount: 5, payload: [0x03]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 3, fragmentIndex: 1, fragmentCount: 5, payload: [0x04]),
        ];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > 0);
        List<byte[]> payloads = accepter.DrainPayloads();

        // Assert — the declared ranges [0,4] and [2,6] would overlap if mode were ignored, but they
        // belong to different Spaces, so this is not a violation.
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        payloads.Should().BeEmpty("declared ranges can only collide within one mode's Space, and every record here is a held Fragment (#257)");
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> — a bare
    /// <see cref="RawReactor"/>, exactly what <see cref="HostilePeer"/> is built from — and returns the
    /// wire session id <paramref name="accepter"/> assigned this Peer: the value a real dialler would
    /// read out of the Accept and stamp on every data datagram addressed here afterwards.
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

        // Accept's body is clientNonce(8), sessionIdForMe(4), maxMessageSize(4), resumeSecret(16) — the
        // session id at offset ControlBodyOffset + 8 is what accepter assigned itself, which is what
        // every data datagram addressed to it from here on must carry.
        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
    }

    /// <summary>
    /// Alternately Pumps <paramref name="accepter"/> and <paramref name="client"/> until
    /// <paramref name="client"/> receives one datagram, or the deadline expires — the raw-client half
    /// of the same wait pattern <see cref="DatagramSessionReactorHarness.PumpBothUntil"/> uses for two
    /// full Reactors.
    /// </summary>
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
        // The remaining ResumeSecretLength bytes are the dialler's resume secret; an all-zero one is
        // fine, since only a Resume reads it and these tests send none.
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
    private static byte[] BuildRecord(byte modeBits, uint sequence, ReadOnlySpan<byte> payload, byte extraFlags = 0)
    {
        byte[] record = new byte[UnfragmentedRecordSize + payload.Length];
        record[0] = (byte)(modeBits | extraFlags);
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        payload.CopyTo(record.AsSpan(UnfragmentedRecordSize));
        return record;
    }

    /// <summary>
    /// Builds one Fragment record: the unfragmented shape's bytes with the fragment bit set, followed
    /// by a two-byte fragment index and a two-byte fragment count, then the payload. <paramref
    /// name="extraFlags"/> is ORed in alongside the fragment bit so a test can additionally set a
    /// spare bit without building the header by hand.
    /// </summary>
    private static byte[] BuildFragmentRecord(
        byte modeBits, uint sequence, ushort fragmentIndex, ushort fragmentCount, ReadOnlySpan<byte> payload, byte extraFlags = 0)
    {
        byte[] record = new byte[FragmentRecordSize + payload.Length];
        record[0] = (byte)(modeBits | FragmentBit | extraFlags);
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(7, 2), fragmentIndex);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(9, 2), fragmentCount);
        payload.CopyTo(record.AsSpan(FragmentRecordSize));
        return record;
    }

    /// <summary>
    /// The shared assertion every malformed-shape test makes: exactly one
    /// <see cref="DiagnosticId.DatagramMalformed"/> record reached the Sink, the drain is empty, and the
    /// discard counter rose by exactly one. Every datagram in this file names the live Session
    /// <see cref="EstablishLiveSessionId"/> set up, so the counter is
    /// <see cref="ReactorCounters.MalformedSessionDatagrams"/> and not
    /// <see cref="ReactorCounters.UnknownSourceDatagrams"/> (issue #275) — which is the one thing this
    /// helper no longer shares with <see cref="DatagramSessionMalformedInputTests"/>'s helper of the
    /// same name, whose datagrams name no Session.
    /// </summary>
    private static void AssertOneMalformedDiscard(DatagramSessionReactorHarness accepter, int expectedBytesReceived)
    {
        accepter.DrainToList().Should().BeEmpty();
        accepter.Reactor.Counters.MalformedSessionDatagrams.Should().Be(1);

        DiagnosticRecord record = accepter.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.DatagramMalformed)
            .Which;

        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
        record.Value0.Should().Be(expectedBytesReceived);
    }
}
