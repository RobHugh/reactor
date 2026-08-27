using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #163: the datagram parse loop on the Reactor's data path. A datagram naming a live Session is
/// parsed header-first, then packet records until the datagram is exhausted, exactly as the parent
/// slice spec (#39) fixes the wire. Every malformed shape here — a short header, a spare bit set, a
/// record whose length runs past the end, or truncation inside a record — must discard the
/// <em>whole</em> datagram and log <see cref="DiagnosticId.DatagramMalformed"/>, the same treatment
/// <see cref="DatagramSessionMalformedInputTests"/> already proves for the control path. Most of this
/// file ends at "the datagram parsed, or it did not", asserting the drain stays empty whatever the
/// input; issue #252 adds the fragment shape as a second thing a record can legally be, so the tests
/// for it also drain and check the payloads a well-formed Fragment delivers.
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

    private const int DatagramHeaderSize = 20;
    private const int UnfragmentedRecordSize = 7;
    private const int FragmentRecordSize = 11;

    private const byte ModeUnreliable = 0b0000_0000;
    private const byte ModeUnreliableSequenced = 0b0000_0001;
    private const byte ModeReliableUnordered = 0b0000_0010;
    private const byte ModeReliableOrdered = 0b0000_0011;
    private const byte FragmentBit = 0b0000_0100;
    private const byte OneSpareBit = 0b0000_1000;

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
        // ever appears: this ticket ends at "the datagram parsed", not at admission.
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
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDiscards > 0);

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
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDiscards > 0);

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
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDiscards > 0);

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
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDiscards > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: datagram.Length);
    }

    [Fact]
    public void Pump_RecordWithAWellFormedFragmentShape_ParsesSuccessfullyAndDeliversBothRecords()
    {
        // Arrange — the fragment shape reads now (issue #252): a record with the bit set and a
        // complete 11-byte header no longer discards the datagram, and an earlier well-formed record
        // still takes effect, the opposite of what this test asserted before the shape was readable.
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

        // Assert — neither record is discarded and both payloads reach the consumer. Whether the
        // Fragment's index and count describe a legal set is #253's question, not this one's.
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        payloads.Should().BeEquivalentTo(
            new[] { new byte[] { 0x9A, 0x9B }, new byte[] { 0x01 } },
            "the well-formed record and the Fragment both parse and both are delivered");
    }

    [Fact]
    public void Pump_DatagramOfOnlyFragmentRecords_ParsesToExhaustionAndDeliversEveryPayload()
    {
        // Arrange — one datagram carrying nothing but Fragments, the second case the acceptance
        // criteria names alongside "only unfragmented" and "mixed".
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

        // Assert
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        payloads.Should().BeEquivalentTo(
            new[] { new byte[] { 0x01 }, new byte[] { 0x02, 0x02 }, new byte[] { 0x03, 0x03, 0x03 } });
    }

    [Fact]
    public void Pump_DatagramMixingUnfragmentedAndFragmentRecords_ParsesEveryRecordWithItsOwnFieldsIntact()
    {
        // Arrange — the mixed case the acceptance criteria calls out as the one a naive parser gets
        // wrong: unfragmented records and Fragments interleaved, across three different modes, each
        // carrying a payload distinct enough that a record misattributed to the wrong shape's byte
        // count would show up as a missing, duplicated or corrupted payload rather than a passing test.
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
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 3, fragmentIndex: 1, fragmentCount: 2, payload: [0xB3]),
            .. BuildRecord(ModeUnreliable, sequence: 5, payload: [0xE5]),
            .. BuildFragmentRecord(ModeUnreliableSequenced, sequence: 6, fragmentIndex: 0, fragmentCount: 2, payload: [0xF6]),
        ];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > 0);
        List<byte[]> payloads = accepter.DrainPayloads();

        // Assert — every record parsed, and every payload reached the consumer exactly once.
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        payloads.Should().BeEquivalentTo(
            new[]
            {
                new byte[] { 0xA0 }, new byte[] { 0xB1 }, new byte[] { 0xA2 },
                new byte[] { 0xB3 }, new byte[] { 0xE5 }, new byte[] { 0xF6 },
            });
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
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDiscards > 0);

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
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDiscards > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: datagram.Length);
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
        byte[] body = new byte[4 + 8 + 8 + CookieLength + 4 + 4 + ResumeSecretLength];
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
        // The remaining ResumeSecretLength bytes are the dialler's resume secret; an all-zero one is
        // fine, since nothing in this ticket's path ever reads it.
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
    /// The shared assertion every malformed-shape test makes, matching
    /// <see cref="DatagramSessionMalformedInputTests"/>'s helper of the same name: exactly one
    /// <see cref="DiagnosticId.DatagramMalformed"/> record reached the Sink, the drain is empty, and the
    /// discard counter rose by exactly one.
    /// </summary>
    private static void AssertOneMalformedDiscard(DatagramSessionReactorHarness accepter, int expectedBytesReceived)
    {
        accepter.DrainToList().Should().BeEmpty();
        accepter.Reactor.Counters.UnknownSourceDiscards.Should().Be(1);

        DiagnosticRecord record = accepter.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.DatagramMalformed)
            .Which;

        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
        record.Value0.Should().Be(expectedBytesReceived);
    }
}
