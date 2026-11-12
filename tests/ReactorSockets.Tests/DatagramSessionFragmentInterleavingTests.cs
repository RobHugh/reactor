using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Reassembly (#257, #258, #259) stays correct while other traffic moves through the same
/// Peer. Fragment sets can progress in both reliable Spaces at once, unfragmented Messages can arrive
/// interleaved with — even inside the same datagram as — Fragments, a set's pieces can arrive out of
/// order, and a set can span several datagrams with its completing piece landing anywhere within one.
/// None of these interleavings is exercised by <see cref="DatagramSessionFragmentHoldingTests"/> or
/// <see cref="DatagramSessionFragmentDeliveryTests"/>, which each drive a single set in isolation. Built
/// the same way those two are — a live Session driven by hand from a bare <see cref="RawReactor"/>,
/// since these tests need exact Fragment sequences and byte contents no ordinary <c>Send</c> call would
/// produce on its own.
/// </summary>
public class DatagramSessionFragmentInterleavingTests
{
    private const int ControlDatagramSize = 256;
    private const uint ControlSession = 0;

    private const byte HelloType = 1;
    private const int ControlBodyOffset = 5;
    private const int CookieLength = 32;

    private const uint ProtocolVersion = 1;

    private const int DatagramHeaderSize = 20;
    private const int UnfragmentedRecordSize = 7;
    private const int FragmentRecordSize = 11;

    private const byte ModeReliableOrdered = 0b0000_0011;
    private const byte ModeReliableUnordered = 0b0000_0010;

    [Fact]
    public void Pump_TwoFragmentSetsInDifferentReliableSpacesArrivingAlternately_BothCompleteAndDeliverToTheRightSpace()
    {
        // Arrange — a two-piece ReliableOrdered set and a two-piece ReliableUnordered set, both starting
        // at sequence 0 in their own Space, sent one piece at a time so the two sets' pieces alternate:
        // Ordered piece 0, Unordered piece 0, Ordered piece 1 (completes Ordered), Unordered piece 1
        // (completes Unordered).
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] orderedFragment0 = BuildFragmentRecord(ModeReliableOrdered, sequence: 0, fragmentIndex: 0, fragmentCount: 2, payload: [0x10]);
        byte[] unorderedFragment0 = BuildFragmentRecord(ModeReliableUnordered, sequence: 0, fragmentIndex: 0, fragmentCount: 2, payload: [0x30]);
        byte[] orderedFragment1 = BuildFragmentRecord(ModeReliableOrdered, sequence: 1, fragmentIndex: 1, fragmentCount: 2, payload: [0x20]);
        byte[] unorderedFragment1 = BuildFragmentRecord(ModeReliableUnordered, sequence: 1, fragmentIndex: 1, fragmentCount: 2, payload: [0x40]);

        // Act — alternate the two sets' pieces across four separate datagrams.
        SendAndPumpUntilReceived(accepter, client, clientLink, [.. BuildHeader(sessionId), .. orderedFragment0]);
        accepter.DrainPayloads().Should().BeEmpty("neither set has a complete piece yet");

        SendAndPumpUntilReceived(accepter, client, clientLink, [.. BuildHeader(sessionId), .. unorderedFragment0]);
        accepter.DrainPayloads().Should().BeEmpty("neither set has a complete piece yet");

        SendAndPumpUntilReceived(accepter, client, clientLink, [.. BuildHeader(sessionId), .. orderedFragment1]);
        List<byte[]> afterOrderedCompletes = accepter.DrainPayloads();

        SendAndPumpUntilReceived(accepter, client, clientLink, [.. BuildHeader(sessionId), .. unorderedFragment1]);
        List<byte[]> afterUnorderedCompletes = accepter.DrainPayloads();

        // Assert — each set delivers its own bytes exactly once, at the Pump that completed it.
        afterOrderedCompletes.Should().ContainSingle().Which.Should().Equal(new byte[] { 0x10, 0x20 });
        afterUnorderedCompletes.Should().ContainSingle().Which.Should().Equal(new byte[] { 0x30, 0x40 });
    }

    [Fact]
    public void Pump_AFragmentSetCompletingInOneSpace_DeliversWhileTheOtherSpacesSetStaysIncomplete()
    {
        // Arrange — separate Spaces exist to remove head-of-line blocking between them; reassembly must
        // not reintroduce it through shared state. A ReliableUnordered set is left with only its first
        // piece held while a ReliableOrdered set completes in the same Frame.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] unorderedFragment0 = BuildFragmentRecord(ModeReliableUnordered, sequence: 0, fragmentIndex: 0, fragmentCount: 2, payload: [0xAA]);
        byte[] orderedFragment0 = BuildFragmentRecord(ModeReliableOrdered, sequence: 0, fragmentIndex: 0, fragmentCount: 2, payload: [0x01]);
        byte[] orderedFragment1 = BuildFragmentRecord(ModeReliableOrdered, sequence: 1, fragmentIndex: 1, fragmentCount: 2, payload: [0x02]);

        // Act — the incomplete Unordered set's only piece, then both pieces of the Ordered set together
        // in one datagram, completing it.
        SendAndPumpUntilReceived(accepter, client, clientLink, [.. BuildHeader(sessionId), .. unorderedFragment0]);
        accepter.DrainPayloads().Should().BeEmpty("the Unordered set has only one of its two pieces");

        byte[] completingDatagram = [.. BuildHeader(sessionId), .. orderedFragment0, .. orderedFragment1];
        SendAndPumpUntilReceived(accepter, client, clientLink, completingDatagram);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert — the Ordered set delivers even though the Unordered set, in the other Space, never
        // completed; the incomplete set is not what is being waited on.
        delivered.Should().ContainSingle("the Ordered Space's set completed independently of the Unordered Space's incomplete one")
            .Which.Should().Equal(new byte[] { 0x01, 0x02 });
    }

    [Fact]
    public void Pump_ADatagramCarryingAnUnfragmentedRecordAndACompletingFragmentSet_DeliversBothMessages()
    {
        // Arrange — one datagram carries an ordinary unfragmented record at sequence 0, then both pieces
        // of a two-piece ReliableUnordered set at sequences 1 and 2, completing it in the same datagram.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] unfragmented = BuildRecord(ModeReliableUnordered, sequence: 0, payload: [0xBB]);
        byte[] fragment0 = BuildFragmentRecord(ModeReliableUnordered, sequence: 1, fragmentIndex: 0, fragmentCount: 2, payload: [0x01]);
        byte[] fragment1 = BuildFragmentRecord(ModeReliableUnordered, sequence: 2, fragmentIndex: 1, fragmentCount: 2, payload: [0x02]);
        byte[] datagram = [.. BuildHeader(sessionId), .. unfragmented, .. fragment0, .. fragment1];

        // Act
        SendAndPumpUntilReceived(accepter, client, clientLink, datagram);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert — the unfragmented record's own Message and the assembled Fragment set's Message both
        // arrive from the one datagram that carried them, in the order their records were processed.
        // byte[].Equals is reference equality, so each element is compared for content individually
        // rather than via List<byte[]>.Should().Equal, which would compare the arrays by reference.
        delivered.Should().HaveCount(2);
        delivered[0].Should().Equal(new byte[] { 0xBB });
        delivered[1].Should().Equal(new byte[] { 0x01, 0x02 });
    }

    [Fact]
    public void Pump_AFragmentSetArrivingInReverseOrder_DeliversTheSameBytesAsOneArrivingInOrder()
    {
        // Arrange — a three-piece ReliableUnordered set whose records are placed in the datagram, and so
        // classified, in the reverse of fragment-index order: index 2 first, then 1, then 0.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] fragment2 = BuildFragmentRecord(ModeReliableUnordered, sequence: 2, fragmentIndex: 2, fragmentCount: 3, payload: [0x03]);
        byte[] fragment1 = BuildFragmentRecord(ModeReliableUnordered, sequence: 1, fragmentIndex: 1, fragmentCount: 3, payload: [0x02]);
        byte[] fragment0 = BuildFragmentRecord(ModeReliableUnordered, sequence: 0, fragmentIndex: 0, fragmentCount: 3, payload: [0x01]);
        byte[] datagram = [.. BuildHeader(sessionId), .. fragment2, .. fragment1, .. fragment0];

        // Act
        SendAndPumpUntilReceived(accepter, client, clientLink, datagram);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert — the assembled bytes are ordered by fragment index, not by arrival order.
        delivered.Should().ContainSingle().Which.Should().Equal(new byte[] { 0x01, 0x02, 0x03 });
    }

    [Fact]
    public void Pump_AFragmentSetArrivingShuffled_DeliversTheSameBytesAsOneArrivingInOrder()
    {
        // Arrange — the same three-piece set as the reverse-order test, placed in a shuffled order that
        // is neither in-order nor strictly reversed: index 1, then 2, then 0.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] fragment1 = BuildFragmentRecord(ModeReliableUnordered, sequence: 1, fragmentIndex: 1, fragmentCount: 3, payload: [0x02]);
        byte[] fragment2 = BuildFragmentRecord(ModeReliableUnordered, sequence: 2, fragmentIndex: 2, fragmentCount: 3, payload: [0x03]);
        byte[] fragment0 = BuildFragmentRecord(ModeReliableUnordered, sequence: 0, fragmentIndex: 0, fragmentCount: 3, payload: [0x01]);
        byte[] datagram = [.. BuildHeader(sessionId), .. fragment1, .. fragment2, .. fragment0];

        // Act
        SendAndPumpUntilReceived(accepter, client, clientLink, datagram);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert — the assembled bytes are ordered by fragment index, not by arrival order.
        delivered.Should().ContainSingle().Which.Should().Equal(new byte[] { 0x01, 0x02, 0x03 });
    }

    [Fact]
    public void Pump_ASetSpanningDatagramsWhoseLastFragmentArrivesFirstInItsDatagram_CompletesTheSet()
    {
        // Arrange — a three-piece ReliableUnordered set with its first two pieces in one datagram and
        // its completing third piece in a later datagram, placed first among that later datagram's own
        // records, ahead of an unrelated record.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] fragment0 = BuildFragmentRecord(ModeReliableUnordered, sequence: 0, fragmentIndex: 0, fragmentCount: 3, payload: [0x01]);
        byte[] fragment1 = BuildFragmentRecord(ModeReliableUnordered, sequence: 1, fragmentIndex: 1, fragmentCount: 3, payload: [0x02]);
        byte[] fragment2 = BuildFragmentRecord(ModeReliableUnordered, sequence: 2, fragmentIndex: 2, fragmentCount: 3, payload: [0x03]);
        byte[] unrelated = BuildRecord(ModeReliableUnordered, sequence: 3, payload: [0xCC]);

        // Act — the first datagram leaves the set incomplete; the second completes it, with the
        // completing piece first in that datagram.
        SendAndPumpUntilReceived(accepter, client, clientLink, [.. BuildHeader(sessionId), .. fragment0, .. fragment1]);
        accepter.DrainPayloads().Should().BeEmpty("the set's third piece has not arrived yet");

        byte[] completingDatagram = [.. BuildHeader(sessionId), .. fragment2, .. unrelated];
        SendAndPumpUntilReceived(accepter, client, clientLink, completingDatagram);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert — the set completes and delivers, alongside the unrelated record that followed it.
        // byte[].Equals is reference equality; see the note in the "both messages" test above.
        delivered.Should().HaveCount(2);
        delivered[0].Should().Equal(new byte[] { 0x01, 0x02, 0x03 });
        delivered[1].Should().Equal(new byte[] { 0xCC });
    }

    [Fact]
    public void Pump_ASetSpanningDatagramsWhoseLastFragmentArrivesLastInItsDatagram_CompletesTheSet()
    {
        // Arrange — the same shape as the "first in its datagram" test, but with the completing piece
        // placed last among its datagram's own records, behind the unrelated record instead of ahead of
        // it: the set must complete regardless of where within the datagram its last piece sits.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] fragment0 = BuildFragmentRecord(ModeReliableUnordered, sequence: 0, fragmentIndex: 0, fragmentCount: 3, payload: [0x01]);
        byte[] fragment1 = BuildFragmentRecord(ModeReliableUnordered, sequence: 1, fragmentIndex: 1, fragmentCount: 3, payload: [0x02]);
        byte[] fragment2 = BuildFragmentRecord(ModeReliableUnordered, sequence: 2, fragmentIndex: 2, fragmentCount: 3, payload: [0x03]);
        byte[] unrelated = BuildRecord(ModeReliableUnordered, sequence: 3, payload: [0xCC]);

        // Act — the first datagram leaves the set incomplete; the second completes it, with the
        // completing piece last in that datagram.
        SendAndPumpUntilReceived(accepter, client, clientLink, [.. BuildHeader(sessionId), .. fragment0, .. fragment1]);
        accepter.DrainPayloads().Should().BeEmpty("the set's third piece has not arrived yet");

        byte[] completingDatagram = [.. BuildHeader(sessionId), .. unrelated, .. fragment2];
        SendAndPumpUntilReceived(accepter, client, clientLink, completingDatagram);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert — the unrelated record delivers immediately when its own record is processed, and the
        // set completes and delivers when its own, later record is processed. byte[].Equals is
        // reference equality; see the note in the "both messages" test above.
        delivered.Should().HaveCount(2);
        delivered[0].Should().Equal(new byte[] { 0xCC });
        delivered[1].Should().Equal(new byte[] { 0x01, 0x02, 0x03 });
    }

    private static void SendAndPumpUntilReceived(
        DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink, byte[] datagram)
    {
        ulong before = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> — a bare
    /// <see cref="RawReactor"/> — and returns the wire session id <paramref name="accepter"/> assigned
    /// this Peer, mirroring <see cref="DatagramSessionFragmentDeliveryTests"/>'s helper of the same name.
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

        byte[] helloWithCookie = DatagramSessionWire.BuildHelloWithCookie(clientNonce, epoch, cookie);
        client.SendTo(clientLink, destination, helloWithCookie);
        byte[] acceptDatagram = ExchangeUntilReceived(client, accepter);

        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
    }

    /// <summary>
    /// Alternately Pumps <paramref name="accepter"/> and <paramref name="client"/> until
    /// <paramref name="client"/> receives one datagram, or the deadline expires.
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
        byte[] record = new byte[UnfragmentedRecordSize + payload.Length];
        record[0] = modeBits;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        payload.CopyTo(record.AsSpan(UnfragmentedRecordSize));
        return record;
    }

    /// <summary>
    /// Builds one Fragment record: the unfragmented shape's bytes with the fragment bit set, followed by
    /// a two-byte fragment index and a two-byte fragment count, then the payload.
    /// </summary>
    private static byte[] BuildFragmentRecord(
        byte modeBits, uint sequence, ushort fragmentIndex, ushort fragmentCount, ReadOnlySpan<byte> payload)
    {
        const byte fragmentBit = 0b0000_0100;
        byte[] record = new byte[FragmentRecordSize + payload.Length];
        record[0] = (byte)(modeBits | fragmentBit);
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(7, 2), fragmentIndex);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(9, 2), fragmentCount);
        payload.CopyTo(record.AsSpan(FragmentRecordSize));
        return record;
    }
}
