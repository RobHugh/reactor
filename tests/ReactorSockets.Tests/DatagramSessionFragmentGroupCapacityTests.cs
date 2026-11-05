using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #423: no peer can push a Space past the waiting Fragment groups its receive window has room for.
/// A group is at least two Fragments wide, so a window of <c>n</c> slots holds at most <c>n / 2</c>
/// waiting groups, but only while no two of them share a slot and no waiting group loses its slots.
/// Each case here broke one of those two conditions. Every datagram is built by hand and sent from a
/// bare <see cref="RawReactor"/>, the same way <see cref="DatagramSessionHostileFragmentDiscardTests"/>
/// addresses a live Session.
/// </summary>
public class DatagramSessionFragmentGroupCapacityTests
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

    private const byte ModeReliableUnordered = 0b0000_0010;
    private const byte ModeReliableOrdered = 0b0000_0011;
    private const byte FragmentBit = 0b0000_0100;

    private const int MaxMessageSize = 512;

    /// <summary>Holds four waiting groups per Space, so the fifth is the one past the bound.</summary>
    private const int ReceiveWindowFragments = 8;

    /// <summary>A two-piece group this wide fits a 512-byte read share alone but never beside another.</summary>
    private const int WidePieceLength = 200;

    [Fact]
    public void Pump_OverlappingGroupsBehindAHole_DeliversOnlyTheRealGroup()
    {
        // Arrange — a hole at sequence 0 holds the base still. Each later Fragment names a group whose
        // first piece is the previous group's last, so a completeness check reading lengths alone
        // reports every one of them complete, and they outnumber the four waiting entries the window has.
        using DatagramSessionReactorHarness accepter = NewAccepter(arenaSize: null);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        _ = FindAccepterPeer(accepter);
        SendOverlappingGroupsBehindAHole(accepter, client, clientLink, sessionId);

        // Act
        SendAndPump(accepter, client, clientLink, sessionId, Record(ModeReliableOrdered, 0, [0xAA]));
        List<string> delivered = accepter.DrainPayloads().ConvertAll(Convert.ToHexString);

        // Assert — the hole's own Message and the one honest group, and nothing built from a piece two
        // groups both claimed.
        delivered.Should().Equal("AA", "1112");
    }

    [Fact]
    public void Pump_OverlappingGroupsBehindAHole_KeepsTheWindowMoving()
    {
        // Arrange — the same sequence, then the hole filled. Sequence 8 is out of window until the base
        // has moved past the hole.
        using DatagramSessionReactorHarness accepter = NewAccepter(arenaSize: null);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId peer = FindAccepterPeer(accepter);
        SendOverlappingGroupsBehindAHole(accepter, client, clientLink, sessionId);
        SendAndPump(accepter, client, clientLink, sessionId, Record(ModeReliableOrdered, 0, [0xAA]));
        ulong outOfWindowBefore = accepter.Reactor.CountersOf(peer).OutOfWindowPackets;

        // Act
        SendAndPump(accepter, client, clientLink, sessionId, Record(ModeReliableOrdered, ReceiveWindowFragments, [0xBB]));

        // Assert
        accepter.Reactor.CountersOf(peer).OutOfWindowPackets.Should().Be(outOfWindowBefore);
    }

    [Fact]
    public void Pump_MoreDeferredGroupsThanTheWindowHolds_RefusesTheExcessUntilRetransmittedAndLosesNothing()
    {
        // Arrange — no overlap and no hole. All seven groups ride one datagram, so all arrive in one
        // Frame: the first takes the Peer's read share and the rest are deferred for arena room while
        // the base moves on past them. That is honest traffic arriving faster than the arena drains.
        using DatagramSessionReactorHarness accepter = NewAccepter(arenaSize: 2 * MaxMessageSize);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        _ = FindAccepterPeer(accepter);

        List<string> expected = [];
        List<byte> records = [];
        for (uint group = 0; group < 7; group++)
        {
            byte[] first = Enumerable.Repeat((byte)(0xA0 + group), WidePieceLength).ToArray();
            byte[] second = Enumerable.Repeat((byte)(0xB0 + group), WidePieceLength).ToArray();
            expected.Add(Convert.ToHexString([.. first, .. second]));
            records.AddRange(Fragment(ModeReliableUnordered, group * 2, 0, 2, first));
            records.AddRange(Fragment(ModeReliableUnordered, (group * 2) + 1, 1, 2, second));
        }

        // Act — the same bytes twice, the second time standing in for the far end's retransmission of
        // whatever the first left unacknowledged.
        List<string> delivered = [];
        for (int attempt = 0; attempt < 2; attempt++)
        {
            SendAndPump(accepter, client, clientLink, sessionId, [.. records]);
            delivered.AddRange(accepter.DrainPayloads().ConvertAll(Convert.ToHexString));
            for (int frame = 0; frame < 8; frame++)
            {
                accepter.Reactor.Pump();
                delivered.AddRange(accepter.DrainPayloads().ConvertAll(Convert.ToHexString));
            }
        }

        // Assert
        delivered.Should().BeEquivalentTo(expected, "every group is delivered exactly once and whole");
    }

    [Fact]
    public void Pump_ALaterGroupWouldReuseAWaitingGroupsSlot_DeliversEveryGroupWhole()
    {
        // Arrange — the waiting set is nowhere near full here, so only the slots are at stake. Group
        // 2..3 is deferred for arena room, and group 4..10 runs far enough round the eight-slot ring that
        // sequence 10 lands on the slot holding sequence 2.
        using DatagramSessionReactorHarness accepter = NewAccepter(arenaSize: 2 * MaxMessageSize);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        _ = FindAccepterPeer(accepter);

        byte[] takesTheShare0 = Enumerable.Repeat((byte)0xA0, WidePieceLength).ToArray();
        byte[] takesTheShare1 = Enumerable.Repeat((byte)0xA1, WidePieceLength).ToArray();
        byte[] waiting0 = Enumerable.Repeat((byte)0xB0, WidePieceLength).ToArray();
        byte[] waiting1 = Enumerable.Repeat((byte)0xB1, WidePieceLength).ToArray();

        List<byte> records =
        [
            .. Fragment(ModeReliableUnordered, 0, 0, 2, takesTheShare0),
            .. Fragment(ModeReliableUnordered, 1, 1, 2, takesTheShare1),
            .. Fragment(ModeReliableUnordered, 2, 0, 2, waiting0),
            .. Fragment(ModeReliableUnordered, 3, 1, 2, waiting1),
        ];

        List<string> expectedLong = [];
        for (ushort index = 0; index < 7; index++)
        {
            byte[] piece = Enumerable.Repeat((byte)(0xC0 + index), 40).ToArray();
            expectedLong.Add(Convert.ToHexString(piece));
            records.AddRange(Fragment(ModeReliableUnordered, 4u + index, index, 7, piece));
        }

        // Act
        List<string> delivered = [];
        SendAndPump(accepter, client, clientLink, sessionId, [.. records]);
        delivered.AddRange(accepter.DrainPayloads().ConvertAll(Convert.ToHexString));
        accepter.Reactor.Pump();
        delivered.AddRange(accepter.DrainPayloads().ConvertAll(Convert.ToHexString));
        SendAndPump(accepter, client, clientLink, sessionId, Fragment(ModeReliableUnordered, 10, 6, 7, Enumerable.Repeat((byte)0xC6, 40).ToArray()));
        delivered.AddRange(accepter.DrainPayloads().ConvertAll(Convert.ToHexString));

        // Assert
        delivered.Should().BeEquivalentTo(
            Convert.ToHexString([.. takesTheShare0, .. takesTheShare1]),
            Convert.ToHexString([.. waiting0, .. waiting1]),
            string.Concat(expectedLong));
    }

    [Fact]
    public void Pump_ASenderRunsALapAheadOfALostFragment_DeliversEveryMessageWholeAndInOrder()
    {
        // Arrange — a real sender retaining twice the receive window, and one lost Fragment. Each
        // Fragment fills a Datagram of its own, so the proxy can drop exactly sequence 1. The sender
        // carries on until sequence 8 reaches the slot still holding sequence 0, the piece of Message 0
        // that arrived. Overwriting it would leave Message 0 unable to complete and ReliableOrdered
        // delivery unable ever to pass it.
        const int fragmentSize = 32_750;
        const int messageSize = 2 * fragmentSize;
        const int retentionBytes = 2 * ReceiveWindowFragments * fragmentSize;
        const int messages = 6;
        TimeSpan linger = TimeSpan.FromSeconds(10);
        using DatagramSessionReactorHarness sender = new(
            capacity: 2, bindLoopback: false, maxMessageSize: messageSize, retentionBytes: retentionBytes,
            fragmentSize: fragmentSize, linger: linger);
        using DatagramSessionReactorHarness receiver = new(
            capacity: 2, maxMessageSize: messageSize, retentionBytes: retentionBytes, fragmentSize: fragmentSize,
            receiveWindowFragments: ReceiveWindowFragments, linger: linger, timeProvider: sender.TimeProvider);
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        // One clean round trip seeds the estimator. Unseeded, the RTO resolves to the Linger itself.
        sender.Reactor.Send(peer, DeliveryMode.ReliableUnordered, [1]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);
        sender.TimeProvider.Advance(TimeSpan.FromMilliseconds(20));
        receiver.Reactor.Send(receiverPeer, DeliveryMode.Unreliable, [9]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);
        sender.Reactor.Pump();
        sender.DrainAndDiscard();
        receiver.DrainAndDiscard();

        List<string> expected = [];
        List<string> delivered = [];
        void Round()
        {
            // Acks ride outgoing datagrams only (ADR 0017), so the receiver has to be sending something.
            receiver.Reactor.Send(receiverPeer, DeliveryMode.Unreliable, [9]);
            DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);
            sender.Reactor.Pump();
            sender.DrainAndDiscard();
            delivered.AddRange(receiver.DrainPayloads().ConvertAll(p => $"{p[0]:X2}{p[^1]:X2}"));
        }

        // Act — each sequence relayed alone, then time for the retransmissions to bring back the lost
        // Fragment and whatever was refused while the groups behind it waited.
        for (int sent = 0; sent < messages; sent++)
        {
            byte[] message = new byte[messageSize];
            message.AsSpan(0, fragmentSize).Fill((byte)(0xA0 + sent));
            message.AsSpan(fragmentSize).Fill((byte)(0xB0 + sent));
            expected.Add($"{message[0]:X2}{message[^1]:X2}");
            sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, message).Should().Be(SendOutcome.Sent);
            for (int piece = 0; piece < 2; piece++)
            {
                if (sent == 0 && piece == 1)
                {
                    proxy.DropNext(RelayDirection.ClientToUpstream);
                }

                Round();
            }
        }

        for (TimeSpan elapsed = TimeSpan.Zero; elapsed < 3 * linger && delivered.Count < messages; elapsed += TimeSpan.FromMilliseconds(100))
        {
            sender.TimeProvider.Advance(TimeSpan.FromMilliseconds(100));
            Round();
        }

        // Assert
        delivered.Should().Equal(expected);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void Pump_ArbitraryFragmentsUnderArenaPressure_NeverThrows(int seed)
    {
        // Arrange — every Fragment legal on its own and nothing else about them agreed: overlapping
        // groups, disagreeing counts and arrival in any order. Each round covers every sequence of one
        // window's span in one Space and repeats, standing in for retransmission, so a refused record
        // comes back and neither window wedges behind a hole. Bursts arrive inside one Frame, so the
        // arena refuses most of what completes.
        using DatagramSessionReactorHarness accepter = NewAccepter(arenaSize: 2 * MaxMessageSize);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        _ = FindAccepterPeer(accepter);
        Random random = new(seed);
        uint orderedCursor = 0;
        uint unorderedCursor = 0;

        // Act
        Action flood = () =>
        {
            for (int round = 0; round < 300; round++)
            {
                bool ordered = random.Next(2) == 0;
                byte mode = ordered ? ModeReliableOrdered : ModeReliableUnordered;
                uint cursor = ordered ? orderedCursor : unorderedCursor;
                byte[][] datagrams = new byte[ReceiveWindowFragments][];
                for (uint offset = 0; offset < ReceiveWindowFragments; offset++)
                {
                    datagrams[offset] = [.. BuildHeader(sessionId), .. ArbitraryFragment(random, mode, cursor + offset)];
                }

                for (int attempt = 0; attempt < 3; attempt++)
                {
                    random.Shuffle(datagrams);
                    SendInBursts(accepter, client, clientLink, datagrams, random);
                }

                orderedCursor += ordered ? ReceiveWindowFragments : 0u;
                unorderedCursor += ordered ? 0u : ReceiveWindowFragments;
            }
        };

        // Assert
        flood.Should().NotThrow();
    }

    private static void SendOverlappingGroupsBehindAHole(
        DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink, uint sessionId)
    {
        SendAndPump(accepter, client, clientLink, sessionId, Fragment(ModeReliableOrdered, 1, 0, 2, [0x11]));
        for (uint sequence = 2; sequence < ReceiveWindowFragments; sequence++)
        {
            SendAndPump(accepter, client, clientLink, sessionId, Fragment(ModeReliableOrdered, sequence, 1, 2, [(byte)(0x10 + sequence)]));
        }
    }

    private static DatagramSessionReactorHarness NewAccepter(int? arenaSize) => new(
        capacity: 2,
        maxMessageSize: MaxMessageSize,
        receiveWindowFragments: ReceiveWindowFragments,
        arenaSize: arenaSize);

    private static byte[] ArbitraryFragment(Random random, byte mode, uint sequence)
    {
        // Mostly two-piece groups, the narrowest a group can be and so the most a window can hold.
        ushort count = (ushort)(random.Next(4) == 0 ? random.Next(3, ReceiveWindowFragments + 1) : 2);
        ushort index = (ushort)random.Next(0, count);
        byte[] payload = new byte[random.Next(1, WidePieceLength + 1)];
        random.NextBytes(payload);
        return Fragment(mode, sequence, index, count, payload);
    }

    private static void SendInBursts(
        DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink, byte[][] datagrams, Random random)
    {
        int sent = 0;
        while (sent < datagrams.Length)
        {
            ulong received = accepter.Reactor.Counters.DatagramsReceived;
            int burst = Math.Min(random.Next(1, 6), datagrams.Length - sent);
            for (int i = 0; i < burst; i++)
            {
                client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagrams[sent + i]);
            }

            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived >= received + (ulong)burst);
            accepter.DrainPayloads();
            sent += burst;
        }
    }

    private static void SendAndPump(
        DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink, uint sessionId, byte[] records)
    {
        ulong received = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), [.. BuildHeader(sessionId), .. records]);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > received);
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> and returns the wire
    /// session id <paramref name="accepter"/> assigned this Peer.
    /// </summary>
    private static uint EstablishLiveSessionId(DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink)
    {
        const ulong clientNonce = 0x0102_0304_0506_0708UL;
        Source destination = Source.FromIPEndPoint(accepter.Endpoint);

        client.SendTo(clientLink, destination, BuildHelloDatagram(clientNonce));
        byte[] cookieDatagram = ExchangeUntilReceived(client, accepter);

        long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset + 8, 8)));
        byte[] cookie = cookieDatagram.AsSpan(ControlBodyOffset + 16, CookieLength).ToArray();

        client.SendTo(clientLink, destination, BuildHelloWithCookieDatagram(clientNonce, epoch, cookie));
        byte[] acceptDatagram = ExchangeUntilReceived(client, accepter);

        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
    }

    /// <summary>The <see cref="PeerId"/> the accepter assigned the one Peer this test's handshake produced.</summary>
    private static PeerId FindAccepterPeer(DatagramSessionReactorHarness accepter) =>
        accepter.DrainToList().First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

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
    private static byte[] Record(byte modeBits, uint sequence, ReadOnlySpan<byte> payload)
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
    private static byte[] Fragment(byte modeBits, uint sequence, ushort fragmentIndex, ushort fragmentCount, ReadOnlySpan<byte> payload)
    {
        byte[] record = new byte[FragmentRecordSize + payload.Length];
        record[0] = (byte)(modeBits | FragmentBit);
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(7, 2), fragmentIndex);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(9, 2), fragmentCount);
        payload.CopyTo(record.AsSpan(FragmentRecordSize));
        return record;
    }
}
