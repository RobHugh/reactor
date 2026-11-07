using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// What a datagram naming a Session freed by a Goodbye draws from the Reactor that freed it, over a
/// hand-built client on a bare <see cref="RawReactor"/>. The client sends its Goodbye, the node closes
/// the Peer and remembers its final ack blocks, and each test then sends the freed Session one crafted
/// datagram and reads back what, if anything, the node answers.
/// </summary>
/// <remarks>
/// <para>
/// <b>Proving that nothing was answered.</b> A missing datagram cannot be waited for, so every test that
/// expects no reply sends a sentinel after its probe: the client's own Goodbye again, which always draws
/// one re-ack. Exactly one reply then means the probe drew none. The happy path, and the address and
/// Linger checks, are proved through a real Session in <see cref="DatagramSessionGoodbyeReAckThroughProxyTests"/>.
/// </para>
/// </remarks>
public class DatagramSessionClosedSessionReAckTests
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

    private const byte ModeUnreliable = 0b0000_0000;
    private const byte ModeReliableOrdered = 0b0000_0011;
    private const byte ControlBit = 0b0000_1000;

    /// <summary>The ack the node sends once the client's Goodbye at sequence 0 has closed the Peer.</summary>
    private static readonly AckBlock _finalAck = new(Base: 1, Bits: 0);

    /// <summary>The harness's Linger, which is shorter than the client's and so is the effective one.</summary>
    private static readonly TimeSpan _linger = TimeSpan.FromSeconds(1);

    [Fact]
    public void Pump_AGoodbyeAtASequenceOtherThanTheStoredBase_IsAnsweredWithTheStoredBlocksUnchanged()
    {
        // Arrange
        using DatagramSessionReactorHarness node = new();
        using ClosedClient client = ClosedClient.Establish(node);

        // Act — sequence 5 sits behind a hole at 1, so acknowledging it would describe a gap that is not there.
        List<byte[]> replies = client.Probe(node, [.. BuildHeader(client.SessionId), .. BuildRecord(ModeReliableOrdered | ControlBit, 5)]);

        // Assert
        replies.Should().ContainSingle();
        Decode(replies[0]).ReliableOrderedAck.Should().Be(_finalAck);
    }

    [Fact]
    public void Pump_AnOrdinaryOrderedMessageAtTheStoredBase_IsAnsweredWithoutBeingAcknowledged()
    {
        // Arrange
        using DatagramSessionReactorHarness node = new();
        using ClosedClient client = ClosedClient.Establish(node);

        // Act — a Message the freed Session will never deliver, at exactly the next ordered sequence.
        List<byte[]> replies = client.Probe(node, [.. BuildHeader(client.SessionId), .. BuildRecord(ModeReliableOrdered, 1, payload: [0x7F])]);

        // Assert — acking it would tell the sender a Message arrived that nothing received.
        replies.Should().ContainSingle();
        Decode(replies[0]).ReliableOrderedAck.Should().Be(_finalAck, "only a Goodbye moves a freed Session's ack");
    }

    [Fact]
    public void Pump_ADatagramCarryingNoReliableOrderedRecord_DrawsNoReply()
    {
        // Arrange
        using DatagramSessionReactorHarness node = new();
        using ClosedClient client = ClosedClient.Establish(node);

        // Act
        List<byte[]> replies = client.ProbeThenSentinel(node, [.. BuildHeader(client.SessionId), .. BuildRecord(ModeUnreliable, 3)]);

        // Assert
        replies.Should().ContainSingle("only the sentinel Goodbye was answered");
    }

    [Fact]
    public void Pump_ADatagramWhoseRecordIsTruncated_DrawsNoReplyAndLeavesTheStoredAckUntouched()
    {
        // Arrange
        using DatagramSessionReactorHarness node = new();
        using ClosedClient client = ClosedClient.Establish(node);

        // Act — a Goodbye at the stored base, followed by a record cut off after its first two bytes.
        byte[] goodbyeAtBase = BuildRecord(ModeReliableOrdered | ControlBit, 1);
        List<byte[]> replies = client.ProbeThenSentinel(node, [.. BuildHeader(client.SessionId), .. goodbyeAtBase, ModeReliableOrdered, 0x00]);

        // Assert — had the Goodbye been applied before the body failed to parse, the base would read 2.
        replies.Should().ContainSingle("only the sentinel Goodbye was answered");
        Decode(replies[0]).ReliableOrderedAck.Should().Be(_finalAck);
    }

    [Fact]
    public void Pump_AGoodbyeOneTickInsideTheLinger_IsStillAnswered()
    {
        // Arrange — the effective Linger is the harness's one second, the shorter of the two ends'.
        using DatagramSessionReactorHarness node = new();
        using ClosedClient client = ClosedClient.Establish(node);
        node.TimeProvider.Advance(_linger - TimeSpan.FromTicks(1));

        // Act
        List<byte[]> replies = client.Probe(node, client.Goodbye());

        // Assert
        replies.Should().ContainSingle();
        Decode(replies[0]).ReliableOrderedAck.Should().Be(_finalAck);
    }

    [Fact]
    public void Pump_AGoodbyeExactlyAtTheLinger_DrawsNoReply()
    {
        // Arrange
        using DatagramSessionReactorHarness node = new();
        using ClosedClient client = ClosedClient.Establish(node);
        node.TimeProvider.Advance(_linger);

        // Act
        client.Send(node, client.Goodbye());
        List<byte[]> replies = client.CollectFor(node, TimeSpan.FromMilliseconds(200));

        // Assert
        replies.Should().BeEmpty("a freed Session is remembered for the Linger and not one tick longer");
    }

    [Fact]
    public void Pump_MoreClosedSessionsThanCapacity_ForgetsTheOldestAndStillAnswersTheNewest()
    {
        // Arrange — capacity 2, and three Sessions closed one after another.
        using DatagramSessionReactorHarness node = new(capacity: 2);
        using ClosedClient oldest = ClosedClient.Establish(node);
        using ClosedClient middle = ClosedClient.Establish(node);
        using ClosedClient newest = ClosedClient.Establish(node);

        // Act
        oldest.Send(node, oldest.Goodbye());
        List<byte[]> newestReplies = newest.Probe(node, newest.Goodbye());
        List<byte[]> oldestReplies = oldest.CollectFor(node, TimeSpan.FromMilliseconds(200));

        // Assert
        oldestReplies.Should().BeEmpty("the third close overwrote the oldest Session's entry");
        newestReplies.Should().ContainSingle();
        Decode(newestReplies[0]).ReliableOrderedAck.Should().Be(_finalAck);
    }

    private static DecodedDatagram Decode(byte[] datagram)
    {
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(datagram);
        decoded.IsSuccess.Should().BeTrue();
        return decoded.Datagram!;
    }

    private static byte[] BuildHeader(uint session)
    {
        byte[] header = new byte[DatagramHeaderSize];
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0, 4), session);
        return header;
    }

    private static byte[] BuildRecord(int flags, uint sequence, byte[]? payload = null)
    {
        payload ??= [];
        byte[] record = new byte[UnfragmentedRecordSize + payload.Length];
        record[0] = (byte)flags;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        payload.CopyTo(record.AsSpan(UnfragmentedRecordSize));
        return record;
    }

    /// <summary>
    /// One hand-built client whose Session with the node has been closed by its own Goodbye at
    /// ReliableOrdered sequence 0, with the node's final ack already received and discarded.
    /// </summary>
    private sealed class ClosedClient : IDisposable
    {
        private readonly RawReactor _client;
        private readonly LinkId _link;

        private ClosedClient(RawReactor client, LinkId link, uint sessionId)
        {
            _client = client;
            _link = link;
            SessionId = sessionId;
        }

        /// <summary>The node's local session id for the freed Session, which every probe names.</summary>
        public uint SessionId { get; }

        public static ClosedClient Establish(DatagramSessionReactorHarness node)
        {
            RawReactor client = new(new ReactorOptions { Capacity = 4 });
            LinkId link = client.Bind(new IPEndPoint(IPAddress.Any, 0));
            uint sessionId = EstablishLiveSessionId(node, client, link);
            node.DrainToList().Should().Contain(e => e.Kind == SessionEventKind.PeerConnected);

            // Every Pump is drained here, because the PeerClosed this Goodbye raises is drained only once.
            ClosedClient closed = new(client, link, sessionId);
            client.SendTo(link, Source.FromIPEndPoint(node.Endpoint), closed.Goodbye());
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];
            List<byte[]> finalAck = [];
            PumpDeadline deadline = PumpDeadline.Start();
            while (finalAck.Count == 0 || !events.Any(e => e.Kind == SessionEventKind.PeerClosed))
            {
                node.Reactor.Pump();
                events.AddRange(node.DrainToList());
                closed.Collect(finalAck);
                if (deadline.Expired)
                {
                    throw new InvalidOperationException("The Goodbye did not close the Session within the Pump budget.");
                }

                PumpDeadline.Yield();
            }

            finalAck.Should().ContainSingle("the node acks the Goodbye that closed it");
            events.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
                .Which.Reason.Should().Be(DisconnectReason.PeerGoodbye);
            return closed;
        }

        /// <summary>The Goodbye at sequence 0 that closed this Session, which every re-ack answers.</summary>
        public byte[] Goodbye() => [.. BuildHeader(SessionId), .. BuildRecord(ModeReliableOrdered | ControlBit, 0)];

        /// <summary>Sends <paramref name="datagram"/> and Pumps the node until it has been received.</summary>
        public void Send(DatagramSessionReactorHarness node, byte[] datagram)
        {
            ulong before = node.Reactor.Counters.DatagramsReceived;
            _client.SendTo(_link, Source.FromIPEndPoint(node.Endpoint), datagram);
            node.PumpUntil(() => node.Reactor.Counters.DatagramsReceived > before);
        }

        /// <summary>Sends <paramref name="datagram"/> and returns the one reply it draws.</summary>
        public List<byte[]> Probe(DatagramSessionReactorHarness node, byte[] datagram)
        {
            Send(node, datagram);
            return CollectUntilAReplyArrives(node);
        }

        /// <summary>
        /// Sends <paramref name="probe"/>, then the sentinel <see cref="Goodbye"/>, and returns every reply
        /// received up to and including the sentinel's.
        /// </summary>
        public List<byte[]> ProbeThenSentinel(DatagramSessionReactorHarness node, byte[] probe)
        {
            Send(node, probe);
            Send(node, Goodbye());
            return CollectUntilAReplyArrives(node);
        }

        /// <summary>Pumps both ends for <paramref name="window"/> of real time and returns what arrived.</summary>
        public List<byte[]> CollectFor(DatagramSessionReactorHarness node, TimeSpan window)
        {
            List<byte[]> received = [];
            Stopwatch elapsed = Stopwatch.StartNew();
            while (elapsed.Elapsed < window)
            {
                node.Reactor.Pump();
                Collect(received);
                PumpDeadline.Yield();
            }

            return received;
        }

        public void Dispose() => _client.Dispose();

        private List<byte[]> CollectUntilAReplyArrives(DatagramSessionReactorHarness node)
        {
            List<byte[]> received = [];
            PumpDeadline deadline = PumpDeadline.Start();
            while (received.Count == 0)
            {
                node.Reactor.Pump();
                Collect(received);
                if (deadline.Expired)
                {
                    throw new InvalidOperationException("No reply arrived within the Pump budget.");
                }

                PumpDeadline.Yield();
            }

            // Anything the node sent before the reply that ended the wait is already in flight with it.
            for (int i = 0; i < 5; i++)
            {
                node.Reactor.Pump();
                Collect(received);
                PumpDeadline.Yield();
            }

            return received;
        }

        private void Collect(List<byte[]> received)
        {
            _client.Pump();
            foreach (RawEvent evt in _client.Drain())
            {
                if (evt.Kind == RawEventKind.DatagramReceived)
                {
                    received.Add(evt.Payload.ToArray());
                }
            }
        }
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> and returns the wire session
    /// id <paramref name="node"/> assigned this Peer, mirroring
    /// <see cref="DatagramSessionGoodbyeShapeReceiveTests"/>'s helper of the same name.
    /// </summary>
    private static uint EstablishLiveSessionId(DatagramSessionReactorHarness node, RawReactor client, LinkId clientLink)
    {
        const ulong clientNonce = 0x0102_0304_0506_0708UL;
        Source destination = Source.FromIPEndPoint(node.Endpoint);

        client.SendTo(clientLink, destination, BuildHelloDatagram(clientNonce));
        byte[] cookieDatagram = ExchangeUntilReceived(client, node);

        ulong echoedNonce = BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset, 8));
        long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset + 8, 8)));
        byte[] cookie = cookieDatagram.AsSpan(ControlBodyOffset + 16, CookieLength).ToArray();
        echoedNonce.Should().Be(clientNonce);

        client.SendTo(clientLink, destination, BuildHelloWithCookieDatagram(clientNonce, epoch, cookie));
        byte[] acceptDatagram = ExchangeUntilReceived(client, node);

        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
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
}
