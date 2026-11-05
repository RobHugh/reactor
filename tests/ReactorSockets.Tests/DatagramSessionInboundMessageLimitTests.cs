using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #425: this Reactor's own <see cref="DatagramSessionOptions.MaxMessageSize"/> is a contract on
/// what reaches the consumer. A completed Message above it is discarded and reported with
/// <see cref="DiagnosticId.OversizedMessage"/>, the Session stays open, and the discarded Message's
/// sequences are finished with, so nothing behind it stalls.
/// </summary>
/// <remarks>
/// A conforming sender checks a reliable Message against the limit at <c>Send</c>, so these tests drive a
/// live Session by hand over a bare <see cref="RawReactor"/>. <see cref="MaxMessageSize"/> of 10 over the
/// default 1200-byte fragment size floors the receive window at 1, so <see cref="ReceiveWindowFragments"/>
/// widens it to leave room for a set that assembles past the limit.
/// </remarks>
public class DatagramSessionInboundMessageLimitTests
{
    private const int MaxMessageSize = 10;
    private const int ReceiveWindowFragments = 8;

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

    [Theory]
    [InlineData(ModeReliableOrdered)]
    [InlineData(ModeReliableUnordered)]
    public void Pump_FragmentSetAssemblingPastTheLimit_DiscardsItAndReportsItsLengthAgainstTheLimit(byte mode)
    {
        // Arrange
        using Fixture fixture = new();
        byte[] datagram = [.. fixture.Header(), .. BuildFragmentSet(mode, startSequence: 0, pieceLengths: [5, 5, 1])];

        // Act
        List<byte[]> delivered = fixture.SendAndDrainPayloads(datagram);

        // Assert
        delivered.Should().BeEmpty();
        DiagnosticRecord record = fixture.Accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.OversizedMessage).Subject;
        record.Value0.Should().Be(11);
        record.Value1.Should().Be(MaxMessageSize);
    }

    [Theory]
    [InlineData(ModeReliableOrdered)]
    [InlineData(ModeReliableUnordered)]
    public void Pump_FragmentSetAssemblingToExactlyTheLimit_IsDeliveredWhole(byte mode)
    {
        // Arrange
        using Fixture fixture = new();
        byte[] datagram = [.. fixture.Header(), .. BuildFragmentSet(mode, startSequence: 0, pieceLengths: [5, 5])];

        // Act
        List<byte[]> delivered = fixture.SendAndDrainPayloads(datagram);

        // Assert
        delivered.Should().ContainSingle().Which.Should().HaveCount(MaxMessageSize);
        fixture.Accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.OversizedMessage);
    }

    [Theory]
    [InlineData(ModeUnreliable)]
    [InlineData(ModeUnreliableSequenced)]
    [InlineData(ModeReliableUnordered)]
    [InlineData(ModeReliableOrdered)]
    public void Pump_WholeRecordPastTheLimit_DiscardsItAndReportsItsLengthAgainstTheLimit(byte mode)
    {
        // Arrange
        using Fixture fixture = new();
        byte[] datagram = [.. fixture.Header(), .. BuildRecord(mode, sequence: 0, new byte[MaxMessageSize + 1])];

        // Act
        List<byte[]> delivered = fixture.SendAndDrainPayloads(datagram);

        // Assert
        delivered.Should().BeEmpty();
        DiagnosticRecord record = fixture.Accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.OversizedMessage).Subject;
        record.Value0.Should().Be(MaxMessageSize + 1);
        record.Value1.Should().Be(MaxMessageSize);
    }

    [Theory]
    [InlineData(ModeUnreliable)]
    [InlineData(ModeUnreliableSequenced)]
    [InlineData(ModeReliableUnordered)]
    [InlineData(ModeReliableOrdered)]
    public void Pump_WholeRecordOfExactlyTheLimit_IsDelivered(byte mode)
    {
        // Arrange
        using Fixture fixture = new();
        byte[] datagram = [.. fixture.Header(), .. BuildRecord(mode, sequence: 0, new byte[MaxMessageSize])];

        // Act
        List<byte[]> delivered = fixture.SendAndDrainPayloads(datagram);

        // Assert
        delivered.Should().ContainSingle().Which.Should().HaveCount(MaxMessageSize);
        fixture.Accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.OversizedMessage);
    }

    [Fact]
    public void Pump_MessageAfterADiscardedOrderedFragmentSet_IsStillDelivered()
    {
        // Arrange
        using Fixture fixture = new();
        byte[] oversizedSet = [.. fixture.Header(), .. BuildFragmentSet(ModeReliableOrdered, startSequence: 0, pieceLengths: [5, 5, 1])];
        byte[] next = [.. fixture.Header(), .. BuildRecord(ModeReliableOrdered, sequence: 3, [0xAB])];

        // Act
        fixture.SendAndDrainPayloads(oversizedSet);
        List<byte[]> delivered = fixture.SendAndDrainPayloads(next);

        // Assert
        delivered.Should().ContainSingle().Which.Should().Equal(0xAB);
    }

    [Fact]
    public void Pump_DiscardedOrderedFragmentSetBehindAGap_KeepsItsTurnInSendOrder()
    {
        // Arrange
        using Fixture fixture = new();
        byte[] oversizedSet = [.. fixture.Header(), .. BuildFragmentSet(ModeReliableOrdered, startSequence: 1, pieceLengths: [5, 5, 1])];
        byte[] filler = [.. fixture.Header(), .. BuildRecord(ModeReliableOrdered, sequence: 0, [0xAA])];
        byte[] next = [.. fixture.Header(), .. BuildRecord(ModeReliableOrdered, sequence: 4, [0xBB])];

        // Act
        fixture.SendAndDrainPayloads(oversizedSet);
        int reportedBeforeItsTurn = fixture.Accepter.Sink.Received.Count(r => r.Id == DiagnosticId.OversizedMessage);
        List<byte[]> firstDrain = fixture.SendAndDrainPayloads(filler);
        List<byte[]> secondDrain = fixture.SendAndDrainPayloads(next);

        // Assert
        reportedBeforeItsTurn.Should().Be(0);
        firstDrain.Should().ContainSingle().Which.Should().Equal(0xAA);
        secondDrain.Should().ContainSingle().Which.Should().Equal(0xBB);
        fixture.Accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.OversizedMessage);
    }

    [Fact]
    public void Pump_MessageAfterADiscardedOrderedWholeRecord_IsStillDelivered()
    {
        // Arrange
        using Fixture fixture = new();
        byte[] oversized = [.. fixture.Header(), .. BuildRecord(ModeReliableOrdered, sequence: 0, new byte[MaxMessageSize + 1])];
        byte[] next = [.. fixture.Header(), .. BuildRecord(ModeReliableOrdered, sequence: 1, [0xAB])];

        // Act
        fixture.SendAndDrainPayloads(oversized);
        List<byte[]> delivered = fixture.SendAndDrainPayloads(next);

        // Assert
        delivered.Should().ContainSingle().Which.Should().Equal(0xAB);
    }

    [Fact]
    public void Pump_OrderedWholeRecordPastTheLimitAndWiderThanTheArena_IsDiscardedRatherThanLeftUnadmitted()
    {
        // Arrange — the arena sits at its floor, MaxMessageSize, so it could never hold this record. Left
        // to the arena check, the record would never be admitted and its Space would never move again.
        using Fixture fixture = new(arenaSize: MaxMessageSize);
        byte[] oversized = [.. fixture.Header(), .. BuildRecord(ModeReliableOrdered, sequence: 0, new byte[MaxMessageSize + 1])];
        byte[] next = [.. fixture.Header(), .. BuildRecord(ModeReliableOrdered, sequence: 1, [0xAB])];

        // Act
        fixture.SendAndDrainPayloads(oversized);
        List<byte[]> delivered = fixture.SendAndDrainPayloads(next);

        // Assert
        delivered.Should().ContainSingle().Which.Should().Equal(0xAB);
        fixture.Accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.OversizedMessage);
    }

    [Fact]
    public void Pump_FragmentSetPastTheLimitAndWiderThanTheArena_IsDiscardedRatherThanLeftWaiting()
    {
        // Arrange — the same arena floor: a set this wide could never win a reservation, so without the
        // limit it would wait for room for good, holding its slots.
        using Fixture fixture = new(arenaSize: MaxMessageSize);
        byte[] oversizedSet = [.. fixture.Header(), .. BuildFragmentSet(ModeReliableOrdered, startSequence: 0, pieceLengths: [5, 5, 1])];
        byte[] next = [.. fixture.Header(), .. BuildRecord(ModeReliableOrdered, sequence: 3, [0xAB])];

        // Act
        fixture.SendAndDrainPayloads(oversizedSet);
        List<byte[]> delivered = fixture.SendAndDrainPayloads(next);

        // Assert
        delivered.Should().ContainSingle().Which.Should().Equal(0xAB);
    }

    [Fact]
    public void Pump_RetransmittedCopyOfADiscardedFragmentSet_IsNotAssembledOrReportedAgain()
    {
        // Arrange
        using Fixture fixture = new();
        byte[] oversizedSet = [.. fixture.Header(), .. BuildFragmentSet(ModeReliableUnordered, startSequence: 0, pieceLengths: [5, 5, 1])];

        // Act
        fixture.SendAndDrainPayloads(oversizedSet);
        List<byte[]> delivered = fixture.SendAndDrainPayloads(oversizedSet);

        // Assert
        delivered.Should().BeEmpty();
        fixture.Accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.OversizedMessage);
    }

    [Fact]
    public void Pump_DiscardingAMessage_LeavesTheSessionOpenAndDoesNotCountItAsReceived()
    {
        // Arrange
        using Fixture fixture = new();
        byte[] oversizedSet = [.. fixture.Header(), .. BuildFragmentSet(ModeReliableOrdered, startSequence: 0, pieceLengths: [5, 5, 1])];

        // Act
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = fixture.SendAndDrainEvents(oversizedSet);

        // Assert
        events.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        fixture.Accepter.Reactor.CountersOf(fixture.AccepterPeer).MessagesReceived.Should().Be(0);
    }

    /// <summary>
    /// Builds a complete Fragment set whose pieces have the given lengths, each one carrying its index
    /// as every payload byte.
    /// </summary>
    private static byte[] BuildFragmentSet(byte mode, uint startSequence, int[] pieceLengths)
    {
        List<byte> records = [];
        for (int index = 0; index < pieceLengths.Length; index++)
        {
            byte[] payload = new byte[pieceLengths[index]];
            payload.AsSpan().Fill((byte)index);
            records.AddRange(BuildFragmentRecord(
                mode,
                startSequence + (uint)index,
                (ushort)index,
                (ushort)pieceLengths.Length,
                payload));
        }

        return [.. records];
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
        byte[] record = new byte[FragmentRecordSize + payload.Length];
        record[0] = (byte)(modeBits | FragmentBit);
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(7, 2), fragmentIndex);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(9, 2), fragmentCount);
        payload.CopyTo(record.AsSpan(FragmentRecordSize));
        return record;
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

        // The remaining bytes are the dialler's resume secret, which nothing here reads.
        return BuildControlDatagram(HelloWithCookieType, body);
    }

    /// <summary>A live Session between an accepter at the limit under test and a hand-driven client.</summary>
    private sealed class Fixture : IDisposable
    {
        private readonly RawReactor _client = new(new ReactorOptions { Capacity = 4 });
        private readonly LinkId _clientLink;
        private readonly uint _sessionId;

        /// <param name="arenaSize">
        /// Defaults well past the limit, because a default arena sits at <see cref="MaxMessageSize"/> and
        /// would turn an over-limit Message away for want of room, hiding whether the limit was applied.
        /// </param>
        public Fixture(int arenaSize = 4096)
        {
            Accepter = new DatagramSessionReactorHarness(
                maxMessageSize: MaxMessageSize, receiveWindowFragments: ReceiveWindowFragments, arenaSize: arenaSize);
            _clientLink = _client.Bind(new IPEndPoint(IPAddress.Any, 0));
            _sessionId = EstablishLiveSessionId();
            AccepterPeer = Accepter.DrainToList().Last(e => e.Kind == SessionEventKind.PeerConnected).Peer;
        }

        public DatagramSessionReactorHarness Accepter { get; }

        public PeerId AccepterPeer { get; }

        /// <summary>A 20-byte live-Session datagram header naming this Session with both ack blocks zeroed.</summary>
        public byte[] Header()
        {
            byte[] header = new byte[DatagramHeaderSize];
            BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0, 4), _sessionId);
            return header;
        }

        /// <summary>Sends one datagram, Pumps the accepter until it has arrived, and drains the Messages delivered.</summary>
        public List<byte[]> SendAndDrainPayloads(byte[] datagram)
        {
            SendAndPump(datagram);
            return Accepter.DrainPayloads();
        }

        /// <summary>Sends one datagram, Pumps the accepter until it has arrived, and drains the Events raised.</summary>
        public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> SendAndDrainEvents(byte[] datagram)
        {
            SendAndPump(datagram);
            return Accepter.DrainToList();
        }

        public void Dispose()
        {
            Accepter.Dispose();
            _client.Dispose();
        }

        private void SendAndPump(byte[] datagram)
        {
            ulong receivedBefore = Accepter.Reactor.Counters.DatagramsReceived;
            _client.SendTo(_clientLink, Source.FromIPEndPoint(Accepter.Endpoint), datagram);
            Accepter.PumpUntil(() => Accepter.Reactor.Counters.DatagramsReceived > receivedBefore);
        }

        /// <summary>Completes an ordinary handshake by hand and returns the wire session id the accepter assigned.</summary>
        private uint EstablishLiveSessionId()
        {
            const ulong clientNonce = 0x4250_0000_0000_0001UL;
            Source destination = Source.FromIPEndPoint(Accepter.Endpoint);

            _client.SendTo(_clientLink, destination, BuildHelloDatagram(clientNonce));
            byte[] cookieDatagram = ExchangeUntilReceived();

            long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset + 8, 8)));
            byte[] cookie = cookieDatagram.AsSpan(ControlBodyOffset + 16, CookieLength).ToArray();

            _client.SendTo(_clientLink, destination, BuildHelloWithCookieDatagram(clientNonce, epoch, cookie));
            byte[] acceptDatagram = ExchangeUntilReceived();

            return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
        }

        /// <summary>Alternately Pumps both ends until the client receives one datagram, or the deadline expires.</summary>
        private byte[] ExchangeUntilReceived()
        {
            PumpDeadline deadline = PumpDeadline.Start();
            while (true)
            {
                Accepter.Reactor.Pump();
                _client.Pump();
                foreach (RawEvent evt in _client.Drain())
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
    }
}
