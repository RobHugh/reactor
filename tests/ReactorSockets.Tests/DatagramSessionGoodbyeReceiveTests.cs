using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The receive side of Goodbye (#57). A Goodbye is a zero-length control record in
/// the <c>ReliableOrdered</c> Space. It takes its turn in send order like a Message and acts only when
/// delivered: with no Message left undelivered the Peer closes directly with
/// <see cref="DisconnectReason.PeerGoodbye"/>, otherwise it raises
/// <see cref="SessionEventKind.PeerClosing"/> after the Messages and closes on a later Pump. "Undelivered"
/// here means a Message queued for the Peer in the same Pump the Goodbye was delivered in. Records are
/// hand-built and injected at the receiver, independent of the sender side (ADR 0015).
/// </summary>
public class DatagramSessionGoodbyeReceiveTests
{
    private const int ControlDatagramSize = 256;
    private const uint ControlSession = 0;

    private const byte HelloType = 1;
    private const int ControlBodyOffset = 5;
    private const int CookieLength = 32;

    private const uint ProtocolVersion = DatagramSessionWire.ProtocolVersion;

    private const int DatagramHeaderSize = 20;
    private const int UnfragmentedRecordSize = 7;

    private const byte ModeReliableOrdered = 0b0000_0011;
    private const byte ControlBit = 0b0000_1000;

    [Fact]
    public void Pump_GoodbyeDeliveredInOrderWithNothingOutstanding_ClosesDirectlyWithPeerGoodbyeAndNoPeerClosing()
    {
        // Arrange
        using Scenario scenario = new();

        // Act
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = scenario.Inject(BuildGoodbyeRecord(0));

        // Assert
        events.Select(e => e.Kind).Should().Equal(SessionEventKind.PeerClosed);
        events[0].Peer.Should().Be(scenario.Peer);
        events[0].Reason.Should().Be(DisconnectReason.PeerGoodbye);
    }

    [Fact]
    public void Pump_MessageThenGoodbye_CountsOnlyTheMessageAsReceived()
    {
        // Arrange
        using Scenario scenario = new();

        // Act
        scenario.Inject(BuildMessageRecord(0, [0xA0]), BuildGoodbyeRecord(1));

        // Assert — the Peer is still open (Closing), so its counters are readable.
        scenario.Accepter.Reactor.CountersOf(scenario.Peer).MessagesReceived.Should().Be(1);
    }

    [Fact]
    public void Pump_MessagesThenGoodbyeInOneDatagram_RaisesMessagesThenPeerClosingThenPeerClosedOnALaterPump()
    {
        // Arrange
        using Scenario scenario = new();

        // Act
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> first = scenario.Inject(
            BuildMessageRecord(0, [0xA0]), BuildMessageRecord(1, [0xA1]), BuildGoodbyeRecord(2));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> second = scenario.Accepter.PumpAndDrainAll(1);

        // Assert
        first.Select(e => e.Kind).Should().Equal(
            SessionEventKind.MessageReceived, SessionEventKind.MessageReceived, SessionEventKind.PeerClosing);
        second.Select(e => e.Kind).Should().Equal(SessionEventKind.PeerClosed);
        second[0].Reason.Should().Be(DisconnectReason.PeerGoodbye);
    }

    [Fact]
    public void Send_PeerInReceiverSideClosing_ReturnsClosed()
    {
        // Arrange
        using Scenario scenario = new();
        scenario.Inject(BuildMessageRecord(0, [0xA0]), BuildGoodbyeRecord(1));

        // Act
        SendOutcome outcome = scenario.Accepter.Reactor.Send(scenario.Peer, DeliveryMode.ReliableOrdered, [0x01]);

        // Assert
        outcome.Should().Be(SendOutcome.Closed);
    }

    [Fact]
    public void Pump_MessageAfterGoodbyeInTheSameDatagram_IsNotDelivered()
    {
        // Arrange
        using Scenario scenario = new();

        // Act
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = scenario.Inject(
            BuildMessageRecord(0, [0xA0]), BuildGoodbyeRecord(1), BuildMessageRecord(2, [0xA2]));

        // Assert
        events.Select(e => e.Kind).Should().Equal(SessionEventKind.MessageReceived, SessionEventKind.PeerClosing);
        scenario.Accepter.Reactor.CountersOf(scenario.Peer).MessagesReceived.Should().Be(1);
    }

    [Fact]
    public void Pump_GoodbyeArrivesBeforeAnEarlierMessage_ActsOnlyAfterThatMessageIsDelivered()
    {
        // Arrange
        using Scenario scenario = new();

        // Act — the Goodbye (sequence 1) first, then the Message (sequence 0) it must wait behind.
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> early = scenario.Inject(BuildGoodbyeRecord(1));
        SendOutcome whileWaiting = scenario.Accepter.Reactor.Send(scenario.Peer, DeliveryMode.ReliableOrdered, [0x01]);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> late = scenario.Inject(BuildMessageRecord(0, [0xA0]));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> final = scenario.Accepter.PumpAndDrainAll(1);

        // Assert
        early.Should().BeEmpty();
        whileWaiting.Should().Be(SendOutcome.Sent);
        late.Select(e => e.Kind).Should().Equal(SessionEventKind.MessageReceived, SessionEventKind.PeerClosing);
        final.Select(e => e.Kind).Should().Equal(SessionEventKind.PeerClosed);
    }

    /// <summary>An accepter with one established Peer, and a bare client to inject hand-built datagrams from.</summary>
    private sealed class Scenario : IDisposable
    {
        private readonly RawReactor _client = new(new ReactorOptions { Capacity = 4 });
        private readonly LinkId _clientLink;
        private readonly uint _sessionId;

        public Scenario()
        {
            Accepter = new DatagramSessionReactorHarness();
            _clientLink = _client.Bind(new IPEndPoint(IPAddress.Any, 0));
            _sessionId = EstablishLiveSessionId(Accepter, _client, _clientLink);
            Peer = FindAccepterPeer(Accepter);
        }

        public DatagramSessionReactorHarness Accepter { get; }

        public PeerId Peer { get; }

        /// <summary>
        /// Sends one datagram carrying <paramref name="records"/> and Pumps, draining after every Pump,
        /// until the accepter has received it. Returns everything drained.
        /// </summary>
        public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Inject(params byte[][] records)
        {
            byte[] datagram = [.. BuildHeader(_sessionId), .. records.SelectMany(r => r)];
            ulong receivedBefore = Accepter.Reactor.Counters.DatagramsReceived;
            _client.SendTo(_clientLink, Source.FromIPEndPoint(Accepter.Endpoint), datagram);

            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> all = [];
            PumpDeadline deadline = PumpDeadline.Start();
            while (Accepter.Reactor.Counters.DatagramsReceived == receivedBefore)
            {
                if (deadline.Expired)
                {
                    throw new InvalidOperationException("The injected datagram was not received within the Pump budget.");
                }

                Accepter.Reactor.Pump();
                all.AddRange(Accepter.DrainToList());
                PumpDeadline.Yield();
            }

            return all;
        }

        public void Dispose()
        {
            Accepter.Dispose();
            _client.Dispose();
        }
    }

    /// <summary>
    /// The <see cref="PeerId"/> the accepter assigned the one Peer this test's handshake produced,
    /// mirroring <see cref="DatagramSessionAckBlockTests"/>'s helper of the same name.
    /// </summary>
    private static PeerId FindAccepterPeer(DatagramSessionReactorHarness accepter)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();
        return drained.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;
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

        byte[] helloWithCookie = DatagramSessionWire.BuildHelloWithCookie(clientNonce, epoch, cookie);
        client.SendTo(clientLink, destination, helloWithCookie);
        byte[] acceptDatagram = ExchangeUntilReceived(client, accepter);

        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
    }

    /// <summary>
    /// Alternately Pumps <paramref name="accepter"/> and <paramref name="client"/> until
    /// <paramref name="client"/> receives one datagram, or the deadline expires, mirroring
    /// <see cref="DatagramSessionDataDatagramParseTests"/>'s helper of the same name.
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
                throw new InvalidOperationException("The expected reply did not arrive within the Pump budget.");
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

    /// <summary>Builds one unfragmented <c>ReliableOrdered</c> Message record by hand.</summary>
    private static byte[] BuildMessageRecord(uint sequence, byte[] payload)
    {
        byte[] record = new byte[UnfragmentedRecordSize + payload.Length];
        record[0] = ModeReliableOrdered;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        payload.CopyTo(record.AsSpan(UnfragmentedRecordSize));
        return record;
    }

    /// <summary>Builds a 20-byte live-Session datagram header naming <paramref name="session"/> with both ack blocks zeroed.</summary>
    private static byte[] BuildHeader(uint session)
    {
        byte[] header = new byte[DatagramHeaderSize];
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0, 4), session);
        return header;
    }

    /// <summary>
    /// Builds one Goodbye-shaped record by hand: <c>ReliableOrdered</c> mode bits plus the control bit,
    /// a <c>u32</c> sequence, and a <c>u16</c> length of zero. Built independently of the production
    /// codec, which is internal and out of reach from this test assembly (ADR 0015).
    /// </summary>
    private static byte[] BuildGoodbyeRecord(uint sequence)
    {
        byte[] record = new byte[UnfragmentedRecordSize];
        record[0] = (byte)(ModeReliableOrdered | ControlBit);
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), 0);
        return record;
    }
}
