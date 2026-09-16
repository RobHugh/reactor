using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #339: the receive half of the idle-triggered reliable Ping (#53). A Ping is a zero-length,
/// unfragmented, <c>ReliableUnordered</c> control record — the wire shape <see
/// cref="PacketRecordCodec.TryWriteControlRecord"/> (#338) already writes and nothing yet sends. These
/// tests hand-build one and inject it into an Established Session exactly as
/// <see cref="DatagramSessionDataDatagramParseTests"/> injects any other record, then prove the four
/// things #339's acceptance criteria ask for: it is admitted into the <c>ReliableUnordered</c> receive
/// window so the ordinary ack block covers it, it is never delivered, it does not disturb Messages
/// arriving around it, and a duplicate counts exactly as a duplicate data record would.
/// </summary>
/// <remarks>
/// The seam is the same as every other datagram-parse test (tests/CLAUDE.md, ADR 0015): a bare
/// <see cref="RawReactor"/> plays the remote end and a <see cref="DatagramSessionReactorHarness"/>
/// accepter is driven through hand-built bytes. Proving the ack block a Ping earns needs the accepter's
/// own next outbound datagram, which nothing here can read directly — <see cref="DatagramSessionOptions"/>
/// exposes no outbound capture — so the accepter is made to <c>Send</c> a Message of its own back to the
/// client, whose reply datagram carries the same header every datagram does (ADR 0009: both ack blocks
/// are always present) and is decoded independently by <see cref="DatagramDecoder"/>.
/// </remarks>
public class DatagramSessionPingReceiveTests
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
    private const int UnfragmentedRecordSize = 7;

    private const byte ModeReliableUnordered = 0b0000_0010;
    private const byte ControlBit = 0b0000_1000;

    [Fact]
    public void Pump_PingAtSequenceNArrives_TheAccepterSNextDatagramCarriesAReliableUnorderedAckBlockCoveringN()
    {
        // Arrange — a lone Ping at sequence 0, the first sequence this Space's receive window ever
        // sees, so admitting it moves the window's cumulative base from 0 to 1.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] pingDatagram = [.. BuildHeader(sessionId), .. BuildControlRecord(sequence: 0)];

        // Act — the Ping arrives and is admitted, then the accepter Sends a Message of its own so its
        // next outbound datagram's header can be read back.
        ulong receivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), pingDatagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > receivedBefore);

        accepter.Reactor.Send(accepterPeer, DeliveryMode.ReliableOrdered, [0x01]).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        byte[] reply = ExchangeUntilReceived(client, accepter);

        // Assert — the ReliableUnordered ack block reflects the admitted Ping: base advanced from 0 to
        // 1, covering sequence 0, exactly as it would for an ordinary data record at that sequence.
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(reply);
        decoded.IsSuccess.Should().BeTrue();
        decoded.Datagram!.ReliableUnorderedAck.Should().Be(new AckBlock(Base: 1, Bits: 0));
    }

    [Fact]
    public void Pump_PingArrives_NoMessageReceivedIsDrainedAndMessagesReceivedStaysUnchanged()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] pingDatagram = [.. BuildHeader(sessionId), .. BuildControlRecord(sequence: 0)];

        // Act
        ulong receivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), pingDatagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > receivedBefore);
        List<byte[]> payloads = accepter.DrainPayloads();

        // Assert — admitted (not malformed) but never delivered: no MessageReceived payload drained,
        // and the Peer's own MessagesReceived counter never moved off zero.
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        payloads.Should().BeEmpty();
        accepter.Reactor.CountersOf(accepterPeer).MessagesReceived.Should().Be(0);
    }

    [Fact]
    public void Pump_PingArrivesBetweenTwoReliableUnorderedMessages_BothMessagesStillDeliver()
    {
        // Arrange — a Message at sequence 0, a Ping at sequence 1, and a second Message at sequence 2,
        // all in one datagram: the Ping must not stop either neighbour being admitted and delivered.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] datagram =
        [
            .. BuildHeader(sessionId),
            .. BuildRecord(ModeReliableUnordered, sequence: 0, payload: [0xA0]),
            .. BuildControlRecord(sequence: 1),
            .. BuildRecord(ModeReliableUnordered, sequence: 2, payload: [0xB2]),
        ];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > 0);
        List<byte[]> payloads = accepter.DrainPayloads();

        // Assert — both Messages arrive; the Ping between them contributes nothing to delivery or to
        // the MessagesReceived count.
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        payloads.Should().BeEquivalentTo(new[] { new byte[] { 0xA0 }, new byte[] { 0xB2 } });
        accepter.Reactor.CountersOf(accepterPeer).MessagesReceived.Should().Be(2);
    }

    [Fact]
    public void Pump_ADuplicatePingArrives_CountsAsADuplicatePacketExactlyAsADuplicateDataRecordWould()
    {
        // Arrange — the same Ping sequence sent twice, in separate datagrams, at sequence 1 rather than
        // 0: leaving sequence 0's slot empty pins the window's base at 0 (AdvanceBase only moves the
        // base through a contiguous run starting at its own slot — DatagramSessionFragmentHoldingTests'
        // duplicate test relies on the same fact), so the repeat lands on a slot already marked admitted
        // inside the window rather than one the base has already passed, which is what
        // ReceiveWindow.Classify reports as Duplicate rather than OutOfWindow.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] firstPingDatagram = [.. BuildHeader(sessionId), .. BuildControlRecord(sequence: 1)];
        byte[] secondPingDatagram = [.. BuildHeader(sessionId), .. BuildControlRecord(sequence: 1)];

        // Act
        ulong firstReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), firstPingDatagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > firstReceivedBefore);
        ulong duplicatesAfterFirst = accepter.Reactor.CountersOf(accepterPeer).DuplicatePackets;

        ulong secondReceivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), secondPingDatagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > secondReceivedBefore);

        // Assert — the first Ping is admitted (no duplicate yet); the second, at the same sequence,
        // raises DuplicatePackets exactly as a duplicate ordinary record would.
        duplicatesAfterFirst.Should().Be(0);
        accepter.Reactor.CountersOf(accepterPeer).DuplicatePackets.Should().Be(1);
        accepter.DrainPayloads().Should().BeEmpty();
        accepter.Reactor.CountersOf(accepterPeer).MessagesReceived.Should().Be(0);
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

        byte[] helloWithCookie = BuildHelloWithCookieDatagram(clientNonce, epoch, cookie);
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
    /// Builds one in-Session control record by hand — the same seven bytes
    /// <see cref="PacketRecordCodec.TryWriteControlRecord"/> writes (#338): <c>ReliableUnordered</c>
    /// mode bits plus the control bit, a <c>u32</c> sequence, and a <c>u16</c> length of zero. Built
    /// independently of the production writer rather than by calling it, since <see
    /// cref="PacketRecordCodec"/> is internal and out of reach from this test assembly (ADR 0015).
    /// </summary>
    private static byte[] BuildControlRecord(uint sequence)
    {
        byte[] record = new byte[UnfragmentedRecordSize];
        record[0] = (byte)(ModeReliableUnordered | ControlBit);
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), 0);
        return record;
    }
}
