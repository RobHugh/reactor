using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A control record that is not a legal Ping is refused like any other malformed record —
/// the whole datagram is discarded and nothing in it is admitted, exactly as
/// <see cref="DatagramSessionDataDatagramParseTests"/> already proves for a spare bit or an overrun
/// length. <see cref="PacketRecordCodec.TryRead"/> refuses the control bit alongside the
/// fragment bit, alongside a nonzero length, or in any mode but <c>ReliableUnordered</c>. This file
/// drives each refusal through the seam rather than trusting the parse loop's internals.
/// </summary>
/// <remarks>
/// The seam is the same as every other datagram-parse test (tests/CLAUDE.md, ADR 0015): a bare
/// <see cref="RawReactor"/> plays the remote end and a <see cref="DatagramSessionReactorHarness"/>
/// accepter is driven through hand-built bytes. Proving "not acked" for the case mixing a legal
/// record ahead of an illegal one needs the accepter's own next outbound datagram, which nothing here
/// can read directly — <see cref="DatagramSessionOptions"/> exposes no outbound capture — so the
/// accepter is made to <c>Send</c> a Message of its own back to the client, whose reply datagram carries
/// the same header every datagram does (ADR 0009) and is decoded independently by
/// <see cref="DatagramDecoder"/>, mirroring <see cref="DatagramSessionPingReceiveTests"/>.
/// </remarks>
public class DatagramSessionMalformedControlRecordTests
{
    private const int ControlDatagramSize = 256;
    private const uint ControlSession = 0;

    private const byte HelloType = 1;
    private const int ControlBodyOffset = 5;
    private const int CookieLength = 32;

    private const uint ProtocolVersion = DatagramSessionWire.ProtocolVersion;

    private const int DatagramHeaderSize = 20;
    private const int UnfragmentedRecordSize = 7;
    private const int FragmentRecordSize = 11;

    private const byte ModeUnreliable = 0b0000_0000;
    private const byte ModeUnreliableSequenced = 0b0000_0001;
    private const byte ModeReliableUnordered = 0b0000_0010;
    private const byte ModeReliableOrdered = 0b0000_0011;
    private const byte FragmentBit = 0b0000_0100;
    private const byte ControlBit = 0b0000_1000;

    [Theory]
    [InlineData(ModeReliableUnordered)]
    [InlineData(ModeReliableOrdered)]
    public void Pump_ControlBitSetAlongsideTheFragmentBit_DiscardsTheWholeDatagram(byte modeBits)
    {
        // Arrange — the control bit is legal only on the unfragmented shape; setting it alongside the
        // fragment bit names a combination #341 refuses outright.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] illegalRecord = BuildFragmentRecord(
            modeBits, sequence: 0, fragmentIndex: 0, fragmentCount: 2, payload: [], extraFlags: ControlBit);

        byte[] datagram = [.. BuildHeader(sessionId), .. illegalRecord];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.MalformedSessionDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: datagram.Length);
    }

    [Theory]
    [InlineData(ModeReliableUnordered)]
    [InlineData(ModeReliableOrdered)]
    public void Pump_ControlRecordWithANonzeroLength_DiscardsTheWholeDatagram(byte modeBits)
    {
        // Arrange — a Ping is always zero length; a control bit on a record carrying a payload is
        // refused rather than treated as a Ping with a payload no Ping ever has.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] illegalRecord = BuildRecord(modeBits, sequence: 0, payload: [0xAA], extraFlags: ControlBit);

        byte[] datagram = [.. BuildHeader(sessionId), .. illegalRecord];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.MalformedSessionDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: datagram.Length);
    }

    [Theory]
    [InlineData(ModeUnreliable)]
    [InlineData(ModeUnreliableSequenced)]
    public void Pump_ControlBitSetInAnUnreliableMode_DiscardsTheWholeDatagram(byte modeBits)
    {
        // Arrange — the control bit is legal only in the two reliable Spaces (a Ping in
        // ReliableUnordered, a Goodbye in ReliableOrdered, #394); under either unreliable mode's two
        // bits it is refused.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] illegalRecord = BuildRecord(modeBits, sequence: 0, payload: [], extraFlags: ControlBit);

        byte[] datagram = [.. BuildHeader(sessionId), .. illegalRecord];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.MalformedSessionDatagrams > 0);

        // Assert
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: datagram.Length);
    }

    [Fact]
    public void Pump_ALegalRecordPrecedesAnIllegalControlRecord_NeitherIsDeliveredOrAcked()
    {
        // Arrange — a legal ReliableUnordered data record ahead of an illegal control record (control
        // bit under Unreliable's mode bits) in the same datagram. The whole datagram is discarded,
        // so the legal record takes no effect either: not delivered, and its sequence never advances the
        // ReliableUnordered receive window's ack base.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] legalRecord = BuildRecord(ModeReliableUnordered, sequence: 0, payload: [0xA0]);
        byte[] illegalRecord = BuildRecord(ModeUnreliable, sequence: 1, payload: [], extraFlags: ControlBit);

        byte[] datagram = [.. BuildHeader(sessionId), .. legalRecord, .. illegalRecord];

        // Act — the malformed datagram arrives and is discarded, then the accepter Sends a Message of
        // its own so its next outbound datagram's ack block can be read back.
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.MalformedSessionDatagrams > 0);
        List<byte[]> payloads = accepter.DrainPayloads();

        accepter.Reactor.Send(accepterPeer, DeliveryMode.ReliableOrdered, [0x01]).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        byte[] reply = ExchangeUntilReceived(client, accepter);

        // Assert — not delivered, and the ReliableUnordered ack base stayed at 0: the legal record ahead
        // of the illegal one was never admitted.
        AssertOneMalformedDiscard(accepter, expectedBytesReceived: datagram.Length);
        payloads.Should().BeEmpty();

        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(reply);
        decoded.IsSuccess.Should().BeTrue();
        decoded.Datagram!.ReliableUnorderedAck.Should().Be(new AckBlock(Base: 0, Bits: 0));
    }

    /// <summary>
    /// The <see cref="PeerId"/> the accepter assigned the one Peer this test's handshake produced,
    /// mirroring <see cref="DatagramSessionPingReceiveTests"/>'s helper of the same name.
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

    /// <summary>Builds a 20-byte live-Session datagram header naming <paramref name="session"/> with both ack blocks zeroed.</summary>
    private static byte[] BuildHeader(uint session)
    {
        byte[] header = new byte[DatagramHeaderSize];
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0, 4), session);
        return header;
    }

    /// <summary>
    /// Builds one unfragmented packet record: flags, a four-byte sequence, a two-byte length, then the
    /// payload. <paramref name="extraFlags"/> is ORed alongside the mode bits so a test can set the
    /// control bit (or any other flag) without building the header by hand.
    /// </summary>
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
    /// Builds one Fragment record: the unfragmented shape's bytes with the fragment bit set, followed by
    /// a two-byte fragment index and a two-byte fragment count, then the payload. <paramref
    /// name="extraFlags"/> is ORed in alongside the fragment bit so a test can additionally set the
    /// control bit without building the header by hand.
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
    /// <see cref="DiagnosticId.DatagramMalformed"/> record reached the Sink, the session-event drain is
    /// empty, and the discard counter rose by exactly one, mirroring
    /// <see cref="DatagramSessionDataDatagramParseTests"/>'s helper of the same name.
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
