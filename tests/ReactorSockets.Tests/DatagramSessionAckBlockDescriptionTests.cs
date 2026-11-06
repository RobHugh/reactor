using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// What the two ack blocks a
/// Peer stamps on its outbound datagrams actually say about its receive state — the cumulative base
/// each reliable Space has reached, plus the 32-bit field describing the 32 sequences above it.
/// </summary>
/// <remarks>
/// <para>
/// <b>The instrument.</b> A hand-built client over a bare <see cref="RawReactor"/> drives arrivals at
/// sequences of this test's own choosing — holes, scattered admissions, and one sequence deliberately
/// further above the base than any bit can reach — which the public <see cref="DatagramSessionReactor.Send"/>
/// allocator cannot be made to produce. What the node then says about them is read off the wire: the
/// node's own datagram, decoded independently by <see cref="DatagramDecoder"/> (#229) rather than by
/// the codec under test.
/// </para>
/// <para>
/// <b>Window width.</b> Every Reactor here is built with a 40-fragment receive window, wide enough that
/// a sequence 35 above the base is inside the window — genuinely admitted, and therefore genuinely
/// undescribable rather than merely refused. At the harness's default width the two facts would be
/// indistinguishable.
/// </para>
/// <para>
/// <b>Not covered here, and why.</b> The <c>u32</c> wrap. A reliable Space's window never slides
/// (#173) — retransmission fills its holes — so its base advances only by admitting every sequence in
/// turn, and walking one to the wrap through the seam would take four billion arrivals. The trick
/// <see cref="DatagramSessionSequenceWrapTests"/> uses, jumping a sliding window's base forward, is
/// available only to the two unreliable Spaces, which are never acknowledged. With no
/// <c>InternalsVisibleTo</c> (ADR 0015) the reliable describe-across-the-wrap case has no seam to be
/// asserted at; it is recorded rather than papered over with a test that only looks like it covers it.
/// </para>
/// </remarks>
public class DatagramSessionAckBlockDescriptionTests
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
    private const byte ModeUnreliableSequenced = 0b0000_0001;
    private const byte ModeReliableUnordered = 0b0000_0010;
    private const byte ModeReliableOrdered = 0b0000_0011;

    /// <summary>
    /// Wide enough to hold a sequence 35 above the base, so "admitted but beyond the 32 bits" is a state
    /// the window can actually be put into. See the class remarks.
    /// </summary>
    private const int WideReceiveWindow = 40;

    [Fact]
    public void Pump_AWindowWithScatteredAdmissionsAboveAHole_DescribesTheHolesBaseAndExactlyThoseBits()
    {
        // Arrange
        using DatagramSessionReactorHarness node = new(receiveWindowFragments: WideReceiveWindow);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);

        // Assert — a Peer that has received nothing still describes both Spaces, as zeroed blocks
        // rather than as no block at all.
        DecodedDatagram fresh = FlushAndDecode(node, client, nodePeer, marker: 1);
        fresh.ReliableOrderedAck.Should().Be(new AckBlock(0, 0), "nothing has been admitted in either Space");
        fresh.ReliableUnorderedAck.Should().Be(new AckBlock(0, 0));

        // Act — sequences 1, 3 and 7 arrive in ReliableOrdered while 0 never does, so the base stays at
        // the hole and each admission is describable only by its own bit: bit 0 is base + 1.
        AdmitAt(node, client, clientLink, sessionId, ModeReliableOrdered, 1, 3, 7);

        // Assert
        const uint scatteredBits = (1u << 0) | (1u << 2) | (1u << 6);
        DecodedDatagram scattered = FlushAndDecode(node, client, nodePeer, marker: 2);
        scattered.ReliableOrderedAck.Should().Be(
            new AckBlock(0, scatteredBits),
            "the base is still the hole at 0, and sequences 1, 3 and 7 set bits 0, 2 and 6 and no others");
        scattered.ReliableUnorderedAck.Should().Be(
            new AckBlock(0, 0), "the two reliable Spaces are independent, and nothing arrived in this one");

        // Act — sequence 35 arrives: inside the 40-slot window, so genuinely admitted, but 35 above the
        // base, where no bit reaches.
        AdmitAt(node, client, clientLink, sessionId, ModeReliableOrdered, 35);

        // Assert
        DecodedDatagram beyondTheBits = FlushAndDecode(node, client, nodePeer, marker: 3);
        beyondTheBits.ReliableOrderedAck.Should().Be(
            new AckBlock(0, scatteredBits),
            "a sequence more than 32 above the base has no bit to hold it and is simply not described");

        // Act — the hole is filled. The base advances across 0 and 1 in one step and stops at 2, the
        // next hole, and the bitfield is re-read against the new base rather than carried over.
        AdmitAt(node, client, clientLink, sessionId, ModeReliableOrdered, 0);

        // Assert — sequence 3 is now base + 1 and sequence 7 is base + 5; sequence 35 is base + 33 and
        // so is still out of reach.
        DecodedDatagram afterTheJump = FlushAndDecode(node, client, nodePeer, marker: 4);
        afterTheJump.ReliableOrderedAck.Should().Be(
            new AckBlock(2, (1u << 0) | (1u << 4)),
            "filling the hole advanced the base to the next one and shifted the bitfield to match, in one read");
    }

    [Fact]
    public void Pump_TwoDatagramsWithNoArrivalBetweenThem_CarryIdenticalAckBlocks()
    {
        // Arrange
        using DatagramSessionReactorHarness node = new(receiveWindowFragments: WideReceiveWindow);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);
        AdmitAt(node, client, clientLink, sessionId, ModeReliableOrdered, 0, 1, 3);

        // Act
        DecodedDatagram first = FlushAndDecode(node, client, nodePeer, marker: 1);
        DecodedDatagram second = FlushAndDecode(node, client, nodePeer, marker: 2);
        AdmitAt(node, client, clientLink, sessionId, ModeReliableOrdered, 2);
        DecodedDatagram third = FlushAndDecode(node, client, nodePeer, marker: 3);

        // Assert — generated fresh from present state each time: unchanged state describes identically,
        // and an arrival in between is reflected on the very next Datagram with no residue of the old
        // base or the old bits.
        first.ReliableOrderedAck.Should().Be(new AckBlock(2, 1u << 0), "0 and 1 retired, 2 is the hole, 3 is base + 1");
        second.ReliableOrderedAck.Should().Be(first.ReliableOrderedAck, "nothing arrived between the two Datagrams");
        third.ReliableOrderedAck.Should().Be(
            new AckBlock(4, 0), "filling the hole at 2 retired 2 and 3 together, leaving nothing above the new base");
    }

    [Fact]
    public void Pump_HeavyAdmissionInBothUnreliableSpaces_LeavesBothReliableBlocksUntouchedAndTheHeaderTwentyBytes()
    {
        // Arrange
        using DatagramSessionReactorHarness node = new(receiveWindowFragments: WideReceiveWindow);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);

        // Act — 20 sequences admitted in each unreliable Space. Neither is ever acknowledged, so however
        // much arrives there, no block moves and no third block appears.
        for (uint sequence = 0; sequence < 20; sequence++)
        {
            AdmitAt(node, client, clientLink, sessionId, ModeUnreliable, sequence);
            AdmitAt(node, client, clientLink, sessionId, ModeUnreliableSequenced, sequence);
        }

        // Assert
        (byte[] Raw, DecodedDatagram Decoded) captured = FlushAndCapture(node, client, nodePeer, marker: 1);
        captured.Decoded.ReliableOrderedAck.Should().Be(new AckBlock(0, 0), "no ack is ever generated for an unreliable Space");
        captured.Decoded.ReliableUnorderedAck.Should().Be(new AckBlock(0, 0));
        captured.Raw.Length.Should().Be(
            DatagramHeaderSize + UnfragmentedRecordSize + 1,
            "the header is the same 20 bytes carrying exactly two ack blocks — a third would have grown it");
    }

    [Fact]
    public void Pump_AdmissionsInReliableUnorderedOnly_MoveThatBlockAndLeaveReliableOrderedZeroed()
    {
        // Arrange
        using DatagramSessionReactorHarness node = new(receiveWindowFragments: WideReceiveWindow);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);

        // Act
        AdmitAt(node, client, clientLink, sessionId, ModeReliableUnordered, 0, 2);

        // Assert — each block describes its own Space's window and nothing else.
        DecodedDatagram decoded = FlushAndDecode(node, client, nodePeer, marker: 1);
        decoded.ReliableUnorderedAck.Should().Be(
            new AckBlock(1, 1u << 0), "0 retired, 1 is the hole, and 2 is base + 1");
        decoded.ReliableOrderedAck.Should().Be(
            new AckBlock(0, 0), "an admission in one reliable Space changes neither the base nor the bits of the other");
    }

    /// <summary>
    /// Drives one crafted arrival per sequence in <paramref name="sequences"/>, each an empty record in
    /// <paramref name="modeBits"/>' Space, and waits for the node to have taken each one in.
    /// </summary>
    private static void AdmitAt(
        DatagramSessionReactorHarness node,
        RawReactor client,
        LinkId clientLink,
        uint sessionId,
        byte modeBits,
        params uint[] sequences)
    {
        foreach (uint sequence in sequences)
        {
            byte[] datagram = [.. BuildHeader(sessionId), .. BuildRecord(modeBits, sequence, [])];
            ulong before = node.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, Source.FromIPEndPoint(node.Endpoint), datagram);
            node.PumpUntil(() => node.Reactor.Counters.DatagramsReceived > before);
        }

        node.DrainAndDiscard();
    }

    /// <summary>
    /// Makes the node flush one Datagram — a Peer is flushed only when it has something queued — and
    /// returns it decoded. <paramref name="marker"/> is the one payload byte the record carries, so the
    /// datagram this call produced is the one picked out of whatever else is in flight.
    /// </summary>
    private static DecodedDatagram FlushAndDecode(
        DatagramSessionReactorHarness node,
        RawReactor client,
        PeerId peer,
        byte marker) => FlushAndCapture(node, client, peer, marker).Decoded;

    private static (byte[] Raw, DecodedDatagram Decoded) FlushAndCapture(
        DatagramSessionReactorHarness node,
        RawReactor client,
        PeerId peer,
        byte marker)
    {
        node.Reactor.Send(peer, DeliveryMode.Unreliable, [marker]).Should().Be(SendOutcome.Sent);

        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            node.Reactor.Pump();
            client.Pump();
            foreach (RawEvent evt in client.Drain())
            {
                if (evt.Kind != RawEventKind.DatagramReceived)
                {
                    continue;
                }

                byte[] raw = evt.Payload.ToArray();
                DatagramDecodeResult result = DatagramDecoder.TryDecode(raw);
                if (result.IsSuccess
                    && result.Datagram!.Records.Any(record => record.Payload.Length == 1 && record.Payload[0] == marker))
                {
                    return (raw, result.Datagram);
                }
            }

            if (deadline.Expired)
            {
                throw new InvalidOperationException($"The node's Datagram carrying marker {marker} did not arrive within the Pump budget.");
            }

            PumpDeadline.Yield();
        }
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
    /// Completes an ordinary handshake by hand over <paramref name="client"/> — a bare
    /// <see cref="RawReactor"/> — and returns the wire session id <paramref name="node"/> assigned this
    /// Peer, mirroring <see cref="DatagramSessionCounterExchangeTests"/>'s helper of the same name.
    /// </summary>
    private static uint EstablishLiveSessionId(DatagramSessionReactorHarness node, RawReactor client, LinkId clientLink)
    {
        const ulong clientNonce = 0x0102_0304_0506_0708UL;
        Source destination = Source.FromIPEndPoint(node.Endpoint);

        byte[] hello = BuildHelloDatagram(clientNonce);
        client.SendTo(clientLink, destination, hello);
        byte[] cookieDatagram = ExchangeUntilReceived(client, node);

        ulong echoedNonce = BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset, 8));
        long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset + 8, 8)));
        byte[] cookie = cookieDatagram.AsSpan(ControlBodyOffset + 16, CookieLength).ToArray();
        echoedNonce.Should().Be(clientNonce);

        byte[] helloWithCookie = BuildHelloWithCookieDatagram(clientNonce, epoch, cookie);
        client.SendTo(clientLink, destination, helloWithCookie);
        byte[] acceptDatagram = ExchangeUntilReceived(client, node);

        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
    }

    /// <summary>The <see cref="PeerId"/> the node assigned the one Peer this test's handshake produced.</summary>
    private static PeerId FindNodePeer(DatagramSessionReactorHarness node)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = node.DrainToList();
        return drained.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;
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
