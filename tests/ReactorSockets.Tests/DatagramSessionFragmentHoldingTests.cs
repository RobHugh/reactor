using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #257: an admitted Fragment is diverted into its Space's receive-window slot instead of being
/// delivered — no frame-arena reservation, no pending <see cref="SessionEventKind.MessageReceived"/>,
/// no rise in <see cref="PeerCounters.MessagesReceived"/>. Recognising a complete Fragment set is
/// issue #258's concern, not this one's: these tests only prove a held Fragment is invisible to a
/// consumer and that an ordinary record alongside it is unaffected. Built the same way
/// <see cref="DatagramSessionHostileFragmentDiscardTests"/> is — a live Session driven by hand from a
/// bare <see cref="RawReactor"/>, since establishing a Session and then addressing it needs the
/// bidirectional handshake exchange a bare Reactor gives a test.
/// </summary>
public class DatagramSessionFragmentHoldingTests
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

    private const byte ModeReliableOrdered = 0b0000_0011;
    private const byte ModeReliableUnordered = 0b0000_0010;

    [Fact]
    public void Pump_AnAdmittedFragmentAlongsideAnOrdinaryRecord_DeliversOnlyTheOrdinaryRecord()
    {
        // Arrange — one datagram carries a Fragment at sequence 0 (the first of a two-piece set that is
        // never completed here — #258's concern) followed by an ordinary, unfragmented record at
        // sequence 1, both ReliableUnordered so nothing here is gated behind ordered delivery release —
        // that behaviour belongs to ReliableOrdered alone and is not what this test is about.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);
        PeerCounters before = accepter.Reactor.CountersOf(accepterPeer);

        byte[] fragment = BuildFragmentRecord(
            ModeReliableUnordered, sequence: 0, fragmentIndex: 0, fragmentCount: 2, payload: [0xAA]);
        byte[] ordinary = BuildRecord(ModeReliableUnordered, sequence: 1, payload: [0xBB]);
        byte[] datagram = [.. BuildHeader(sessionId), .. fragment, .. ordinary];

        // Act
        ulong before2 = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before2);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert — the Fragment is held, not delivered; the ordinary record beside it is unaffected.
        delivered.Should().ContainSingle().Which.Should().Equal(new byte[] { 0xBB });

        PeerCounters after = accepter.Reactor.CountersOf(accepterPeer);
        after.MessagesReceived.Should().Be(
            before.MessagesReceived + 1, "only the ordinary record raises MessagesReceived, not the Fragment");
        after.OutOfWindowPackets.Should().Be(before.OutOfWindowPackets);
        after.DuplicatePackets.Should().Be(before.DuplicatePackets);
    }

    [Fact]
    public void Pump_ADuplicateFragment_DoesNotDeliverAnythingAndRaisesTheDuplicateCounter()
    {
        // Arrange — a Fragment at sequence 1 admits and is held, leaving sequence 0's slot empty so the
        // window's base stays pinned at 0 rather than advancing past sequence 1 (AdvanceBase only moves
        // the base through a contiguous run starting at its own slot). The same Fragment sent again then
        // lands on a slot the window already marked admitted, inside the window rather than behind it.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] fragment = BuildFragmentRecord(
            ModeReliableOrdered, sequence: 1, fragmentIndex: 1, fragmentCount: 2, payload: [0xAA]);
        byte[] datagram = [.. BuildHeader(sessionId), .. fragment];

        SendAndPumpUntilReceived(accepter, client, clientLink, datagram);
        accepter.DrainAndDiscard();
        PeerCounters before = accepter.Reactor.CountersOf(accepterPeer);

        // Act — the exact same Fragment record again, with a different payload byte, proving the
        // second arrival is refused as a duplicate rather than overwriting the held slot.
        byte[] resent = BuildFragmentRecord(
            ModeReliableOrdered, sequence: 1, fragmentIndex: 1, fragmentCount: 2, payload: [0xFF]);
        byte[] resendDatagram = [.. BuildHeader(sessionId), .. resent];
        SendAndPumpUntilReceived(accepter, client, clientLink, resendDatagram);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert
        delivered.Should().BeEmpty("a duplicate Fragment is never delivered, held or otherwise");
        PeerCounters after = accepter.Reactor.CountersOf(accepterPeer);
        after.DuplicatePackets.Should().Be(before.DuplicatePackets + 1);
        after.MessagesReceived.Should().Be(before.MessagesReceived);

        Action stillUsable = () => accepter.Reactor.CountersOf(accepterPeer);
        stillUsable.Should().NotThrow();
    }

    [Fact]
    public void Pump_AnOutOfWindowFragment_DoesNotDeliverAnythingAndRaisesTheOutOfWindowCounter()
    {
        // Arrange — a small window whose base is walked forward past sequence 0 by ordinary records,
        // so a Fragment arriving late at sequence 0 falls below the base.
        const int windowCapacity = 4;
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: windowCapacity, maxMessageSize: 1300);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        for (uint sequence = 0; sequence < windowCapacity * 2; sequence++)
        {
            byte[] ordinary = BuildRecord(ModeReliableOrdered, sequence, payload: [0x01]);
            byte[] datagram = [.. BuildHeader(sessionId), .. ordinary];
            SendAndPumpUntilReceived(accepter, client, clientLink, datagram);
            accepter.DrainAndDiscard();
        }

        PeerCounters before = accepter.Reactor.CountersOf(accepterPeer);

        // Act — a Fragment at sequence 0, long since retired behind the base.
        byte[] staleFragment = BuildFragmentRecord(
            ModeReliableOrdered, sequence: 0, fragmentIndex: 0, fragmentCount: 2, payload: [0xAA]);
        byte[] staleDatagram = [.. BuildHeader(sessionId), .. staleFragment];
        SendAndPumpUntilReceived(accepter, client, clientLink, staleDatagram);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert
        delivered.Should().BeEmpty("an out-of-window Fragment is never delivered, held or otherwise");
        PeerCounters after = accepter.Reactor.CountersOf(accepterPeer);
        after.OutOfWindowPackets.Should().Be(before.OutOfWindowPackets + 1);
        after.MessagesReceived.Should().Be(before.MessagesReceived);

        Action stillUsable = () => accepter.Reactor.CountersOf(accepterPeer);
        stillUsable.Should().NotThrow();
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
    /// this Peer, mirroring <see cref="DatagramSessionHostileFragmentDiscardTests"/>'s helper of the
    /// same name.
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

    /// <summary>The <see cref="PeerId"/> the accepter assigned the one Peer this test's handshake produced.</summary>
    private static PeerId FindAccepterPeer(DatagramSessionReactorHarness accepter)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();
        return drained.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;
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
