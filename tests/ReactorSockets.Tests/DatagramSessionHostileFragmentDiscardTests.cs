using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #255: the codec's refusal of impossible Fragment metadata (#253), proved against a live
/// Session rather than in isolation — a real attacker sits at a hostile peer driving an established
/// Session, not at a unit test calling the codec directly. Every datagram here is built and sent by
/// hand over a bare <see cref="RawReactor"/> — the same instrument <see cref="HostilePeer"/> is built
/// from, and the one <see cref="DatagramSessionDataDatagramParseTests"/> and
/// <see cref="DatagramSessionUnreliableDuplicateTests"/> already use for a live Session's data path,
/// since establishing a Session and then addressing it needs the bidirectional handshake exchange a
/// bare Reactor gives a test, not the fire-and-forget <see cref="HostilePeer.SendDatagram"/>.
/// </summary>
/// <remarks>
/// Each malformed datagram carries a well-formed record at sequence 0 <em>ahead of</em> the malformed
/// Fragment tail, in <see cref="ReactorSockets.Session.PacketRecordMode.ReliableOrdered"/> — a Space
/// whose window never slides, so a record it never classifies leaves <c>Base</c> at exactly zero. The
/// whole datagram discarding is proved two ways: directly, by the drain staying empty (no
/// <see cref="SessionEventKind.MessageReceived"/> for either record, so the leading well-formed one was
/// never delivered either); and by the window itself, sending a second, genuinely well-formed record at
/// the same sequence 0 afterwards and confirming it is admitted rather than refused as a duplicate —
/// which only happens if the first record's sequence was never recorded.
/// </remarks>
public class DatagramSessionHostileFragmentDiscardTests
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

    private const byte ModeReliableOrdered = 0b0000_0011;
    private const byte FragmentBit = 0b0000_0100;

    [Fact]
    public void Pump_HostileFragmentWithIndexAtItsCount_DiscardsWholeDatagramAndLeavesTheWindowUntouched()
    {
        AssertMalformedFragmentDiscardsWholeDatagramAndLeavesWindowUntouched(
            fragmentIndex: 2, fragmentCount: 2);
    }

    [Fact]
    public void Pump_HostileFragmentWithIndexBeyondItsCount_DiscardsWholeDatagramAndLeavesTheWindowUntouched()
    {
        AssertMalformedFragmentDiscardsWholeDatagramAndLeavesWindowUntouched(
            fragmentIndex: 5, fragmentCount: 2);
    }

    [Fact]
    public void Pump_HostileFragmentWithCountOfZero_DiscardsWholeDatagramAndLeavesTheWindowUntouched()
    {
        AssertMalformedFragmentDiscardsWholeDatagramAndLeavesWindowUntouched(
            fragmentIndex: 0, fragmentCount: 0);
    }

    [Fact]
    public void Pump_HostileFragmentWithCountOfOne_DiscardsWholeDatagramAndLeavesTheWindowUntouched()
    {
        AssertMalformedFragmentDiscardsWholeDatagramAndLeavesWindowUntouched(
            fragmentIndex: 0, fragmentCount: 1);
    }

    /// <summary>
    /// The shared body every case above drives: a hostile datagram carrying one well-formed record at
    /// sequence 0 ahead of a Fragment whose <paramref name="fragmentIndex"/> and
    /// <paramref name="fragmentCount"/> the codec refuses (#253), then a genuinely well-formed datagram
    /// that resends sequence 0 alone — proving the window never saw the first one.
    /// </summary>
    private static void AssertMalformedFragmentDiscardsWholeDatagramAndLeavesWindowUntouched(
        ushort fragmentIndex, ushort fragmentCount)
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        PeerCounters before = accepter.Reactor.CountersOf(accepterPeer);

        byte[] wellFormedRecord = BuildRecord(ModeReliableOrdered, sequence: 0, payload: [0xAA]);
        byte[] malformedFragmentRecord = BuildFragmentRecord(
            ModeReliableOrdered, sequence: 1, fragmentIndex, fragmentCount, payload: [0xBB]);
        byte[] hostileDatagram =
            [.. BuildHeader(sessionId), .. wellFormedRecord, .. malformedFragmentRecord];

        // Act — the hostile datagram, sent from a bare RawReactor exactly as HostilePeer is built to
        // send from.
        Action sendHostileDatagram = () =>
        {
            client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), hostileDatagram);
            accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDiscards > 0);
        };

        // Assert — nothing throws, the whole datagram is discarded (both the malformed tail and the
        // well-formed record ahead of it), the malformed-discard counter rises by one, and the Session
        // produces no event.
        sendHostileDatagram.Should().NotThrow();

        List<byte[]> deliveredFromHostileDatagram = accepter.DrainPayloads();
        deliveredFromHostileDatagram.Should().BeEmpty(
            "a datagram is a container, not a Message: a forged tail must discard the well-formed " +
            "record that parsed ahead of it too");
        accepter.Reactor.Counters.UnknownSourceDiscards.Should().Be(1);
        accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.DatagramMalformed);

        PeerCounters afterDiscard = accepter.Reactor.CountersOf(accepterPeer);
        afterDiscard.Should().Be(before, "a discarded datagram never reaches record classification at all");

        // Act — a following, genuinely well-formed record at the very sequence the hostile datagram
        // tried to smuggle in.
        byte[] wellFormedFollowUp = BuildRecord(ModeReliableOrdered, sequence: 0, payload: [0xCC]);
        byte[] followUpDatagram = [.. BuildHeader(sessionId), .. wellFormedFollowUp];
        ulong receivedBefore = accepter.Reactor.Counters.DatagramsReceived;

        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), followUpDatagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > receivedBefore);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert — sequence 0 admits now, exactly as it would have had the hostile datagram never
        // arrived. Had the hostile datagram's leading record secretly reached the window, this would be
        // a duplicate and refused instead.
        delivered.Should().ContainSingle(
                "sequence 0 admits at the sequence it would have, undisturbed by the discarded datagram")
            .Which.Should().Equal(new byte[] { 0xCC });

        PeerCounters afterFollowUp = accepter.Reactor.CountersOf(accepterPeer);
        afterFollowUp.MessagesReceived.Should().Be(before.MessagesReceived + 1);
        afterFollowUp.OutOfWindowPackets.Should().Be(before.OutOfWindowPackets, "the window was never touched by the discarded datagram");
        afterFollowUp.DuplicatePackets.Should().Be(before.DuplicatePackets, "the window was never touched by the discarded datagram");

        // The Session stays usable afterwards: CountersOf still resolves this Peer without throwing.
        Action stillUsable = () => accepter.Reactor.CountersOf(accepterPeer);
        stillUsable.Should().NotThrow();
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> — a bare
    /// <see cref="RawReactor"/>, exactly what <see cref="HostilePeer"/> is built from — and returns the
    /// wire session id <paramref name="accepter"/> assigned this Peer, mirroring
    /// <see cref="DatagramSessionDataDatagramParseTests"/>'s helper of the same name.
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
