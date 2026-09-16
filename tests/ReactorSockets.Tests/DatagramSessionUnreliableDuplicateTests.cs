using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.6.12 (issue #222; ADR 0017): plain <see cref="DeliveryMode.Unreliable"/> suppresses a
/// duplicate within a bounded sliding window of 64 sequences and delivers one delayed past it, so
/// suppression is bounded rather than absolute. Both directions of that boundary matter, which is why
/// the two tests below sit either side of it.
/// </summary>
/// <remarks>
/// The instrument is the one <see cref="DatagramSessionUnreliableLossTests"/> and
/// <see cref="DatagramSessionSpaceRoutingTests"/> already use: a raw dialler completing an ordinary
/// handshake by hand, then hand-built data datagrams sent at the live session id the accepter
/// assigned. Two live Sessions cannot express these tests â€” a Send allocates the next sequence in its
/// Space and nothing lets a test replay an old one, which is exactly what a duplicate is.
/// </remarks>
public class DatagramSessionUnreliableDuplicateTests
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
    private const byte ModeUnreliable = 0b0000_0000;

    /// <summary>
    /// The newest sequence every test below drives the window to before replaying an older one, chosen
    /// well clear of zero so a sequence 65 below it is still a real sequence rather than a wrap.
    /// </summary>
    private const uint Newest = 100;

    [Fact]
    public void Pump_ADuplicateAtDistanceSixtyThree_IsSuppressed()
    {
        // Arrange â€” sequence 37 arrives, then 100, which leaves 37 sitting at distance 63: the last
        // sequence the 64-entry window still remembers.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        const uint replayed = Newest - 63;
        SendUnreliable(accepter, client, clientLink, sessionId, replayed, [1]);
        SendUnreliable(accepter, client, clientLink, sessionId, Newest, [2]);
        accepter.DrainAndDiscard();

        PeerCounters before = accepter.Reactor.CountersOf(accepterPeer);

        // Act â€” the same sequence again, now at the far edge of the window.
        List<byte[]> delivered = SendUnreliable(accepter, client, clientLink, sessionId, replayed, [1]);

        // Assert
        delivered.Should().BeEmpty("a repeat still inside the 64-sequence window is suppressed");
        PeerCounters after = accepter.Reactor.CountersOf(accepterPeer);
        after.MessagesReceived.Should().Be(before.MessagesReceived, "a suppressed repeat is not counted as received");

        // The repeat lands on the out-of-window figure rather than the duplicate one: the receive
        // window's base had already passed this sequence, so that window refused and counted it before
        // the suppression window was asked. Both descriptions are true of the same record, and it is
        // counted once â€” which is the point. The duplicate figure is what rises when the receive window
        // admitted a repeat and only the suppression window recognised it.
        (after.OutOfWindowPackets + after.DuplicatePackets)
            .Should().Be(before.OutOfWindowPackets + before.DuplicatePackets + 1, "the repeat was refused and counted exactly once");
    }

    [Fact]
    public void Pump_ADuplicateAtDistanceSixtyFive_IsDelivered()
    {
        // Arrange â€” sequence 35 arrives, then 100, which puts 35 at distance 65: past everything the
        // 64-entry window can still remember.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        const uint replayed = Newest - 65;
        SendUnreliable(accepter, client, clientLink, sessionId, replayed, [1]);
        SendUnreliable(accepter, client, clientLink, sessionId, Newest, [2]);
        accepter.DrainAndDiscard();

        ulong receivedBefore = accepter.Reactor.CountersOf(accepterPeer).MessagesReceived;

        // Act â€” the same sequence again, now beyond the window.
        List<byte[]> delivered = SendUnreliable(accepter, client, clientLink, sessionId, replayed, [1]);

        // Assert â€” suppression is bounded, so this one is delivered rather than silently dropped.
        delivered.Should().ContainSingle("a repeat delayed past 64 sequences is delivered")
            .Which.Should().Equal(new byte[] { 1 });
        accepter.Reactor.CountersOf(accepterPeer).MessagesReceived.Should().Be(
            receivedBefore + 1, "what was delivered was counted as received");
    }

    [Fact]
    public void Pump_AnOutOfOrderFirstArrivalInsideTheWindow_IsDelivered()
    {
        // Arrange â€” unlike UnreliableSequenced, plain Unreliable has no strictly-newer rule: a sequence
        // arriving out of order is a first arrival, not a stale one, and is delivered.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        SendUnreliable(accepter, client, clientLink, sessionId, Newest, [2]);
        accepter.DrainAndDiscard();
        ulong receivedBefore = accepter.Reactor.CountersOf(accepterPeer).MessagesReceived;

        // Act â€” a sequence below the newest, never seen before.
        List<byte[]> delivered = SendUnreliable(accepter, client, clientLink, sessionId, Newest - 10, [3]);

        // Assert
        delivered.Should().ContainSingle("an out-of-order first arrival is not a duplicate")
            .Which.Should().Equal(new byte[] { 3 });
        accepter.Reactor.CountersOf(accepterPeer).MessagesReceived.Should().Be(receivedBefore + 1);
    }

    [Fact]
    public void Pump_TheSuppressionWidth_DoesNotFollowTheConsumersReceiveWindow()
    {
        // The 64 is a protocol constant, not a knob. An accepter whose receive window is deliberately
        // tiny still suppresses a repeat 63 sequences back â€” had suppression been sized from
        // ReceiveWindowFragments, this window would be 2 wide and the repeat would be delivered.
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: 2, maxMessageSize: 1300);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        const uint replayed = Newest - 63;
        SendUnreliable(accepter, client, clientLink, sessionId, replayed, [1]);
        SendUnreliable(accepter, client, clientLink, sessionId, Newest, [2]);
        accepter.DrainAndDiscard();
        ulong receivedBefore = accepter.Reactor.CountersOf(accepterPeer).MessagesReceived;

        // Act
        List<byte[]> delivered = SendUnreliable(accepter, client, clientLink, sessionId, replayed, [1]);

        // Assert
        delivered.Should().BeEmpty("suppression is 64 wide whatever the consumer sized their receive window at");
        accepter.Reactor.CountersOf(accepterPeer).MessagesReceived.Should().Be(receivedBefore);
    }

    /// <summary>
    /// Sends one Unreliable record at <paramref name="sequence"/>, Pumps the accepter until the
    /// datagram has been taken off the wire, and returns whatever it delivered for that Frame.
    /// </summary>
    private static List<byte[]> SendUnreliable(
        DatagramSessionReactorHarness accepter,
        RawReactor client,
        LinkId clientLink,
        uint sessionId,
        uint sequence,
        byte[] payload)
    {
        byte[] datagram = [.. BuildHeader(sessionId), .. BuildRecord(ModeUnreliable, sequence, payload)];
        ulong before = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);
        return accepter.DrainPayloads();
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> â€” a bare
    /// <see cref="RawReactor"/> â€” and returns the wire session id <paramref name="accepter"/> assigned
    /// this Peer, mirroring <see cref="DatagramSessionUnreliableLossTests"/>'s helper of the same name.
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
        const int unfragmentedRecordSize = 7;
        byte[] record = new byte[unfragmentedRecordSize + payload.Length];
        record[0] = modeBits;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        payload.CopyTo(record.AsSpan(unfragmentedRecordSize));
        return record;
    }
}
