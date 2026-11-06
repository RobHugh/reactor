using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Session identity — the receiver-assigned wire id, the in-process
/// <see cref="PeerId"/>, and the demultiplex path. The two identities are deliberately different
/// things: the wire carries a random 32-bit token chosen by whichever end will receive datagrams
/// stamped with it, while a <see cref="PeerId"/> is a dense Slot index and a generation, which is
/// exactly why it never leaves the process.
/// </summary>
public class DatagramSessionIdentityTests
{
    private const int ControlDatagramSize = 256;
    private const uint ControlSession = 0;

    private const byte HelloType = 1;
    private const byte HelloWithCookieType = 3;
    private const byte AcceptType = 4;

    private const int HelloBodyLength = 12;
    private const int HelloWithCookieBodyLength = 92;

    private const int CookieLength = 32;

    private const uint ProtocolVersion = 1;
    private const ulong DialerSendIdleIntervalMilliseconds = 60_000;
    private const ulong DialerLingerMilliseconds = 30_000;

    // ---- wire helpers ----

    private static byte[] BuildControlDatagram(byte controlType, ReadOnlySpan<byte> body)
    {
        byte[] datagram = new byte[ControlDatagramSize];
        BinaryPrimitives.WriteUInt32BigEndian(datagram, ControlSession);
        datagram[4] = controlType;
        body.CopyTo(datagram.AsSpan(5));
        return datagram;
    }

    private static byte[] BuildHello(ulong clientNonce)
    {
        byte[] body = new byte[HelloBodyLength];
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(0, 4), ProtocolVersion);
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(4, 8), clientNonce);
        return BuildControlDatagram(HelloType, body);
    }

    private static byte[] BuildHelloWithCookie(ulong clientNonce, long epoch, ReadOnlySpan<byte> cookie, uint sessionIdForMe)
    {
        byte[] body = new byte[HelloWithCookieBodyLength];
        int offset = 0;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), ProtocolVersion);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), clientNonce);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), unchecked((ulong)epoch));
        offset += 8;
        cookie.CopyTo(body.AsSpan(offset, CookieLength));
        offset += CookieLength;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), sessionIdForMe);
        offset += 4;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), 4096);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), DialerSendIdleIntervalMilliseconds);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), DialerLingerMilliseconds);
        return BuildControlDatagram(HelloWithCookieType, body);
    }

    /// <summary>
    /// A well-formed data datagram naming <paramref name="session"/>, which is not a control datagram
    /// and so takes the demux path: just the 20-byte header (both
    /// ack blocks zeroed) and no records. Issue #163 gave the data path a real parser, so a datagram
    /// used here to prove <em>attribution</em> — which Session a stranger or a rebound address lands
    /// on — must itself be well-formed, or it would be discarded as malformed for a reason these tests
    /// are not about.
    /// </summary>
    private static byte[] BuildDataDatagram(uint session)
    {
        byte[] datagram = new byte[20];
        BinaryPrimitives.WriteUInt32BigEndian(datagram, session);
        return datagram;
    }

    private static (long Epoch, byte[] Cookie) ParseCookie(byte[] datagram)
    {
        long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(datagram.AsSpan(13, 8)));
        return (epoch, datagram.AsSpan(21, CookieLength).ToArray());
    }

    private static uint ParseAcceptSessionId(byte[] datagram) => BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(13, 4));

    private static bool Contains(byte[] haystack, byte[] needle) => haystack.AsSpan().IndexOf(needle) >= 0;

    // ---- handshake plumbing ----

    /// <summary>
    /// Drives a whole handshake from a hand-built counterparty and returns the wire session id the
    /// Reactor assigned itself, the <see cref="PeerId"/> it drained, and every datagram it sent back.
    /// </summary>
    private static (uint AssignedSessionId, PeerId Peer, List<byte[]> Replies) CompleteHandshake(
        DatagramSessionReactorHarness accepter, RawPeer peer, ulong clientNonce)
    {
        List<byte[]> replies = [];

        ulong sentBefore = accepter.Reactor.Counters.DatagramsSent;
        peer.Send(accepter.Endpoint, BuildHello(clientNonce));
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsSent > sentBefore);
        byte[] cookieReply = peer.Receive();
        replies.Add(cookieReply);
        (long epoch, byte[] cookie) = ParseCookie(cookieReply);

        sentBefore = accepter.Reactor.Counters.DatagramsSent;
        peer.Send(accepter.Endpoint, BuildHelloWithCookie(clientNonce, epoch, cookie, sessionIdForMe: 0xABCD_1234));
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsSent > sentBefore);
        byte[] acceptReply = peer.Receive();
        replies.Add(acceptReply);
        acceptReply[4].Should().Be(AcceptType);

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();
        drained.Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerConnected);

        return (ParseAcceptSessionId(acceptReply), drained[0].Peer, replies);
    }

    // ---- the in-process identity ----

    [Fact]
    public void Connect_TwoSessionsAgainstOneReactor_AreAddressedByDifferentPeerIds()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using RawPeer first = new();
        using RawPeer second = new();

        // Act
        (_, PeerId firstPeer, _) = CompleteHandshake(accepter, first, clientNonce: 1);
        (_, PeerId secondPeer, _) = CompleteHandshake(accepter, second, clientNonce: 2);

        // Assert
        firstPeer.Should().NotBe(secondPeer);
    }

    [Fact]
    public void Connect_OnceTheHandshakeCompletes_DrainsThePeerIdItReturned()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);

        // Act
        PeerId dialled = dialler.Reactor.Connect(accepter.Endpoint);
        (var diallerEvents, _) = DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);

        // Assert
        diallerEvents.Should().ContainSingle()
            .Which.Should().BeEquivalentTo((SessionEventKind.PeerConnected, dialled, default(DisconnectReason)));
    }

    // ---- the wire identity ----

    [Fact]
    public void OnHelloWithCookie_WhenTheHandshakeCompletes_AssignsANonZeroWireSessionId()
    {
        // Arrange — a wire session id of zero means "control datagram", so a Session may never hold it.
        using DatagramSessionReactorHarness accepter = new();
        using RawPeer peer = new();

        // Act
        (uint assigned, _, _) = CompleteHandshake(accepter, peer, clientNonce: 7);

        // Assert
        assigned.Should().NotBe(ControlSession);
    }

    [Fact]
    public void OnHelloWithCookie_AcrossSeveralSessions_AssignsWireIdsThatAreNotSlotIndices()
    {
        // Arrange — a Slot index is dense and guessable, so an off-path attacker who knew the wire id
        // were the index could address a Session it was never introduced to. The wire id is drawn from
        // entropy instead, and this is the observable consequence: no assigned id lands in the tiny
        // range a Slot index occupies, and no two are adjacent.
        const int capacity = 4;
        using DatagramSessionReactorHarness accepter = new(capacity: capacity);
        List<RawPeer> peers = [.. Enumerable.Range(0, 3).Select(_ => new RawPeer())];
        try
        {
            // Act
            List<uint> assigned = [.. peers.Select((peer, index) => CompleteHandshake(accepter, peer, (ulong)index + 1).AssignedSessionId)];

            // Assert
            assigned.Should().OnlyHaveUniqueItems();
            assigned.Should().OnlyContain(id => id > capacity, "a Slot index is guessable, so it is not what goes on the wire");
            assigned.Order().Zip(assigned.Order().Skip(1), (low, high) => high - low)
                .Should().OnlyContain(gap => gap > 1, "consecutively assigned ids would betray the allocation order");
        }
        finally
        {
            foreach (RawPeer peer in peers)
            {
                peer.Dispose();
            }
        }
    }

    [Fact]
    public void OnHelloWithCookie_OnTwoReactorsWithIndependentEntropy_AssignsDifferentWireSessionIds()
    {
        // Arrange
        using DatagramSessionReactorHarness first = new();
        using DatagramSessionReactorHarness second = new();
        using RawPeer firstPeer = new();
        using RawPeer secondPeer = new();

        // Act
        (uint firstAssigned, _, _) = CompleteHandshake(first, firstPeer, clientNonce: 11);
        (uint secondAssigned, _, _) = CompleteHandshake(second, secondPeer, clientNonce: 11);

        // Assert
        firstAssigned.Should().NotBe(secondAssigned);
    }

    [Fact]
    public void Connect_OnTwoReactors_GivesTheirFirstPeersDifferentPeerIds()
    {
        // Arrange — the first Peer of any Reactor sits in Slot 0 on generation 1, so a PeerId that
        // carried only those two would be the same value on both of these.
        using DatagramSessionReactorHarness first = new();
        using DatagramSessionReactorHarness second = new();
        using RawPeer firstPeer = new();
        using RawPeer secondPeer = new();

        // Act
        (_, PeerId firstEstablished, _) = CompleteHandshake(first, firstPeer, clientNonce: 31);
        (_, PeerId secondEstablished, _) = CompleteHandshake(second, secondPeer, clientNonce: 31);

        // Assert
        firstEstablished.Should().NotBe(secondEstablished);
    }

    [Fact]
    public void Close_APeerIdIssuedByAnotherReactor_LeavesThisReactorsPeerOpen()
    {
        // Arrange
        using DatagramSessionReactorHarness first = new();
        using DatagramSessionReactorHarness second = new();
        using RawPeer firstPeer = new();
        using RawPeer secondPeer = new();
        (_, PeerId issuedByFirst, _) = CompleteHandshake(first, firstPeer, clientNonce: 37);
        (uint assignedBySecond, _, _) = CompleteHandshake(second, secondPeer, clientNonce: 37);
        second.Reactor.Pump();

        // Act — a consumer running two Reactors hands the wrong one's PeerId over.
        second.Reactor.Close(issuedByFirst);

        // Assert — it addressed nothing here, so nothing closed.
        second.DrainToList().Should().BeEmpty();

        // And the Peer that PeerId would have collided with is still established.
        secondPeer.Send(second.Endpoint, BuildDataDatagram(assignedBySecond));
        second.Reactor.Pump();
        second.DrainToList().Should().BeEmpty();
        second.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
    }

    [Fact]
    public void OnHelloWithCookie_WhileEstablishingASession_PutsNoPartOfThePeerIdOnTheWire()
    {
        // Arrange — necessary rather than sufficient: it proves the Index and Generation pair appears
        // in no datagram under either byte order, not that the identity is unrecoverable by any means.
        using DatagramSessionReactorHarness accepter = new();
        using RawPeer peer = new();

        // Act
        (_, PeerId established, List<byte[]> replies) = CompleteHandshake(accepter, peer, clientNonce: 13);

        // Assert
        byte[] bigEndian = new byte[sizeof(int) + sizeof(ulong)];
        BinaryPrimitives.WriteInt32BigEndian(bigEndian.AsSpan(0, 4), established.Index);
        BinaryPrimitives.WriteUInt64BigEndian(bigEndian.AsSpan(4, 8), established.Generation);

        byte[] littleEndian = new byte[sizeof(int) + sizeof(ulong)];
        BinaryPrimitives.WriteInt32LittleEndian(littleEndian.AsSpan(0, 4), established.Index);
        BinaryPrimitives.WriteUInt64LittleEndian(littleEndian.AsSpan(4, 8), established.Generation);

        bool anyLeaked = replies.Any(reply => Contains(reply, bigEndian) || Contains(reply, littleEndian));
        anyLeaked.Should().BeFalse(
            "the in-process identity is a dense Slot index, which is why it is never the wire identity");
    }

    // ---- demultiplexing ----

    [Fact]
    public void Pump_WhenALiveSessionsDatagramArrivesFromANewAddress_StillAttributesItToThatSession()
    {
        // Arrange — a Peer behind a NAT that rebinds, or a device that changes network, keeps its
        // Session: the identity is a token rather than an address, so the source is deliberately not
        // checked on the demux path.
        using DatagramSessionReactorHarness accepter = new();
        using RawPeer peer = new();
        (uint assigned, _, _) = CompleteHandshake(accepter, peer, clientNonce: 17);
        ulong unknownBefore = accepter.Reactor.Counters.UnknownSourceDatagrams;

        // Act
        using RawPeer rebound = new();
        rebound.Send(accepter.Endpoint, BuildDataDatagram(assigned));
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > 0);

        // Assert
        accepter.Reactor.Counters.UnknownSourceDatagrams.Should().Be(unknownBefore);
    }

    [Fact]
    public void Pump_WhenAStrangerNamesAnUnknownSessionId_DiscardsItAndCreatesNoSession()
    {
        // Arrange — a Session is created by the handshake and by nothing else.
        using DatagramSessionReactorHarness accepter = new();
        using RawPeer stranger = new();

        // Act
        stranger.Send(accepter.Endpoint, BuildDataDatagram(0xDEAD_BEEF));
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDatagrams > 0);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();

        // Assert
        drained.Should().BeEmpty();
        accepter.Reactor.Counters.UnknownSourceDatagrams.Should().Be(1);
        accepter.Sink.Received.Should().Contain(record => record.Id == DiagnosticId.DatagramMalformed);
    }

    [Fact]
    public void Pump_WhenADatagramNamesAClosedSessionId_DiscardsItRatherThanRevivingTheSession()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using RawPeer peer = new();
        (uint assigned, PeerId established, _) = CompleteHandshake(accepter, peer, clientNonce: 19);
        accepter.Reactor.Pump(); // Ends the Frame the PeerConnected belongs to, so the drain below sees only the close.
        accepter.CloseImmediately(established);
        accepter.DrainToList().Where(e => e.Kind == SessionEventKind.PeerClosed).Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerClosed);
        ulong unknownBefore = accepter.Reactor.Counters.UnknownSourceDatagrams;

        // Act
        peer.Send(accepter.Endpoint, BuildDataDatagram(assigned));
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDatagrams > unknownBefore);

        // Assert
        accepter.Reactor.Counters.UnknownSourceDatagrams.Should().Be(unknownBefore + 1);
        accepter.PumpAndDrainAll(3).Should().BeEmpty();
    }

    // ---- closing ----

    [Fact]
    public void Close_AnInterruptedPeer_DrainsPeerClosedForThatPeerIdWithLocalClose()
    {
        // Arrange — an Established Peer's Close now starts a Goodbye (issue #397), so the immediate
        // close this test meant is exercised on an Interrupted Peer, which keeps it.
        using DatagramSessionReactorHarness accepter = new();
        using RawPeer peer = new();
        (_, PeerId established, _) = CompleteHandshake(accepter, peer, clientNonce: 23);
        accepter.Reactor.Pump(); // Ends the Frame the PeerConnected belongs to, so the drain below sees only the close.

        // Act
        accepter.CloseImmediately(established);

        // Assert
        accepter.DrainToList().Where(e => e.Kind == SessionEventKind.PeerClosed).Should().ContainSingle()
            .Which.Should().BeEquivalentTo((SessionEventKind.PeerClosed, established, DisconnectReason.LocalClose));
    }

    [Fact]
    public void Close_APeerIdThisReactorNeverIssued_IsHarmless()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();

        // Act
        Action close = () => accepter.Reactor.Close(new PeerId(Origin: 0, Index: 0, Generation: 999));

        // Assert
        close.Should().NotThrow();
        accepter.DrainToList().Should().BeEmpty();
    }

    [Fact]
    public void Close_CalledTwiceOnTheSamePeer_DrainsNoSecondPeerClosed()
    {
        // Arrange — a Peer closing under a caller is an ordinary race rather than a programmer error,
        // and the Slot's generation is what makes the second handle detectably stale.
        using DatagramSessionReactorHarness accepter = new();
        using RawPeer peer = new();
        (_, PeerId established, _) = CompleteHandshake(accepter, peer, clientNonce: 29);
        accepter.Reactor.Pump(); // Ends the Frame the PeerConnected belongs to, so the drain below sees only the close.
        accepter.CloseImmediately(established);
        accepter.DrainToList().Where(e => e.Kind == SessionEventKind.PeerClosed).Should().ContainSingle();

        // Act
        Action closeAgain = () => accepter.Reactor.Close(established);

        // Assert — the Frame still holds exactly the one close: a drain is repeatable within a Frame,
        // so a second PeerClosed would show up here as a second item rather than as a fresh drain.
        closeAgain.Should().NotThrow();
        accepter.DrainToList().Where(e => e.Kind == SessionEventKind.PeerClosed).Should().ContainSingle();
    }

    /// <summary>
    /// A plain BCL UDP socket standing in for a counterparty this Reactor has never seen. Unlike
    /// <see cref="HostilePeer"/> it reads as well as writes, because an identity test must see the wire
    /// session id the Reactor chose rather than merely put bytes in front of it.
    /// </summary>
    private sealed class RawPeer : IDisposable
    {
        private readonly Socket _socket;

        public RawPeer()
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            _socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            _socket.ReceiveTimeout = 5000;
        }

        public void Send(IPEndPoint destination, byte[] datagram) => _socket.SendTo(datagram, destination);

        public byte[] Receive()
        {
            byte[] buffer = new byte[512];
            EndPoint remote = new IPEndPoint(IPAddress.Any, 0);
            int read = _socket.ReceiveFrom(buffer, ref remote);
            return buffer[..read];
        }

        public void Dispose() => _socket.Dispose();
    }
}
