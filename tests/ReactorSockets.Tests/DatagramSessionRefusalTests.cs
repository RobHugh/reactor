using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Refusal — a version mismatch, capacity exhaustion, and the admission cap.
/// Every test drives a real <see cref="DatagramSessionReactor"/> with hand-built control datagrams (a
/// 256-byte, big-endian, zero-padded control datagram — session id, control type, body, padding) sent from a <see cref="RawPeer"/>, a plain BCL
/// UDP socket standing in for a counterparty this Reactor has never seen. A genuine Cookie is always
/// obtained by sending a real Hello and reading the Reactor's own reply, never fabricated, because the
/// Reactor authenticates it before doing anything else.
/// </summary>
public class DatagramSessionRefusalTests
{
    private const int ControlDatagramSize = 256;
    private const uint ControlSession = 0;

    private const byte HelloType = 1;
    private const byte CookieType = 2;
    private const byte HelloWithCookieType = 3;
    private const byte AcceptType = 4;
    private const byte RefusalType = 5;

    private const int HelloBodyLength = 12;
    private const int CookieBodyLength = 48;
    private const int HelloWithCookieBodyLength = 92;
    private const int RefusalBodyLength = 17;

    private const int CookieLength = 32;
    private const int ResumeSecretLength = 16;

    private const uint ProtocolVersion = 1;

    private const byte RefusalReasonVersion = 1;
    private const byte RefusalReasonCapacity = 2;

    // ---- wire helpers: build ----

    private static byte[] BuildControlDatagram(byte controlType, ReadOnlySpan<byte> body)
    {
        byte[] datagram = new byte[ControlDatagramSize];
        BinaryPrimitives.WriteUInt32BigEndian(datagram, ControlSession);
        datagram[4] = controlType;
        body.CopyTo(datagram.AsSpan(5));
        return datagram;
    }

    private static byte[] BuildHello(ulong clientNonce, uint version = ProtocolVersion)
    {
        byte[] body = new byte[HelloBodyLength];
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(0, 4), version);
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(4, 8), clientNonce);
        return BuildControlDatagram(HelloType, body);
    }

    private static byte[] BuildCookie(ulong clientNonce, long epoch, ReadOnlySpan<byte> cookie)
    {
        byte[] body = new byte[CookieBodyLength];
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(0, 8), clientNonce);
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(8, 8), unchecked((ulong)epoch));
        cookie.CopyTo(body.AsSpan(16, CookieLength));
        return BuildControlDatagram(CookieType, body);
    }

    private static byte[] BuildHelloWithCookie(
        uint version,
        ulong clientNonce,
        long epoch,
        ReadOnlySpan<byte> cookie,
        uint sessionIdForMe,
        uint maxMessageSize,
        ReadOnlySpan<byte> resumeSecret,
        ulong sendIdleIntervalMilliseconds = 60_000,
        ulong lingerMilliseconds = 30_000)
    {
        byte[] body = new byte[HelloWithCookieBodyLength];
        int offset = 0;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), version);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), clientNonce);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), unchecked((ulong)epoch));
        offset += 8;
        cookie.CopyTo(body.AsSpan(offset, CookieLength));
        offset += CookieLength;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), sessionIdForMe);
        offset += 4;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), maxMessageSize);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), sendIdleIntervalMilliseconds);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), lingerMilliseconds);
        offset += 8;
        resumeSecret.CopyTo(body.AsSpan(offset, ResumeSecretLength));
        return BuildControlDatagram(HelloWithCookieType, body);
    }

    private static byte[] BuildRefusal(ulong clientNonce, byte reason, uint versionOffered, uint versionRequired)
    {
        byte[] body = new byte[RefusalBodyLength];
        int offset = 0;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), clientNonce);
        offset += 8;
        body[offset] = reason;
        offset += 1;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), versionOffered);
        offset += 4;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), versionRequired);
        return BuildControlDatagram(RefusalType, body);
    }

    // ---- wire helpers: parse ----

    private static ulong ParseHelloClientNonce(byte[] datagram) => BinaryPrimitives.ReadUInt64BigEndian(datagram.AsSpan(9, 8));

    private static (ulong ClientNonce, long Epoch, byte[] Cookie) ParseCookie(byte[] datagram)
    {
        ulong clientNonce = BinaryPrimitives.ReadUInt64BigEndian(datagram.AsSpan(5, 8));
        long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(datagram.AsSpan(13, 8)));
        byte[] cookie = datagram.AsSpan(21, CookieLength).ToArray();
        return (clientNonce, epoch, cookie);
    }

    private static (ulong ClientNonce, byte Reason, uint VersionOffered, uint VersionRequired) ParseRefusal(byte[] datagram)
    {
        ulong clientNonce = BinaryPrimitives.ReadUInt64BigEndian(datagram.AsSpan(5, 8));
        byte reason = datagram[13];
        uint versionOffered = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(14, 4));
        uint versionRequired = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(18, 4));
        return (clientNonce, reason, versionOffered, versionRequired);
    }

    // ---- handshake plumbing shared by every test below ----

    /// <summary>
    /// Sends a Hello for <paramref name="clientNonce"/> from <paramref name="peer"/> and returns the
    /// genuine epoch and cookie the Reactor's own <c>CookieMinter</c> replied with — never fabricated,
    /// because the Reactor authenticates every cookie it is handed back before doing anything else.
    /// </summary>
    private static (long Epoch, byte[] Cookie) ObtainCookie(DatagramSessionReactorHarness accepter, RawPeer peer, ulong clientNonce)
    {
        ulong sentBefore = accepter.Reactor.Counters.DatagramsSent;
        peer.Send(accepter.Endpoint, BuildHello(clientNonce));
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsSent > sentBefore);
        byte[] reply = peer.Receive(out _);
        (ulong nonce, long epoch, byte[] cookie) = ParseCookie(reply);
        nonce.Should().Be(clientNonce);
        return (epoch, cookie);
    }

    /// <summary>
    /// Drives a full handshake to completion for <paramref name="clientNonce"/> and returns the
    /// <see cref="PeerId"/> the accepter assigned it, so a later test can close exactly that Peer.
    /// </summary>
    private static PeerId CompleteHandshake(DatagramSessionReactorHarness accepter, RawPeer peer, ulong clientNonce)
    {
        byte[] reply = ReplyToHandshake(accepter, peer, clientNonce);
        reply[4].Should().Be(AcceptType);

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();
        return drained.Should().ContainSingle(evt => evt.Kind == SessionEventKind.PeerConnected).Which.Peer;
    }

    /// <summary>
    /// Runs a handshake for <paramref name="clientNonce"/> and returns the reply to its
    /// Hello-with-cookie, for a test where that reply may be a Refusal.
    /// </summary>
    private static byte[] ReplyToHandshake(DatagramSessionReactorHarness accepter, RawPeer peer, ulong clientNonce)
    {
        (long epoch, byte[] cookie) = ObtainCookie(accepter, peer, clientNonce);
        ulong sentBefore = accepter.Reactor.Counters.DatagramsSent;
        peer.Send(accepter.Endpoint, BuildHelloWithCookie(
            ProtocolVersion, clientNonce, epoch, cookie, sessionIdForMe: 1, maxMessageSize: 4096, resumeSecret: new byte[ResumeSecretLength]));
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsSent > sentBefore);
        return peer.Receive(out _);
    }

    // =========================================================================================
    // Version refusal
    // =========================================================================================

    [Fact]
    public void OnHelloWithCookie_OfferingAVersionTheReactorDoesNotSpeak_RefusesNamingBothVersions()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using RawPeer peer = new();
        const ulong clientNonce = 0x1122_3344_5566_7788UL;
        (long epoch, byte[] cookie) = ObtainCookie(accepter, peer, clientNonce);
        ulong sentBefore = accepter.Reactor.Counters.DatagramsSent;

        // Act
        peer.Send(accepter.Endpoint, BuildHelloWithCookie(
            version: 2, clientNonce, epoch, cookie, sessionIdForMe: 1, maxMessageSize: 4096, resumeSecret: new byte[ResumeSecretLength]));
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsSent > sentBefore);
        byte[] reply = peer.Receive(out _);

        // Assert
        reply[4].Should().Be(RefusalType);
        (ulong nonce, byte reason, uint versionOffered, uint versionRequired) = ParseRefusal(reply);
        nonce.Should().Be(clientNonce);
        reason.Should().Be(RefusalReasonVersion);
        versionOffered.Should().Be(2);
        versionRequired.Should().Be(ProtocolVersion);
    }

    [Fact]
    public void OnHelloWithCookie_OfferingAVersionTheReactorDoesNotSpeak_LogsVersionRefusedAtWarnNotError()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using RawPeer peer = new();
        const ulong clientNonce = 0x2233_4455_6677_8899UL;
        (long epoch, byte[] cookie) = ObtainCookie(accepter, peer, clientNonce);

        // Act
        peer.Send(accepter.Endpoint, BuildHelloWithCookie(
            version: 2, clientNonce, epoch, cookie, sessionIdForMe: 1, maxMessageSize: 4096, resumeSecret: new byte[ResumeSecretLength]));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(record => record.Id == DiagnosticId.VersionRefused));

        // Assert — a remote can produce this at will, so it is a Sink record at Warn, never an Error.
        DiagnosticRecord record = accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.VersionRefused).Which;
        record.Level.Should().Be(DiagnosticLevel.Warn);
    }

    [Fact]
    public void OnHelloWithCookie_OfferingAVersionTheReactorDoesNotSpeak_ProducesNoSessionEventAndLeavesCapacityFree()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new(capacity: 1);
        using RawPeer peer = new();
        const ulong clientNonce = 0x3344_5566_7788_99AAUL;
        (long epoch, byte[] cookie) = ObtainCookie(accepter, peer, clientNonce);

        // Act
        peer.Send(accepter.Endpoint, BuildHelloWithCookie(
            version: 2, clientNonce, epoch, cookie, sessionIdForMe: 1, maxMessageSize: 4096, resumeSecret: new byte[ResumeSecretLength]));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(record => record.Id == DiagnosticId.VersionRefused));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();

        // Assert — no SessionEvent for the refusal.
        drained.Should().BeEmpty();

        // Assert — the capacity Slot the refused Hello never occupied is still free.
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        PeerId dialled = dialler.Reactor.Connect(accepter.Endpoint);
        (var diallerEvents, var accepterEvents) = DatagramSessionReactorHarness.PumpBothUntilConnected(
            dialler, accepter);

        diallerEvents.Should().ContainSingle()
            .Which.Should().BeEquivalentTo((SessionEventKind.PeerConnected, dialled, default(DisconnectReason)));
        accepterEvents.Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerConnected);
    }

    [Fact]
    public void OnHelloWithCookie_OfferingVersionZero_IsRefusedTheSameWayAsAnUnknownVersion()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using RawPeer peer = new();
        const ulong clientNonce = 0x4455_6677_8899_AABBUL;
        (long epoch, byte[] cookie) = ObtainCookie(accepter, peer, clientNonce);
        ulong sentBefore = accepter.Reactor.Counters.DatagramsSent;

        // Act
        peer.Send(accepter.Endpoint, BuildHelloWithCookie(
            version: 0, clientNonce, epoch, cookie, sessionIdForMe: 1, maxMessageSize: 4096, resumeSecret: new byte[ResumeSecretLength]));
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsSent > sentBefore);
        byte[] reply = peer.Receive(out _);

        // Assert
        reply[4].Should().Be(RefusalType);
        (ulong nonce, byte reason, uint versionOffered, uint versionRequired) = ParseRefusal(reply);
        nonce.Should().Be(clientNonce);
        reason.Should().Be(RefusalReasonVersion);
        versionOffered.Should().Be(0);
        versionRequired.Should().Be(ProtocolVersion);
    }

    [Fact]
    public void Connect_WhenTheFarEndRefusesForVersion_DrainsPeerClosedWithVersionMismatch()
    {
        // Arrange — a hand-built accepter, since the two harnessed Reactors necessarily agree on
        // version and could never produce this Refusal themselves.
        using RawPeer accepter = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);

        // Act
        PeerId dialled = dialler.Reactor.Connect(accepter.LocalEndPoint);
        byte[] hello = accepter.Receive(out IPEndPoint diallerEndpoint);
        ulong clientNonce = ParseHelloClientNonce(hello);

        accepter.Send(diallerEndpoint, BuildCookie(clientNonce, epoch: 0, cookie: new byte[CookieLength]));
        dialler.Reactor.Pump();
        accepter.Receive(out _); // The dialler's HelloWithCookie — its content does not matter here.

        accepter.Send(diallerEndpoint, BuildRefusal(clientNonce, RefusalReasonVersion, versionOffered: 2, versionRequired: ProtocolVersion));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained =
            dialler.PumpUntilDrained(e => e.Any(x => x.Kind == SessionEventKind.PeerClosed));

        // Assert
        drained.Should().ContainSingle()
            .Which.Should().BeEquivalentTo((SessionEventKind.PeerClosed, dialled, DisconnectReason.VersionMismatch));
    }

    [Fact]
    public void OnRefusal_UnknownReasonByte_IsDiscardedRatherThanMappedToADefaultDisconnectReason()
    {
        // Arrange — the discard arm refuses: a reason byte this profile does not understand must not
        // be silently treated as some default disconnection.
        using RawPeer accepter = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);

        dialler.Reactor.Connect(accepter.LocalEndPoint);
        byte[] hello = accepter.Receive(out IPEndPoint diallerEndpoint);
        ulong clientNonce = ParseHelloClientNonce(hello);
        accepter.Send(diallerEndpoint, BuildCookie(clientNonce, epoch: 0, cookie: new byte[CookieLength]));
        dialler.Reactor.Pump();
        accepter.Receive(out _);

        // Act
        accepter.Send(diallerEndpoint, BuildRefusal(clientNonce, reason: 99, versionOffered: 0, versionRequired: 0));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = dialler.PumpAndDrainAll(3);

        // Assert
        drained.Should().BeEmpty();
    }

    [Fact]
    public void OnRefusal_ClientNonceMatchesNoOutstandingDial_IsIgnoredWithoutThrowOrEvent()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new();
        using RawPeer peer = new();

        // Act
        Action act = () => peer.Send(dialler.Endpoint, BuildRefusal(clientNonce: 0xDEAD_BEEF_DEAD_BEEFUL, RefusalReasonVersion, 2, 1));
        act.Should().NotThrow();
        dialler.PumpUntil(() => dialler.Reactor.Counters.UnknownSourceDatagrams > 0);

        // Assert
        dialler.DrainToList().Should().BeEmpty();
    }

    [Fact]
    public void OnRefusal_ArrivingFromADifferentSourceThanTheOneDialled_IsIgnored()
    {
        // Arrange
        using RawPeer peer = new();
        using RawPeer impostor = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);

        dialler.Reactor.Connect(peer.LocalEndPoint);
        byte[] hello = peer.Receive(out IPEndPoint diallerEndpoint);
        ulong clientNonce = ParseHelloClientNonce(hello);
        peer.Send(diallerEndpoint, BuildCookie(clientNonce, epoch: 0, cookie: new byte[CookieLength]));
        dialler.Reactor.Pump();
        peer.Receive(out _);

        // Act — the impostor knows the right nonce but sends from a different address than the one dialled.
        impostor.Send(diallerEndpoint, BuildRefusal(clientNonce, RefusalReasonVersion, 2, 1));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = dialler.PumpAndDrainAll(3);

        // Assert
        drained.Should().BeEmpty();
    }

    // =========================================================================================
    // Capacity refusal
    // =========================================================================================

    [Fact]
    public void OnHelloWithCookie_EveryCapacitySlotOccupied_RefusesWithReasonCapacity()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new(capacity: 2);
        using RawPeer peerA = new();
        using RawPeer peerB = new();
        using RawPeer peerC = new();
        CompleteHandshake(accepter, peerA, 0x0000_0000_0000_0001UL);
        CompleteHandshake(accepter, peerB, 0x0000_0000_0000_0002UL);

        const ulong clientNonceC = 0x0000_0000_0000_0003UL;
        (long epoch, byte[] cookie) = ObtainCookie(accepter, peerC, clientNonceC);
        ulong sentBefore = accepter.Reactor.Counters.DatagramsSent;

        // Act
        peerC.Send(accepter.Endpoint, BuildHelloWithCookie(
            ProtocolVersion, clientNonceC, epoch, cookie, sessionIdForMe: 1, maxMessageSize: 4096, resumeSecret: new byte[ResumeSecretLength]));
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsSent > sentBefore);
        byte[] reply = peerC.Receive(out _);

        // Assert
        reply[4].Should().Be(RefusalType);
        (ulong nonce, byte reason, _, _) = ParseRefusal(reply);
        nonce.Should().Be(clientNonceC);
        reason.Should().Be(RefusalReasonCapacity);
    }

    [Fact]
    public void OnHelloWithCookie_EveryCapacitySlotOccupied_RaisesAdmissionDiscardsAndLogsCapacityExhausted()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new(capacity: 2);
        using RawPeer peerA = new();
        using RawPeer peerB = new();
        using RawPeer peerC = new();
        CompleteHandshake(accepter, peerA, 0x0000_0000_0000_0011UL);
        CompleteHandshake(accepter, peerB, 0x0000_0000_0000_0012UL);

        const ulong clientNonceC = 0x0000_0000_0000_0013UL;
        (long epoch, byte[] cookie) = ObtainCookie(accepter, peerC, clientNonceC);
        ulong discardsBefore = accepter.Reactor.Counters.AdmissionDiscards;

        // Act
        peerC.Send(accepter.Endpoint, BuildHelloWithCookie(
            ProtocolVersion, clientNonceC, epoch, cookie, sessionIdForMe: 1, maxMessageSize: 4096, resumeSecret: new byte[ResumeSecretLength]));
        accepter.PumpUntil(() => accepter.Reactor.Counters.AdmissionDiscards > discardsBefore);

        // Assert
        (accepter.Reactor.Counters.AdmissionDiscards - discardsBefore).Should().Be(1);
        DiagnosticRecord record = accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.CapacityExhausted).Which;
        record.Value0.Should().Be(2); // the configured capacity
        record.Value1.Should().Be(1); // turned away in this Frame
    }

    [Fact]
    public void OnHelloWithCookie_EveryCapacitySlotOccupied_LeavesTheExistingSessionsUnaffected()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new(capacity: 2);
        using RawPeer peerA = new();
        using RawPeer peerB = new();
        using RawPeer peerC = new();
        CompleteHandshake(accepter, peerA, 0x0000_0000_0000_0021UL);
        CompleteHandshake(accepter, peerB, 0x0000_0000_0000_0022UL);

        const ulong clientNonceC = 0x0000_0000_0000_0023UL;
        (long epoch, byte[] cookie) = ObtainCookie(accepter, peerC, clientNonceC);
        ulong sentBefore = accepter.Reactor.Counters.DatagramsSent;

        // Act
        peerC.Send(accepter.Endpoint, BuildHelloWithCookie(
            ProtocolVersion, clientNonceC, epoch, cookie, sessionIdForMe: 1, maxMessageSize: 4096, resumeSecret: new byte[ResumeSecretLength]));
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsSent > sentBefore);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();

        // Assert — no PeerClosed for either established Session, this Frame or any afterwards.
        drained.Should().NotContain(evt => evt.Kind == SessionEventKind.PeerClosed);
        accepter.PumpAndDrainAll(3).Should().BeEmpty();
    }

    [Fact]
    public void Connect_WhenTheFarEndRefusesForCapacity_DrainsPeerClosedWithRemoteAtCapacity()
    {
        // Arrange
        using RawPeer accepter = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);

        // Act
        PeerId dialled = dialler.Reactor.Connect(accepter.LocalEndPoint);
        byte[] hello = accepter.Receive(out IPEndPoint diallerEndpoint);
        ulong clientNonce = ParseHelloClientNonce(hello);

        accepter.Send(diallerEndpoint, BuildCookie(clientNonce, epoch: 0, cookie: new byte[CookieLength]));
        dialler.Reactor.Pump();
        accepter.Receive(out _);

        accepter.Send(diallerEndpoint, BuildRefusal(clientNonce, RefusalReasonCapacity, versionOffered: 0, versionRequired: 0));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained =
            dialler.PumpUntilDrained(e => e.Any(x => x.Kind == SessionEventKind.PeerClosed));

        // Assert
        drained.Should().ContainSingle()
            .Which.Should().BeEquivalentTo((SessionEventKind.PeerClosed, dialled, DisconnectReason.RemoteAtCapacity));
    }

    [Fact]
    public void OnHelloWithCookie_AfterASessionClosesAndFreesItsSlot_TheNextHandshakeIsAccepted()
    {
        // Arrange — with the sole Slot occupied, a second Hello-with-cookie is refused for Capacity.
        using DatagramSessionReactorHarness accepter = new(capacity: 1);
        using RawPeer peerA = new();
        using RawPeer peerB = new();
        PeerId peerAId = CompleteHandshake(accepter, peerA, 0x0000_0000_0000_0031UL);

        const ulong clientNonceB = 0x0000_0000_0000_0032UL;
        (long firstEpoch, byte[] firstCookie) = ObtainCookie(accepter, peerB, clientNonceB);
        ulong sentBefore = accepter.Reactor.Counters.DatagramsSent;
        peerB.Send(accepter.Endpoint, BuildHelloWithCookie(
            ProtocolVersion, clientNonceB, firstEpoch, firstCookie, sessionIdForMe: 1, maxMessageSize: 4096, resumeSecret: new byte[ResumeSecretLength]));
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsSent > sentBefore);
        byte[] refusalReply = peerB.Receive(out _);
        ParseRefusal(refusalReply).Reason.Should().Be(RefusalReasonCapacity);

        // Act — close the first Session, freeing its Slot, then retry the same peer.
        accepter.CloseImmediately(peerAId);
        accepter.Reactor.Pump();
        accepter.DrainToList();

        (long secondEpoch, byte[] secondCookie) = ObtainCookie(accepter, peerB, clientNonceB);
        ulong sentBefore2 = accepter.Reactor.Counters.DatagramsSent;
        peerB.Send(accepter.Endpoint, BuildHelloWithCookie(
            ProtocolVersion, clientNonceB, secondEpoch, secondCookie, sessionIdForMe: 1, maxMessageSize: 4096, resumeSecret: new byte[ResumeSecretLength]));
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsSent > sentBefore2);
        byte[] acceptReply = peerB.Receive(out _);

        // Assert — capacity refusal is not sticky.
        acceptReply[4].Should().Be(AcceptType);
    }

    // =========================================================================================
    // Admission cap
    // =========================================================================================

    [Fact]
    public void Pump_TenHellosArrivingInOneFrameAgainstAnAdmissionCapOfFour_AdmitsExactlyFourAndDiscardsSix()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new(capacity: 8, admissionCap: 4);
        List<RawPeer> peers = [.. Enumerable.Range(0, 10).Select(_ => new RawPeer())];
        try
        {
            ulong sentBefore = accepter.Reactor.Counters.DatagramsSent;
            ulong discardsBefore = accepter.Reactor.Counters.AdmissionDiscards;

            // Act — every Hello lands before the Reactor is Pumped even once, so all ten arrive in the
            // same Frame; the short sleep gives loopback delivery time to land every send in the
            // receive queue before that one Pump call reads it.
            for (int i = 0; i < peers.Count; i++)
            {
                peers[i].Send(accepter.Endpoint, BuildHello((ulong)(i + 1)));
            }

            Thread.Sleep(20);
            accepter.Reactor.Pump();

            // Assert — measured as work performed (Cookies sent back, Admissions declined), never as
            // elapsed time.
            (accepter.Reactor.Counters.DatagramsSent - sentBefore).Should().Be(4);
            (accepter.Reactor.Counters.AdmissionDiscards - discardsBefore).Should().Be(6);
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
    public void Pump_AdmissionCapExceeded_AdmissionDeferralsStaysZeroBecauseUdpHasNowhereForTheWorkToWait()
    {
        // UDP gives a datagram beyond the cap nowhere to wait: OnLinkReadable has already taken it off
        // the socket by the time the cap is checked, so it is gone rather than merely postponed. A
        // Deferral is a promise that a later Pump will find the same work still there — a promise only
        // the transports with an OS backlog (Raw, the stream Session profile) can make. This is the
        // distinction ReactorCounters.AdmissionDeferrals/AdmissionDiscards exists to record: on the
        // datagram Session profile, Deferrals never moves at all.
        using DatagramSessionReactorHarness accepter = new(capacity: 8, admissionCap: 2);
        using RawPeer peerA = new();
        using RawPeer peerB = new();
        using RawPeer peerC = new();

        // Act
        peerA.Send(accepter.Endpoint, BuildHello(1));
        peerB.Send(accepter.Endpoint, BuildHello(2));
        peerC.Send(accepter.Endpoint, BuildHello(3));
        Thread.Sleep(20);
        accepter.Reactor.Pump();

        // Assert
        accepter.Reactor.Counters.AdmissionDiscards.Should().BeGreaterThan(0);
        accepter.Reactor.Counters.AdmissionDeferrals.Should().Be(0);
    }

    [Fact]
    public void Pump_AFrameThatTurnsWorkAway_EmitsExactlyOneAdmissionCapReachedRecordCarryingTheCapAndCount()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new(capacity: 8, admissionCap: 3);
        List<RawPeer> peers = [.. Enumerable.Range(0, 7).Select(_ => new RawPeer())];
        try
        {
            // Act — a flood would otherwise produce one record per discarded datagram; the Reactor
            // reports the Frame's declined Admissions once each, at the Frame's end.
            for (int i = 0; i < peers.Count; i++)
            {
                peers[i].Send(accepter.Endpoint, BuildHello((ulong)(i + 1)));
            }

            Thread.Sleep(20);
            accepter.Reactor.Pump();

            // Assert
            List<DiagnosticRecord> capRecords = [.. accepter.Sink.Received.Where(r => r.Id == DiagnosticId.AdmissionCapReached)];
            capRecords.Should().ContainSingle();
            capRecords[0].Value0.Should().Be(3);
            capRecords[0].Value1.Should().Be(4);
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
    public void Pump_AFrameThatTurnsNothingAway_EmitsNoAdmissionCapReachedRecord()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new(capacity: 8, admissionCap: 4);
        using RawPeer peer = new();

        // Act
        peer.Send(accepter.Endpoint, BuildHello(1));
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > 0);

        // Assert
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.AdmissionCapReached);
    }

    [Fact]
    public void Pump_AFloodOfSeveralThousandHellosAtASmallAdmissionCap_NeverThrowsNeverExceedsTheCapPerFrameAndStaysUsableAfterward()
    {
        // Arrange — expressed entirely as work performed, never as elapsed time: the number of
        // Admissions made per Frame, never more than the cap, and whether a legitimate handshake still
        // completes once the flood is over.
        const int admissionCap = 5;
        const int floodPeers = 50;
        const int roundsPerPeer = 60; // 3000 Hellos total.
        using DatagramSessionReactorHarness accepter = new(capacity: 8, admissionCap: admissionCap);
        List<RawPeer> peers = [.. Enumerable.Range(0, floodPeers).Select(_ => new RawPeer())];
        try
        {
            // Act
            Action flood = () =>
            {
                for (int round = 0; round < roundsPerPeer; round++)
                {
                    for (int p = 0; p < peers.Count; p++)
                    {
                        peers[p].Send(accepter.Endpoint, BuildHello((ulong)((round * floodPeers) + p + 1)));
                    }

                    ulong sentBefore = accepter.Reactor.Counters.DatagramsSent;
                    accepter.Reactor.Pump();
                    ulong admittedThisFrame = accepter.Reactor.Counters.DatagramsSent - sentBefore;
                    admittedThisFrame.Should().BeLessThanOrEqualTo((ulong)admissionCap);
                }
            };

            flood.Should().NotThrow();

            // Assert — a legitimate handshake still completes after the flood.
            using RawPeer lastPeer = new();
            ulong sentBeforeLast = accepter.Reactor.Counters.DatagramsSent;
            lastPeer.Send(accepter.Endpoint, BuildHello(999_999));
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsSent > sentBeforeLast);
            byte[] reply = lastPeer.Receive(out _);
            reply[4].Should().Be(CookieType);
        }
        finally
        {
            foreach (RawPeer peer in peers)
            {
                peer.Dispose();
            }
        }
    }

    // =========================================================================================
    // Per-address limit (issue #427)
    // =========================================================================================

    [Fact]
    public void OnHelloWithCookie_AddressAtItsPeerLimit_RefusesForCapacityAndAdmitsNoPeer()
    {
        // Arrange — the same socket offers a fresh nonce each time, the case the issue describes.
        using DatagramSessionReactorHarness accepter = new(capacity: 4, maxPeersPerAddress: 2);
        using RawPeer peer = new();
        CompleteHandshake(accepter, peer, 0x0000_0000_0000_0031UL);
        CompleteHandshake(accepter, peer, 0x0000_0000_0000_0032UL);

        // Act
        byte[] reply = ReplyToHandshake(accepter, peer, 0x0000_0000_0000_0033UL);

        // Assert
        reply[4].Should().Be(RefusalType);
        ParseRefusal(reply).Reason.Should().Be(RefusalReasonCapacity);
        accepter.DrainToList().Should().BeEmpty();
    }

    [Fact]
    public void OnHelloWithCookie_AddressAtItsPeerLimit_CountsADiscardAndLogsAddressPeerLimitReached()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new(capacity: 4, maxPeersPerAddress: 2);
        using RawPeer peer = new();
        CompleteHandshake(accepter, peer, 0x0000_0000_0000_00A1UL);
        CompleteHandshake(accepter, peer, 0x0000_0000_0000_00A2UL);
        ulong discardsBefore = accepter.Reactor.Counters.AdmissionDiscards;

        // Act
        ReplyToHandshake(accepter, peer, 0x0000_0000_0000_00A3UL);

        // Assert
        (accepter.Reactor.Counters.AdmissionDiscards - discardsBefore).Should().Be(1);
        DiagnosticRecord record = accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.AddressPeerLimitReached).Which;
        record.Level.Should().Be(DiagnosticLevel.Warn);
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
        record.Source.Should().Be(Source.FromIPEndPoint(peer.LocalEndPoint));
        record.Value0.Should().Be(2);
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.CapacityExhausted);
    }

    [Fact]
    public void OnHelloWithCookie_AddressAtItsPeerLimit_LeavesTheRemainingSlotsForOtherAddresses()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new(capacity: 3, maxPeersPerAddress: 2);
        using RawPeer greedy = new();
        using RawPeer other = new(IPAddress.Parse("127.0.0.2"));
        CompleteHandshake(accepter, greedy, 0x0000_0000_0000_0041UL);
        CompleteHandshake(accepter, greedy, 0x0000_0000_0000_0042UL);
        ReplyToHandshake(accepter, greedy, 0x0000_0000_0000_0043UL);

        // Act
        ReplyToHandshake(accepter, other, 0x0000_0000_0000_0044UL);

        // Assert
        accepter.DrainToList().Should().ContainSingle(evt => evt.Kind == SessionEventKind.PeerConnected);
    }

    [Fact]
    public void OnHelloWithCookie_ANewPortOnAnAddressAtItsPeerLimit_IsRefused()
    {
        // Arrange — a cookie is minted per port, so a limit that counted ports would limit nothing.
        using DatagramSessionReactorHarness accepter = new(capacity: 4, maxPeersPerAddress: 1);
        using RawPeer firstPort = new();
        using RawPeer secondPort = new();
        CompleteHandshake(accepter, firstPort, 0x0000_0000_0000_0051UL);

        // Act
        ReplyToHandshake(accepter, secondPort, 0x0000_0000_0000_0052UL);

        // Assert
        accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.AddressPeerLimitReached)
            .Which.Source.Should().Be(Source.FromIPEndPoint(secondPort.LocalEndPoint));
    }

    [Fact]
    public void OnHelloWithCookie_AddressWhosePeerIsClosing_StaysCharged()
    {
        // Arrange — a Closing Peer still holds its Slot until Linger releases it.
        using DatagramSessionReactorHarness accepter = new(capacity: 4, maxPeersPerAddress: 1);
        using RawPeer closing = new();
        using RawPeer sameAddress = new();
        PeerId admitted = CompleteHandshake(accepter, closing, 0x0000_0000_0000_00B1UL);
        accepter.Reactor.Close(admitted);

        // Act
        ReplyToHandshake(accepter, sameAddress, 0x0000_0000_0000_00B2UL);

        // Assert
        accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.AddressPeerLimitReached);
    }

    [Fact]
    public void OnHelloWithCookie_AddressWhosePeerIsInterrupted_StaysCharged()
    {
        // Arrange — an Interrupted Peer still holds its Slot, reserved for a Resume. The Peer's announced
        // send-idle interval is 60 seconds and the default tolerance is three missed heartbeats.
        using DatagramSessionReactorHarness accepter = new(capacity: 4, maxPeersPerAddress: 1);
        using RawPeer interrupted = new();
        using RawPeer sameAddress = new();
        PeerId admitted = CompleteHandshake(accepter, interrupted, 0x0000_0000_0000_00C1UL);
        accepter.TimeProvider.Advance(TimeSpan.FromSeconds(181));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained =
            accepter.PumpUntilDrained(e => e.Any(x => x.Kind == SessionEventKind.PeerInterrupted && x.Peer == admitted));

        // Act
        ReplyToHandshake(accepter, sameAddress, 0x0000_0000_0000_00C2UL);

        // Assert
        drained.Should().Contain(x => x.Kind == SessionEventKind.PeerInterrupted && x.Peer == admitted);
        accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.AddressPeerLimitReached);
    }

    [Fact]
    public void Connect_ToASecondEndpointFromAnAddressAtItsPeerLimit_DrainsPeerClosedWithRemoteAtCapacity()
    {
        // Arrange — the count is per Reactor, so another bound endpoint is not more room for one address.
        using DatagramSessionReactorHarness accepter = new(
            capacity: 4,
            maxPeersPerAddress: 1,
            endpoints: [new IPEndPoint(IPAddress.Loopback, 0), new IPEndPoint(IPAddress.Parse("127.0.0.2"), 0)]);
        using DatagramSessionReactorHarness dialler = new();
        using RawPeer first = new();
        CompleteHandshake(accepter, first, 0x0000_0000_0000_00D1UL);

        // Act
        PeerId dialled = dialler.Reactor.Connect(accepter.Reactor.BoundEndpoints[1]);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        PumpDeadline deadline = PumpDeadline.Start();
        while (!drained.Any(evt => evt.Kind == SessionEventKind.PeerClosed) && !deadline.Expired)
        {
            accepter.Reactor.Pump();
            dialler.Reactor.Pump();
            drained.AddRange(dialler.DrainToList());
        }

        // Assert
        drained.Should().ContainSingle()
            .Which.Should().BeEquivalentTo((SessionEventKind.PeerClosed, dialled, DisconnectReason.RemoteAtCapacity));
    }

    [Fact]
    public void OnHelloWithCookie_RetransmittedByAnAddressAtItsPeerLimit_IsAnsweredWithItsAccept()
    {
        // Arrange — the Accept can be lost, and the Peer the retransmission asks about already holds its Slot.
        using DatagramSessionReactorHarness accepter = new(capacity: 4, maxPeersPerAddress: 1);
        using RawPeer peer = new();
        const ulong clientNonce = 0x0000_0000_0000_0061UL;
        (long epoch, byte[] cookie) = ObtainCookie(accepter, peer, clientNonce);
        byte[] helloWithCookie = BuildHelloWithCookie(
            ProtocolVersion, clientNonce, epoch, cookie, sessionIdForMe: 1, maxMessageSize: 4096, resumeSecret: new byte[ResumeSecretLength]);
        ulong sentBefore = accepter.Reactor.Counters.DatagramsSent;
        peer.Send(accepter.Endpoint, helloWithCookie);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsSent > sentBefore);
        peer.Receive(out _);
        sentBefore = accepter.Reactor.Counters.DatagramsSent;

        // Act
        peer.Send(accepter.Endpoint, helloWithCookie);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsSent > sentBefore);
        byte[] reply = peer.Receive(out _);

        // Assert
        reply[4].Should().Be(AcceptType);
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.AddressPeerLimitReached);
    }

    [Fact]
    public void OnHelloWithCookie_AddressWhoseOnlyPeerClosed_IsAdmittedAgain()
    {
        // Arrange — the Peer is abandoned with its Goodbye unanswered, so its Slot is released by Linger.
        using DatagramSessionReactorHarness accepter = new(capacity: 4, maxPeersPerAddress: 1);
        using RawPeer closed = new();
        using RawPeer sameAddress = new();
        PeerId admitted = CompleteHandshake(accepter, closed, 0x0000_0000_0000_0071UL);
        accepter.Reactor.Close(admitted);
        accepter.TimeProvider.Advance(accepter.Options.Linger + TimeSpan.FromSeconds(1));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained =
            accepter.PumpUntilDrained(e => e.Any(x => x.Kind == SessionEventKind.PeerClosed && x.Peer == admitted));

        // Act
        ReplyToHandshake(accepter, sameAddress, 0x0000_0000_0000_0072UL);

        // Assert
        drained.Should().Contain(x => x.Kind == SessionEventKind.PeerClosed && x.Peer == admitted);
        accepter.DrainToList().Should().ContainSingle(evt => evt.Kind == SessionEventKind.PeerConnected);
    }

    [Fact]
    public void OnHelloWithCookie_AddressThisReactorIsDialling_IsNotChargedForTheDial()
    {
        // Arrange — a dial is this Reactor's own choice, so it spends none of the remote address's limit.
        using DatagramSessionReactorHarness accepter = new(capacity: 4, maxPeersPerAddress: 1);
        using RawPeer dialled = new();
        using RawPeer dialling = new();
        accepter.Reactor.Connect(dialled.LocalEndPoint);

        // Act
        ReplyToHandshake(accepter, dialling, 0x0000_0000_0000_0081UL);

        // Assert
        accepter.DrainToList().Should().ContainSingle(evt => evt.Kind == SessionEventKind.PeerConnected);
    }

    [Fact]
    public void OnHelloWithCookie_PeerResumedToAnotherAddress_StaysChargedToTheAddressOfItsHandshake()
    {
        // Arrange — were the charge to follow the Peer, one address could fill its limit, move those Peers
        // to a second address it controls, and fill its limit again.
        using DatagramSessionResumeScenario scenario = DatagramSessionResumeScenario.Establish(
            DatagramSessionResumeScenario.ResumableLinger, accepterMaxPeersPerAddress: 1);
        scenario.InterruptBothEnds();
        scenario.ResumeThroughNewPort(IPAddress.Parse("127.0.0.2"));
        using RawPeer sameAddressAsTheHandshake = new();

        // Act
        ReplyToHandshake(scenario.Accepter, sameAddressAsTheHandshake, 0x0000_0000_0000_0091UL);

        // Assert
        scenario.Accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.AddressPeerLimitReached);
    }

    /// <summary>
    /// A plain BCL UDP socket bound to loopback, standing in for a counterparty this Reactor has never
    /// seen. It is bidirectional:
    /// unlike <see cref="HostilePeer"/>, a refusal test must read a genuine Cookie or Refusal back off
    /// the wire, not merely put bytes on it.
    /// </summary>
    private sealed class RawPeer : IDisposable
    {
        private readonly Socket _socket;

        public RawPeer(IPAddress? address = null)
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
            _socket.Bind(new IPEndPoint(address ?? IPAddress.Loopback, 0));
            _socket.ReceiveTimeout = 5000;
        }

        public IPEndPoint LocalEndPoint => (IPEndPoint)_socket.LocalEndPoint!;

        public void Send(IPEndPoint destination, byte[] datagram) => _socket.SendTo(datagram, destination);

        public byte[] Receive(out IPEndPoint from)
        {
            byte[] buffer = new byte[512];
            EndPoint remote = new IPEndPoint(IPAddress.Any, 0);
            int read = _socket.ReceiveFrom(buffer, ref remote);
            from = (IPEndPoint)remote;
            return buffer[..read];
        }

        public void Dispose() => _socket.Dispose();
    }
}
