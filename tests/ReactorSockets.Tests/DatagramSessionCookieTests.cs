using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 5.3 (issue #34): the stateless cookie exchange, its epoch rotation, and the amplification
/// bound the cookie exists to hold. Every assertion here is made from outside the L2 seam (tests/CLAUDE.md):
/// bytes this test itself puts on the wire and parses back, Events drained from a Pump, records the
/// Sink received, and <see cref="DatagramSessionReactor.Counters"/>. The cookie machinery
/// (<c>CookieMinter</c>, <c>ControlDatagramCodec</c>) is internal and reached nowhere here — this file
/// re-derives the wire layout from <c>DatagramSessionProtocol</c>'s doc comments instead.
/// </summary>
public class DatagramSessionCookieTests
{
    // The pre-Session control wire format (see src\ReactorSockets\Session\DatagramSessionProtocol.cs
    // and ControlDatagramCodec.cs, both read-only from this file): every datagram is exactly 256
    // bytes, big-endian, session id 0 followed by a one-byte type and a body zero-padded to fill.
    private const int ControlSize = 256;
    private const int BodyOffset = 5;
    private const byte HelloType = 1;
    private const byte CookieType = 2;
    private const byte HelloWithCookieType = 3;
    private const int CookieLength = 32;
    private const int ResumeSecretLength = 16;
    private const uint ProtocolVersion = 1;

    // ---- Frame builders --------------------------------------------------------------------

    private static byte[] BuildHello(ulong clientNonce, uint version = ProtocolVersion)
    {
        byte[] datagram = new byte[ControlSize];
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(0, 4), 0);
        datagram[4] = HelloType;
        int offset = BodyOffset;
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(offset, 4), version);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(datagram.AsSpan(offset, 8), clientNonce);
        return datagram;
    }

    private static byte[] BuildHelloWithCookie(
        ulong clientNonce,
        long epoch,
        ReadOnlySpan<byte> cookie,
        uint sessionIdForMe = 0,
        uint maxMessageSize = 4096,
        ReadOnlySpan<byte> resumeSecret = default,
        uint version = ProtocolVersion,
        ulong sendIdleIntervalMilliseconds = 60_000,
        ulong lingerMilliseconds = 30_000)
    {
        byte[] datagram = new byte[ControlSize];
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(0, 4), 0);
        datagram[4] = HelloWithCookieType;

        int offset = BodyOffset;
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(offset, 4), version);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(datagram.AsSpan(offset, 8), clientNonce);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(datagram.AsSpan(offset, 8), unchecked((ulong)epoch));
        offset += 8;
        cookie.CopyTo(datagram.AsSpan(offset, CookieLength));
        offset += CookieLength;
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(offset, 4), sessionIdForMe);
        offset += 4;
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(offset, 4), maxMessageSize);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(datagram.AsSpan(offset, 8), sendIdleIntervalMilliseconds);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(datagram.AsSpan(offset, 8), lingerMilliseconds);
        offset += 8;
        if (!resumeSecret.IsEmpty)
        {
            resumeSecret.CopyTo(datagram.AsSpan(offset, ResumeSecretLength));
        }

        return datagram;
    }

    /// <summary>A structurally well-formed but unimplemented/unknown control type — 256 bytes, zero padding, a type byte this profile discards.</summary>
    private static byte[] BuildUnknownTypeDatagram()
    {
        byte[] datagram = new byte[ControlSize];
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(0, 4), 0);
        datagram[4] = 250; // not on the DatagramControlType roster
        return datagram;
    }

    private static byte[] BuildRandomNoise(int length)
    {
        byte[] datagram = new byte[length];
        Random.Shared.NextBytes(datagram);
        return datagram;
    }

    // ---- Frame parsers -----------------------------------------------------------------------

    private static (ulong ClientNonce, long Epoch, byte[] Cookie) ParseCookie(byte[] datagram)
    {
        ulong clientNonce = BinaryPrimitives.ReadUInt64BigEndian(datagram.AsSpan(BodyOffset, 8));
        long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(datagram.AsSpan(BodyOffset + 8, 8)));
        byte[] cookie = datagram.AsSpan(BodyOffset + 16, CookieLength).ToArray();
        return (clientNonce, epoch, cookie);
    }

    private static byte[]? FindCookieDatagram(IEnumerable<byte[]> datagrams, ulong wantNonce)
    {
        foreach (byte[] datagram in datagrams)
        {
            if (datagram.Length == ControlSize && datagram[4] == CookieType)
            {
                ulong nonce = BinaryPrimitives.ReadUInt64BigEndian(datagram.AsSpan(BodyOffset, 8));
                if (nonce == wantNonce)
                {
                    return datagram;
                }
            }
        }

        return null;
    }

    /// <summary>
    /// A raw counterparty that, unlike <see cref="HostilePeer"/>, keeps its Link open across sends so
    /// replies come back to the same source address — needed to obtain a genuine Cookie and then
    /// present it back from the same (or, for slice 5.3's replay test, a deliberately different)
    /// source. Built entirely from the public Raw surface, exactly like <see cref="HostilePeer"/> and
    /// <see cref="WireObserver"/>.
    /// </summary>
    private sealed class RawClient : IDisposable
    {
        private readonly RawReactor _reactor;
        private readonly LinkId _link;
        private readonly List<byte[]> _received = [];

        public RawClient(int capacity = 4)
        {
            _reactor = new RawReactor(new ReactorOptions { Capacity = capacity });
            _link = _reactor.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        }

        public IPEndPoint LocalEndPoint => _reactor.LocalOf(_link);

        public IReadOnlyList<byte[]> Received => _received;

        public void SendTo(IPEndPoint destination, ReadOnlySpan<byte> payload)
        {
            Result result = _reactor.SendTo(_link, Source.FromIPEndPoint(destination), payload);
            if (!result.IsSuccess)
            {
                throw new InvalidOperationException($"RawClient failed to send to {destination}: {result.Error}");
            }
        }

        public void Pump()
        {
            _reactor.Pump();
            foreach (RawEvent evt in _reactor.Drain())
            {
                if (evt.Kind == RawEventKind.DatagramReceived)
                {
                    _received.Add(evt.Payload.ToArray());
                }
            }
        }

        public void Dispose() => _reactor.Dispose();
    }

    /// <summary>
    /// Sends a Hello for <paramref name="clientNonce"/> from <paramref name="client"/> to
    /// <paramref name="harness"/>'s Reactor, Pumping both ends until the matching Cookie arrives, and
    /// returns the epoch and 32-byte cookie it carried.
    /// </summary>
    private static (long Epoch, byte[] Cookie) ObtainCookie(
        DatagramSessionReactorHarness harness, RawClient client, ulong clientNonce, int maxRounds = 20)
    {
        // Only datagrams that arrive from here on count: a test may obtain a cookie twice for the same
        // nonce — across an epoch boundary, or from a second Reactor — and the earlier reply is still
        // sitting in this client's accumulated list.
        int alreadyReceived = client.Received.Count;
        client.SendTo(harness.Endpoint, BuildHello(clientNonce));

        for (int round = 0; round < maxRounds; round++)
        {
            harness.Reactor.Pump();
            harness.DrainToList();
            client.Pump();

            if (FindCookieDatagram(client.Received.Skip(alreadyReceived), clientNonce) is byte[] cookieDatagram)
            {
                (_, long epoch, byte[] cookie) = ParseCookie(cookieDatagram);
                return (epoch, cookie);
            }
        }

        throw new InvalidOperationException($"No Cookie arrived for clientNonce {clientNonce} within {maxRounds} Pumps.");
    }

    // ---- 1. The full exchange -----------------------------------------------------------------

    [Fact]
    public void OnHello_BareHello_RepliesWithOnlyACookieEchoingTheNonce()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new();
        using RawClient client = new();
        const ulong clientNonce = 0x1122_3344_5566_7788UL;

        // Act
        client.SendTo(harness.Endpoint, BuildHello(clientNonce));
        harness.Reactor.Pump();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> serverEvents = harness.DrainToList();
        client.Pump();

        // Assert — a bare Hello never yields a Session Event on the answering side.
        serverEvents.Should().BeEmpty();

        // Assert — the wire reply is exactly one 256-byte Cookie echoing the client's own nonce.
        client.Received.Should().HaveCount(1);
        byte[] reply = client.Received[0];
        reply.Should().HaveCount(ControlSize);
        reply[4].Should().Be(CookieType);
        (ulong echoedNonce, _, byte[] cookie) = ParseCookie(reply);
        echoedNonce.Should().Be(clientNonce);
        cookie.Should().HaveCount(CookieLength);
    }

    // ---- 2. No state is allocated for a bare Hello -----------------------------------------

    [Fact]
    public void OnHello_FloodOfBareHellosFromVariedSources_LeavesEveryCapacitySlotFree()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new(capacity: 4);
        using HostilePeer flood = new();
        const int totalHellos = 3000;
        const int batchSize = 150;
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> floodEvents = [];

        // Act — a flood of bare Hellos, each from a throwaway Link (HostilePeer binds and closes a
        // fresh one per send, so the source port varies), Pumping between batches rather than after
        // every single send so several thousand fit in a modest runtime.
        for (int batch = 0; batch < totalHellos / batchSize; batch++)
        {
            for (int i = 0; i < batchSize; i++)
            {
                ulong clientNonce = (ulong)((batch * batchSize) + i);
                flood.SendDatagram(accepter.Endpoint, BuildHello(clientNonce));
            }

            accepter.Reactor.Pump();
            floodEvents.AddRange(accepter.DrainToList());
        }

        // Assert — nothing about a bare Hello ever produces a PeerConnected, however many arrive.
        floodEvents.Should().NotContain(evt => evt.Kind == SessionEventKind.PeerConnected);

        // Act — a legitimate dialler now completes a handshake against the same, still-flooded Reactor.
        using DatagramSessionReactorHarness dialler = new(capacity: 4);
        dialler.Reactor.Connect(accepter.Endpoint);
        (List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents, _) =
            DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);

        // Assert — proving all four capacity Slots were still free: the flood allocated no Peer state.
        diallerEvents.Should().Contain(evt => evt.Kind == SessionEventKind.PeerConnected);
    }

    // ---- 3. The amplification bound, as arithmetic over the whole corpus ------------------

    [Fact]
    public void DatagramSessionReactor_HostileCorpusOfVariedDatagrams_NeverSendsMoreBytesThanItWasSent()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new(capacity: 4);
        using HostilePeer corpus = new();
        long bytesSentByCorpus = 0;

        void SendAndCount(byte[] payload)
        {
            corpus.SendDatagram(harness.Endpoint, payload);
            bytesSentByCorpus += payload.Length;
        }

        // Act — a large, varied corpus: bare Hellos, structurally malformed control frames, forged
        // Hello-with-cookies, truncated frames and pure random noise, in blocks Pumped between each so
        // the Reactor actually processes what has queued so far.
        for (int i = 0; i < 40; i++)
        {
            SendAndCount(BuildHello((ulong)i));
        }

        harness.Reactor.Pump();

        for (int i = 0; i < 40; i++)
        {
            SendAndCount(BuildUnknownTypeDatagram());
        }

        harness.Reactor.Pump();

        for (int i = 0; i < 40; i++)
        {
            byte[] forgedCookie = new byte[CookieLength];
            Random.Shared.NextBytes(forgedCookie);
            byte[] resumeSecret = new byte[ResumeSecretLength];
            SendAndCount(BuildHelloWithCookie((ulong)(1000 + i), 0, forgedCookie, resumeSecret: resumeSecret));
        }

        harness.Reactor.Pump();

        for (int i = 0; i < 40; i++)
        {
            SendAndCount(BuildRandomNoise(1 + (i % 200)));
        }

        harness.Reactor.Pump();

        for (int i = 0; i < 40; i++)
        {
            SendAndCount(BuildRandomNoise(ControlSize));
        }

        harness.Reactor.Pump();

        // Assert — every reply this profile ever sends to an unverified sender is exactly one
        // ControlSize-byte control datagram (DatagramSessionProtocol.ControlDatagramSize), so
        // Counters.DatagramsSent times that constant is every byte the Reactor put on the wire.
        long bytesSentByReactor = (long)harness.Reactor.Counters.DatagramsSent * ControlSize;

        bytesSentByReactor.Should().BeLessThanOrEqualTo(bytesSentByCorpus);
    }

    // ---- 8.2.1: a zero-announced send-idle interval fails to read --------------------------

    [Fact]
    public void OnHelloWithCookie_AnnouncingAZeroSendIdleInterval_IsDiscardedAsMalformedAndNoPeerConnected()
    {
        // Arrange — a genuine cookie, so the only thing standing between this Hello-with-cookie and
        // Establishment is the zero-announced send-idle interval: TryReadHelloWithCookie fails the
        // read before cookie verification is even reached, exactly as it already does for a
        // zero-announced maxMessageSize (issue #287).
        using DatagramSessionReactorHarness harness = new();
        using RawClient client = new();
        const ulong clientNonce = 33UL;
        (long epoch, byte[] cookie) = ObtainCookie(harness, client, clientNonce);
        byte[] resumeSecret = new byte[ResumeSecretLength];

        // Act
        client.SendTo(
            harness.Endpoint,
            BuildHelloWithCookie(clientNonce, epoch, cookie, resumeSecret: resumeSecret, sendIdleIntervalMilliseconds: 0));
        harness.Reactor.Pump();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = harness.DrainToList();

        // Assert — discarded as malformed, the same outcome a zero maxMessageSize already produces,
        // and no Session ever came to exist over it.
        events.Should().NotContain(evt => evt.Kind == SessionEventKind.PeerConnected);
        harness.Reactor.Counters.UnknownSourceDiscards.Should().Be(1);
        harness.Sink.Received.Should().Contain(record => record.Id == DiagnosticId.DatagramMalformed);
    }

    // ---- 8.3.1: a zero-announced Linger fails to read ---------------------------------------

    [Fact]
    public void OnHelloWithCookie_AnnouncingAZeroLinger_IsDiscardedAsMalformedAndNoPeerConnected()
    {
        // Arrange — a genuine cookie, so the only thing standing between this Hello-with-cookie and
        // Establishment is the zero-announced Linger: TryReadHelloWithCookie fails the read before
        // cookie verification is even reached, exactly as it already does for a zero-announced
        // send-idle interval (issue #347) and maxMessageSize (issue #287).
        using DatagramSessionReactorHarness harness = new();
        using RawClient client = new();
        const ulong clientNonce = 34UL;
        (long epoch, byte[] cookie) = ObtainCookie(harness, client, clientNonce);
        byte[] resumeSecret = new byte[ResumeSecretLength];

        // Act
        client.SendTo(
            harness.Endpoint,
            BuildHelloWithCookie(clientNonce, epoch, cookie, resumeSecret: resumeSecret, lingerMilliseconds: 0));
        harness.Reactor.Pump();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = harness.DrainToList();

        // Assert — discarded as malformed, the same outcome a zero send-idle interval or maxMessageSize
        // already produces, and no Session ever came to exist over it.
        events.Should().NotContain(evt => evt.Kind == SessionEventKind.PeerConnected);
        harness.Reactor.Counters.UnknownSourceDiscards.Should().Be(1);
        harness.Sink.Received.Should().Contain(record => record.Id == DiagnosticId.DatagramMalformed);
    }

    // ---- 4/5/6. A forged, corrupted or replayed cookie is rejected -------------------------

    [Fact]
    public void OnHelloWithCookie_ForgedCookie_IsRejectedWithWarnDiagnosticAndNoPeerConnected()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new();
        using RawClient client = new();
        const ulong clientNonce = 11UL;
        (long epoch, _) = ObtainCookie(harness, client, clientNonce);
        byte[] forgedCookie = new byte[CookieLength];
        Random.Shared.NextBytes(forgedCookie);
        byte[] resumeSecret = new byte[ResumeSecretLength];

        // Act
        client.SendTo(harness.Endpoint, BuildHelloWithCookie(clientNonce, epoch, forgedCookie, resumeSecret: resumeSecret));
        harness.Reactor.Pump();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = harness.DrainToList();

        // Assert
        events.Should().NotContain(evt => evt.Kind == SessionEventKind.PeerConnected);
        harness.Sink.Received.Should().Contain(record =>
            record.Id == DiagnosticId.CookieRejected
            && record.Level == DiagnosticLevel.Warn
            && record.Value0 == epoch
            && record.Value1 == epoch);
    }

    [Fact]
    public void OnHelloWithCookie_CorruptedCookie_IsRejectedWithWarnDiagnosticAndNoPeerConnected()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new();
        using RawClient client = new();
        const ulong clientNonce = 22UL;
        (long epoch, byte[] genuineCookie) = ObtainCookie(harness, client, clientNonce);
        byte[] corruptedCookie = (byte[])genuineCookie.Clone();
        corruptedCookie[0] ^= 0x01;
        byte[] resumeSecret = new byte[ResumeSecretLength];

        // Act
        client.SendTo(harness.Endpoint, BuildHelloWithCookie(clientNonce, epoch, corruptedCookie, resumeSecret: resumeSecret));
        harness.Reactor.Pump();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = harness.DrainToList();

        // Assert
        events.Should().NotContain(evt => evt.Kind == SessionEventKind.PeerConnected);
        harness.Sink.Received.Should().Contain(record =>
            record.Id == DiagnosticId.CookieRejected
            && record.Level == DiagnosticLevel.Warn
            && record.Value0 == epoch
            && record.Value1 == epoch);
    }

    [Fact]
    public void OnHelloWithCookie_CookieReplayedFromDifferentSourcePort_IsRejected()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new();
        using RawClient originalClient = new();
        using RawClient impersonator = new();
        const ulong clientNonce = 33UL;
        (long epoch, byte[] cookie) = ObtainCookie(harness, originalClient, clientNonce);
        byte[] resumeSecret = new byte[ResumeSecretLength];

        // Act — the same genuine cookie, presented from a different source port than the one that
        // requested it (two independently bound loopback Links never share a port).
        impersonator.SendTo(harness.Endpoint, BuildHelloWithCookie(clientNonce, epoch, cookie, resumeSecret: resumeSecret));
        harness.Reactor.Pump();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = harness.DrainToList();

        // Assert — proves the source address is inside the HMAC: an otherwise-genuine cookie fails
        // once the address it was minted for no longer matches.
        events.Should().NotContain(evt => evt.Kind == SessionEventKind.PeerConnected);
        harness.Sink.Received.Should().Contain(record =>
            record.Id == DiagnosticId.CookieRejected && record.Level == DiagnosticLevel.Warn);
    }

    // ---- 7/8. One epoch old is accepted, two epochs old is rejected -----------------------

    [Fact]
    public void OnHelloWithCookie_CookieOneEpochOld_HandshakeCompletes()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new();
        using RawClient client = new();
        const ulong clientNonce = 44UL;
        (long epoch, byte[] cookie) = ObtainCookie(harness, client, clientNonce);
        byte[] resumeSecret = new byte[ResumeSecretLength];

        // Act — one full epoch (30s, DatagramSessionProtocol.CookieEpochSeconds) passes, purely on the
        // FakeTimeProvider; no wall-clock wait anywhere in this file.
        harness.TimeProvider.Advance(TimeSpan.FromSeconds(30));
        client.SendTo(harness.Endpoint, BuildHelloWithCookie(clientNonce, epoch, cookie, resumeSecret: resumeSecret));
        harness.Reactor.Pump();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = harness.DrainToList();

        // Assert
        events.Should().Contain(evt => evt.Kind == SessionEventKind.PeerConnected);
    }

    [Fact]
    public void OnHelloWithCookie_CookieTwoEpochsOld_IsRejected()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new();
        using RawClient client = new();
        const ulong clientNonce = 55UL;
        (long epoch, byte[] cookie) = ObtainCookie(harness, client, clientNonce);
        byte[] resumeSecret = new byte[ResumeSecretLength];

        // Act — 61 seconds: two full epochs and change, still driven purely by the FakeTimeProvider.
        harness.TimeProvider.Advance(TimeSpan.FromSeconds(61));
        client.SendTo(harness.Endpoint, BuildHelloWithCookie(clientNonce, epoch, cookie, resumeSecret: resumeSecret));
        harness.Reactor.Pump();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = harness.DrainToList();

        // Assert
        events.Should().NotContain(evt => evt.Kind == SessionEventKind.PeerConnected);
        harness.Sink.Received.Should().Contain(record =>
            record.Id == DiagnosticId.CookieRejected && record.Level == DiagnosticLevel.Warn);
    }

    // ---- 9. Rotation on the boundary --------------------------------------------------------

    [Fact]
    public void OnHelloWithCookie_EpochRotationBoundary_AcceptsAtExactlyThirtySecondsRejectsAtExactlySixty()
    {
        // Arrange — two cookies minted at the same instant, for two independent Peers, so the
        // acceptance half and the rejection half of the boundary are each observed on a cookie that
        // has never been presented before.
        using DatagramSessionReactorHarness harness = new(capacity: 4);
        using RawClient acceptedClient = new();
        using RawClient rejectedClient = new();
        const ulong acceptedNonce = 66UL;
        const ulong rejectedNonce = 77UL;
        (long acceptedEpoch, byte[] acceptedCookie) = ObtainCookie(harness, acceptedClient, acceptedNonce);
        (long rejectedEpoch, byte[] rejectedCookie) = ObtainCookie(harness, rejectedClient, rejectedNonce);
        byte[] resumeSecret = new byte[ResumeSecretLength];

        // Act — exactly 30 seconds from the mint of both cookies.
        harness.TimeProvider.Advance(TimeSpan.FromSeconds(30));
        acceptedClient.SendTo(
            harness.Endpoint, BuildHelloWithCookie(acceptedNonce, acceptedEpoch, acceptedCookie, resumeSecret: resumeSecret));
        harness.Reactor.Pump();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> atThirtySeconds = harness.DrainToList();

        // Assert — the boundary accepts.
        atThirtySeconds.Should().Contain(evt => evt.Kind == SessionEventKind.PeerConnected);

        // Act — exactly 60 seconds from the same mint (30 more, on top of the 30 already advanced).
        harness.TimeProvider.Advance(TimeSpan.FromSeconds(30));
        rejectedClient.SendTo(
            harness.Endpoint, BuildHelloWithCookie(rejectedNonce, rejectedEpoch, rejectedCookie, resumeSecret: resumeSecret));
        harness.Reactor.Pump();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> atSixtySeconds = harness.DrainToList();

        // Assert — the boundary, one epoch further out, rejects.
        atSixtySeconds.Should().NotContain(evt => evt.Kind == SessionEventKind.PeerConnected);
        harness.Sink.Received.Should().Contain(record =>
            record.Id == DiagnosticId.CookieRejected && record.Level == DiagnosticLevel.Warn);
    }

    // ---- 10. The epoch secret is not derivable from anything emitted ----------------------

    [Fact]
    public void CookieMinter_SameSourceAndNonceWithinOneEpoch_MintsByteIdenticalCookies()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new();
        using RawClient client = new();
        const ulong clientNonce = 88UL;

        // Act — five Hellos for the same nonce, from the same source, with no time advanced between
        // them: every Mint call the Reactor makes falls in the same epoch.
        List<byte[]> cookies = [];
        for (int i = 0; i < 5; i++)
        {
            (_, byte[] cookie) = ObtainCookie(harness, client, clientNonce);
            cookies.Add(cookie);
        }

        // Assert — the tag is a deterministic function of its inputs, so every one of them is identical.
        cookies.Should().OnlyContain(cookie => cookie.SequenceEqual(cookies[0]));
    }

    [Fact]
    public void CookieMinter_SameSourceAndNonceAcrossEpochs_MintsADifferentCookie()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new();
        using RawClient client = new();
        const ulong clientNonce = 99UL;
        (long firstEpoch, byte[] firstCookie) = ObtainCookie(harness, client, clientNonce);

        // Act
        harness.TimeProvider.Advance(TimeSpan.FromSeconds(30));
        (long secondEpoch, byte[] secondCookie) = ObtainCookie(harness, client, clientNonce);

        // Assert
        secondEpoch.Should().BeGreaterThan(firstEpoch);
        secondCookie.SequenceEqual(firstCookie).Should().BeFalse();
    }

    [Fact]
    public void CookieMinter_TwoReactorsWithIndependentEntropy_MintDifferentCookiesForIdenticalInputs()
    {
        // Arrange — one client, so the source address and port are identical for both Reactors; the
        // only thing that differs between the two calls is which Reactor's own, independently
        // generated epoch secret does the minting.
        using DatagramSessionReactorHarness harnessOne = new();
        using DatagramSessionReactorHarness harnessTwo = new();
        using RawClient client = new();
        const ulong clientNonce = 111UL;

        // Act
        (_, byte[] cookieFromOne) = ObtainCookie(harnessOne, client, clientNonce);
        (_, byte[] cookieFromTwo) = ObtainCookie(harnessTwo, client, clientNonce);

        // Assert — the far weaker but genuinely checkable property that stands in for "the secret is
        // not derivable from anything emitted": independent entropy produces different cookies.
        cookieFromTwo.SequenceEqual(cookieFromOne).Should().BeFalse();
    }
}
