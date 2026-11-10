using System.Buffers.Binary;
using System.Security.Cryptography;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #456: a pending dial retransmits its current handshake flight on a jittered backoff, and ends
/// with <see cref="DisconnectReason.TransportFailed"/> once this end's Linger has passed since
/// <see cref="DatagramSessionReactor.Connect"/> with no answer. Each test drives a real dialler and a
/// real accepter through a <see cref="LossyProxy"/> on one advancing <c>FakeTimeProvider</c>, and
/// observes only Events, Counters and the datagrams the proxy saw.
/// </summary>
public class DatagramSessionHandshakeRetransmitThroughProxyTests
{
    private const int BodyOffset = 5;
    private const byte HelloType = 1;
    private const byte CookieType = 2;
    private const byte HelloWithCookieType = 3;
    private const byte AcceptType = 4;
    private const int ControlSize = 256;

    private static readonly TimeSpan _linger = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan _round = TimeSpan.FromMilliseconds(20);

    private static bool IsControl(byte[] datagram, byte type) =>
        datagram.Length == ControlSize && BinaryPrimitives.ReadUInt32BigEndian(datagram) == 0 && datagram[4] == type;

    private static byte[] BuildHello(ulong clientNonce)
    {
        byte[] datagram = new byte[ControlSize];
        datagram[4] = HelloType;
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(BodyOffset, 4), 1);
        BinaryPrimitives.WriteUInt64BigEndian(datagram.AsSpan(BodyOffset + 4, 8), clientNonce);
        return datagram;
    }

    /// <summary>A predicate that drops the first datagram of <paramref name="type"/> and passes every other one.</summary>
    private static Func<byte[], bool> DropFirst(byte type)
    {
        bool dropped = false;
        return datagram =>
        {
            if (dropped || !IsControl(datagram, type))
            {
                return false;
            }

            dropped = true;
            return true;
        };
    }

    /// <summary>
    /// Advances the shared clock one round at a time and pumps dialler, proxy, accepter, proxy, until both
    /// ends have raised <see cref="SessionEventKind.PeerConnected"/> or <paramref name="rounds"/> run out.
    /// </summary>
    private static (List<SessionEventKind> Dialler, List<SessionEventKind> Accepter) RunHandshake(
        DatagramSessionReactorHarness dialler,
        LossyProxy proxy,
        DatagramSessionReactorHarness accepter,
        int rounds)
    {
        List<SessionEventKind> diallerEvents = [];
        List<SessionEventKind> accepterEvents = [];
        for (int round = 0; round < rounds; round++)
        {
            dialler.TimeProvider.Advance(_round);
            DatagramSessionReactorHarness.PumpRoundThroughProxy(dialler, proxy, accepter);
            diallerEvents.AddRange(dialler.DrainToList().Select(e => e.Kind));
            accepterEvents.AddRange(accepter.DrainToList().Select(e => e.Kind));
            if (diallerEvents.Contains(SessionEventKind.PeerConnected) && accepterEvents.Contains(SessionEventKind.PeerConnected))
            {
                break;
            }
        }

        return (diallerEvents, accepterEvents);
    }

    [Fact]
    public void Connect_FirstHelloIsLost_HelloIsResentAndTheDialCompletes()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, linger: _linger);
        using DatagramSessionReactorHarness accepter = new(linger: _linger, timeProvider: dialler.TimeProvider);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        proxy.DropNext(RelayDirection.ClientToUpstream);

        // Act
        dialler.Reactor.Connect(proxy.PublicEndPoint);
        (List<SessionEventKind> diallerEvents, List<SessionEventKind> accepterEvents) = RunHandshake(dialler, proxy, accepter, rounds: 40);

        // Assert
        diallerEvents.Should().ContainSingle(k => k == SessionEventKind.PeerConnected);
        accepterEvents.Should().ContainSingle(k => k == SessionEventKind.PeerConnected);
        proxy.Captures.Count(c => c.Direction == RelayDirection.ClientToUpstream && IsControl(c.Payload, HelloType))
            .Should().BeGreaterThan(1, "the dialler resent its Hello");
    }

    [Fact]
    public void Connect_FirstCookieIsLost_HelloIsResentAndTheDialCompletes()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, linger: _linger);
        using DatagramSessionReactorHarness accepter = new(linger: _linger, timeProvider: dialler.TimeProvider);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        proxy.SetPayloadDropRule(RelayDirection.UpstreamToClient, DropFirst(CookieType));

        // Act
        dialler.Reactor.Connect(proxy.PublicEndPoint);
        (List<SessionEventKind> diallerEvents, List<SessionEventKind> accepterEvents) = RunHandshake(dialler, proxy, accepter, rounds: 40);

        // Assert
        diallerEvents.Should().ContainSingle(k => k == SessionEventKind.PeerConnected);
        accepterEvents.Should().ContainSingle(k => k == SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Connect_FirstAcceptIsLost_HelloWithCookieIsResentAndTheAccepterAdmitsOnce()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, linger: _linger);
        using DatagramSessionReactorHarness accepter = new(linger: _linger, timeProvider: dialler.TimeProvider);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        proxy.SetPayloadDropRule(RelayDirection.UpstreamToClient, DropFirst(AcceptType));

        // Act
        dialler.Reactor.Connect(proxy.PublicEndPoint);
        (List<SessionEventKind> diallerEvents, List<SessionEventKind> accepterEvents) = RunHandshake(dialler, proxy, accepter, rounds: 40);

        // Assert
        diallerEvents.Should().ContainSingle(k => k == SessionEventKind.PeerConnected);
        accepterEvents.Should().ContainSingle(k => k == SessionEventKind.PeerConnected, "a repeated Hello-with-cookie is answered with the same Accept");
        proxy.Captures.Count(c => c.Direction == RelayDirection.ClientToUpstream && IsControl(c.Payload, HelloWithCookieType))
            .Should().BeGreaterThan(1, "the dialler resent its Hello-with-cookie");
    }

    [Fact]
    public void Connect_HelloQueuedBehindAFloodOfHellos_DialCompletesOnceTheFloodHasCleared()
    {
        // Arrange — an admission cap of one, so the first flooded Hello spends the whole Pump's cap.
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, linger: _linger);
        using DatagramSessionReactorHarness accepter = new(admissionCap: 1, linger: _linger, timeProvider: dialler.TimeProvider);
        using HostilePeer flooder = new();
        for (ulong nonce = 1; nonce <= 20; nonce++)
        {
            flooder.SendDatagram(accepter.Endpoint, BuildHello(nonce));
        }

        dialler.Reactor.Connect(accepter.Endpoint);
        accepter.Reactor.Pump();
        ulong discardsAfterFlood = accepter.Reactor.Counters.AdmissionDiscards;

        // Act
        List<SessionEventKind> diallerEvents = [];
        List<SessionEventKind> accepterEvents = [];
        for (int round = 0; round < 40 && !diallerEvents.Contains(SessionEventKind.PeerConnected); round++)
        {
            dialler.TimeProvider.Advance(_round);
            dialler.Reactor.Pump();
            accepter.Reactor.Pump();
            diallerEvents.AddRange(dialler.DrainToList().Select(e => e.Kind));
            accepterEvents.AddRange(accepter.DrainToList().Select(e => e.Kind));
        }

        // Assert
        discardsAfterFlood.Should().BeGreaterThan(0UL, "the dialler's first Hello was read after the cap was spent");
        diallerEvents.Should().ContainSingle(k => k == SessionEventKind.PeerConnected);
        accepterEvents.Should().ContainSingle(k => k == SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Connect_NothingAnswers_EndsTransportFailedAtTheLingerAndFreesTheSlot()
    {
        // Arrange — capacity one, so a second Connect succeeds only once the first Slot is free.
        using DatagramSessionReactorHarness dialler = new(capacity: 1, bindLoopback: false, linger: _linger);
        using DatagramSessionReactorHarness accepter = new(linger: _linger, timeProvider: dialler.TimeProvider);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        proxy.StopRelayingBothDirections();
        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);

        // Act
        TimeSpan elapsed = TimeSpan.Zero;
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];
        while (elapsed < _linger * 2 && !events.Any(e => e.Kind == SessionEventKind.PeerClosed))
        {
            dialler.TimeProvider.Advance(_round);
            elapsed += _round;
            DatagramSessionReactorHarness.PumpRoundThroughProxy(dialler, proxy, accepter);
            events.AddRange(dialler.DrainToList());
        }

        Action redial = () => dialler.Reactor.Connect(proxy.PublicEndPoint);

        // Assert
        events.Should().ContainSingle().Which.Should().Be((SessionEventKind.PeerClosed, peer, DisconnectReason.TransportFailed));
        elapsed.Should().BeGreaterThanOrEqualTo(_linger).And.BeLessThan(_linger + _round + _round);
        proxy.Captures.Count(c => c.Direction == RelayDirection.ClientToUpstream && IsControl(c.Payload, HelloType))
            .Should().BeGreaterThan(1, "the Hello was retransmitted before the dial gave up");
        redial.Should().NotThrow("the timed-out dial freed its capacity Slot");
    }

    [Fact]
    public void Connect_AcceptsLostPastTheAccepterReceiveIdleDeadline_StillAnsweredOnceAndTheAccepterResumes()
    {
        // Arrange — the dialler announces a 20 ms send-idle interval, so the accepter's receive-idle
        // deadline is 60 ms. Maximal jitter makes the resend gaps 40, 60, then 100 ms, so the accepter is
        // interrupted before the fourth Hello-with-cookie, the first whose Accept is let through. The
        // Interrupted accepted Peer still answers it with the same Accept, and the dialler's Resume brings
        // it back. The accepter pings on the same interval, so each end keeps hearing the other.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, linger: _linger, sendIdleInterval: _round, entropy: new ConstantByteJitter(0xFF));
        using DatagramSessionReactorHarness accepter = new(
            linger: _linger, sendIdleInterval: _round, timeProvider: dialler.TimeProvider);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        int acceptsDropped = 0;
        proxy.SetPayloadDropRule(RelayDirection.UpstreamToClient, d => IsControl(d, AcceptType) && acceptsDropped++ < 3);
        dialler.Reactor.Connect(proxy.PublicEndPoint);

        // Act — run past the accepter's effective Linger after the interruption.
        List<SessionEventKind> diallerEvents = [];
        List<SessionEventKind> accepterEvents = [];
        for (int round = 0; round < 100; round++)
        {
            dialler.TimeProvider.Advance(_round);
            DatagramSessionReactorHarness.PumpRoundThroughProxy(dialler, proxy, accepter);
            diallerEvents.AddRange(dialler.DrainToList().Select(e => e.Kind));
            accepterEvents.AddRange(accepter.DrainToList().Select(e => e.Kind));
        }

        // Assert
        acceptsDropped.Should().BeGreaterThan(3, "a fourth Accept was let through");
        diallerEvents.Should().ContainSingle(k => k == SessionEventKind.PeerConnected);
        accepterEvents.Should().Equal(
            SessionEventKind.PeerConnected, SessionEventKind.PeerInterrupted, SessionEventKind.PeerResumed);
    }

    [Fact]
    public void Close_OnAPendingDial_EndsLocalCloseAndStopsRetransmitting()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, linger: _linger);
        using DatagramSessionReactorHarness accepter = new(linger: _linger, timeProvider: dialler.TimeProvider);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        proxy.StopRelayingBothDirections();
        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        for (int round = 0; round < 10; round++)
        {
            dialler.TimeProvider.Advance(_round);
            DatagramSessionReactorHarness.PumpRoundThroughProxy(dialler, proxy, accepter);
        }

        int hellosBeforeClose = proxy.Captures.Count(c => IsControl(c.Payload, HelloType));
        dialler.DrainAndDiscard();

        // Act
        dialler.Reactor.Close(peer);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = dialler.DrainToList();
        for (int round = 0; round < 60; round++)
        {
            dialler.TimeProvider.Advance(_round);
            DatagramSessionReactorHarness.PumpRoundThroughProxy(dialler, proxy, accepter);
            events.AddRange(dialler.DrainToList());
        }

        // Assert
        hellosBeforeClose.Should().BeGreaterThan(1, "the dial was retransmitting before Close");
        events.Should().ContainSingle().Which.Should().Be((SessionEventKind.PeerClosed, peer, DisconnectReason.LocalClose));
        proxy.Captures.Count(c => IsControl(c.Payload, HelloType)).Should().Be(hellosBeforeClose);
    }

    /// <summary>A jitter source that fills every draw with one byte, so each backoff delay is fixed.</summary>
    private sealed class ConstantByteJitter(byte value) : RandomNumberGenerator
    {
        public override void GetBytes(byte[] data) => data.AsSpan().Fill(value);

        public override void GetBytes(Span<byte> data) => data.Fill(value);
    }
}
