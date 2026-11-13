using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A Peer a <see cref="StreamSessionReactor"/> dialled that has not established one
/// <see cref="StreamSessionOptions.Linger"/> after <see cref="StreamSessionReactor.Connect"/> closes with
/// <see cref="DisconnectReason.TransportFailed"/> and frees its Slot, whether its TCP connect is still
/// pending or the far end connected and sent no handshake record (issue #483). A dial that establishes
/// before that deadline is unaffected. The far end is a plain <see cref="RawStreamListener"/>, and time
/// moves only through <see cref="StreamSessionReactorHarness.TimeProvider"/>.
/// </summary>
public class StreamSessionDialTimeoutTests
{
    private static readonly TimeSpan _linger = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan _oneTick = TimeSpan.FromTicks(1);

    /// <summary>
    /// Accepts the connection <paramref name="dialer"/> just opened to <paramref name="listener"/>, pumps
    /// once so the connect resolves and this end's Hello goes out, and reads that Hello. The dial is then
    /// waiting for the far end's Hello. The clock does not move. The caller owns disposing the result.
    /// </summary>
    private static RawStreamListener.Accepted AcceptAndReadHello(StreamSessionReactorHarness dialer, RawStreamListener listener)
    {
        RawStreamListener.Accepted far = listener.Accept();
        dialer.Reactor.Pump();
        byte[] hello = far.Receive(StreamSessionTestRecords.HandshakeRecordSize);
        hello[0].Should().Be(StreamSessionTestRecords.HelloRecordType, "the dial must be waiting for the far end's Hello");
        return far;
    }

    [Fact]
    public void Pump_DialAwaitingTheFarEndsHelloAtItsLinger_ClosesWithTransportFailed()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new(capacity: 1, linger: _linger);
        using RawStreamListener listener = new();
        PeerId peer = dialer.Reactor.Connect(listener.Endpoint);
        using RawStreamListener.Accepted far = AcceptAndReadHello(dialer, listener);

        // Act
        dialer.TimeProvider.Advance(_linger);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = dialer.PumpAndDrainAll(1);

        // Assert
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Should().Be((SessionEventKind.PeerClosed, peer, DisconnectReason.TransportFailed));
    }

    [Fact]
    public void Pump_DialAwaitingTheFarEndsHelloOneTickShortOfItsLinger_StaysPending()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new(capacity: 1, linger: _linger);
        using RawStreamListener listener = new();
        PeerId peer = dialer.Reactor.Connect(listener.Endpoint);
        using RawStreamListener.Accepted far = AcceptAndReadHello(dialer, listener);

        // Act
        dialer.TimeProvider.Advance(_linger - _oneTick);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = dialer.PumpAndDrainAll(3);

        // Assert — still pending: no close, and a Send is refused as not yet established.
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        dialer.Reactor.Send(peer, [1]).Should().Be(SendOutcome.NotEstablished);
    }

    [Fact]
    public void Connect_AfterADialClosedAtItsLinger_ReusesTheFreedSlot()
    {
        // Arrange — capacity 1, so a second Connect throws unless the timed-out dial freed its Slot.
        using StreamSessionReactorHarness dialer = new(capacity: 1, linger: _linger);
        using RawStreamListener listener = new();
        dialer.Reactor.Connect(listener.Endpoint);
        using RawStreamListener.Accepted far = AcceptAndReadHello(dialer, listener);
        dialer.TimeProvider.Advance(_linger);
        dialer.PumpAndDrainAll(1).Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed);

        // Act
        Action dialAgain = () => dialer.Reactor.Connect(listener.Endpoint);

        // Assert
        dialAgain.Should().NotThrow("the timed-out dial's Slot is free again");
    }

    [Fact]
    public void Pump_DialStillConnectingAtItsLinger_ClosesWithTransportFailed()
    {
        // Arrange — no Pump has run since Connect, so the dial is still in its TCP connect phase: the
        // deadline check in a Pump runs before that Pump polls the connect.
        using StreamSessionReactorHarness dialer = new(capacity: 1, linger: _linger);
        using RawStreamListener listener = new();
        PeerId peer = dialer.Reactor.Connect(listener.Endpoint);

        // Act
        dialer.TimeProvider.Advance(_linger);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = dialer.PumpAndDrainAll(1);

        // Assert
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Should().Be((SessionEventKind.PeerClosed, peer, DisconnectReason.TransportFailed));
    }

    [Fact]
    public void Pump_FarEndsHelloArrivingOneTickShortOfTheLinger_EstablishesTheDial()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new(capacity: 1, linger: _linger);
        using RawStreamListener listener = new();
        PeerId peer = dialer.Reactor.Connect(listener.Endpoint);
        using RawStreamListener.Accepted far = AcceptAndReadHello(dialer, listener);
        dialer.TimeProvider.Advance(_linger - _oneTick);

        // Act
        far.Send(StreamSessionTestRecords.BuildHello(sendIdleMs: 60_000, lingerMs: 60_000));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained =
            dialer.PumpUntilDrained(events => events.Any(e => e.Kind == SessionEventKind.PeerConnected));

        // Assert
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerConnected && e.Peer == peer);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    [Fact]
    public void Pump_DialEstablishedBeforeItsLinger_IsNotClosedOnceTheLingerHasPassed()
    {
        // Arrange — the far end announces a 60 s send-idle interval, so its receive-idle deadline
        // (180 s at the default three missed heartbeats) is far beyond the time this test advances.
        using StreamSessionReactorHarness dialer = new(capacity: 1, linger: _linger);
        using RawStreamListener listener = new();
        PeerId peer = dialer.Reactor.Connect(listener.Endpoint);
        using RawStreamListener.Accepted far = AcceptAndReadHello(dialer, listener);
        far.Send(StreamSessionTestRecords.BuildHello(sendIdleMs: 60_000, lingerMs: 60_000));
        dialer.PumpUntilDrained(events => events.Any(e => e.Kind == SessionEventKind.PeerConnected))
            .Should().ContainSingle(e => e.Kind == SessionEventKind.PeerConnected && e.Peer == peer);

        // Act
        dialer.TimeProvider.Advance(_linger);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = dialer.PumpAndDrainAll(3);

        // Assert
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    [Fact]
    public void Pump_FarEndsHelloAlreadyBufferedWhenTheLingerPasses_EstablishesTheDial()
    {
        // Arrange — the Hello is in the socket buffer before the Pump that crosses the deadline, so the
        // readiness pass reads it before the deadline is checked.
        using StreamSessionReactorHarness dialer = new(capacity: 1, linger: _linger);
        using RawStreamListener listener = new();
        PeerId peer = dialer.Reactor.Connect(listener.Endpoint);
        using RawStreamListener.Accepted far = AcceptAndReadHello(dialer, listener);
        far.Send(StreamSessionTestRecords.BuildHello(sendIdleMs: 60_000, lingerMs: 60_000));
        Thread.Sleep(20);
        dialer.TimeProvider.Advance(_linger);

        // Act
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = dialer.PumpAndDrainAll(1);

        // Assert
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerConnected && e.Peer == peer);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }
}
