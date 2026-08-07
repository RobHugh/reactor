using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 5.6 (issue #37): endpoints and binding, the Frame/Drain contract, disposal, and the
/// arena/read-share arrangement — everything a consumer can observe about a
/// <see cref="DatagramSessionReactor"/> before any data plane exists. The data-plane arena assertion
/// ("many Peers deliver simultaneously and the arena fills") belongs to slice 6, once a datagram can
/// actually reach the arena; this file only proves the arena is sized and rejected correctly at
/// construction.
/// </summary>
public class DatagramSessionEndpointTests
{
    // --- Binding ---------------------------------------------------------

    [Fact]
    public void BoundEndpoints_OneDeclaredLoopbackEndpointOnPortZero_ExposesExactlyOneNonZeroIPv4Entry()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new();

        // Act
        IReadOnlyList<IPEndPoint> bound = harness.Reactor.BoundEndpoints;

        // Assert
        bound.Should().ContainSingle()
            .Which.Should().Match<IPEndPoint>(endpoint =>
                endpoint.AddressFamily == AddressFamily.InterNetwork && endpoint.Port != 0);
    }

    [Fact]
    public void BoundEndpoints_TwoDeclaredLoopbackEndpoints_ExposesBothInDeclarationOrder()
    {
        // Arrange — two distinct loopback addresses rather than the same one twice: a port-zero
        // endpoint declared twice is rejected at validation, because binds are exclusive and the second
        // could never succeed.
        IPEndPoint first = new(IPAddress.Loopback, 0);
        IPEndPoint second = new(IPAddress.Parse("127.0.0.2"), 0);
        using DatagramSessionReactorHarness harness = new(endpoints: [first, second]);

        // Act
        IReadOnlyList<IPEndPoint> bound = harness.Reactor.BoundEndpoints;

        // Assert
        bound.Should().HaveCount(2);
        bound[0].Address.Should().Be(first.Address);
        bound[1].Address.Should().Be(second.Address);
        bound[0].Port.Should().NotBe(bound[1].Port);
    }

    [Fact]
    public void BoundEndpoints_NoDeclaredEndpoints_IsEmptyBeforeAnyConnect()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new(bindLoopback: false);

        // Act
        IReadOnlyList<IPEndPoint> bound = harness.Reactor.BoundEndpoints;

        // Assert
        bound.Should().BeEmpty();
    }

    [Fact]
    public void Connect_FromANoEndpointReactor_BindsExactlyOneWildcardSocketInTheDestinationsFamilyAndASecondConnectAddsNone()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        using DatagramSessionReactorHarness otherAccepter = new();

        // Act
        dialler.Reactor.Connect(accepter.Endpoint);
        dialler.Reactor.Connect(otherAccepter.Endpoint);

        // Assert — both destinations are IPv4, so the second Connect shares the socket the first bound.
        dialler.Reactor.BoundEndpoints.Should().ContainSingle()
            .Which.Should().Match<IPEndPoint>(bound =>
                bound.AddressFamily == accepter.Endpoint.AddressFamily && bound.Port != 0);
    }

    [Fact]
    public void Connect_TwiceToTheSameDestinationFamilyFromANoEndpointReactor_SharesOneSocket()
    {
        // Arrange — the dial-only wildcard is per family, not per dial.
        using DatagramSessionReactorHarness firstAccepter = new();
        using DatagramSessionReactorHarness secondAccepter = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);

        // Act
        dialler.Reactor.Connect(firstAccepter.Endpoint);
        dialler.Reactor.Connect(secondAccepter.Endpoint);

        // Assert
        dialler.Reactor.BoundEndpoints.Should().ContainSingle();
    }

    [Fact]
    public void BoundEndpoints_OneDeclaredIPv6LoopbackEndpoint_ReportsANonZeroPortInTheIPv6Family()
    {
        // Arrange — asserting only family and port: IPv6 routing is not assumed to work on CI runners.
        using DatagramSessionReactorHarness harness =
            new(endpoints: [new IPEndPoint(IPAddress.IPv6Loopback, 0)]);

        // Act
        IReadOnlyList<IPEndPoint> bound = harness.Reactor.BoundEndpoints;

        // Assert
        bound.Should().ContainSingle()
            .Which.Should().Match<IPEndPoint>(endpoint =>
                endpoint.AddressFamily == AddressFamily.InterNetworkV6 && endpoint.Port != 0);
    }

    // --- Frame and Drain ---------------------------------------------------

    [Fact]
    public void Drain_BeforeAnyPump_YieldsNothing()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new();

        // Act
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = harness.DrainToList();

        // Assert
        drained.Should().BeEmpty();
    }

    [Fact]
    public void Drain_CalledTwiceAfterOnePump_YieldsTheSameEventsBothTimes()
    {
        // Arrange — a Drain only enumerates _pendingEvents (DatagramSessionReactor.GetPendingEvent /
        // SessionEventEnumerator); nothing about calling it clears that list, so within one Frame it is
        // repeatable rather than one-shot.
        using DatagramSessionReactorHarness accepter = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        dialler.Reactor.Connect(accepter.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntil(
            dialler, accepter, () => accepter.DrainToList().Count > 0, maxRounds: 6);

        // Act
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> firstDrain = accepter.DrainToList();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> secondDrain = accepter.DrainToList();

        // Assert
        firstDrain.Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerConnected);
        secondDrain.Should().BeEquivalentTo(firstDrain);
    }

    [Fact]
    public void Drain_AfterTheNextPumpWithNoTraffic_YieldsNothing()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        dialler.Reactor.Connect(accepter.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntil(
            dialler, accepter, () => accepter.DrainToList().Count > 0, maxRounds: 6);
        accepter.DrainToList().Should().ContainSingle();

        // Act — a further Pump with nothing waiting.
        accepter.Reactor.Pump();

        // Assert
        accepter.DrainToList().Should().BeEmpty();
    }

    [Fact]
    public void Pump_WithNothingToDo_DrainsNothingAndEmitsNoWarnOrAboveRecordAndDoesNotThrow()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new();

        // Act
        Action act = harness.Reactor.Pump;

        // Assert
        act.Should().NotThrow();
        harness.DrainToList().Should().BeEmpty();
        harness.Sink.Received.Should().NotContain(record => record.Level >= DiagnosticLevel.Warn);
    }

    // --- Disposal ------------------------------------------------------------

    [Fact]
    public void Dispose_WithABoundEndpoint_ReleasesThePortSoAPlainSocketCanBindItAfterwards()
    {
        // Arrange — the harness's own Dispose (called at end of scope) is idempotent, so disposing the
        // Reactor early in the Act is safe.
        using DatagramSessionReactorHarness harness = new();
        IPEndPoint boundEndpoint = harness.Reactor.BoundEndpoints[0];

        // Act
        harness.Reactor.Dispose();

        // Assert
        using Socket probe = new(boundEndpoint.AddressFamily, SocketType.Dgram, ProtocolType.Udp);
        Action act = () => probe.Bind(boundEndpoint);
        act.Should().NotThrow();
    }

    [Fact]
    public void Dispose_CalledTwice_DoesNotThrow()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new();
        harness.Reactor.Dispose();

        // Act
        Action act = harness.Reactor.Dispose;

        // Assert
        act.Should().NotThrow();
    }

    [Fact]
    public void Pump_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new();
        harness.Reactor.Dispose();

        // Act
        Action act = harness.Reactor.Pump;

        // Assert
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void Drain_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new();
        harness.Reactor.Dispose();

        // Act
        Action act = () => harness.Reactor.Drain();

        // Assert
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void Connect_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new();
        harness.Reactor.Dispose();

        // Act
        Action act = () => harness.Reactor.Connect(new IPEndPoint(IPAddress.Loopback, 12345));

        // Assert
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void Close_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new();
        PeerId peer = harness.Reactor.Connect(new IPEndPoint(IPAddress.Loopback, 12345));
        harness.Reactor.Dispose();

        // Act
        Action act = () => harness.Reactor.Close(peer);

        // Assert
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void Dispose_WithALiveSession_DoesNotThrowAndEmitsNoErrorLevelRecord()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        dialler.Reactor.Connect(accepter.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntil(
            dialler, accepter, () => accepter.DrainToList().Count > 0, maxRounds: 6);
        accepter.DrainToList().Should().ContainSingle()
            .Which.Kind.Should().Be(SessionEventKind.PeerConnected);

        // Act
        Action act = accepter.Reactor.Dispose;

        // Assert
        act.Should().NotThrow();
        accepter.Sink.Received.Should().NotContain(record => record.Level >= DiagnosticLevel.Error);
    }

    // --- Arena and read share ------------------------------------------------

    [Fact]
    public void Construct_ArenaSizeEqualToMaxMessageSizeWithCapacitySixtyFour_SucceedsAndCompletesAHandshake()
    {
        // Arrange — the read-share arithmetic (arena ÷ capacity) is not independently observable yet:
        // this slice has no data plane, so nothing reaches the arena. What IS observable is that a
        // Reactor built at the arena's exact floor, with a realistic capacity, constructs and completes
        // a handshake. The "many Peers fill the arena simultaneously" assertion belongs to slice 6.
        const int maxMessageSize = 4096;
        using DatagramSessionReactorHarness accepter =
            new(capacity: 64, maxMessageSize: maxMessageSize, arenaSize: maxMessageSize);
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, capacity: 64);

        // Act
        dialler.Reactor.Connect(accepter.Endpoint);
        (var diallerEvents, var accepterEvents) = DatagramSessionReactorHarness.PumpBothUntil(
            dialler, accepter, () => false, maxRounds: 6);

        // Assert
        diallerEvents.Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerConnected);
        accepterEvents.Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Construct_ArenaSizeBelowMaxMessageSize_ThrowsArgumentException()
    {
        // Arrange
        const int maxMessageSize = 4096;
        Action act = () => _ = new DatagramSessionReactorHarness(
            maxMessageSize: maxMessageSize, arenaSize: maxMessageSize - 1);

        // Act & Assert
        act.Should().Throw<ArgumentException>();
    }
}
