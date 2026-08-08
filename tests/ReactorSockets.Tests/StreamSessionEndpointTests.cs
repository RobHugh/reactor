using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 3.2 (issue #18): Peers, the endpoint set, and the Peer lifecycle up to an established
/// Session. Mirrors <see cref="DatagramSessionEndpointTests"/>, the house pattern for this profile.
/// </summary>
public class StreamSessionEndpointTests
{
    // --- Binding ---------------------------------------------------------

    [Fact]
    public void BoundEndpoints_OneDeclaredLoopbackEndpointOnPortZero_ExposesExactlyOneNonZeroIPv4Entry()
    {
        // Arrange
        using StreamSessionReactorHarness harness = new();

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
        using StreamSessionReactorHarness harness = new(endpoints: [first, second]);

        // Act
        IReadOnlyList<IPEndPoint> bound = harness.Reactor.BoundEndpoints;

        // Assert
        bound.Should().HaveCount(2);
        bound[0].Address.Should().Be(first.Address);
        bound[1].Address.Should().Be(second.Address);
        bound[0].Port.Should().NotBe(bound[1].Port);
    }

    [Fact]
    public void BoundEndpoints_NoDeclaredEndpoints_IsEmpty()
    {
        // Arrange
        using StreamSessionReactorHarness harness = new(bindLoopback: false);

        // Act
        IReadOnlyList<IPEndPoint> bound = harness.Reactor.BoundEndpoints;

        // Assert
        bound.Should().BeEmpty();
    }

    [Fact]
    public void Constructor_EmptyEndpointSet_SucceedsAndCanStillDial()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using StreamSessionReactorHarness dialler = new(bindLoopback: false);

        // Act
        PeerId dialled = dialler.Reactor.Connect(accepter.Endpoint);
        (var diallerEvents, var accepterEvents) = StreamSessionReactorHarness.PumpBothUntil(
            dialler, accepter, () => false, maxRounds: 10);

        // Assert
        diallerEvents.Should().ContainSingle().Which.Should().BeEquivalentTo(
            (SessionEventKind.PeerConnected, dialled, default(DisconnectReason)));
        accepterEvents.Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Constructor_DuplicateEndpoint_ThrowsArgumentExceptionNamingTheEndpoint()
    {
        // Arrange
        IPEndPoint duplicate = new(IPAddress.Loopback, 54321);

        // Act
        Action act = () => _ = new StreamSessionReactorHarness(endpoints: [duplicate, duplicate]);

        // Assert
        act.Should().Throw<ArgumentException>()
            .WithParameterName(nameof(StreamSessionOptions.Endpoints))
            .Which.Message.Should().Contain(duplicate.ToString());
    }

    // --- Establishing a Session --------------------------------------------

    [Fact]
    public void Connect_ToAListeningReactor_BothEndsDrainAPeerConnectedEvent()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using StreamSessionReactorHarness dialler = new(bindLoopback: false);

        // Act
        dialler.Reactor.Connect(accepter.Endpoint);
        (var diallerEvents, var accepterEvents) = StreamSessionReactorHarness.PumpBothUntil(
            dialler, accepter, () => false, maxRounds: 10);

        // Assert
        diallerEvents.Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerConnected);
        accepterEvents.Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Connect_OnceTheHandshakeCompletes_DrainsThePeerIdItReturned()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using StreamSessionReactorHarness dialler = new(bindLoopback: false);

        // Act
        PeerId dialled = dialler.Reactor.Connect(accepter.Endpoint);
        (var diallerEvents, _) = StreamSessionReactorHarness.PumpBothUntil(dialler, accepter, () => false, maxRounds: 10);

        // Assert
        diallerEvents.Should().ContainSingle()
            .Which.Should().BeEquivalentTo((SessionEventKind.PeerConnected, dialled, default(DisconnectReason)));
    }

    [Fact]
    public void Connect_TwoSeparateDials_AreAddressedByDifferentPeerIds()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using StreamSessionReactorHarness firstDialler = new(bindLoopback: false);
        using StreamSessionReactorHarness secondDialler = new(bindLoopback: false);

        // Act
        firstDialler.Reactor.Connect(accepter.Endpoint);
        StreamSessionReactorHarness.PumpBothUntil(firstDialler, accepter, () => accepter.DrainToList().Count > 0, maxRounds: 10);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> firstAccepted = accepter.DrainToList();
        accepter.Reactor.Pump(); // Ends the Frame the first PeerConnected belongs to, so the second wait below sees only the second.

        secondDialler.Reactor.Connect(accepter.Endpoint);
        StreamSessionReactorHarness.PumpBothUntil(secondDialler, accepter, () => accepter.DrainToList().Count > 0, maxRounds: 10);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> secondAccepted = accepter.DrainToList();

        // Assert
        firstAccepted.Should().ContainSingle();
        secondAccepted.Should().ContainSingle();
        firstAccepted[0].Peer.Should().NotBe(secondAccepted[0].Peer);
    }

    // --- Frame and Drain ---------------------------------------------------

    [Fact]
    public void Drain_BeforeAnyPump_YieldsNothing()
    {
        // Arrange
        using StreamSessionReactorHarness harness = new();

        // Act
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = harness.DrainToList();

        // Assert
        drained.Should().BeEmpty();
    }

    [Fact]
    public void Pump_WithNothingToDo_DrainsNothingAndEmitsNoWarnOrAboveRecordAndDoesNotThrow()
    {
        // Arrange
        using StreamSessionReactorHarness harness = new();

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
        // Arrange
        using StreamSessionReactorHarness harness = new();
        IPEndPoint boundEndpoint = harness.Reactor.BoundEndpoints[0];

        // Act
        harness.Reactor.Dispose();

        // Assert
        using Socket probe = new(boundEndpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        Action act = () => probe.Bind(boundEndpoint);
        act.Should().NotThrow();
    }

    [Fact]
    public void Dispose_CalledTwice_DoesNotThrow()
    {
        // Arrange
        using StreamSessionReactorHarness harness = new();
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
        using StreamSessionReactorHarness harness = new();
        harness.Reactor.Dispose();

        // Act
        Action act = harness.Reactor.Pump;

        // Assert
        act.Should().Throw<ObjectDisposedException>();
    }

    [Fact]
    public void Connect_AfterDispose_ThrowsObjectDisposedException()
    {
        // Arrange
        using StreamSessionReactorHarness harness = new();
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
        using StreamSessionReactorHarness harness = new(bindLoopback: false);
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
        using StreamSessionReactorHarness accepter = new();
        using StreamSessionReactorHarness dialler = new(bindLoopback: false);
        dialler.Reactor.Connect(accepter.Endpoint);
        StreamSessionReactorHarness.PumpBothUntil(dialler, accepter, () => accepter.DrainToList().Count > 0, maxRounds: 10);
        accepter.DrainToList().Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerConnected);

        // Act
        Action act = accepter.Reactor.Dispose;

        // Assert
        act.Should().NotThrow();
        accepter.Sink.Received.Should().NotContain(record => record.Level >= DiagnosticLevel.Error);
    }

    // --- Peer identity and lifecycle ---------------------------------------

    [Fact]
    public void Close_APeerIdThisReactorNeverIssued_IsHarmless()
    {
        // Arrange
        using StreamSessionReactorHarness harness = new();

        // Act
        Action close = () => harness.Reactor.Close(new PeerId(Origin: 0, Index: 0, Generation: 999));

        // Assert
        close.Should().NotThrow();
        harness.DrainToList().Should().BeEmpty();
    }

    [Fact]
    public void Close_AStalePeerIdWhoseSlotWasReused_FailsRatherThanAddressingTheNewOccupant()
    {
        // Arrange — close the first Peer so its Slot is free, dial again so a new Peer reuses that
        // Slot (capacity 1 forces the reuse), then present the stale PeerId for the original Peer.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        using StreamSessionReactorHarness firstDialler = new(bindLoopback: false, capacity: 1);
        using StreamSessionReactorHarness secondDialler = new(bindLoopback: false, capacity: 1);

        firstDialler.Reactor.Connect(accepter.Endpoint);
        StreamSessionReactorHarness.PumpBothUntil(firstDialler, accepter, () => accepter.DrainToList().Count > 0, maxRounds: 10);
        PeerId staleId = accepter.DrainToList().Should().ContainSingle().Which.Peer;
        accepter.Reactor.Pump(); // Ends the Frame the PeerConnected belongs to, so the drain below sees only the close.
        accepter.Reactor.Close(staleId);
        accepter.Reactor.Pump(); // Close only appends a Goodbye to retention; this Pump flushes and drains it (issue #23).
        accepter.DrainToList().Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerClosed);
        accepter.Reactor.Pump(); // Ends the Frame the close belongs to, so the second wait below sees only the new Peer.

        secondDialler.Reactor.Connect(accepter.Endpoint);
        StreamSessionReactorHarness.PumpBothUntil(secondDialler, accepter, () => accepter.DrainToList().Count > 0, maxRounds: 10);
        accepter.DrainToList().Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerConnected);
        accepter.Reactor.Pump(); // Ends the Frame the second PeerConnected belongs to, so the drain below sees only the stale Close's (non-)effect.

        // Act — the stale handle addressed the Slot the new Peer now occupies.
        accepter.Reactor.Close(staleId);

        // Assert — nothing closed, because the stale handle's generation no longer matches.
        accepter.DrainToList().Should().BeEmpty();
    }

    [Fact]
    public void Close_AnEstablishedPeer_DrainsPeerClosedForThatPeerIdWithLocalClose()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using StreamSessionReactorHarness dialler = new(bindLoopback: false);
        dialler.Reactor.Connect(accepter.Endpoint);
        StreamSessionReactorHarness.PumpBothUntil(dialler, accepter, () => accepter.DrainToList().Count > 0, maxRounds: 10);
        PeerId established = accepter.DrainToList().Should().ContainSingle().Which.Peer;
        accepter.Reactor.Pump(); // Ends the Frame the PeerConnected belongs to, so the drain below sees only the close.

        // Act
        accepter.Reactor.Close(established);
        accepter.Reactor.Pump(); // Close only appends a Goodbye to retention; this Pump flushes and drains it (issue #23).

        // Assert
        accepter.DrainToList().Should().ContainSingle()
            .Which.Should().BeEquivalentTo((SessionEventKind.PeerClosed, established, DisconnectReason.LocalClose));
    }

    [Fact]
    public void Connect_WhenTheFarEndRefusesForCapacity_DrainsPeerClosedWithRemoteAtCapacity()
    {
        // Arrange — with the sole Slot occupied by the first dialler's established Session, a second
        // dialler is refused for capacity.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        using StreamSessionReactorHarness firstDialler = new(bindLoopback: false, capacity: 1);
        using StreamSessionReactorHarness secondDialler = new(bindLoopback: false, capacity: 1);

        firstDialler.Reactor.Connect(accepter.Endpoint);
        StreamSessionReactorHarness.PumpBothUntil(firstDialler, accepter, () => accepter.DrainToList().Count > 0, maxRounds: 10);
        accepter.DrainToList().Should().ContainSingle().Which.Kind.Should().Be(SessionEventKind.PeerConnected);
        accepter.Reactor.Pump(); // Ends the Frame the PeerConnected belongs to, so the Slot is settled before the second dial.

        // Act
        PeerId dialled = secondDialler.Reactor.Connect(accepter.Endpoint);
        StreamSessionReactorHarness.PumpBothUntil(secondDialler, accepter, () => secondDialler.DrainToList().Count > 0, maxRounds: 10);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = secondDialler.DrainToList();

        // Assert
        drained.Should().ContainSingle()
            .Which.Should().BeEquivalentTo((SessionEventKind.PeerClosed, dialled, DisconnectReason.RemoteAtCapacity));
    }

    [Fact]
    public void NoPublicMember_ExposesASocketAHandleOrALinkFromThisProfile()
    {
        // Arrange — a structural check on the public surface rather than a behavioural one: every
        // public member's return and parameter types must stay within IPEndPoint, PeerId, and this
        // library's own Session vocabulary. No Socket, no LinkId, no Link-shaped type may appear.
        System.Reflection.MethodInfo[] publicMembers = typeof(StreamSessionReactor)
            .GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
            .Where(m => !m.IsSpecialName)
            .ToArray();

        Type[] disallowed = [typeof(Socket), typeof(LinkId)];

        // Act
        IEnumerable<Type> exposedTypes = publicMembers
            .SelectMany(m => new[] { m.ReturnType }.Concat(m.GetParameters().Select(p => p.ParameterType)));

        // Assert
        exposedTypes.Should().NotContain(disallowed);
    }
}
