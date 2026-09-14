using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// <see cref="PeerId"/> is shared by both Session profiles (ADR 0010), so the Origin that makes an
/// identity mean something to exactly one Reactor (ADR 0016) has to hold <i>across</i> the profiles
/// and not only within one. The two Reactors here are a datagram Session and a stream Session, which
/// is the case each profile's own identity tests cannot see: they run two Reactors of one kind.
/// </summary>
/// <remarks>
/// This is the regression the tests below exist for. The Origin counter was first written as a static
/// field of the generic <c>ShareHolderTable&lt;T&gt;</c>, and each profile's occupant type is its own
/// closed instantiation with its own copy of that field, so the two profiles counted independently
/// and each issued Origin 1 to its first Reactor. The defect ADR 0016 was written to fix survived it,
/// one profile over, until the Origin became the Reactor's and came from
/// <c>Core.ReactorOrigin</c>.
/// </remarks>
public sealed class PeerIdOriginTests
{
    [Fact]
    public void Establish_OnADatagramAndAStreamReactor_GivesTheirFirstPeersDifferentPeerIds()
    {
        // Arrange — the first Peer of any Reactor of either profile sits in Slot 0 on generation 1,
        // so the Origin is the only component that can tell these two identities apart.
        using DatagramSessionReactorHarness datagram = new(bindLoopback: false);
        using DatagramSessionReactorHarness remote = new();
        using StreamSessionReactorHarness stream = new();

        // Act
        PeerId issuedByDatagram = datagram.Reactor.Connect(remote.Endpoint);
        PeerId issuedByStream = stream.EstablishOnePeer().Peer;

        // Assert
        issuedByStream.Should().NotBe(issuedByDatagram);
    }

    [Fact]
    public void Close_APeerIdIssuedByTheOtherProfilesReactor_LeavesThisReactorsPeerOpen()
    {
        // Arrange
        using DatagramSessionReactorHarness datagram = new(bindLoopback: false);
        using DatagramSessionReactorHarness remote = new();
        using StreamSessionReactorHarness stream = new();
        PeerId issuedByDatagram = datagram.Reactor.Connect(remote.Endpoint);
        StreamSessionReactorHarness.HeldPeer held = stream.EstablishOnePeer();
        stream.DrainToList();

        // Act — a consumer running one Reactor of each profile hands the wrong one's PeerId over.
        stream.Reactor.Close(issuedByDatagram);
        stream.Reactor.Pump();

        // Assert — it addressed nothing here, so nothing closed and no Event was raised.
        stream.DrainToList().Should().BeEmpty();

        // And the Peer it would have collided with is still open, so Send still resolves it.
        stream.Reactor.Send(held.Peer, [1, 2, 3]).Should().Be(SendOutcome.Sent);
    }
}
