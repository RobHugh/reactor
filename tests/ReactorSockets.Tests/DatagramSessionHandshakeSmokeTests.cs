using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

public class DatagramSessionHandshakeSmokeTests
{
    [Fact]
    public void Connect_BetweenTwoReactorsOnLoopback_EstablishesASessionAtBothEnds()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);
        IPEndPoint destination = accepter.Endpoint;

        // Act
        PeerId dialled = dialler.Reactor.Connect(destination);
        (var diallerEvents, var accepterEvents) = DatagramSessionReactorHarness.PumpBothUntil(
            dialler, accepter, () => false, maxRounds: 6);

        // Assert
        diallerEvents.Should().ContainSingle()
            .Which.Should().BeEquivalentTo((SessionEventKind.PeerConnected, dialled, default(DisconnectReason)));
        accepterEvents.Should().ContainSingle()
            .Which.Kind.Should().Be(SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Connect_FromAReactorWithNoDeclaredEndpoints_BindsOneWildcardSocketInTheDestinationsFamily()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);

        // Act
        dialler.Reactor.Connect(accepter.Endpoint);

        // Assert
        dialler.Reactor.BoundEndpoints.Should().ContainSingle()
            .Which.Should().Match<IPEndPoint>(bound =>
                bound.AddressFamily == accepter.Endpoint.AddressFamily && bound.Port != 0);
    }

    [Fact]
    public void Pump_WhenAStrangerSendsRubbish_DiscardsItWithoutAnEventOrAThrow()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using RawReactorHarness stranger = new();
        stranger.SendFromCounterparty(accepter.Endpoint, [1, 2, 3]);

        // Act
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDiscards > 0);

        // Assert
        accepter.DrainToList().Should().BeEmpty();
        accepter.Reactor.Counters.UnknownSourceDiscards.Should().Be(1);
        accepter.Sink.Received.Should().Contain(record => record.Id == DiagnosticId.DatagramMalformed);
    }
}
