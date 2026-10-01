using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #399 (parent #57): a Closing datagram Peer whose Goodbye is never acknowledged closes with
/// <see cref="DisconnectReason.LingerExpired"/> once its effective Linger has elapsed since
/// <see cref="DatagramSessionReactor.Close"/>, and not before. If the ack arrives first, it closes with
/// <see cref="DisconnectReason.LocalClose"/> and nothing closes it a second time. Drives a real pair
/// through a <see cref="LossyProxy"/> on a shared <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/>.
/// Every Pump drains its Events, since a Pump clears any still-undrained ones when it starts.
/// </summary>
public class DatagramSessionClosingLingerTests
{
    private static readonly TimeSpan _linger = TimeSpan.FromSeconds(5);

    private static (DatagramSessionReactorHarness Dialler, DatagramSessionReactorHarness Accepter, LossyProxy Proxy, PeerId Peer) Connect()
    {
        DatagramSessionReactorHarness dialler = new(bindLoopback: false, linger: _linger);
        DatagramSessionReactorHarness accepter = new(linger: _linger, timeProvider: dialler.TimeProvider);
        LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);
        return (dialler, accepter, proxy, peer);
    }

    private static List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> PumpDialler(DatagramSessionReactorHarness dialler)
    {
        dialler.Reactor.Pump();
        return dialler.DrainToList();
    }

    [Fact]
    public void Pump_ClosingPeerWhoseGoodbyeIsNeverAcked_ClosesWithLingerExpiredExactlyWhenLingerElapsesSinceClose()
    {
        // Arrange
        (DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, LossyProxy proxy, PeerId peer) = Connect();
        using (dialler)
        using (accepter)
        using (proxy)
        {
            proxy.StopRelayingBothDirections();
            dialler.TimeProvider.Advance(TimeSpan.FromSeconds(3));
            dialler.Reactor.Close(peer);
            PumpDialler(dialler).Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);

            // Act
            dialler.TimeProvider.Advance(_linger - TimeSpan.FromMilliseconds(1));
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> justBefore = PumpDialler(dialler);
            dialler.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> atLinger = PumpDialler(dialler);
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> afterwards = PumpDialler(dialler);

            // Assert
            justBefore.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed, "Linger has not elapsed since Close");
            atLinger.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
                .Which.Should().Be((SessionEventKind.PeerClosed, peer, DisconnectReason.LingerExpired));
            afterwards.Should().BeEmpty();
        }
    }

    [Fact]
    public void Pump_ClosingPeerWhoseGoodbyeIsAckedBeforeLinger_ClosesWithLocalCloseAndNothingClosesItAgain()
    {
        // Arrange
        (DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, LossyProxy proxy, PeerId peer) = Connect();
        using (dialler)
        using (accepter)
        using (proxy)
        {
            dialler.Reactor.Close(peer);
            dialler.TimeProvider.Advance(_linger / 2);
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];

            // Act
            DatagramSessionReactorHarness.PumpRoundThroughProxy(dialler, proxy, accepter);
            events.AddRange(dialler.DrainToList());
            events.AddRange(PumpDialler(dialler));
            dialler.TimeProvider.Advance(_linger);
            events.AddRange(PumpDialler(dialler));
            events.AddRange(PumpDialler(dialler));

            // Assert
            events.Where(e => e.Kind == SessionEventKind.PeerClosed).Should().ContainSingle()
                .Which.Should().Be((SessionEventKind.PeerClosed, peer, DisconnectReason.LocalClose));
        }
    }
}
