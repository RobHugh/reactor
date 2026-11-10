using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A dialled datagram Peer that has sent a Goodbye (Closing after a local
/// <see cref="DatagramSessionReactor.Close"/>) never sends a Resume, even when its receive-idle deadline
/// passes. A Closing Peer does not go Interrupted at all, so it raises no
/// <see cref="SessionEventKind.PeerInterrupted"/> either, and it ends through Linger measured from Close.
/// Resume datagrams are recognised on the wire by <see cref="DatagramSessionResumeScenario.IsResume"/>.
/// </summary>
public class DatagramSessionClosingNeverResumesTests
{
    private const int MissedHeartbeats = 3;

    private static readonly TimeSpan _sendIdleInterval = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan _linger = TimeSpan.FromSeconds(10);

    private static (DatagramSessionReactorHarness Dialler, DatagramSessionReactorHarness Accepter, LossyProxy Proxy, PeerId Peer) Connect()
    {
        DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats,
            timeProvider: dialler.TimeProvider);
        LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);
        return (dialler, accepter, proxy, peer);
    }

    [Fact]
    public void Pump_ClosingDialledPeerWhoseRemoteGoesSilent_SendsNoResumeAndClosesWithLingerExpired()
    {
        // Arrange
        (DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, LossyProxy proxy, PeerId peer) = Connect();
        using (dialler)
        using (accepter)
        using (proxy)
        {
            proxy.StopRelayingBothDirections();
            dialler.Reactor.Close(peer);
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = [];

            // Act: well past the receive-idle deadline (MissedHeartbeats send-idle intervals), inside Linger
            int pumpsPastDeadline = MissedHeartbeats + 3;
            for (int i = 0; i < pumpsPastDeadline; i++)
            {
                dialler.TimeProvider.Advance(_sendIdleInterval);
                dialler.Reactor.Pump();
                events.AddRange(dialler.DrainToList());
                proxy.Pump();
            }

            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> beforeLinger = [.. events];
            for (int i = 0; i < 6; i++)
            {
                dialler.TimeProvider.Advance(_sendIdleInterval);
                dialler.Reactor.Pump();
                events.AddRange(dialler.DrainToList());
                proxy.Pump();
            }

            // Assert
            beforeLinger.Should().BeEmpty("the Peer stayed Closing: not Interrupted, not closed");
            proxy.Captures.Where(c => c.Direction == RelayDirection.ClientToUpstream && DatagramSessionResumeScenario.IsResume(c.Payload)).Should().BeEmpty(
                "a Closing dialled Peer sends no Resume");
            events.Should().ContainSingle()
                .Which.Should().Be((SessionEventKind.PeerClosed, peer, DisconnectReason.LingerExpired));
        }
    }
}
