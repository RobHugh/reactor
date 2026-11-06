using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Once a Peer is <see cref="SessionEventKind.PeerInterrupted"/>, later Pumps
/// must not raise that Event for it again, even though its receive-idle deadline
/// (<c>HasExceededReceiveIdleDeadline</c>, issue #352) stays exceeded forever after the path is cut.
/// </summary>
/// <remarks>
/// Drives a real <see cref="DatagramSessionReactor"/> pair through a <see cref="LossyProxy"/> on a
/// shared <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/>, cuts both directions with
/// <see cref="LossyProxy.StopRelayingBothDirections"/>, crosses the receive-idle deadline once, then
/// keeps Pumping across several further deadline lengths and asserts only the one Event ever arrived.
/// </remarks>
public class DatagramSessionInterruptedPeerIdleTests
{
    private static readonly TimeSpan _sendIdleInterval = TimeSpan.FromSeconds(1);

    private const int MissedHeartbeats = 3;

    /// <summary>Far longer than the deadline this file crosses repeatedly, so nothing here is a Linger expiry.</summary>
    private static readonly TimeSpan _linger = TimeSpan.FromMinutes(10);

    [Fact]
    public void Pump_KeepsPumpingPastAnAlreadyInterruptedPeersDeadline_RaisesPeerInterruptedOnlyOnce()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);

        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        // Act — cut the path, cross the receive-idle deadline once.
        proxy.StopRelayingBothDirections();

        TimeSpan deadline = _sendIdleInterval * MissedHeartbeats;
        DatagramSessionReactorHarness.SettleThenAdvance(dialler, deadline);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> firstCrossing = dialler.PumpAndDrainAll(1);
        firstCrossing.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer.Equals(peer));

        // Act — keep Pumping across several further deadline lengths; the deadline stays exceeded the
        // whole time since nothing relays any datagram back.
        for (int i = 0; i < 5; i++)
        {
            DatagramSessionReactorHarness.SettleThenAdvance(dialler, deadline);
        }

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> laterEvents = dialler.PumpAndDrainAll(1);

        // Assert — no further PeerInterrupted for a Peer already Interrupted.
        laterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerInterrupted);
    }
}
