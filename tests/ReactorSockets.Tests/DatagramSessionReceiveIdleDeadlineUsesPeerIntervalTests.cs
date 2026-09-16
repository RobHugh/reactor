using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #357 (parent #54): a count only means something relative to the peer's emission interval, so
/// each end's receive-idle deadline (issue #352's <c>EnforceReceiveIdleTimer</c>) is computed from the
/// interval the <em>peer</em> announced — <see cref="PeerSlot.RemoteSendIdleInterval"/> — never the
/// end's own <see cref="DatagramSessionOptions.SendIdleInterval"/>. A dialler with a 1-second interval
/// and an accepter with a 5-second interval make the asymmetry observable: each end's deadline tracks
/// the other end's interval, not its own.
/// </summary>
/// <remarks>
/// Both harnesses share one <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/> (via the
/// dialler's clock passed to the <see cref="LossyProxy"/>) so a single clock advance moves both ends'
/// timers together, exactly as <see cref="DatagramSessionReceiveIdleExpiryTests"/> already relies on.
/// After the handshake, <see cref="LossyProxy.StopRelayingBothDirections"/> cuts both directions so
/// neither end's <c>LastReceiveTimestamp</c> moves again, and
/// <see cref="DatagramSessionReactorHarness.SettleThenAdvance"/> keeps the observed Pump-interval floor
/// small ahead of each large clock advance the deadline needs.
/// </remarks>
public class DatagramSessionReceiveIdleDeadlineUsesPeerIntervalTests
{
    private static readonly TimeSpan _diallerSendIdleInterval = TimeSpan.FromSeconds(1);

    private static readonly TimeSpan _accepterSendIdleInterval = TimeSpan.FromSeconds(5);

    private const int MissedHeartbeats = 3;

    /// <summary>Far longer than the dialler's own deadline (15s) so nothing here is mistaken for a Linger expiry.</summary>
    private static readonly TimeSpan _linger = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The margin "just before"/"just after" is measured by: one Pump interval, the same one
    /// <see cref="DatagramSessionReactorHarness.SettleThenAdvance"/> leaves for the final advance.
    /// </summary>
    private static readonly TimeSpan _margin = TimeSpan.FromMilliseconds(1);

    // =========================================================================================
    // AC1 — the accepter hears nothing from a 1-second peer, so it is interrupted at about 3s
    // =========================================================================================

    [Fact]
    public void Pump_AccepterHearsNothingFromOneSecondPeer_IsInterruptedAtPeerIntervalTimesMissedHeartbeats()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, sendIdleInterval: _diallerSendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: _accepterSendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats, timeProvider: dialler.TimeProvider);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);

        dialler.Reactor.Connect(proxy.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        // Act — every datagram either way is silently lost from here on, so LastReceiveTimestamp never
        // moves again after the establishment that already stamped it.
        proxy.StopRelayingBothDirections();

        TimeSpan deadline = _diallerSendIdleInterval * MissedHeartbeats;

        // Assert — one Pump interval short of the deadline: nothing has happened yet.
        DatagramSessionReactorHarness.SettleThenAdvance(accepter, deadline - _margin);
        accepter.DrainToList().Should().NotContain(
            e => e.Kind == SessionEventKind.PeerInterrupted, "the accepter's receive-idle deadline (from the dialler's 1s interval) has not passed yet");

        // Act — the final millisecond: the deadline has now passed.
        accepter.TimeProvider.Advance(_margin);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = accepter.PumpAndDrainAll(1);

        // Assert
        events.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer.Equals(accepterPeer));
    }

    // =========================================================================================
    // AC2 — the dialler hears nothing from a 5-second peer, so it is interrupted at about 15s,
    //       and in particular not at 3s (its own interval × missed heartbeats)
    // =========================================================================================

    [Fact]
    public void Pump_DiallerHearsNothingFromFiveSecondPeer_IsInterruptedAtPeerIntervalTimesMissedHeartbeatsNotItsOwn()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, sendIdleInterval: _diallerSendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: _accepterSendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats, timeProvider: dialler.TimeProvider);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);

        PeerId diallerPeer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        // Act — every datagram either way is silently lost from here on.
        proxy.StopRelayingBothDirections();

        TimeSpan ownIntervalDeadline = _diallerSendIdleInterval * MissedHeartbeats;
        TimeSpan peerIntervalDeadline = _accepterSendIdleInterval * MissedHeartbeats;

        // Assert — at the dialler's own interval × missed heartbeats (3s), nothing has happened: the
        // deadline is computed from the peer's announced interval, not the dialler's own.
        DatagramSessionReactorHarness.SettleThenAdvance(dialler, ownIntervalDeadline);
        dialler.PumpAndDrainAll(1).Should().NotContain(
            e => e.Kind == SessionEventKind.PeerInterrupted, "the dialler's own send-idle interval must not drive its receive-idle deadline");

        // Assert — one Pump interval short of the peer-interval deadline (15s): still nothing.
        DatagramSessionReactorHarness.SettleThenAdvance(dialler, peerIntervalDeadline - ownIntervalDeadline - _margin);
        dialler.DrainToList().Should().NotContain(
            e => e.Kind == SessionEventKind.PeerInterrupted, "the accepter's 5s interval deadline has not passed yet");

        // Act — the final millisecond: the deadline has now passed.
        dialler.TimeProvider.Advance(_margin);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = dialler.PumpAndDrainAll(1);

        // Assert
        events.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer.Equals(diallerPeer));
    }
}
