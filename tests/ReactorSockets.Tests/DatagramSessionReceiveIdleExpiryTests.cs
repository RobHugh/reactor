using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #352 (parent #54): the datagram profile's second liveness timer. A Peer that hears nothing
/// from its remote for <c>RemoteSendIdleInterval Ã— ResolvedMissedHeartbeats</c> moves to
/// <see cref="SessionEventKind.PeerInterrupted"/>, mirroring <c>StreamSessionReactor</c>'s own
/// receive-idle deadline (issue #117) at the datagram profile's own shape: no socket to unregister, no
/// Linger to start counting down yet (#55), just the Event.
/// </summary>
/// <remarks>
/// Every test drives two real <see cref="DatagramSessionReactor"/>s through a <see cref="LossyProxy"/>
/// on a shared <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/>, then cuts both
/// directions with <see cref="LossyProxy.StopRelayingBothDirections"/> so each Reactor keeps Pumping
/// while hearing nothing back â€” exactly what a real path failure looks like from either end.
/// <see cref="DatagramSessionReactorHarness.SettleThenAdvance"/> keeps the observed Pump-interval floor
/// small ahead of the large clock advance the deadline needs, the same technique
/// <see cref="DatagramSessionPingRetransmissionTests"/> already relies on.
/// </remarks>
public class DatagramSessionReceiveIdleExpiryTests
{
    /// <summary>
    /// Short enough that the deadline this file exercises (Ã— <see cref="MissedHeartbeats"/>) is a small
    /// clock advance, and, at <see cref="MissedHeartbeats"/> below, far under the Linger this file uses
    /// so nothing here is mistaken for a Linger expiry.
    /// </summary>
    private static readonly TimeSpan _sendIdleInterval = TimeSpan.FromSeconds(1);

    private const int MissedHeartbeats = 3;

    /// <summary>Far longer than <see cref="_sendIdleInterval"/> Ã— <see cref="MissedHeartbeats"/> (issue #352's own guidance): out of scope here (#55), but must not itself expire mid-test.</summary>
    private static readonly TimeSpan _linger = TimeSpan.FromMinutes(10);

    /// <summary>
    /// The margin "just before"/"just after" is measured by: one Pump interval, the same one
    /// <see cref="DatagramSessionReactorHarness.SettleThenAdvance"/> leaves for the final advance.
    /// </summary>
    private static readonly TimeSpan _margin = TimeSpan.FromMilliseconds(1);

    // =========================================================================================
    // AC1/AC2 â€” no PeerInterrupted just before the deadline, PeerInterrupted just after
    // =========================================================================================

    [Fact]
    public void Pump_NoDatagramForRemoteSendIdleIntervalTimesMissedHeartbeats_MovesThePeerToInterrupted()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);

        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        // Act â€” every datagram either way is silently lost from here on, so LastReceiveTimestamp never
        // moves again after the establishment that already stamped it (issue #351).
        proxy.StopRelayingBothDirections();

        TimeSpan deadline = _sendIdleInterval * MissedHeartbeats;

        // Assert â€” one Pump interval short of the deadline: nothing has happened yet.
        DatagramSessionReactorHarness.SettleThenAdvance(dialler, deadline - _margin);
        dialler.DrainToList().Should().NotContain(
            e => e.Kind == SessionEventKind.PeerInterrupted, "the receive-idle deadline has not passed yet");

        // Act â€” the final millisecond: the deadline has now passed.
        dialler.TimeProvider.Advance(_margin);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = dialler.PumpAndDrainAll(1);

        // Assert
        events.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer.Equals(peer));
    }

    // =========================================================================================
    // AC3 â€” no Diagnostic is recorded by the transition
    // =========================================================================================

    [Fact]
    public void Pump_ReceiveIdleExpiry_RecordsNoDiagnostic()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);

        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);
        proxy.StopRelayingBothDirections();

        TimeSpan deadline = _sendIdleInterval * MissedHeartbeats;
        DatagramSessionReactorHarness.SettleThenAdvance(dialler, deadline - _margin);
        dialler.DrainToList();
        dialler.Sink.Received.Clear();

        // Act â€” cross the deadline.
        dialler.TimeProvider.Advance(_margin);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = dialler.PumpAndDrainAll(1);

        // Assert
        events.Should().Contain(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer.Equals(peer));
        dialler.Sink.Received.Should().BeEmpty("a receive-idle expiry is already an Event (ADR 0013) â€” no Diagnostic doubles it");
    }

    // =========================================================================================
    // AC4 â€” the Peer is not closed
    // =========================================================================================

    [Fact]
    public void Pump_ReceiveIdleExpiry_DoesNotCloseThePeer()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);

        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);
        proxy.StopRelayingBothDirections();

        TimeSpan deadline = _sendIdleInterval * MissedHeartbeats;
        DatagramSessionReactorHarness.SettleThenAdvance(dialler, deadline - _margin);
        dialler.DrainToList();

        // Act â€” cross the deadline.
        dialler.TimeProvider.Advance(_margin);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = dialler.PumpAndDrainAll(1);

        // Assert â€” interrupted, not closed, and CountersOf (which throws for a closed Peer) still answers.
        events.Should().Contain(e => e.Kind == SessionEventKind.PeerInterrupted);
        events.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        Action countersOf = () => dialler.Reactor.CountersOf(peer);
        countersOf.Should().NotThrow("an Interrupted Peer is still open â€” only #55's Linger closes it");
    }
}
