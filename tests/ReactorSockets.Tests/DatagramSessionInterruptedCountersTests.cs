using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #370 (parent #55): per-Peer counters keep running across an Interruption — a counter's
/// lifetime is the Peer's, not the transport's (ADR 0013), so <see cref="PeerCounters"/> read through
/// <see cref="DatagramSessionReactor.CountersOf"/> must neither reset at
/// <see cref="SessionEventKind.PeerInterrupted"/> nor stop moving afterwards. Drives a real
/// <see cref="DatagramSessionReactor"/> pair through a <see cref="LossyProxy"/> on a shared
/// <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/>, seeds the Peer's shared
/// <see cref="RetransmissionTimer"/> with one clean round trip (mirrors
/// <see cref="DatagramSessionPingRetransmissionTests.SeedEstimator"/>), then cuts the path so a
/// retained <c>ReliableOrdered</c> record keeps expiring into a proxy that drops every relay, both
/// before and after the receive-idle deadline moves the Peer to Interrupted.
/// </summary>
public class DatagramSessionInterruptedCountersTests
{
    private static readonly TimeSpan _sendIdleInterval = TimeSpan.FromSeconds(1);

    private const int MissedHeartbeats = 3;

    /// <summary>
    /// Ten minutes — far longer than every clock advance this file makes (a 3-second receive-idle
    /// deadline plus five 10-second retransmit windows, fifty-three seconds in all), so nothing here is
    /// mistaken for a Linger expiry.
    /// </summary>
    private static readonly TimeSpan _linger = TimeSpan.FromMinutes(10);

    /// <summary>
    /// The seeding round trip (the same 20ms case <see cref="DatagramSessionPingRetransmissionTests"/>
    /// uses): seeds the shared <see cref="RetransmissionTimer"/> at a 60ms timeout, so this file's
    /// retransmissions stay dense relative to the 10-minute Linger rather than waiting out an unseeded
    /// estimator's Linger-ceilinged RTO.
    /// </summary>
    private static readonly TimeSpan _seedRoundTrip = TimeSpan.FromMilliseconds(20);

    [Fact]
    public void Pump_APeerInterruptedWithARetainedReliableRecord_KeepsCountersRunningAndRetransmittingWhileInterrupted()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);

        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        SeedEstimator(dialler, proxy, accepter, peer, accepterPeer);

        // Act — cut the path, then Send a ReliableOrdered Message that only ever reaches a proxy now
        // dropping every relay: it is retained, unacknowledged, and its retransmit timer is armed.
        proxy.StopRelayingBothDirections();
        dialler.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [3, 3, 3]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Pump();
        dialler.DrainToList();

        PeerCounters beforeInterruption = dialler.Reactor.CountersOf(peer);

        // Act — cross the receive-idle deadline once, exactly as DatagramSessionReceiveIdleExpiryTests
        // does: the deadline is measured in two steps so the crossing Pump lands on the final millisecond.
        TimeSpan deadline = _sendIdleInterval * MissedHeartbeats;
        DatagramSessionReactorHarness.SettleThenAdvance(dialler, deadline);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> crossing = dialler.PumpAndDrainAll(1);
        crossing.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer.Equals(peer));

        PeerCounters afterInterruption = dialler.Reactor.CountersOf(peer);

        // Assert — AC1: every counter read through CountersOf before the transition is at least what it
        // was; PeerInterrupted resets none of them.
        afterInterruption.MessagesReceived.Should().BeGreaterThanOrEqualTo(beforeInterruption.MessagesReceived);
        afterInterruption.PacketsRetransmitted.Should().BeGreaterThanOrEqualTo(beforeInterruption.PacketsRetransmitted);
        afterInterruption.BytesSent.Should().BeGreaterThanOrEqualTo(beforeInterruption.BytesSent);
        afterInterruption.BytesReceived.Should().BeGreaterThanOrEqualTo(beforeInterruption.BytesReceived);
        afterInterruption.SocketWrites.Should().BeGreaterThanOrEqualTo(beforeInterruption.SocketWrites);

        // Act — keep Pumping across five further 10-second windows, entirely after PeerInterrupted: the
        // retained record's retransmit timer keeps expiring into a proxy that drops every relay, so both
        // figures below keep climbing while the Peer stays Interrupted (never Interrupted a second time).
        for (int i = 0; i < 5; i++)
        {
            DatagramSessionReactorHarness.SettleThenAdvance(dialler, TimeSpan.FromSeconds(10));
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = dialler.PumpAndDrainAll(1);
            events.Should().NotContain(
                e => e.Kind == SessionEventKind.PeerInterrupted, "a Peer already Interrupted is never interrupted again");
        }

        PeerCounters afterRetransmissionWindow = dialler.Reactor.CountersOf(peer);

        // Assert — AC2: a strict rise, measured over a window entirely after PeerInterrupted, proves the
        // retained record keeps being resent while Interrupted rather than only up to the transition.
        afterRetransmissionWindow.PacketsRetransmitted.Should().BeGreaterThan(
            afterInterruption.PacketsRetransmitted,
            "the retained ReliableOrdered record keeps expiring and being resent while the Peer is Interrupted");
        afterRetransmissionWindow.BytesSent.Should().BeGreaterThan(
            afterInterruption.BytesSent,
            "each resend hands the transport more bytes for this Peer even though nothing is ever delivered");
    }

    /// <summary>
    /// Sends one ReliableOrdered Message, flushes it, and acknowledges it exactly
    /// <see cref="_seedRoundTrip"/> later through the accepter's own reply — the only way an ack block
    /// reaches the wire — so the Peer's shared <see cref="RetransmissionTimer"/> takes its one clean
    /// first sample. Matches <see cref="DatagramSessionPingRetransmissionTests.SeedEstimator"/>'s shape:
    /// a short, known timeout keeps this file's clock advances small relative to the ten-minute Linger
    /// it must stay far under.
    /// </summary>
    private static void SeedEstimator(
        DatagramSessionReactorHarness dialler,
        LossyProxy proxy,
        DatagramSessionReactorHarness accepter,
        PeerId peer,
        PeerId accepterPeer)
    {
        dialler.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Pump();
        proxy.Pump();
        accepter.Reactor.Pump();
        accepter.DrainPayloads();

        dialler.TimeProvider.Advance(_seedRoundTrip);
        accepter.Reactor.Send(accepterPeer, DeliveryMode.Unreliable, [9]).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        proxy.Pump();
        dialler.Reactor.Pump();
        dialler.DrainPayloads();
    }
}
