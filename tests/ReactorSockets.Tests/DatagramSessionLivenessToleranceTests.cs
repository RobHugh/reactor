using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #358 (parent #54): the datagram profile's receive-idle deadline is
/// <c>RemoteSendIdleInterval Ã— ResolvedMissedHeartbeats</c> (issue #352), not anything derived from the
/// adaptive RTO. This file proves the two stay independent under exactly the condition that would expose
/// a deadline wired to the RTO instead: a path degraded so the Reactor under test's own reliable sends
/// are never acknowledged and its RTO backs off far past the liveness deadline, while its peer's own
/// traffic keeps arriving often enough that the Session never has reason to consider the peer dead. Only
/// once the peer's own traffic stops too does the deadline that actually governs â€” <c>peerSendIdle Ã—
/// count</c>, unmoved by how far the RTO had backed off â€” fire.
/// </summary>
/// <remarks>
/// <b>Which direction degrades.</b> The dialler is the Reactor under test. Every datagram it sends
/// (<see cref="RelayDirection.ClientToUpstream"/>) is dropped for the rest of the run, so nothing it ever
/// sends â€” its seeded probe's retransmissions included â€” is ever acknowledged: its RTO backs off without
/// limit, up to the ten-minute Linger ceiling this file configures precisely so the RTO has room to climb
/// far past the three-second liveness deadline before anything else intervenes. The accepter's own
/// traffic (<see cref="RelayDirection.UpstreamToClient"/>) is left flowing: with its own
/// <see cref="DatagramSessionOptions.SendIdleInterval"/> set to the same one second used for the
/// deadline, its send-idle timer (issue #340) queues and flushes a Ping to the dialler roughly every
/// second with no help from this test, which is what keeps <c>PeerSlot.LastReceiveTimestamp</c> on the
/// dialler refreshing well inside the three-second deadline for as long as the accepter is allowed to
/// reach it.
/// </remarks>
public class DatagramSessionLivenessToleranceTests
{
    /// <summary>The interval both ends announce. Short so the liveness deadline below is a small clock advance next to the RTO's ten-minute ceiling.</summary>
    private static readonly TimeSpan _sendIdleInterval = TimeSpan.FromSeconds(1);

    private const int MissedHeartbeats = 3;

    /// <summary>The RTO's ceiling (ADR 0009): long enough that consecutive losses can back the RTO off far past <see cref="_sendIdleInterval"/> Ã— <see cref="MissedHeartbeats"/> before saturating.</summary>
    private static readonly TimeSpan _linger = TimeSpan.FromMinutes(10);

    /// <summary>The round trip the estimator is seeded with, matching <see cref="DatagramSessionAdaptiveRtoTests"/>'s own figure: smoothed 20ms plus four variances of 10ms seeds a 60ms timeout.</summary>
    private static readonly TimeSpan _seedRoundTrip = TimeSpan.FromMilliseconds(20);

    /// <summary>One Pump interval, the same margin <see cref="DatagramSessionReceiveIdleExpiryTests"/> uses to distinguish "just before" from "just after" the deadline.</summary>
    private static readonly TimeSpan _margin = TimeSpan.FromMilliseconds(1);

    /// <summary>
    /// How long the degradation phase runs, in simulated time: comfortably long enough for the RTO's
    /// repeated doubling from its seeded 60ms to climb past several times the three-second liveness
    /// deadline, while short enough to stay nowhere near the ten-minute Linger ceiling.
    /// </summary>
    private static readonly TimeSpan _degradationWindow = TimeSpan.FromSeconds(20);

    /// <summary>
    /// How finely the degradation phase steps the shared clock. Small enough next to the accepter's
    /// one-second send-idle interval that its periodic Ping is relayed promptly rather than only once
    /// after a large jump, which is what keeps the dialler's receive-idle deadline from being judged on a
    /// coarser cadence than the real traffic that is meant to satisfy it.
    /// </summary>
    private static readonly TimeSpan _degradationStep = TimeSpan.FromMilliseconds(200);

    /// <summary>
    /// The accepter's own retention, sized for far more than the roughly twenty Pings its one-second
    /// send-idle timer queues over <see cref="_degradationWindow"/>. Large on purpose: with
    /// <see cref="RelayDirection.ClientToUpstream"/> permanently dropped, the dialler never acknowledges
    /// any of them, so nothing ever frees a slot â€” a default-sized pool would exhaust after only a
    /// handful of Pings (a known trap: it holds roughly three in-flight records) and the accepter would
    /// stop being able to queue a fresh one, silently starving the very traffic this file's degradation
    /// phase depends on to keep the dialler's receive-idle deadline from being judged early. Each fresh
    /// Ping is queued, flushed and forgotten by <c>PendingSends</c> on its own send-idle tick regardless
    /// of whether an earlier one was ever acknowledged, so a large enough pool is the only thing this
    /// file needs to keep that cadence steady for the whole window.
    /// </summary>
    private const int AccepterRetentionBytes = 1200 * 40;

    // =========================================================================================
    // AC1/AC2 (issue #358) â€” the RTO backs off far past the liveness deadline while the peer's own
    // traffic keeps the Session alive, and PeerInterrupted arrives at peerSendIdle Ã— count, not later,
    // only once that traffic stops too.
    // =========================================================================================

    [Fact]
    public void Pump_RtoBacksOffFarPastPeerSendIdleTimesMissedHeartbeatsOnLostAcksAlone_InterruptsExactlyAtThatDeadlineOnceThePeersOwnTrafficAlsoStops()
    {
        // Arrange â€” one clock shared by both Reactors and the proxy, so a single advance moves every
        // timer in the scenario together (see this file's own remarks and FragmentSizeDiagnosisFixture).
        FakeTimeProvider clock = new();
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false,
            sendIdleInterval: _sendIdleInterval,
            linger: _linger,
            missedHeartbeats: MissedHeartbeats,
            timeProvider: clock);
        using DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: _sendIdleInterval,
            linger: _linger,
            missedHeartbeats: MissedHeartbeats,
            retentionBytes: AccepterRetentionBytes,
            timeProvider: clock);
        using LossyProxy proxy = new(accepter.Endpoint, clock);

        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        SeedTheEstimator(dialler, proxy, accepter, peer, accepterPeer);

        // Act â€” every datagram the dialler sends from here on is read off the wire and never forwarded:
        // nothing it sends, including the probe below and its own retransmissions, is ever acknowledged.
        proxy.SetPayloadDropRule(RelayDirection.ClientToUpstream, _ => true);

        dialler.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [2]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Pump();
        dialler.Sink.Received.Clear();

        // The degradation phase: the shared clock steps forward finely enough that the accepter's own
        // one-second send-idle Ping is queued, flushed and relayed to the dialler on schedule, refreshing
        // its receive-idle deadline well inside the three-second bound below, while the dialler's own
        // unacknowledged probe keeps expiring and backing its RTO off further every time it does.
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEventsDuringDegradation = [];
        int steps = (int)(_degradationWindow.Ticks / _degradationStep.Ticks);
        for (int i = 0; i < steps; i++)
        {
            clock.Advance(_degradationStep);
            DatagramSessionReactorHarness.PumpRoundThroughProxy(dialler, proxy, accepter);
            diallerEventsDuringDegradation.AddRange(dialler.DrainToList());
            accepter.DrainToList();
        }

        // Assert â€” the RTO backed off far past the liveness deadline (a concrete bound, not merely > 0),
        // and the Session never once considered the peer dead while that was happening.
        TimeSpan livenessDeadline = _sendIdleInterval * MissedHeartbeats;
        List<(long Rto, long Count)> backedOff = BackedOffRecords(dialler, peer);
        backedOff.Should().NotBeEmpty("the dialler's unacknowledged probe must have expired and backed off at least once");
        backedOff.Should().BeInAscendingOrder(entry => entry.Count, "each consecutive loss raises the backoff count by one");
        backedOff[^1].Rto.Should().BeGreaterThan(
            (long)(livenessDeadline.TotalMilliseconds * 3),
            $"the RTO must have backed off to well past three times the {livenessDeadline.TotalMilliseconds}ms liveness deadline, " +
            "not merely past it, to rule out a coincidental near-miss");
        diallerEventsDuringDegradation.Should().NotContain(
            e => e.Kind == SessionEventKind.PeerInterrupted,
            "the accepter's own periodic Ping kept arriving well inside the liveness deadline throughout, however far the dialler's own RTO backed off");

        // Act â€” one more datagram lands at a known instant, marking exactly when the peer's own traffic
        // last reached the dialler before the path goes silent in both directions.
        ulong datagramsBeforeTheLastOne = dialler.Reactor.Counters.DatagramsReceived;
        accepter.Reactor.Send(accepterPeer, DeliveryMode.Unreliable, [9]).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        proxy.Pump();
        dialler.Reactor.Pump();
        dialler.Reactor.Counters.DatagramsReceived.Should().BeGreaterThan(
            datagramsBeforeTheLastOne, "this datagram must actually have landed, to anchor the deadline measured below");
        dialler.DrainToList();

        // Act â€” now every datagram either way is silently lost, so LastReceiveTimestamp never moves again.
        proxy.StopRelayingBothDirections();

        // Assert â€” one Pump interval short of the deadline measured from that last arrival: nothing has
        // happened yet, however far the RTO had backed off before relaying stopped.
        DatagramSessionReactorHarness.SettleThenAdvance(dialler, livenessDeadline - _margin);
        dialler.DrainToList().Should().NotContain(
            e => e.Kind == SessionEventKind.PeerInterrupted, "the receive-idle deadline has not passed yet");

        // Act â€” the final millisecond: the deadline has now passed.
        dialler.TimeProvider.Advance(_margin);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = dialler.PumpAndDrainAll(1);

        // Assert â€” PeerInterrupted arrives at exactly peerSendIdle Ã— count, not later, and not because
        // the RTO said so.
        events.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer.Equals(peer));
    }

    /// <summary>
    /// Sends one ReliableOrdered Message from the dialler, flushes it, and acknowledges it exactly
    /// <see cref="_seedRoundTrip"/> later through the accepter's own reply â€” the only way an ack block
    /// reaches the wire â€” so the Peer's shared <see cref="RetransmissionTimer"/> takes its one clean first
    /// sample and the probe armed after this method returns inherits that estimate. Matches
    /// <see cref="DatagramSessionPingRetransmissionTests.SeedEstimator"/>'s shape.
    /// </summary>
    private static void SeedTheEstimator(
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

    /// <summary>Every <see cref="DiagnosticId.RetransmissionTimerBackedOff"/> record recorded for <paramref name="peer"/>, as the pair of figures it carries.</summary>
    private static List<(long Rto, long Count)> BackedOffRecords(DatagramSessionReactorHarness dialler, PeerId peer) =>
        dialler.Sink.Received
            .Where(record => record.Id == DiagnosticId.RetransmissionTimerBackedOff && record.Peer == peer)
            .Select(record => (record.Value0, record.Value1))
            .ToList();
}
