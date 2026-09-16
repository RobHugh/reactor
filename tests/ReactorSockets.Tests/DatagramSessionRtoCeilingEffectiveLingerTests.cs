using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #365 (parent #55): Linger is the RTO's ceiling (subtask 6.4), and the ceiling must read the
/// same <em>effective</em> (negotiated, minimum-of-both-ends) Linger that Linger expiry (#367) will read
/// — never this end's own locally configured <see cref="DatagramSessionOptions.Linger"/>. This end is
/// given the longer Linger of the pair (30s) and the peer the shorter one (3s, itself well above the RTO
/// floor), so the negotiated minimum — 3s — differs from this end's own configured value. Sustained
/// backoff, driven exactly as <see cref="DatagramSessionAdaptiveRtoTests"/> and
/// <see cref="DatagramSessionPingRetransmissionTests"/> drive it, must saturate at 3000ms, never at
/// 30000ms.
/// </summary>
/// <remarks>
/// <b>Timing budget.</b> Seeding the estimator costs one 20ms round trip; settling the cadence costs
/// 1ms; the backoff run below costs 60 + 120 + 240 + 480 + 960 + 1920 + 3000 = 6780ms. The whole run is
/// therefore done inside about 6.8 seconds of the dialler's clock. <see cref="_sendIdleInterval"/> is two
/// hours, so the receive-idle deadline (send-idle interval times the default three missed heartbeats,
/// issue #352) sits six hours away — far past this test's own budget — and Interruption (issue #353)
/// never happens here at all, sidestepping the interaction with Linger expiry (#367) that a shorter
/// send-idle interval would otherwise invite.
/// </remarks>
public class DatagramSessionRtoCeilingEffectiveLingerTests
{
    /// <summary>
    /// Far longer than any RTO this file's estimator produces, so the receive-idle deadline it implies
    /// (three missed heartbeats away) never arrives during the run — see this class's own remarks.
    /// </summary>
    private static readonly TimeSpan _sendIdleInterval = TimeSpan.FromHours(2);

    /// <summary>This end's own configured Linger — the wrong ceiling, and the one a mutant would read.</summary>
    private static readonly TimeSpan _diallerLinger = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The peer's announced Linger — shorter than <see cref="_diallerLinger"/> and comfortably above the
    /// RTO floor, so ADR 0012's minimum makes it, not <see cref="_diallerLinger"/>, the negotiated
    /// (effective) Linger this end's ceiling must read.
    /// </summary>
    private static readonly TimeSpan _accepterLinger = TimeSpan.FromSeconds(3);

    /// <summary>The negotiated minimum of <see cref="_diallerLinger"/> and <see cref="_accepterLinger"/> — the correct RTO ceiling.</summary>
    private static readonly TimeSpan _effectiveLinger = TimeSpan.FromSeconds(3);

    /// <summary>The round trip the seeding exchange measures, matching the other RTO test files.</summary>
    private static readonly TimeSpan _roundTrip = TimeSpan.FromMilliseconds(20);

    /// <summary>The timeout <see cref="_roundTrip"/> seeds: smoothed 20ms plus four variances of 10ms.</summary>
    private static readonly TimeSpan _seededTimeout = TimeSpan.FromMilliseconds(60);

    [Fact]
    public void Pump_SustainedBackoffWithALongerLocalLingerThanThePeers_SaturatesAtTheNegotiatedEffectiveLingerNotTheLocalOne()
    {
        // Arrange — this end announces 30s, the peer 3s; ADR 0012's minimum makes 3s the negotiated
        // (effective) Linger this end's ceiling must read.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, sendIdleInterval: _sendIdleInterval, linger: _diallerLinger);
        using DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: _sendIdleInterval, linger: _accepterLinger);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        SeedEstimator(dialler, proxy, accepter, peer, accepterPeer, _roundTrip);
        SettleCadence(dialler);

        // Act — a fresh reliable Message is sent, then relaying is cut in both directions so neither the
        // Message nor any ack for it ever crosses the proxy again: nothing is left to reset the backoff,
        // so every advance below expires the same sequence once more.
        proxy.StopRelayingBothDirections();
        dialler.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Pump();
        proxy.Pump();
        dialler.Sink.Received.Clear();

        // Each advance is exactly the deadline currently armed, so every Pump below expires the one
        // outstanding sequence and backs off once more, exactly as DatagramSessionAdaptiveRtoTests and
        // DatagramSessionPingRetransmissionTests drive the same estimator.
        TimeSpan[] deadlines =
        [
            _seededTimeout,       // 60ms   -> backs off to 120
            _seededTimeout * 2,   // 120ms  -> 240
            _seededTimeout * 4,   // 240ms  -> 480
            _seededTimeout * 8,   // 480ms  -> 960
            _seededTimeout * 16,  // 960ms  -> 1920
            _seededTimeout * 32,  // 1920ms -> 3840, clamped to the 3000ms effective Linger
            _effectiveLinger,     // 3000ms -> clamped again
        ];

        foreach (TimeSpan deadline in deadlines)
        {
            dialler.TimeProvider.Advance(deadline);
            dialler.Reactor.Pump();
            proxy.Pump();
        }

        // Assert — the doubling saturates at the negotiated 3000ms effective Linger, never at this end's
        // own 30000ms configured Linger. A ceiling reading _options.Linger would report 3840 then 7680
        // for the last two entries instead of 3000 and 3000, so this sequence fails under that mutation.
        List<(long Rto, long Count)> reported = BackedOffRecords(dialler, peer);
        reported.Should().Equal(
            [(120, 1), (240, 2), (480, 3), (960, 4), (1920, 5), (3000, 6), (3000, 7)],
            "each expiry doubles the 60ms estimate until the negotiated 3000ms effective Linger saturates it");
        reported.Should().OnlyContain(
            entry => entry.Rto <= (long)_effectiveLinger.TotalMilliseconds,
            "the RTO never exceeds the negotiated effective Linger, however many consecutive losses occur");
        reported.Should().OnlyContain(
            entry => entry.Rto < (long)_diallerLinger.TotalMilliseconds,
            "the RTO must not climb toward this end's own, longer, locally configured Linger");
    }

    /// <summary>
    /// Sends one ReliableOrdered Message, flushes it, and acknowledges it exactly <paramref name="roundTrip"/>
    /// later through the accepter's own reply, so the Peer's shared <see cref="RetransmissionTimer"/>
    /// takes its one clean first sample. Mirrors <see cref="DatagramSessionPingRetransmissionTests.SeedEstimator"/>.
    /// </summary>
    private static void SeedEstimator(
        DatagramSessionReactorHarness dialler,
        LossyProxy proxy,
        DatagramSessionReactorHarness accepter,
        PeerId peer,
        PeerId accepterPeer,
        TimeSpan roundTrip)
    {
        dialler.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Pump();
        proxy.Pump();
        accepter.Reactor.Pump();
        accepter.DrainPayloads();

        dialler.TimeProvider.Advance(roundTrip);
        accepter.Reactor.Send(accepterPeer, DeliveryMode.Unreliable, [9]).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        proxy.Pump();
        dialler.Reactor.Pump();
        dialler.DrainPayloads();
    }

    /// <summary>
    /// One short Pump interval on the dialler's own clock, so the RTO's Pump-interval floor is a known
    /// small value rather than whatever the seeding exchange's own advance happened to be. Mirrors
    /// <see cref="DatagramSessionAdaptiveRtoTests.SettleCadence"/>.
    /// </summary>
    private static void SettleCadence(DatagramSessionReactorHarness dialler)
    {
        dialler.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
        dialler.Reactor.Pump();
    }

    /// <summary>Every <see cref="DiagnosticId.RetransmissionTimerBackedOff"/> record recorded for <paramref name="peer"/>, as the pair of figures it carries.</summary>
    private static List<(long Rto, long Count)> BackedOffRecords(DatagramSessionReactorHarness dialler, PeerId peer) =>
        dialler.Sink.Received
            .Where(record => record.Id == DiagnosticId.RetransmissionTimerBackedOff && record.Peer == peer)
            .Select(record => (record.Value0, record.Value1))
            .ToList();
}
