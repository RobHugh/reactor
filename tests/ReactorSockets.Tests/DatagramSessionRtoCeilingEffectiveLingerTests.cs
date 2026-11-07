using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Linger is the RTO's ceiling, and the ceiling must read the same <em>effective</em> (negotiated,
/// minimum-of-both-ends) Linger that Linger expiry reads — never this end's own locally configured <see cref="DatagramSessionOptions.Linger"/>. This end is
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
    /// later through the accepter's own reply, so the Peer's shared <c>RetransmissionTimer</c>
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

    /// <summary>
    /// Covers the flush site (<c>FlushOutboundDatagrams</c> → <c>OutstandingReliableSequences.RecordSent</c>):
    /// a first, never-yet-seeded reliable Send arms its retransmit deadline directly from
    /// <c>RetransmissionTimer.EffectiveTimeout</c> at the moment it reaches the wire (issue #365,
    /// parent #55). With no round trip ever sampled, <c>RetransmissionTimer.Timeout</c> is
    /// <see cref="TimeSpan.MaxValue"/>, so the ceiling alone decides the deadline — the negotiated
    /// (effective) Linger if the flush site reads <see cref="PeerSlot.EffectiveLinger"/> as it must, or
    /// this end's own far longer configured Linger if it read <see cref="DatagramSessionOptions.Linger"/>
    /// instead (the mutation this test was written to kill: reverting the flush site left every other
    /// test in this file, and the whole suite, green).
    /// </summary>
    [Fact]
    public void Pump_AFirstUnseededSendWithASmallerNegotiatedLingerThanThisEndsOwnLinger_RetransmitsAtTheNegotiatedLingerNotTheLocalOne()
    {
        // Arrange — this end announces 30s, the peer 200ms; ADR 0012's minimum makes 200ms the negotiated
        // (effective) Linger the flush site's arming must read. The accepter is never pumped again after
        // the handshake, so no ack ever returns and the estimator this deadline is armed under never
        // seeds — Timeout stays TimeSpan.MaxValue and the ceiling alone decides when this fires.
        TimeSpan effectiveLinger = TimeSpan.FromMilliseconds(200);
        using DatagramSessionReactorHarness dialler = new(
            sendIdleInterval: _sendIdleInterval, linger: _diallerLinger);
        using DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: _sendIdleInterval, linger: effectiveLinger);

        PeerId peer = dialler.Reactor.Connect(accepter.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);

        // Act — one reliable Send, flushed once, then left to expire with nothing acking it.
        dialler.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Pump();

        dialler.TimeProvider.Advance(effectiveLinger - TimeSpan.FromMilliseconds(1));
        dialler.Reactor.Pump();

        // Assert — nothing has retransmitted one millisecond short of the negotiated Linger.
        dialler.Reactor.CountersOf(peer).PacketsRetransmitted.Should().Be(
            0, "the negotiated 200ms effective Linger has not yet elapsed");

        dialler.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
        dialler.Reactor.Pump();

        // Assert — the first retransmission lands exactly at the negotiated effective Linger. A flush
        // site reading this end's own 30s Linger would still show zero here.
        dialler.Reactor.CountersOf(peer).PacketsRetransmitted.Should().Be(
            1, "the deadline armed at flush must be the negotiated 200ms effective Linger, not this end's own 30s Linger");
    }

    /// <summary>
    /// Covers the ack site (<c>ParseDataDatagram</c> → <c>OutstandingReliableSequences.ApplyAck</c>):
    /// once a horizon slide newly arms a previously-deferred sequence, that arm must use the negotiated
    /// (effective) Linger, not this end's own configured one (issue #365, parent #55). Thirty-three
    /// records fill the initial ack horizon <c>[0, 32]</c> and arm normally at Send; a 34th, sequence 33,
    /// is sent and reaches the wire while the horizon still sits at 0, so it is recorded outstanding but
    /// left unarmed (<see cref="OutstandingReliableSequences.HorizonDeferrals"/>). A proxy drop keeps
    /// sequence 33 itself from ever reaching the accepter, so the ack it eventually sends back — base 33,
    /// covering everything below it — slides the horizon exactly onto sequence 33 without acknowledging
    /// it: this is the one call in this whole run where <c>ApplyAck</c>'s newly-covered arm fires. Nothing
    /// has been sampled yet at that point (issue #365's own estimator is still unseeded), so
    /// <c>RetransmissionTimer.Timeout</c> is still <see cref="TimeSpan.MaxValue"/> and the ceiling
    /// alone decides the deadline this arm sets, exactly as the flush-site test above reasons.
    /// </summary>
    [Fact]
    public void Pump_AHorizonSlideArmingADeferredSequence_RetransmitsAtTheNegotiatedLingerNotTheLocalOne()
    {
        // Arrange — this end announces 30s, the peer 300ms; the negotiated (effective) Linger the ack
        // site's newly-covered arm must read is 300ms. Fragment size is raised to the RFC 791 floor so
        // thirty-four single-byte reliable sends cost thirty-four cheap retention slots rather than the
        // default fragment size's much larger ones.
        TimeSpan effectiveLinger = TimeSpan.FromMilliseconds(300);
        const int fragmentSize = 576;
        const int retentionBytes = 40 * fragmentSize;
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false,
            sendIdleInterval: _sendIdleInterval,
            linger: _diallerLinger,
            fragmentSize: fragmentSize,
            retentionBytes: retentionBytes);
        using DatagramSessionReactorHarness accepter = new(
            sendIdleInterval: _sendIdleInterval,
            linger: effectiveLinger,
            fragmentSize: fragmentSize,
            retentionBytes: retentionBytes);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);

        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        // Act — thirty-three sends fill the initial ack horizon [0, 32] and arm at Send; all thirty-three
        // reach the accepter, whose own receive base advances to 33.
        for (int sequence = 0; sequence < 33; sequence++)
        {
            dialler.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [(byte)sequence]).Should().Be(SendOutcome.Sent);
        }

        dialler.Reactor.Pump();
        proxy.Pump();
        accepter.Reactor.Pump();
        accepter.DrainPayloads();

        // A 34th send — sequence 33 — reaches the wire while the horizon still sits at 0 (nothing has
        // acked anything yet), so it is recorded outstanding but left unarmed. The proxy drops it before
        // it ever reaches the accepter, so the accepter's own receive base never advances past it.
        dialler.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [33]).Should().Be(SendOutcome.Sent);
        proxy.DropNext(RelayDirection.ClientToUpstream);
        dialler.Reactor.Pump();
        proxy.Pump();

        // The accepter has nothing queued to send, so it sends something to force a flush: its next
        // Datagram carries an ack block with base 33 (everything below is cumulatively acknowledged) and
        // no bit for sequence 33 itself, since it never arrived. Applying that ack is what slides the
        // dialler's horizon exactly onto sequence 33 and newly arms it.
        accepter.Reactor.Send(accepterPeer, DeliveryMode.Unreliable, [9]).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        proxy.Pump();
        dialler.Reactor.Pump();
        dialler.DrainPayloads();

        dialler.Reactor.CountersOf(peer).PacketsRetransmitted.Should().Be(
            0, "sequence 33 has only just been armed by the horizon slide, not expired");

        // Advance to one millisecond short of the negotiated Linger from the moment the horizon slide
        // armed sequence 33, then across it.
        dialler.TimeProvider.Advance(effectiveLinger - TimeSpan.FromMilliseconds(1));
        dialler.Reactor.Pump();

        dialler.Reactor.CountersOf(peer).PacketsRetransmitted.Should().Be(
            0, "the negotiated 300ms effective Linger has not yet elapsed since the horizon slide armed sequence 33");

        dialler.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
        dialler.Reactor.Pump();

        // Assert — sequence 33 retransmits exactly at the negotiated effective Linger measured from when
        // the ack site's horizon slide armed it. An ack site reading this end's own 30s Linger instead of
        // slot.EffectiveLinger would still show zero here.
        dialler.Reactor.CountersOf(peer).PacketsRetransmitted.Should().Be(
            1, "the horizon slide's arm must use the negotiated 300ms effective Linger, not this end's own 30s Linger");
    }
}
