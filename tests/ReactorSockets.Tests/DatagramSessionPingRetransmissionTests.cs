using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A Ping (issue #340) is not a special-cased timer of its own — it is an
/// ordinary <c>ReliableUnordered</c> sequence that took retention's <c>isControl</c> path at
/// <c>QueuePing</c>, and every test here proves it inherits the adaptive timer, backoff and
/// retransmission machinery <see cref="DatagramSessionRetransmitResendTests"/> and
/// <see cref="DatagramSessionAdaptiveRtoTests"/> already prove for an ordinary data record, with no
/// Ping-specific code path anywhere in between. Every test drives two real
/// <see cref="DatagramSessionReactor"/>s through a <see cref="LossyProxy"/>, on a shared
/// <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/>, never a sleep.
/// </summary>
/// <remarks>
/// <b>The Pump-interval floor.</b> <see cref="RetransmissionTimer.EffectiveTimeout"/> floors the RTO at
/// <see cref="PumpIntervalObserver.Interval"/> — the gap since the Reactor's own previous Pump — so a
/// test that advances the clock by a large amount (waiting out <see cref="DatagramSessionOptions.SendIdleInterval"/>)
/// and then Pumps once would corrupt that floor for the very Pump that arms the Ping's retransmit
/// deadline. <see cref="SettleThenAdvance"/> splits every such wait into "advance to one millisecond
/// short, Pump once to reset the observed cadence to something small, advance the last millisecond" —
/// the same technique <see cref="DatagramSessionAdaptiveRtoTests.SettleCadence"/> uses for exact-figure
/// assertions.
/// </remarks>
public class DatagramSessionPingRetransmissionTests
{
    private const byte ControlBit = 0b0000_1000;

    /// <summary>
    /// A send-idle interval far longer than any RTO this file's estimator ever produces (the ceiling is
    /// the harness's own one-second Linger): an interval this large means a Ping's resend can only
    /// be explained by the RTO, never by a second send-idle interval quietly having elapsed too.
    /// </summary>
    private static readonly TimeSpan _sendIdleInterval = TimeSpan.FromHours(2);

    // =========================================================================================
    // A dropped Ping is resent with the same sequence, the control bit still set, length 0
    // =========================================================================================

    [Fact]
    public void Pump_ADroppedPing_IsResentWithTheSameSequenceStillAControlRecordAndZeroLength()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, sendIdleInterval: _sendIdleInterval);
        using DatagramSessionReactorHarness accepter = new(sendIdleInterval: _sendIdleInterval);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        // Act — the send-idle interval elapses, queuing and flushing the Ping, and this proxy relay
        // drops it before the accepter ever sees it.
        SettleThenAdvance(dialler, _sendIdleInterval);
        proxy.DropNext(RelayDirection.ClientToUpstream);
        dialler.Reactor.Pump();
        proxy.Pump();

        DecodedPacketRecord originalPing = DecodeSingleRecord(
            proxy.Captures.Last(c => c.Direction == RelayDirection.ClientToUpstream).Payload);
        (originalPing.Flags & ControlBit).Should().Be(ControlBit, "the send-idle timer queues a control record");
        originalPing.Length.Should().Be(0, "a control record never carries a payload");

        accepter.Reactor.Pump();
        accepter.DrainPayloads().Should().BeEmpty("the Ping was dropped before it reached the accepter");

        // Act — comfortably past the unseeded estimator's RTO (the one-second Linger ceiling), which is
        // nowhere near a second send-idle interval.
        dialler.TimeProvider.Advance(dialler.Options.Linger + TimeSpan.FromSeconds(1));
        dialler.Reactor.Pump();
        proxy.Pump();

        // Assert — the resend is the same sequence, still a control record, still zero length: a resent
        // Ping is repacked through the control-record shape, never through the ordinary zero-length
        // Message path.
        DecodedPacketRecord resent = DecodeSingleRecord(
            proxy.Forwarded.Last(c => c.Direction == RelayDirection.ClientToUpstream).Payload);
        resent.Sequence.Should().Be(originalPing.Sequence, "a resend never takes a fresh sequence");
        resent.Flags.Should().Be(originalPing.Flags, "the resend is still a control record, not an ordinary empty Message");
        resent.Length.Should().Be(0, "a resent Ping never carries a payload");
    }

    // =========================================================================================
    // The resend happens at the effective RTO, not at SendIdleInterval
    // =========================================================================================

    [Fact]
    public void Pump_ADroppedPing_IsResentWellBeforeASecondSendIdleIntervalCouldElapse()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, sendIdleInterval: _sendIdleInterval);
        using DatagramSessionReactorHarness accepter = new(sendIdleInterval: _sendIdleInterval);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        SettleThenAdvance(dialler, _sendIdleInterval);
        proxy.DropNext(RelayDirection.ClientToUpstream);
        dialler.Reactor.Pump();
        proxy.Pump();

        ulong retransmittedBefore = dialler.Reactor.CountersOf(peer).PacketsRetransmitted;

        // Act — the estimator's RTO (unseeded, so the one-second Linger ceiling) passes, a figure the
        // test names explicitly to keep it far under the two-hour SendIdleInterval it is being
        // contrasted against.
        TimeSpan wellPastTheRto = dialler.Options.Linger + TimeSpan.FromSeconds(1);
        wellPastTheRto.Should().BeLessThan(
            TimeSpan.FromHours(1), "the RTO this test waits out must stay far shorter than the send-idle interval");
        dialler.TimeProvider.Advance(wellPastTheRto);
        dialler.Reactor.Pump();
        proxy.Pump();

        // Assert — the retransmitted counter is what only a genuine resend moves: a second send-idle
        // Ping would instead take a fresh sequence through the ordinary Send-queuing path and never
        // touch this counter, so this is what proves the RTO drove the resend rather than the interval.
        (dialler.Reactor.CountersOf(peer).PacketsRetransmitted - retransmittedBefore).Should().BeGreaterThan(
            0UL, "the Ping was resent because its RTO passed, nowhere near a second send-idle interval");
    }

    // =========================================================================================
    // Two different RTT histories give the Ping different resend deadlines, while
    // SendIdleInterval stays identical in both
    // =========================================================================================

    [Fact]
    public void Pump_TwoDifferentRttHistories_GiveThePingDifferentResendDeadlinesWithTheSameSendIdleInterval()
    {
        // Arrange/Act — the first-sample RFC 6298 rule seeds SRTT = R, RTTVAR = R / 2, so the timeout is
        // R + 4*(R/2) = 3R: a fast history seeds a short deadline, a slow one a longer deadline, purely
        // from the estimator, since both runs share the same two-hour SendIdleInterval.
        TimeSpan fastRoundTrip = TimeSpan.FromMilliseconds(20);
        TimeSpan slowRoundTrip = TimeSpan.FromMilliseconds(100);

        AssertPingResendDeadline(fastRoundTrip, expectedDeadline: TimeSpan.FromMilliseconds(60));
        AssertPingResendDeadline(slowRoundTrip, expectedDeadline: TimeSpan.FromMilliseconds(300));
    }

    /// <summary>
    /// Builds a fresh connected pair, seeds the shared estimator with one clean round trip of
    /// <paramref name="roundTrip"/>, drops the Ping the send-idle timer then queues, and asserts its
    /// resend deadline is exactly <paramref name="expectedDeadline"/>: nothing at one millisecond short
    /// of it, and the resend at it.
    /// </summary>
    private static void AssertPingResendDeadline(TimeSpan roundTrip, TimeSpan expectedDeadline)
    {
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, sendIdleInterval: _sendIdleInterval);
        using DatagramSessionReactorHarness accepter = new(sendIdleInterval: _sendIdleInterval);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        SeedEstimator(dialler, proxy, accepter, peer, accepterPeer, roundTrip);

        // The seeding send's own flush is what SendIdleInterval is measured from, and SeedEstimator
        // already advanced the clock by roundTrip waiting for the ack — so only the remainder needs to
        // elapse here for the timer to queue the Ping.
        SettleThenAdvance(dialler, _sendIdleInterval - roundTrip);
        proxy.DropNext(RelayDirection.ClientToUpstream);
        dialler.Reactor.Pump();
        proxy.Pump();

        ulong retransmittedBefore = dialler.Reactor.CountersOf(peer).PacketsRetransmitted;

        // Act — one millisecond short of the predicted deadline: nothing resent yet.
        dialler.TimeProvider.Advance(expectedDeadline - TimeSpan.FromMilliseconds(1));
        dialler.Reactor.Pump();
        proxy.Pump();
        dialler.Reactor.CountersOf(peer).PacketsRetransmitted.Should().Be(
            retransmittedBefore, $"the {expectedDeadline.TotalMilliseconds}ms deadline has not passed yet");

        // Act — the final millisecond: the resend happens exactly at the predicted deadline.
        dialler.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
        dialler.Reactor.Pump();
        proxy.Pump();
        dialler.Reactor.CountersOf(peer).PacketsRetransmitted.Should().Be(
            retransmittedBefore + 1, $"the {expectedDeadline.TotalMilliseconds}ms deadline, set by this run's own RTT sample, has now passed");
    }

    // =========================================================================================
    // A Ping dropped repeatedly backs off the same way a dropped data record does
    // =========================================================================================

    [Fact]
    public void Pump_APingDroppedRepeatedly_BacksOffWithTheSameDoublingSequenceAsADroppedDataRecord()
    {
        // Arrange — seeded at the same 20ms round trip DatagramSessionAdaptiveRtoTests uses, so the
        // doubling sequence below is the literal figures that file already proves for a dropped data
        // record: 60ms seeds the timeout, then each further loss doubles it until the one-second Linger
        // ceiling saturates it.
        TimeSpan roundTrip = TimeSpan.FromMilliseconds(20);

        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, sendIdleInterval: _sendIdleInterval);
        using DatagramSessionReactorHarness accepter = new(sendIdleInterval: _sendIdleInterval);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        SeedEstimator(dialler, proxy, accepter, peer, accepterPeer, roundTrip);

        // Every relay carrying a control record is dropped, forever — not just the next one — so the
        // Ping is lost on its first transmission and on every resend that follows.
        proxy.SetPayloadDropRule(RelayDirection.ClientToUpstream, IsControlRecordPayload);

        // The seeding send's own flush is what SendIdleInterval is measured from, and SeedEstimator
        // already advanced the clock by roundTrip waiting for the ack — so only the remainder needs to
        // elapse here for the timer to queue the Ping.
        SettleThenAdvance(dialler, _sendIdleInterval - roundTrip);
        dialler.Reactor.Pump();
        proxy.Pump();
        dialler.Sink.Received.Clear();

        // Act — each advance is exactly the deadline currently armed, so every Pump below expires the
        // Ping once more and backs off again.
        TimeSpan[] deadlines =
        [
            TimeSpan.FromMilliseconds(60),   // seeded timeout   -> backs off to 120
            TimeSpan.FromMilliseconds(120),  // 120ms            -> 240
            TimeSpan.FromMilliseconds(240),  // 240ms            -> 480
            TimeSpan.FromMilliseconds(480),  // 480ms            -> 960
            TimeSpan.FromMilliseconds(960),  // 960ms            -> 1920, clamped to the one-second Linger
            dialler.Options.Linger,          // 1000ms           -> clamped again
        ];

        foreach (TimeSpan deadline in deadlines)
        {
            dialler.TimeProvider.Advance(deadline);
            dialler.Reactor.Pump();
            proxy.Pump();
        }

        // Assert — the identical doubling sequence DatagramSessionAdaptiveRtoTests proves for a dropped
        // ReliableOrdered data record, read off the same RetransmissionTimerBackedOff diagnostic: a
        // Ping's own backoff is indistinguishable from an ordinary reliable sequence's.
        List<(long Rto, long Count)> reported = BackedOffRecords(dialler, peer);
        reported.Should().Equal(
            [(120, 1), (240, 2), (480, 3), (960, 4), (1000, 5), (1000, 6)],
            "a repeatedly dropped Ping backs off by the same doubling estimator as a dropped data record");
    }

    /// <summary>
    /// Whether <paramref name="payload"/> — a captured datagram, exactly as observed by
    /// <see cref="LossyProxy"/> — carries at least one control record: the wire shape a Ping, and only a
    /// Ping, ever takes (issue #338).
    /// </summary>
    private static bool IsControlRecordPayload(byte[] payload)
    {
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(payload);
        return decoded.IsSuccess && decoded.Datagram!.Records.Any(r => (r.Flags & ControlBit) == ControlBit);
    }

    /// <summary>
    /// Sends one ReliableOrdered Message, flushes it, and acknowledges it exactly
    /// <paramref name="roundTrip"/> later through the accepter's own reply — the only way an ack block
    /// reaches the wire (a Datagram is only sent for a Peer with records queued) — so the Peer's shared
    /// <see cref="RetransmissionTimer"/> takes its one clean first sample and the Ping this same slot
    /// queues afterwards inherits that estimate. Matches
    /// <see cref="DatagramSessionAdaptiveRtoTests.SeedTheEstimator"/>'s shape, at the full two-Reactor
    /// protocol level rather than through a hand-crafted client.
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
    /// Splits a large wait into "advance to one millisecond short, Pump once, advance the final
    /// millisecond" so the Pump that actually queues and arms the Ping — the caller's own Pump right
    /// after this call returns — observes a one-millisecond cadence rather than the whole wait. Without
    /// this, <see cref="RetransmissionTimer.EffectiveTimeout"/>'s Pump-interval floor would read the
    /// entire <paramref name="interval"/> and swamp the RTO every test in this file asserts on (see this
    /// class's own remarks).
    /// </summary>
    private static void SettleThenAdvance(DatagramSessionReactorHarness dialler, TimeSpan interval)
    {
        dialler.TimeProvider.Advance(interval - TimeSpan.FromMilliseconds(1));
        dialler.Reactor.Pump();
        dialler.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
    }

    /// <summary>Decodes <paramref name="datagram"/> and returns its one packet record, failing the test if decoding fails or more than one record is present.</summary>
    private static DecodedPacketRecord DecodeSingleRecord(byte[] datagram)
    {
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(datagram);
        decoded.IsSuccess.Should().BeTrue(decoded.Error);
        return decoded.Datagram!.Records.Should().ContainSingle().Which;
    }

    /// <summary>Every <see cref="DiagnosticId.RetransmissionTimerBackedOff"/> record recorded for <paramref name="peer"/>, as the pair of figures it carries.</summary>
    private static List<(long Rto, long Count)> BackedOffRecords(DatagramSessionReactorHarness dialler, PeerId peer) =>
        dialler.Sink.Received
            .Where(record => record.Id == DiagnosticId.RetransmissionTimerBackedOff && record.Peer == peer)
            .Select(record => (record.Value0, record.Value1))
            .ToList();
}
