using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The dialling side's own re-dial of a lost transport, during a
/// <see cref="StreamSessionReactor.Pump"/>, on the schedule <see cref="RedialSchedule"/> (issue #111)
/// derives from the observed Pump interval (issue #112) and this Peer's own effective Linger — never a
/// separate attempt-count knob. Every test here drives a real dialling <see cref="StreamSessionReactor"/>
/// through the public L2 surface only (tests/CLAUDE.md, ADR 0015) against a hand-built raw counterparty
/// that <em>listens</em> rather than dials (<see cref="RawStreamListener"/>) — the mirror image of every
/// other stream Session test file's raw counterparty, needed here because this Reactor is the one doing
/// the dialling. Every test is driven entirely by <see cref="StreamSessionReactorHarness.TimeProvider"/>
/// — no test here sleeps, and no attempt's actual jittered delay is ever asserted exactly: a test only
/// ever advances the clock by at least the schedule's own ceiling for that attempt, which is due
/// regardless of where <see cref="RedialSchedule"/>'s jitter happened to land.
/// </summary>
public class StreamSessionRedialTests
{
    private static readonly TimeSpan _harnessLinger = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Dials <paramref name="dialer"/> at <paramref name="listener"/>, completes the handshake by hand
    /// (this Reactor's own Hello, then a raw Hello reply announcing <paramref name="resumeSecret"/> and
    /// <paramref name="lingerMs"/>), and
    /// returns the established <see cref="PeerId"/> paired with the raw socket now holding the far end,
    /// leaving <paramref name="dialer"/>'s clock advanced by a small, known amount between the two Pumps
    /// the handshake takes — the cadence <see cref="StreamSessionReactor.ObservedPumpInterval"/> reads at
    /// the moment a later interruption begins this Peer's re-dial schedule.
    /// </summary>
    private static (PeerId Peer, RawStreamListener.Accepted Far) EstablishDialledSession(
        StreamSessionReactorHarness dialer, RawStreamListener listener, byte[] resumeSecret, ulong lingerMs = 1000)
    {
        PeerId peer = dialer.Reactor.Connect(listener.Endpoint);
        RawStreamListener.Accepted far = listener.Accept();

        dialer.Reactor.Pump();
        byte[] hello = far.Receive(StreamSessionTestRecords.HandshakeRecordSize);
        hello[0].Should().Be(StreamSessionTestRecords.HelloRecordType);

        dialer.TimeProvider.Advance(_cadence);
        far.Send(StreamSessionTestRecords.BuildHello(lingerMs: lingerMs, resumeSecret: resumeSecret));

        PeerId? connected = null;
        for (int i = 0; i < 20 && connected is null; i++)
        {
            dialer.Reactor.Pump();
            foreach ((SessionEventKind Kind, PeerId Peer, DisconnectReason Reason) evt in dialer.DrainToList())
            {
                if (evt.Kind == SessionEventKind.PeerConnected)
                {
                    connected = evt.Peer;
                }
            }
        }

        connected.Should().Be(peer, "the dial must establish before a test can interrupt it");
        return (peer, far);
    }

    /// <summary>The known cadence <see cref="EstablishDialledSession"/> advances the clock by mid-handshake.</summary>
    private static readonly TimeSpan _cadence = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Interrupts <paramref name="dialer"/>'s established Peer by resetting <paramref name="far"/>
    /// abruptly, advancing the clock by <see cref="_cadence"/> first so the Pump that observes the reset
    /// also reports <see cref="_cadence"/> as <see cref="StreamSessionReactor.ObservedPumpInterval"/> —
    /// the floor <see cref="RedialSchedule"/>'s first delay is derived from. Pumps until
    /// <see cref="SessionEventKind.PeerInterrupted"/> drains and returns everything drained on the way.
    /// </summary>
    private static List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> InterruptDialledPeer(
        StreamSessionReactorHarness dialer, RawStreamListener.Accepted far, PeerId peer)
    {
        far.ResetAbruptly();

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        bool interrupted() => drained.Any(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == peer);
        for (int i = 0; i < 20 && !interrupted(); i++)
        {
            dialer.TimeProvider.Advance(_cadence);
            dialer.Reactor.Pump();
            drained.AddRange(dialer.DrainToList());
        }

        interrupted().Should().BeTrue("the reset must be observed as an interruption within the Pump budget");
        return drained;
    }

    // =========================================================================================
    // An Interrupted dialling-side Peer re-dials without being asked, targeting the
    // endpoint originally dialled, during ordinary Pump calls with FakeTimeProvider advanced
    // =========================================================================================

    [Fact]
    public void Pump_InterruptedDiallingPeer_ReDialsTheOriginalEndpointWithoutTheConsumerAsking()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new();
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 9);
        (PeerId peer, RawStreamListener.Accepted far) = EstablishDialledSession(dialer, listener, resumeSecret);
        InterruptDialledPeer(dialer, far, peer);
        far.Dispose();

        // Act — advance past the first delay's ceiling (min(observed cadence, effective Linger), so
        // min(50ms, 1s) = 50ms here) and pump: a fresh dial to the same listener must land.
        dialer.TimeProvider.Advance(_cadence);
        dialer.Reactor.Pump();

        // Assert — the listener this Reactor originally dialled receives a second connection, on its
        // own, with no further consumer action between the reset and now.
        listener.TryAccept(out RawStreamListener.Accepted redial, timeoutMs: 2000).Should().BeTrue(
            "an Interrupted dialling Peer must re-dial its original endpoint without being asked");
        redial.Dispose();
    }

    [Fact]
    public void Pump_InterruptedWithACadenceTooFastToMeasure_FloorsTheFirstDelayAtTheSmallestWaitNotTheLargest()
    {
        // Arrange — a loop pumping faster than its own clock resolves reports an observed interval of
        // exactly zero. Zero is a cadence too fast to measure, not an absent one, so the floor derived
        // from it is one tick: the next Pump is the floor (issue #112) and it has already arrived. The
        // jitter is deterministic and maximal (0xFF draws the top of any range), so the first delay is
        // exactly that one tick — where deferring to the effective Linger instead would have made it a
        // full second, and no attempt would follow the single tick advanced below.
        using StreamSessionReactorHarness dialer = new(entropy: new ConstantByteJitter(0xFF));
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 11);
        (PeerId peer, RawStreamListener.Accepted far) = EstablishDialledSession(dialer, listener, resumeSecret);

        // Arrange — interrupt without advancing the clock at all, so the Pump that observes the reset
        // reports a zero interval rather than the handshake's cadence.
        far.ResetAbruptly();
        bool interrupted = false;
        for (int i = 0; i < 20 && !interrupted; i++)
        {
            dialer.Reactor.Pump();
            interrupted = dialer.DrainToList().Any(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == peer);
        }

        interrupted.Should().BeTrue("the reset must be observed as an interruption within the Pump budget");
        far.Dispose();

        // Act
        dialer.TimeProvider.Advance(TimeSpan.FromTicks(1));
        dialer.Reactor.Pump();

        // Assert
        listener.TryAccept(out RawStreamListener.Accepted redial, timeoutMs: 2000).Should().BeTrue(
            "a cadence too fast to measure floors the first delay at one tick, not at the effective Linger");
        redial.Dispose();
    }

    // =========================================================================================
    // A re-dial that connects sends a Resume record naming the secret the far end issued, handing
    // off to the resume match on the listening side (#123) — never closes, and stops the schedule
    // =========================================================================================

    [Fact]
    public void Pump_ReDialThatConnects_SendsTheResumeRecordAndRaisesNoFurtherAttempts()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new();
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 3);
        (PeerId peer, RawStreamListener.Accepted far) = EstablishDialledSession(dialer, listener, resumeSecret);
        InterruptDialledPeer(dialer, far, peer);
        far.Dispose();

        // Act — the first attempt connects (the listener is still up); one more Pump lets PollConnect
        // observe it and send the Resume record.
        dialer.TimeProvider.Advance(_cadence);
        dialer.Reactor.Pump();
        listener.TryAccept(out RawStreamListener.Accepted redialed, timeoutMs: 2000).Should().BeTrue();
        dialer.Reactor.Pump();

        // Assert — the record presented is a Resume naming exactly the secret this end's far end
        // issued at the original Hello, not a fresh Hello.
        byte[] record = redialed.Receive(StreamSessionTestRecords.HandshakeRecordSize);
        record[0].Should().Be(StreamSessionTestRecords.ResumeRecordType);
        record.AsSpan(1, StreamSessionTestRecords.ResumeSecretLength).ToArray().Should().Equal(resumeSecret);

        // Assert — never closed by any of this: the whole point of a re-dial is that it never touches
        // ClosePeer on the way to a successful hand-off.
        dialer.DrainToList().Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);

        // Act — mid-schedule success stops the schedule: advance well past several further delays and
        // pump repeatedly. No third connection ever arrives.
        for (int i = 0; i < 5; i++)
        {
            dialer.TimeProvider.Advance(TimeSpan.FromSeconds(1));
            dialer.Reactor.Pump();
        }

        listener.TryAccept(out _, timeoutMs: 200).Should().BeFalse(
            "a re-dial that already succeeded must raise no further attempts");
        redialed.Dispose();
    }

    // =========================================================================================
    // A re-dial attempt that fails — nothing listening at the endpoint any more — never closes the
    // Peer, which stays Interrupted until Linger
    // =========================================================================================

    [Fact]
    public void Pump_ReDialAttemptThatFails_LeavesThePeerInterruptedRatherThanClosingIt()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new();
        IPEndPoint originalEndpoint;
        PeerId peer;
        RawStreamListener.Accepted far;
        using (RawStreamListener listener = new())
        {
            byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 5);
            (peer, far) = EstablishDialledSession(dialer, listener, resumeSecret);
            originalEndpoint = listener.Endpoint;
            InterruptDialledPeer(dialer, far, peer);
            far.Dispose();
        }

        // Act — the listener is gone (disposed, so nothing answers that port), so the first attempt is
        // waited out until it actually resolves as refused rather than merely left pending: a Peer that
        // is still connecting proves nothing about what a *failed* attempt does to it.
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        dialer.TimeProvider.Advance(_cadence);
        dialer.Reactor.Pump();
        drained.AddRange(dialer.DrainToList());
        PumpUntilAnotherRedialFailure(dialer, alreadyFailed: 0);
        drained.AddRange(dialer.DrainToList());

        // Assert — the attempt genuinely failed, and the Peer is still there afterwards: Interrupted,
        // not closed, with the Linger it was given still unspent.
        dialer.Sink.Received.Should().Contain(r => r.Id == DiagnosticId.RedialFailed,
            "the attempt must have resolved as refused for this test to be about a failed attempt at all");
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        Action stillReadable = () => dialer.Reactor.CountersOf(peer);
        stillReadable.Should().NotThrow("a failed re-dial attempt leaves the Peer Interrupted, not closed");
        _ = originalEndpoint;
    }

    // =========================================================================================
    // AC — re-dialling stops at the effective Linger: the Peer closes with LingerExpired, never an
    // attempt-count reason
    // =========================================================================================

    [Fact]
    public void Pump_ReDiallingPastTheEffectiveLinger_ClosesWithLingerExpiredNotAnAttemptCountReason()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new();
        PeerId peer;
        RawStreamListener.Accepted far;
        using (RawStreamListener listener = new())
        {
            byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 7);
            (peer, far) = EstablishDialledSession(dialer, listener, resumeSecret);
            InterruptDialledPeer(dialer, far, peer);
            far.Dispose();
        }

        // Act — jump straight past the effective Linger (1 second, the harness's fixed value) in one
        // leap: EnforceIdleTimers' Linger check runs ahead of ProcessRedials in the same Pump, so this
        // must close the Peer this same call rather than let a redial due at the same instant win.
        dialer.TimeProvider.Advance(_harnessLinger + TimeSpan.FromMilliseconds(1));
        dialer.Reactor.Pump();

        // Assert
        dialer.DrainToList().Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerClosed && e.Peer == peer)
            .Which.Reason.Should().Be(DisconnectReason.LingerExpired);
    }

    // =========================================================================================
    // No re-dial from the listening side: it has no address to dial, and Linger alone governs it
    // =========================================================================================

    [Fact]
    public void Pump_InterruptedListeningSidePeer_NeverReDialsAndOnlyLingerClosesIt()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained =
            accepter.InterruptHeldPeer(held[0]);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);

        // Act — advance generously, in several steps well short of Linger, well past where several
        // re-dial attempts would have landed had this side (wrongly) scheduled any.
        for (int i = 0; i < 4; i++)
        {
            accepter.TimeProvider.Advance(TimeSpan.FromMilliseconds(200));
            accepter.Reactor.Pump();
            accepter.DrainToList().Should().BeEmpty(
                "a listening-side Interrupted Peer has no address to re-dial and nothing to report before Linger");
        }

        // Act — the plain Linger deadline, nothing earlier and nothing later.
        accepter.TimeProvider.Advance(_harnessLinger - TimeSpan.FromMilliseconds(800) + TimeSpan.FromMilliseconds(1));
        accepter.Reactor.Pump();

        // Assert
        accepter.DrainToList().Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerClosed && e.Peer == held[0].Peer)
            .Which.Reason.Should().Be(DisconnectReason.LingerExpired);
    }

    // =========================================================================================
    // No re-dial after a local Goodbye: LocalGoodbyeSent is checked even when the append itself was
    // deferred (retention exhausted) and never actually reached PendingCloseReasonAfterRetentionDrains
    // =========================================================================================

    [Fact]
    public void Pump_TransportLostAfterCloseWithADeferredGoodbyeAppend_NeverReDials()
    {
        // Arrange — retention sized to hold exactly one maximum Message and nothing more, so Close's own
        // Goodbye append finds RetentionOutcome.Exhausted and LocalGoodbyeSent is set well before
        // PendingCloseReasonAfterRetentionDrains ever is (mirrors StreamSessionGoodbyeTests' own
        // deferred-append scenario, on the dialling side this time).
        const int maxMessageSize = 16;
        const int retentionBytes = maxMessageSize + 4;
        using StreamSessionReactorHarness dialer = new(maxMessageSize: maxMessageSize, retentionBytes: retentionBytes);
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 1);
        (PeerId peer, RawStreamListener.Accepted far) = EstablishDialledSession(dialer, listener, resumeSecret);

        byte[] message = StreamSessionTestRecords.PayloadOf(maxMessageSize, 4);
        dialer.Reactor.Send(peer, message).Should().Be(SendOutcome.Sent);
        dialer.Reactor.Close(peer);
        dialer.Reactor.Pump();
        far.Receive(StreamSessionTestRecords.BuildFrame(message).Length);
        dialer.DrainToList().Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed,
            "the Goodbye append is exhausted and deferred, not yet committed");

        // Act — the transport is lost before the deferred Goodbye ever lands: LocalGoodbyeSent is true,
        // PendingCloseReasonAfterRetentionDrains is still null, and InterruptPeer — not ClosePeer —
        // is what runs.
        far.ResetAbruptly();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerInterrupted); i++)
        {
            dialer.TimeProvider.Advance(_cadence);
            dialer.Reactor.Pump();
            drained.AddRange(dialer.DrainToList());
        }

        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == peer);

        // Advance well past where a re-dial would have fired had one (wrongly) been scheduled.
        for (int i = 0; i < 4; i++)
        {
            dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(200));
            dialer.Reactor.Pump();
        }

        listener.TryAccept(out _, timeoutMs: 200).Should().BeFalse(
            "a Peer this end already said Goodbye to must never be re-dialled, even though the append itself was still deferred");
    }

    // =========================================================================================
    // RedialAttempted (306) and RedialFailed (307). Every delay assertion
    // below is a bound — [0, 2ⁿ × the observed cadence] — never an exact figure, except the one test
    // that substitutes a deterministic jitter source (ADR 0011): a test asserting an exact jittered
    // delay would pass locally and flake in CI. RedialSchedule's own doubling and ceiling arithmetic,
    // in isolation, already has its exact-sequence and far-attempts-never-exceed-Linger coverage in
    // RedialScheduleTests.cs (issue #111); what these tests add is that the Reactor reports what that
    // schedule actually drew.
    //
    // A genuinely refused loopback connect resolves on this platform's own connect-refused timer, not
    // on the fake clock — real seconds regardless of how far TimeProvider is advanced, since it is the
    // OS deciding nobody answered, not anything this schedule owns. PumpUntilAnotherRedialFailure below
    // is a bounded real-time spin for exactly that resolution, the same category of wait
    // TryAccept/TryReceiveWithinTimeout already use elsewhere in this file for genuine socket
    // readiness; every schedule-timing assertion still advances only TimeProvider.
    // =========================================================================================

    /// <summary>
    /// A jitter source that returns the same byte for every draw (mirrors <c>RedialScheduleTests</c>'
    /// own), used here for the one test allowed to assert an exact delay: <c>0xFF</c> draws the top of
    /// every range, so each attempt's delay equals the schedule's computed delay before jitter, and the
    /// doubling itself is what the sequence pins.
    /// </summary>
    private sealed class ConstantByteJitter(byte value) : RandomNumberGenerator
    {
        public override void GetBytes(byte[] data) => GetBytes(data.AsSpan());

        public override void GetBytes(Span<byte> data) => data.Fill(value);
    }

    /// <summary>
    /// Spins Pump calls, with no further TimeProvider advance, until one more
    /// <see cref="DiagnosticId.RedialFailed"/> has been logged beyond <paramref name="alreadyFailed"/>
    /// or <paramref name="timeoutMs"/> of real wall-clock time has passed. Bounded by a
    /// <see cref="Stopwatch"/> rather than a Pump count: a refused loopback connect on this platform
    /// resolves on the OS's own timer, independent of how fast this loop can call Pump.
    /// </summary>
    private static void PumpUntilAnotherRedialFailure(StreamSessionReactorHarness dialer, int alreadyFailed, int timeoutMs = 5000)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();
        while (dialer.Sink.Received.Count(r => r.Id == DiagnosticId.RedialFailed) <= alreadyFailed
            && stopwatch.ElapsedMilliseconds < timeoutMs)
        {
            dialer.Reactor.Pump();
        }
    }

    [Fact]
    public void Pump_AReDialAttempt_LogsRedialAttemptedAtInfoNamingThePeerItBelongsTo()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new();
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 23);
        (PeerId peer, RawStreamListener.Accepted far) = EstablishDialledSession(dialer, listener, resumeSecret);
        InterruptDialledPeer(dialer, far, peer);
        far.Dispose();
        dialer.Sink.Received.Clear();

        // Act
        dialer.TimeProvider.Advance(_cadence);
        dialer.Reactor.Pump();

        // Assert — a re-dial is the ordinary behaviour of a dialling Peer that lost its transport, not
        // a condition a consumer needs to act on. Value0 is 1: this is the first attempt since the
        // schedule started, and Value1 lies within the bound the first delay can never exceed.
        DiagnosticRecord record = dialer.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.RedialAttempted).Subject;
        record.Level.Should().Be(DiagnosticLevel.Info);
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Peer);
        record.Peer.Should().Be(peer);
        record.Value0.Should().Be(1);
        record.Value1.Should().BeInRange(0, (long)_cadence.TotalMilliseconds);
    }

    [Fact]
    public void Pump_ASecondReDialAttemptAfterTheFirstFails_LogsRedialAttemptedWithARisingAttemptNumberAndABoundedDelay()
    {
        // Arrange — real entropy (the harness default), so the jitter is genuinely non-deterministic and
        // only a bound may be asserted on the delay. The listener stays disposed, so the first attempt
        // fails and a second is due.
        using StreamSessionReactorHarness dialer = new();
        PeerId peer;
        RawStreamListener.Accepted far;
        using (RawStreamListener listener = new())
        {
            byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 25);
            (peer, far) = EstablishDialledSession(dialer, listener, resumeSecret);
            InterruptDialledPeer(dialer, far, peer);
            far.Dispose();
        }

        dialer.Sink.Received.Clear();

        // Act — the first attempt begins, is waited out for a real refusal, and then the second attempt
        // is due once its own delay elapses on the fake clock.
        dialer.TimeProvider.Advance(_cadence);
        dialer.Reactor.Pump();
        PumpUntilAnotherRedialFailure(dialer, alreadyFailed: 0);
        dialer.TimeProvider.Advance(_cadence * 2);
        dialer.Reactor.Pump();

        // Assert — attempt 2, and its delay never exceeds twice the observed cadence — the bound the
        // schedule's doubling can never cross for the second draw.
        List<DiagnosticRecord> attempts = [.. dialer.Sink.Received.Where(r => r.Id == DiagnosticId.RedialAttempted)];
        attempts.Select(r => r.Value0).Should().Equal(1, 2);
        attempts[1].Value1.Should().BeInRange(0, (long)(_cadence.TotalMilliseconds * 2));
    }

    [Fact]
    public void Pump_ReDialDelay_IsHeldToTheEffectiveLingerRatherThanTheObservedCadence()
    {
        // Arrange — a Linger shorter than the cadence this loop pumps at, so the ceiling bites on the
        // very first computed delay, which is the only place it can be observed through a diagnostic:
        // the ceiling *is* the effective Linger, so a Peer is always closed by Linger expiry before a
        // doubling schedule could ever climb to it. (#111's
        // NextDelay_AskedForFarMoreAttemptsThanTheCeilingAllows_NeverExceedsTheEffectiveLinger pins the
        // many-attempt case on the schedule itself, where no Peer lifetime bounds it.) The jitter is
        // deterministic so the comparison is exact rather than probabilistic: 0xC0 draws ~0.753 of any
        // range, so the ceilinged 20ms yields ~15ms, which fits inside the 20ms Linger and fires — while
        // the uncapped 50ms cadence would have yielded ~37ms, past the Linger, and no attempt could ever
        // have been made at all.
        TimeSpan linger = TimeSpan.FromMilliseconds(20);
        using StreamSessionReactorHarness dialer = new(entropy: new ConstantByteJitter(0xC0), linger: linger);
        PeerId peer;
        RawStreamListener.Accepted far;
        using (RawStreamListener listener = new())
        {
            byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 27);
            (peer, far) = EstablishDialledSession(dialer, listener, resumeSecret);
            InterruptDialledPeer(dialer, far, peer);
            far.Dispose();
        }

        dialer.Sink.Received.Clear();

        // Act — 16ms is past the ceilinged delay and short of the Linger, so the attempt is due and the
        // Peer is still Interrupted when ProcessRedials runs.
        dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(16));
        dialer.Reactor.Pump();

        // Assert — an attempt was made, and its delay is inside the Linger rather than the cadence the
        // schedule would have used unceilinged.
        DiagnosticRecord attempt = dialer.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.RedialAttempted).Subject;
        attempt.Value1.Should().BeLessThanOrEqualTo((long)linger.TotalMilliseconds);
        attempt.Value1.Should().BeLessThan((long)_cadence.TotalMilliseconds);
        dialer.DrainToList().Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    [Fact]
    public void Pump_ReDialAttemptThatFails_LogsRedialFailedCarryingThePlatformErrorTheAttemptNumberAtInfoNamingThePeer()
    {
        // Arrange — nothing listens at the endpoint any more, so the one attempt made must fail.
        using StreamSessionReactorHarness dialer = new();
        PeerId peer;
        RawStreamListener.Accepted far;
        using (RawStreamListener listener = new())
        {
            byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 29);
            (peer, far) = EstablishDialledSession(dialer, listener, resumeSecret);
            InterruptDialledPeer(dialer, far, peer);
            far.Dispose();
        }

        dialer.Sink.Received.Clear();

        // Act
        dialer.TimeProvider.Advance(_cadence);
        dialer.Reactor.Pump();
        PumpUntilAnotherRedialFailure(dialer, alreadyFailed: 0);

        // Assert — the platform error is a real failure code, not the zero-valued Success a connect
        // that actually worked would leave behind; the attempt number is the one that failed; Info, not
        // Warn, because under this profile's premise a failed reconnection attempt is an ordinary,
        // expected outcome, and raising it to Warn would train a consumer to ignore Warn.
        DiagnosticRecord record = dialer.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.RedialFailed).Subject;
        ((SocketError)record.Value0).Should().NotBe(SocketError.Success);
        record.Value1.Should().Be(1);
        record.Level.Should().Be(DiagnosticLevel.Info);
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Peer);
        record.Peer.Should().Be(peer);

        // Assert — never closes: the Peer stays Interrupted and simply waits out the schedule's next
        // delay.
        dialer.DrainToList().Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    [Fact]
    public void Pump_ReDialAttempts_UnderADeterministicJitterSource_PinTheExactDoublingSequence()
    {
        // Arrange — MaximumJitter (0xFF) draws the top of every range, so each attempt's reported delay
        // equals the schedule's computed delay exactly rather than something jittered below it. This is
        // the one test in this file allowed to assert an exact delay (ADR 0011), because the jitter
        // itself is no longer non-deterministic. The endpoint stays unreachable throughout, so the first
        // two attempts fail and the schedule doubles: 50ms, then 100ms, from the 50ms cadence this Pump
        // budget observes.
        using StreamSessionReactorHarness dialer = new(entropy: new ConstantByteJitter(0xFF));
        PeerId peer;
        RawStreamListener.Accepted far;
        using (RawStreamListener listener = new())
        {
            byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 33);
            (peer, far) = EstablishDialledSession(dialer, listener, resumeSecret);
            InterruptDialledPeer(dialer, far, peer);
            far.Dispose();
        }

        dialer.Sink.Received.Clear();

        // Act — attempt 1 begins, is waited out for a real refusal, then attempt 2 begins once its own
        // delay elapses on the fake clock, and is waited out too so attempt 3 can begin.
        dialer.TimeProvider.Advance(_cadence);
        dialer.Reactor.Pump();
        PumpUntilAnotherRedialFailure(dialer, alreadyFailed: 0);
        dialer.TimeProvider.Advance(_cadence * 2);
        dialer.Reactor.Pump();
        PumpUntilAnotherRedialFailure(dialer, alreadyFailed: 1);
        dialer.TimeProvider.Advance(_cadence * 4);
        dialer.Reactor.Pump();

        // Assert — the observed 50ms cadence doubling twice, exactly: 50ms, 100ms, 200ms.
        dialer.Sink.Received.Where(r => r.Id == DiagnosticId.RedialAttempted)
            .Select(r => r.Value1)
            .Should().Equal(50, 100, 200);
    }

    // =========================================================================================
    // A re-dial the listener closes before confirming is a failed attempt, and Linger still closes
    // the Peer
    // =========================================================================================

    /// <summary>
    /// Pumps <paramref name="dialer"/> and drains its Events into <paramref name="collected"/> after
    /// every Pump, until <paramref name="done"/> answers true or <paramref name="timeoutMs"/> of real
    /// wall-clock time has passed. Never advances the clock.
    /// </summary>
    private static void PumpCollectingUntil(
        StreamSessionReactorHarness dialer,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> collected,
        Func<bool> done,
        int timeoutMs = 5000)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();
        while (!done() && stopwatch.ElapsedMilliseconds < timeoutMs)
        {
            dialer.Reactor.Pump();
            collected.AddRange(dialer.DrainToList());
        }
    }

    [Fact]
    public void Pump_ReDialsTheListenerClosesBeforeConfirming_ContinueTheScheduleWithoutANewInterruption()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new(entropy: new ConstantByteJitter(0xFF));
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 41);
        (PeerId peer, RawStreamListener.Accepted far) = EstablishDialledSession(dialer, listener, resumeSecret);
        using IDisposable farGuard = far;
        InterruptDialledPeer(dialer, far, peer);
        far.Dispose();
        dialer.Sink.Received.Clear();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> collected = [];

        // Act — three attempts connect, send their Resume, and are closed by the listener without a reply.
        int[] delays = [50, 100, 200];
        for (int n = 1; n <= delays.Length; n++)
        {
            dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(delays[n - 1]));
            dialer.Reactor.Pump();
            collected.AddRange(dialer.DrainToList());
            listener.TryAccept(out RawStreamListener.Accepted redialed, timeoutMs: 2000).Should().BeTrue();
            using (redialed)
            {
                bool resumeArrived = false;
                PumpCollectingUntil(dialer, collected, () =>
                    resumeArrived = redialed.TryReceiveWithinTimeout(StreamSessionTestRecords.HandshakeRecordSize, out _, timeoutMs: 20));
                resumeArrived.Should().BeTrue("the re-dial must send its Resume before the listener closes it");
            }

            int expectedFailures = n;
            PumpCollectingUntil(dialer, collected, () =>
                dialer.Sink.Received.Count(r => r.Id == DiagnosticId.RedialFailed) >= expectedFailures);
        }

        // Assert
        collected.Should().NotContain(e => e.Kind == SessionEventKind.PeerInterrupted);
        collected.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        List<DiagnosticRecord> attempts = [.. dialer.Sink.Received.Where(r => r.Id == DiagnosticId.RedialAttempted)];
        attempts.Select(r => r.Value0).Should().Equal(1, 2, 3);
        attempts.Select(r => r.Value1).Should().Equal(50, 100, 200);
        dialer.Sink.Received.Where(r => r.Id == DiagnosticId.RedialFailed)
            .Select(r => r.Value1).Should().Equal(1, 2, 3);
        dialer.Sink.Received.Where(r => r.Id == DiagnosticId.RedialFailed)
            .Select(r => (SocketError)r.Value0).Should().OnlyContain(e => e == SocketError.Disconnecting);
    }

    [Fact]
    public void Pump_ReDialsTheListenerResetsBeforeConfirming_ContinueTheScheduleWithoutANewInterruption()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new(entropy: new ConstantByteJitter(0xFF));
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 59);
        (PeerId peer, RawStreamListener.Accepted far) = EstablishDialledSession(dialer, listener, resumeSecret);
        using IDisposable farGuard = far;
        InterruptDialledPeer(dialer, far, peer);
        far.Dispose();
        dialer.Sink.Received.Clear();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> collected = [];

        // Act: three attempts connect and send their Resume, and the listener resets each one, so the read fails.
        int[] delays = [50, 100, 200];
        for (int n = 1; n <= delays.Length; n++)
        {
            dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(delays[n - 1]));
            dialer.Reactor.Pump();
            collected.AddRange(dialer.DrainToList());
            listener.TryAccept(out RawStreamListener.Accepted redialed, timeoutMs: 2000).Should().BeTrue();
            using (redialed)
            {
                bool resumeArrived = false;
                PumpCollectingUntil(dialer, collected, () =>
                    resumeArrived = redialed.TryReceiveWithinTimeout(StreamSessionTestRecords.HandshakeRecordSize, out _, timeoutMs: 20));
                resumeArrived.Should().BeTrue("the re-dial must send its Resume before the listener resets it");
                redialed.ResetAbruptly();
            }

            int expectedFailures = n;
            PumpCollectingUntil(dialer, collected, () =>
                dialer.Sink.Received.Count(r => r.Id == DiagnosticId.RedialFailed) >= expectedFailures);
        }

        // Assert
        collected.Should().NotContain(e => e.Kind == SessionEventKind.PeerInterrupted);
        collected.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        List<DiagnosticRecord> attempts = [.. dialer.Sink.Received.Where(r => r.Id == DiagnosticId.RedialAttempted)];
        attempts.Select(r => r.Value0).Should().Equal(1, 2, 3);
        attempts.Select(r => r.Value1).Should().Equal(50, 100, 200);
        dialer.Sink.Received.Where(r => r.Id == DiagnosticId.RedialFailed)
            .Select(r => r.Value1).Should().Equal(1, 2, 3);
        dialer.Sink.Received.Where(r => r.Id == DiagnosticId.RedialFailed)
            .Select(r => (SocketError)r.Value0).Should().OnlyContain(e => e == SocketError.ConnectionReset);
    }

    [WindowsFact("Linux reports a reset that arrives before the connect is polled through SO_ERROR, so the "
        + "attempt fails as a connect and the Resume is never sent.")]
    public void Pump_ReDialResetBeforeItsResumeIsSent_FailsTheAttemptRatherThanClosingThePeer()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new(entropy: new ConstantByteJitter(0xFF));
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 61);
        (PeerId peer, RawStreamListener.Accepted far) = EstablishDialledSession(dialer, listener, resumeSecret);
        using IDisposable farGuard = far;
        InterruptDialledPeer(dialer, far, peer);
        far.Dispose();
        dialer.Sink.Received.Clear();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> collected = [];

        // Act: the attempt is dialled, and the listener accepts and resets it before the Pump that resolves
        // the connect, so that Pump's Resume send fails.
        dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(50));
        dialer.Reactor.Pump();
        collected.AddRange(dialer.DrainToList());
        listener.TryAccept(out RawStreamListener.Accepted reset, timeoutMs: 2000).Should().BeTrue();
        reset.ResetAbruptly();
        Thread.Sleep(50);
        dialer.Reactor.Pump();
        collected.AddRange(dialer.DrainToList());
        dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(100));
        dialer.Reactor.Pump();
        collected.AddRange(dialer.DrainToList());
        bool secondAccepted = listener.TryAccept(out RawStreamListener.Accepted second, timeoutMs: 2000);
        using IDisposable secondGuard = second;

        // Assert
        collected.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        collected.Should().NotContain(e => e.Kind == SessionEventKind.PeerInterrupted);
        DiagnosticRecord failed = dialer.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.RedialFailed).Subject;
        ((SocketError)failed.Value0).Should().Be(SocketError.SocketError, "the Resume send failed, not the connect");
        failed.Value1.Should().Be(1);
        secondAccepted.Should().BeTrue();
        dialer.Sink.Received.Last(r => r.Id == DiagnosticId.RedialAttempted).Value0.Should().Be(2);
    }

    [Fact]
    public void Pump_ReDialTheListenerNeverAnswers_FailsAtTheReceiveIdleDeadlineAndDialsAgain()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new(entropy: new ConstantByteJitter(0xFF), linger: TimeSpan.FromSeconds(10));
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 47);
        (PeerId peer, RawStreamListener.Accepted far) = EstablishDialledSession(dialer, listener, resumeSecret, lingerMs: 10_000);
        using IDisposable farGuard = far;
        InterruptDialledPeer(dialer, far, peer);
        far.Dispose();
        dialer.Sink.Received.Clear();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> collected = [];

        // Act — the first attempt connects and sends its Resume, then the listener stays silent.
        dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(50));
        dialer.Reactor.Pump();
        collected.AddRange(dialer.DrainToList());
        listener.TryAccept(out RawStreamListener.Accepted silent, timeoutMs: 2000).Should().BeTrue();
        using IDisposable silentGuard = silent;
        bool resumeArrived = false;
        PumpCollectingUntil(dialer, collected, () =>
            resumeArrived = silent.TryReceiveWithinTimeout(StreamSessionTestRecords.HandshakeRecordSize, out _, timeoutMs: 20));
        resumeArrived.Should().BeTrue();
        dialer.TimeProvider.Advance(TimeSpan.FromSeconds(3) - TimeSpan.FromMilliseconds(10));
        dialer.Reactor.Pump();
        collected.AddRange(dialer.DrainToList());
        bool failedBeforeDeadline = dialer.Sink.Received.Any(r => r.Id == DiagnosticId.RedialFailed);
        dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(10));
        dialer.Reactor.Pump();
        collected.AddRange(dialer.DrainToList());
        dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(100));
        dialer.Reactor.Pump();
        collected.AddRange(dialer.DrainToList());
        bool secondAccepted = listener.TryAccept(out RawStreamListener.Accepted second, timeoutMs: 2000);
        using IDisposable secondGuard = second;

        // Assert
        failedBeforeDeadline.Should().BeFalse("the attempt is not failed before the receive-idle deadline");
        DiagnosticRecord failed = dialer.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.RedialFailed).Subject;
        ((SocketError)failed.Value0).Should().Be(SocketError.TimedOut);
        failed.Value1.Should().Be(1);
        secondAccepted.Should().BeTrue();
        collected.Should().NotContain(e => e.Kind == SessionEventKind.PeerInterrupted);
        collected.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        dialer.Sink.Received.Last(r => r.Id == DiagnosticId.RedialAttempted).Value0.Should().Be(2);
    }

    [Fact]
    public void Pump_LingerAndTheReceiveIdleDeadlinePassInOnePump_ClosesWithLingerExpiredRatherThanFailingTheAttempt()
    {
        // Arrange: the raw Hello announces a 1 s Linger and a 1 s send-idle interval, so an unconfirmed attempt's
        // receive-idle deadline is 3 s after it connects and Linger runs out 1 s after the interruption.
        using StreamSessionReactorHarness dialer = new(entropy: new ConstantByteJitter(0xFF));
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 67);
        (PeerId peer, RawStreamListener.Accepted far) = EstablishDialledSession(dialer, listener, resumeSecret);
        using IDisposable farGuard = far;
        InterruptDialledPeer(dialer, far, peer);
        far.Dispose();
        dialer.Sink.Received.Clear();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> collected = [];
        dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(50));
        dialer.Reactor.Pump();
        collected.AddRange(dialer.DrainToList());
        listener.TryAccept(out RawStreamListener.Accepted silent, timeoutMs: 2000).Should().BeTrue();
        using IDisposable silentGuard = silent;
        bool resumeArrived = false;
        PumpCollectingUntil(dialer, collected, () =>
            resumeArrived = silent.TryReceiveWithinTimeout(StreamSessionTestRecords.HandshakeRecordSize, out _, timeoutMs: 20));
        resumeArrived.Should().BeTrue();
        collected.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);

        // Act: one advance passes both deadlines, so a single Pump sees both.
        dialer.TimeProvider.Advance(TimeSpan.FromSeconds(3) + TimeSpan.FromMilliseconds(10));
        dialer.Reactor.Pump();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> deadlinePump = dialer.DrainToList();

        // Assert
        deadlinePump.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.LingerExpired);
        dialer.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.RedialFailed);
    }

    [Fact]
    public void Pump_ReDialAwaitingTheListenersCounter_IsNotDialledAgainBeforeItFails()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new(entropy: new ConstantByteJitter(0xFF), linger: TimeSpan.FromSeconds(10));
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 53);
        (PeerId peer, RawStreamListener.Accepted far) = EstablishDialledSession(dialer, listener, resumeSecret, lingerMs: 10_000);
        using IDisposable farGuard = far;
        InterruptDialledPeer(dialer, far, peer);
        far.Dispose();
        dialer.Sink.Received.Clear();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> collected = [];
        dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(50));
        dialer.Reactor.Pump();
        collected.AddRange(dialer.DrainToList());
        listener.TryAccept(out RawStreamListener.Accepted silent, timeoutMs: 2000).Should().BeTrue();
        using IDisposable silentGuard = silent;
        bool resumeArrived = false;
        PumpCollectingUntil(dialer, collected, () =>
            resumeArrived = silent.TryReceiveWithinTimeout(StreamSessionTestRecords.HandshakeRecordSize, out _, timeoutMs: 20));
        resumeArrived.Should().BeTrue();

        // Act — the listener stays silent while several re-dial delays pass, all below the receive-idle deadline.
        for (int i = 0; i < 5; i++)
        {
            dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(100));
            dialer.Reactor.Pump();
            collected.AddRange(dialer.DrainToList());
        }

        // Assert
        listener.TryAccept(out _, timeoutMs: 200).Should().BeFalse(
            "an attempt awaiting the listener's counter must not be dialled again before it fails");
        dialer.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.RedialAttempted);
        dialer.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.RedialFailed);
        collected.Should().NotContain(e => e.Kind == SessionEventKind.PeerInterrupted);
        collected.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    [Fact]
    public void Pump_ReDialsTheListenerKeepsClosing_CloseWithLingerExpiredAtTheOriginalDeadline()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new(entropy: new ConstantByteJitter(0xFF));
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 43);
        (PeerId peer, RawStreamListener.Accepted far) = EstablishDialledSession(dialer, listener, resumeSecret);
        using IDisposable farGuard = far;
        InterruptDialledPeer(dialer, far, peer);
        far.Dispose();
        DateTimeOffset interruptedAt = dialer.TimeProvider.GetUtcNow();
        dialer.Sink.Received.Clear();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> collected = [];
        DateTimeOffset? closedAt = null;

        // Act
        for (int i = 0; i < 40 && closedAt is null; i++)
        {
            dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(50));
            dialer.Reactor.Pump();
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = dialer.DrainToList();
            collected.AddRange(drained);
            if (drained.Any(e => e.Kind == SessionEventKind.PeerClosed))
            {
                closedAt = dialer.TimeProvider.GetUtcNow();
            }

            while (listener.TryAccept(out RawStreamListener.Accepted c, timeoutMs: 0))
            {
                c.Dispose();
            }
        }

        // Assert
        collected.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.LingerExpired);
        closedAt.Should().NotBeNull();
        TimeSpan elapsed = (closedAt ?? interruptedAt) - interruptedAt;
        elapsed.Should().BeGreaterThanOrEqualTo(_harnessLinger).And.BeLessThanOrEqualTo(_harnessLinger + TimeSpan.FromMilliseconds(50));
        collected.Should().NotContain(e => e.Kind == SessionEventKind.PeerInterrupted);
        long[] attemptNumbers = [.. dialer.Sink.Received.Where(r => r.Id == DiagnosticId.RedialAttempted).Select(r => r.Value0)];
        attemptNumbers.Should().NotBeEmpty();
        attemptNumbers.Should().Equal(Enumerable.Range(1, attemptNumbers.Length).Select(x => (long)x));
    }

    [Fact]
    public void Pump_ListenerReplacedByAFreshReactorOnTheSameEndpoint_DiallerClosesWithLingerExpiredOnce()
    {
        // Arrange
        FakeTimeProvider clock = new();
        using StreamSessionReactorHarness dialer = new(entropy: new ConstantByteJitter(0xFF), timeProvider: clock);
        StreamSessionReactorHarness first = new(timeProvider: clock);
        using IDisposable firstGuard = first;
        PeerId peer = dialer.Reactor.Connect(first.Endpoint);
        bool dialerConnected = false;
        bool firstConnected = false;
        for (int i = 0; i < 50 && !(dialerConnected && firstConnected); i++)
        {
            clock.Advance(TimeSpan.FromMilliseconds(10));
            dialer.Reactor.Pump();
            dialerConnected |= dialer.DrainToList().Any(e => e.Kind == SessionEventKind.PeerConnected && e.Peer == peer);
            first.Reactor.Pump();
            firstConnected |= first.DrainToList().Any(e => e.Kind == SessionEventKind.PeerConnected);
        }

        dialerConnected.Should().BeTrue();
        firstConnected.Should().BeTrue();
        IPEndPoint endpoint = first.Endpoint;
        first.Dispose();
        using StreamSessionReactorHarness second = new(timeProvider: clock, endpoints: [endpoint]);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> dialerEvents = [];
        DateTimeOffset? interruptedAt = null;
        DateTimeOffset? closedAt = null;

        // Act
        for (int i = 0; i < 300 && closedAt is null; i++)
        {
            clock.Advance(TimeSpan.FromMilliseconds(10));
            dialer.Reactor.Pump();
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = dialer.DrainToList();
            dialerEvents.AddRange(drained);
            if (interruptedAt is null && drained.Any(e => e.Kind == SessionEventKind.PeerInterrupted))
            {
                interruptedAt = clock.GetUtcNow();
            }

            if (drained.Any(e => e.Kind == SessionEventKind.PeerClosed))
            {
                closedAt = clock.GetUtcNow();
            }

            second.Reactor.Pump();
            second.DrainToList();
        }

        // Assert
        dialerEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted);
        dialerEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.LingerExpired);
        dialerEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerResumed);
        interruptedAt.Should().NotBeNull();
        closedAt.Should().NotBeNull();
        TimeSpan elapsed = (closedAt ?? default) - (interruptedAt ?? default);
        elapsed.Should().BeGreaterThanOrEqualTo(_harnessLinger).And.BeLessThanOrEqualTo(_harnessLinger + TimeSpan.FromMilliseconds(10));
        second.Sink.Received.Should().Contain(r => r.Id == DiagnosticId.ResumeUnmatched);
        long[] attemptNumbers = [.. dialer.Sink.Received.Where(r => r.Id == DiagnosticId.RedialAttempted).Select(r => r.Value0)];
        attemptNumbers.Should().NotBeEmpty();
        attemptNumbers.Should().Equal(Enumerable.Range(1, attemptNumbers.Length).Select(x => (long)x));
    }
}

/// <summary>
/// A plain BCL <see cref="TcpListener"/>, standing in for the far end of a Session this Reactor
/// dialled — the mirror image of every other stream Session test file's raw counterparty
/// (<c>RawStreamPeer</c>, <c>RawPeer</c>, <c>InterruptedPeer</c>, ...), which all connect outward to a
/// Reactor under test. Here the Reactor under test is the one dialling, so this listens instead.
/// </summary>
internal sealed class RawStreamListener : IDisposable
{
    private readonly TcpListener _listener;

    public RawStreamListener()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
    }

    /// <summary>The endpoint a Reactor under test dials to reach this listener.</summary>
    public IPEndPoint Endpoint => (IPEndPoint)_listener.LocalEndpoint;

    /// <summary>Blocks up to 5 seconds for a connection to arrive, throwing if none does.</summary>
    public Accepted Accept(int timeoutMs = 5000)
    {
        if (!TryAccept(out Accepted accepted, timeoutMs))
        {
            throw new InvalidOperationException("No connection arrived within the timeout.");
        }

        return accepted;
    }

    /// <summary>
    /// Answers whether a connection arrived within <paramref name="timeoutMs"/> rather than throwing or
    /// blocking indefinitely — used both to observe a re-dial actually happening and to prove one did
    /// not, across repeated <see cref="StreamSessionReactor.Pump"/> calls the caller drives.
    /// </summary>
    public bool TryAccept(out Accepted accepted, int timeoutMs = 2000)
    {
        if (_listener.Server.Poll(timeoutMs * 1000, SelectMode.SelectRead))
        {
            accepted = new Accepted(_listener.AcceptSocket());
            return true;
        }

        accepted = null!;
        return false;
    }

    public void Dispose() => _listener.Stop();

    /// <summary>One connection this listener accepted, standing in for the far end of a dialled Session.</summary>
    public sealed class Accepted : IDisposable
    {
        private readonly Socket _socket;

        public Accepted(Socket socket)
        {
            _socket = socket;
            _socket.ReceiveTimeout = 5000;
        }

        /// <summary>Sends every byte of <paramref name="bytes"/> in a single write.</summary>
        public void Send(byte[] bytes) => _socket.Send(bytes);

        /// <summary>Blocks until exactly <paramref name="count"/> bytes have been read off the socket.</summary>
        public byte[] Receive(int count)
        {
            byte[] buffer = new byte[count];
            int totalRead = 0;
            while (totalRead < count)
            {
                int read = _socket.Receive(buffer.AsSpan(totalRead));
                if (read == 0)
                {
                    throw new InvalidOperationException(
                        "The dialling Reactor closed the connection before sending everything expected.");
                }

                totalRead += read;
            }

            return buffer;
        }

        /// <summary>
        /// Reads <paramref name="count"/> bytes within <paramref name="timeoutMs"/>, answering whether
        /// it succeeded rather than throwing or blocking indefinitely — the mirror of
        /// <see cref="StreamSessionReactorHarness.RawStreamPeer.TryReceiveWithinTimeout"/>, needed here
        /// because this class stands in for the far end of a dialled Session (issue #127's rewind
        /// tests: proving nothing is resent before this end's own delivery-counter record answers, and
        /// that only the un-confirmed bytes are once it does).
        /// </summary>
        public bool TryReceiveWithinTimeout(int count, out byte[] record, int timeoutMs = 100)
        {
            _socket.ReceiveTimeout = timeoutMs;
            byte[] buffer = new byte[count];
            int totalRead = 0;
            try
            {
                while (totalRead < count)
                {
                    int read = _socket.Receive(buffer.AsSpan(totalRead));
                    if (read == 0)
                    {
                        record = [];
                        return false;
                    }

                    totalRead += read;
                }
            }
            catch (SocketException)
            {
                record = [];
                return false;
            }
            finally
            {
                _socket.ReceiveTimeout = 5000;
            }

            record = buffer;
            return true;
        }

        /// <summary>
        /// Kills the connection abruptly: <c>SO_LINGER</c> set to zero, then closed, which forces the OS
        /// to send a reset instead of an ordinary FIN — what interrupts the dialling Reactor's Peer.
        /// </summary>
        public void ResetAbruptly()
        {
            _socket.LingerState = new LingerOption(true, 0);
            _socket.Close();
        }

        public void Dispose() => _socket.Dispose();
    }
}
