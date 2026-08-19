using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #131 (4.6.5, part of #30): the cross-Reactor proof that Linger negotiation (ADR 0011) is not
/// merely computed and reported (<c>LingerNegotiated</c>) but actually governs both ends' clocks in
/// lockstep. #121 already makes an Interrupted Peer's own <see cref="Core.PeerSlot.EffectiveLinger"/> —
/// the negotiated minimum, never either side's own configured value in isolation — what closes it; #122
/// already ties a listening-side capacity reservation to that same Peer's lifetime; #125 already makes a
/// dialling-side Peer re-dial on the same schedule, ceilinged at the same figure. Every test here builds
/// one real <see cref="StreamSessionReactor"/> whose own configured <c>Linger</c> deliberately
/// disagrees with a raw counterparty's announced one (tests/CLAUDE.md, ADR 0015 — the seam is a real
/// Reactor against a hand-built wire counterparty, never two managed Reactors wired to each other:
/// StreamSessionRedialTests' own dialling-side tests already show why — once a Peer is Interrupted, a
/// dialling Reactor re-dials on its own, and if the far end were a second live, still-reachable Reactor
/// that redial would genuinely reconnect and resume well inside the boundary a test needs to hold open;
/// a raw counterparty lets a test control exactly when, if ever, a reconnection succeeds). What a raw
/// counterparty's own announced <c>lingerMs</c> stands in for is "the other real Reactor's own configured
/// value" — negotiation itself only ever consults the two announced figures (ADR 0011), never which side
/// is managed by the code under test, so proving the Reactor under test acts on <c>min(own, announced)</c>
/// in both roles (listening and dialling) is the whole of what "two ends act on it in lockstep" reduces
/// to at this seam.
/// </summary>
/// <remarks>
/// The one test that needs "the same instant" to be a literal, not merely a repeated, assertion
/// (<see cref="Pump_BothRolesInterrupted_HoldTheInterruptedPeerJustInsideTheNegotiatedMinimumLingerAndCloseJustOutsideItAtTheSameInstant"/>)
/// shares a single <see cref="FakeTimeProvider"/> across two harnesses, one per role, via
/// <see cref="StreamSessionReactorHarness"/>'s optional <c>timeProvider</c> constructor parameter (added
/// for this issue) — every other test here uses its own harness's default clock, since it makes no
/// cross-harness claim.
/// </remarks>
public class StreamSessionNegotiatedLingerTests
{
    private const byte RefusalReasonCapacity = 2;

    // =========================================================================================
    // AC1-3 — both roles hold an Interrupted Peer just inside the negotiated minimum and have
    // closed it just outside, on one shared clock, whichever side the shorter value belongs to
    // =========================================================================================

    [Fact]
    public void Pump_BothRolesInterrupted_HoldTheInterruptedPeerJustInsideTheNegotiatedMinimumLingerAndCloseJustOutsideItAtTheSameInstant()
    {
        // Arrange — one clock read by both harnesses below, so every advance is literally the same
        // instant on both ends' reckoning, not merely two harnesses given equal TimeSpan values.
        FakeTimeProvider clock = new();
        TimeSpan shorter = TimeSpan.FromMilliseconds(500);
        TimeSpan longer = TimeSpan.FromMilliseconds(2000);

        // Listening role — this Reactor's own configured Linger is the longer figure; the raw
        // counterparty standing in for the dialling side announces the shorter one, so the negotiated
        // minimum is the far side's value even though this Reactor's own is not.
        using StreamSessionReactorHarness listening = new(capacity: 1, linger: longer, timeProvider: clock);
        using StreamSessionReactorHarness.RawStreamPeer listeningPeer = new(listening.Endpoint);
        (PeerId listeningInterrupted, _) = EstablishAndInterruptListeningSide(
            listening, listeningPeer, farLingerMs: (ulong)shorter.TotalMilliseconds);

        // Dialling role — this Reactor's own configured Linger is the shorter figure; the raw listener
        // standing in for the listening side announces the longer one, so the negotiated minimum is
        // this Reactor's own value this time — the mirror image of the listening role above (AC3).
        using StreamSessionReactorHarness dialling = new(linger: shorter, timeProvider: clock);
        using RawStreamListener listener = new();
        (PeerId diallingInterrupted, RawStreamListener.Accepted far) = EstablishAndInterruptDiallingSide(
            dialling, listener, farLingerMs: (ulong)longer.TotalMilliseconds);
        far.Dispose(); // listener is already stopped inside the helper, ahead of any possible re-dial

        // Act — one millisecond short of the negotiated minimum, on the one clock both Reactors read.
        clock.Advance(shorter - TimeSpan.FromMilliseconds(1));
        listening.Reactor.Pump();
        dialling.Reactor.Pump();

        // Assert — both sides still hold the Peer Interrupted, even though the listening Reactor's own
        // configured Linger is four times longer and the dialling Reactor's far end announced the same
        // longer figure — neither side's own value governs here, only the negotiated minimum.
        listening.DrainToList().Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        dialling.DrainToList().Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        Action listeningStillHeld = () => listening.Reactor.CountersOf(listeningInterrupted);
        Action diallingStillHeld = () => dialling.Reactor.CountersOf(diallingInterrupted);
        listeningStillHeld.Should().NotThrow("the negotiated minimum has not yet elapsed on the listening side");
        diallingStillHeld.Should().NotThrow("the negotiated minimum has not yet elapsed on the dialling side");

        // Act — the last millisecond crosses the negotiated minimum, still on the one shared clock.
        clock.Advance(TimeSpan.FromMilliseconds(1));
        listening.Reactor.Pump();
        dialling.Reactor.Pump();

        // Assert — both close at this same instant, naming LingerExpired, neither having waited for its
        // own longer or differently-shorter configured value.
        listening.DrainToList().Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerClosed && e.Peer == listeningInterrupted)
            .Which.Reason.Should().Be(DisconnectReason.LingerExpired);
        dialling.DrainToList().Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerClosed && e.Peer == diallingInterrupted)
            .Which.Reason.Should().Be(DisconnectReason.LingerExpired);
    }

    // =========================================================================================
    // AC4 — the listening side's slot reservation (#122) ends at the same negotiated instant: a
    // fresh dial is refused for capacity just before it and admitted just after, whichever side
    // the shorter value belongs to
    // =========================================================================================

    [Theory]
    [InlineData(2000ul, 500ul)] // this Reactor's own Linger is the longer figure; the far end's is shorter
    [InlineData(500ul, 2000ul)] // this Reactor's own Linger is the shorter figure; the far end's is longer
    public void AttemptFreshConnection_AroundTheNegotiatedMinimumLingerBoundary_IsRefusedJustBeforeAndAdmittedJustAfter(
        ulong ownLingerMs, ulong farLingerMs)
    {
        // Arrange
        TimeSpan negotiated = TimeSpan.FromMilliseconds(Math.Min(ownLingerMs, farLingerMs));
        using StreamSessionReactorHarness accepter = new(capacity: 1, linger: TimeSpan.FromMilliseconds(ownLingerMs));
        using StreamSessionReactorHarness.RawStreamPeer peer = new(accepter.Endpoint);
        (PeerId interrupted, _) = EstablishAndInterruptListeningSide(accepter, peer, farLingerMs);

        // Act — one millisecond short of the negotiated minimum: the reservation still occupies the
        // only Slot.
        accepter.TimeProvider.Advance(negotiated - TimeSpan.FromMilliseconds(1));
        using (StreamSessionReactorHarness.InboundAttemptResult refused = accepter.AttemptFreshConnection())
        {
            // Assert
            refused.Admitted.Should().BeFalse();
            refused.RefusalReason.Should().Be(RefusalReasonCapacity);
        }

        // Act — the last millisecond crosses the negotiated minimum: Linger expiry closes the
        // Interrupted Peer and releases the reservation it was holding.
        accepter.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
        accepter.Reactor.Pump();
        accepter.DrainToList().Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerClosed && e.Peer == interrupted)
            .Which.Reason.Should().Be(DisconnectReason.LingerExpired);

        using (StreamSessionReactorHarness.InboundAttemptResult admitted = accepter.AttemptFreshConnection())
        {
            // Assert — admitted now, as a distinct Peer occupying the freed Slot.
            admitted.Admitted.Should().BeTrue();
            admitted.Peer.Should().NotBeNull();
            admitted.Peer.Should().NotBe(interrupted);
        }
    }

    // =========================================================================================
    // AC5 (first half) — a Resume presenting the held secret just inside the negotiated minimum
    // still resumes, whichever side the shorter value belongs to
    // =========================================================================================

    [Theory]
    [InlineData(2000ul, 500ul)]
    [InlineData(500ul, 2000ul)]
    public void Handshake_AResumePresentingTheHeldSecretJustInsideTheNegotiatedMinimumLinger_StillResumes(
        ulong ownLingerMs, ulong farLingerMs)
    {
        // Arrange
        TimeSpan negotiated = TimeSpan.FromMilliseconds(Math.Min(ownLingerMs, farLingerMs));
        using StreamSessionReactorHarness accepter = new(capacity: 1, linger: TimeSpan.FromMilliseconds(ownLingerMs));
        using StreamSessionReactorHarness.RawStreamPeer peer = new(accepter.Endpoint);
        (PeerId interrupted, byte[] secret) = EstablishAndInterruptListeningSide(accepter, peer, farLingerMs);

        // Act — one millisecond short of the negotiated minimum, a Resume naming the held secret
        // arrives.
        accepter.TimeProvider.Advance(negotiated - TimeSpan.FromMilliseconds(1));
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(secret, deliveryCount: 0));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.PumpAndDrainAll(5);

        // Assert — the same Peer resumes rather than closing, even though the boundary is defined by
        // whichever of the two announced Linger figures is smaller, not by this Reactor's own.
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerResumed && e.Peer == interrupted);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    // =========================================================================================
    // AC5 (second half) — the once-held secret arriving just outside the negotiated minimum finds
    // no reservation left: it is admitted as an ordinary new Peer, not a resume, whichever side the
    // shorter value belonged to
    // =========================================================================================

    [Theory]
    [InlineData(2000ul, 500ul)]
    [InlineData(500ul, 2000ul)]
    public void AttemptFreshConnection_AnnouncingTheOnceHeldSecretJustOutsideTheNegotiatedMinimumLinger_IsAdmittedAsANewPeerNotAResume(
        ulong ownLingerMs, ulong farLingerMs)
    {
        // Arrange
        TimeSpan negotiated = TimeSpan.FromMilliseconds(Math.Min(ownLingerMs, farLingerMs));
        using StreamSessionReactorHarness accepter = new(capacity: 1, linger: TimeSpan.FromMilliseconds(ownLingerMs));
        using StreamSessionReactorHarness.RawStreamPeer peer = new(accepter.Endpoint);
        (PeerId interrupted, byte[] secret) = EstablishAndInterruptListeningSide(accepter, peer, farLingerMs);

        // Act — the negotiated minimum elapses, closing the Interrupted Peer and releasing its
        // reservation; a fresh connection then announces the very secret that Peer once held.
        accepter.TimeProvider.Advance(negotiated);
        accepter.Reactor.Pump();
        accepter.DrainToList().Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerClosed && e.Peer == interrupted)
            .Which.Reason.Should().Be(DisconnectReason.LingerExpired);

        using StreamSessionReactorHarness.InboundAttemptResult outcome =
            accepter.AttemptFreshConnection(resumeSecret: secret);

        // Assert — admitted as an ordinary new Peer: no reservation is left for that secret to match,
        // so a Hello announcing it is treated as any other fresh dial rather than refused as a claim on
        // a Session that no longer exists (issue #124's third refusal path only fires while a
        // reservation is still held).
        outcome.Admitted.Should().BeTrue();
        outcome.Peer.Should().NotBeNull();
        outcome.Peer.Should().NotBe(interrupted);
    }

    /// <summary>
    /// Hand-rolls a Hello announcing <paramref name="farLingerMs"/> against <paramref name="accepter"/>
    /// over <paramref name="peer"/> — <see cref="StreamSessionReactorHarness.EstablishOnePeer"/> always
    /// sends the fixed default, which cannot exercise a negotiated minimum that disagrees with it — then
    /// kills <paramref name="peer"/>'s transport abruptly and pumps until <paramref name="accepter"/>
    /// drains <see cref="SessionEventKind.PeerInterrupted"/>. Returns the Interrupted Peer's id paired
    /// with the resume secret <paramref name="accepter"/> announced to it, read off the wire exactly as
    /// <see cref="StreamSessionReactorHarness.EstablishOnePeer"/> does (never out of the Reactor —
    /// tests/CLAUDE.md, ADR 0015). The listening-role half of issue #131's cross-role proof.
    /// </summary>
    private static (PeerId Peer, byte[] Secret) EstablishAndInterruptListeningSide(
        StreamSessionReactorHarness accepter, StreamSessionReactorHarness.RawStreamPeer peer, ulong farLingerMs)
    {
        peer.Send(StreamSessionTestRecords.BuildHello(lingerMs: farLingerMs));

        PeerId? connected = null;
        for (int i = 0; i < 20 && connected is null; i++)
        {
            accepter.Reactor.Pump();
            foreach ((SessionEventKind Kind, PeerId Peer, DisconnectReason Reason) evt in accepter.DrainToList())
            {
                if (evt.Kind == SessionEventKind.PeerConnected)
                {
                    connected = evt.Peer;
                }
            }
        }

        connected.Should().NotBeNull("the Session must establish before a test can interrupt it");
        byte[] hello = peer.Receive(StreamSessionTestRecords.HandshakeRecordSize);
        byte[] secret = StreamSessionTestRecords.ReadResumeSecretFromHello(hello);

        peer.ResetAbruptly();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == connected); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == connected);
        return (connected!.Value, secret);
    }

    /// <summary>
    /// Dials <paramref name="dialer"/> at <paramref name="listener"/>, completes the handshake by hand
    /// with <paramref name="listener"/>'s reply announcing <paramref name="farLingerMs"/> — mirrors
    /// <c>StreamSessionRedialTests.EstablishDialledSession</c>, parametrized on the announced Linger
    /// rather than its fixed default, since that is exactly the figure this file needs to disagree with
    /// <paramref name="dialer"/>'s own — then kills the far end's transport abruptly and pumps until
    /// <paramref name="dialer"/> drains <see cref="SessionEventKind.PeerInterrupted"/>, stopping
    /// <paramref name="listener"/> along the way (see the in-method remark) so every subsequent re-dial
    /// attempt (#125) fails rather than succeeding and resuming the Peer out from under a boundary a
    /// test still needs held open. The dialling-role half of issue #131's cross-role proof; the caller
    /// owns disposing the returned <see cref="RawStreamListener.Accepted"/>.
    /// </summary>
    private static (PeerId Peer, RawStreamListener.Accepted Far) EstablishAndInterruptDiallingSide(
        StreamSessionReactorHarness dialer, RawStreamListener listener, ulong farLingerMs)
    {
        PeerId peer = dialer.Reactor.Connect(listener.Endpoint);
        RawStreamListener.Accepted far = listener.Accept();

        dialer.Reactor.Pump();
        byte[] hello = far.Receive(StreamSessionTestRecords.HandshakeRecordSize);
        hello[0].Should().Be(StreamSessionTestRecords.HelloRecordType);
        far.Send(StreamSessionTestRecords.BuildHello(lingerMs: farLingerMs));

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

        // Stopped here, well before any redial could ever be scheduled: a re-dial's own delay can floor
        // at a single elapsed tick (StreamSessionRedialTests' own
        // Pump_InterruptedWithACadenceTooFastToMeasure_... test) when this file's helpers, unlike that
        // one's, never advance the clock mid-handshake to give ProcessRedials a measurable cadence — so
        // the very Pump that detects the interruption below can also be the Pump that dials again, and
        // stopping the listener any later would race that attempt against a still-live endpoint.
        listener.Dispose();

        far.ResetAbruptly();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == peer); i++)
        {
            dialer.Reactor.Pump();
            drained.AddRange(dialer.DrainToList());
        }

        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == peer);
        return (peer, far);
    }
}
