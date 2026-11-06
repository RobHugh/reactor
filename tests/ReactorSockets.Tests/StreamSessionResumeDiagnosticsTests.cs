using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// What an operator reads when a Resume is matched and when one is refused,
/// and the three ways a Resume is refused. <see cref="DiagnosticId.ResumeMatched"/> names the resumed
/// Peer, because a match by definition found one. <see cref="DiagnosticId.ResumeUnmatched"/> names a
/// claimed Source instead, because a claim that is not matched comes from a party that is by
/// construction not a Peer — there is no Peer to name, only a claimed origin — and it is
/// <see cref="DiagnosticLevel.Warn"/> rather than <see cref="DiagnosticLevel.Info"/> because a wrong
/// secret, unlike a version mismatch or a capacity refusal, is not a routine outcome of running this
/// profile.
/// </summary>
/// <remarks>
/// The three refusal paths, one section each below: an invalid secret, a once-valid secret arriving
/// after Linger expired — the reconfiguration path, where the claimant is admitted afresh rather than
/// resumed — and a Hello announcing a secret a reservation is holding, which re-announces the
/// parameters a Resume has no field for (issue #115) and is refused as a protocol violation. Drives
/// the public L2 surface only (tests/CLAUDE.md, ADR 0015): the resume secret is read off the wire out
/// of the Hello the Reactor itself sent, never out of the Reactor, and the Diagnostics are observed
/// through the recording Sink a consumer would install. Every Linger deadline is driven by
/// <see cref="StreamSessionReactorHarness.TimeProvider"/> rather than by sleeping; the one wall-clock
/// wait is the bounded receive that proves nothing came back on the wire, which has no other way to
/// establish an absence.
/// </remarks>
public class StreamSessionResumeDiagnosticsTests
{
    private static readonly TimeSpan _effectiveLinger = TimeSpan.FromSeconds(1);

    /// <summary>The wire record type byte a Resume carries, as <c>ResumeUnmatched</c>'s <c>Value1</c> reports it.</summary>
    private const long ResumeRecordType = 3;

    /// <summary>The wire record type byte a Hello carries, as <c>ResumeUnmatched</c>'s <c>Value1</c> reports it.</summary>
    private const long HelloRecordType = 1;

    // =========================================================================================
    // A match: ResumeMatched, once, naming the resumed Peer
    // =========================================================================================

    [Fact]
    public void Handshake_AResumePresentingAHeldPeersSecret_LogsExactlyOneResumeMatchedNamingThatPeer()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeMatched));

        // Assert — one record, and it names the Peer that was interrupted: the same PeerId, generation
        // included, since nothing about an identity is renewed by a Resume.
        accepter.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.ResumeMatched)
            .Which.Peer.Should().Be(held[0].Peer);
    }

    [Fact]
    public void Handshake_AResumePresentingAHeldPeersSecret_LogsResumeMatchedAgainstAPeerSubject()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeMatched));

        // Assert — a match by definition found a Peer, so there is one to name.
        accepter.Sink.Received.Single(r => r.Id == DiagnosticId.ResumeMatched)
            .SubjectKind.Should().Be(DiagnosticSubjectKind.Peer);
    }

    [Fact]
    public void Handshake_AResumePresentingAHeldPeersSecret_LogsResumeMatchedAtInfo()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeMatched));

        // Assert — a reconnection is the outcome reserving a Slot exists to produce, not a condition a
        // consumer has to act on.
        accepter.Sink.Received.Single(r => r.Id == DiagnosticId.ResumeMatched)
            .Level.Should().Be(DiagnosticLevel.Info);
    }

    [Fact]
    public void Handshake_AResumeMatchedWithNothingSentYet_ReportsZeroBytesRewound()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret, deliveryCount: 0));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeMatched));

        // Assert — Value0 carries bytes rewound (issue #127). A delivery count of 0 resolves to the
        // released head, and nothing had been sent to this Peer before the interruption, so the sent
        // cursor has nowhere to move back from: zero bytes rewound is the honest figure here, not a
        // stand-in for a rewind that doesn't exist yet.
        accepter.Sink.Received.Single(r => r.Id == DiagnosticId.ResumeMatched)
            .Value0.Should().Be(0);
    }

    // =========================================================================================
    // Refusal path 1: an invalid secret
    // =========================================================================================

    [Fact]
    public void Handshake_AResumePresentingASecretMatchingNoHeldPeer_LogsResumeUnmatchedAgainstAClaimedSource()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);
        byte[] wrongSecret = WrongSecretFor(held[0]);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer impostor = new(accepter.Endpoint);
        impostor.Send(StreamSessionTestRecords.BuildResume(wrongSecret));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        // Assert — the party that sent it is not a Peer and never became one, so the record names the
        // origin it claimed rather than any Peer.
        accepter.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.ResumeUnmatched)
            .Which.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
    }

    [Fact]
    public void Handshake_AResumePresentingASecretMatchingNoHeldPeer_NamesNoPeerOnTheResumeUnmatchedRecord()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer impostor = new(accepter.Endpoint);
        impostor.Send(StreamSessionTestRecords.BuildResume(WrongSecretFor(held[0])));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        // Assert — the Peer slot is left at its default rather than filled with the Peer the claim
        // aimed at, which would read as that Peer having done something.
        accepter.Sink.Received.Single(r => r.Id == DiagnosticId.ResumeUnmatched)
            .Peer.Should().Be(default(PeerId));
    }

    [Fact]
    public void Handshake_AResumePresentingASecretMatchingNoHeldPeer_LogsResumeUnmatchedAtWarn()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer impostor = new(accepter.Endpoint);
        impostor.Send(StreamSessionTestRecords.BuildResume(WrongSecretFor(held[0])));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        // Assert — Warn, not the Info a version mismatch or a capacity refusal carries: those are
        // routine outcomes of running this profile and a wrong secret is not.
        accepter.Sink.Received.Single(r => r.Id == DiagnosticId.ResumeUnmatched)
            .Level.Should().Be(DiagnosticLevel.Warn);
    }

    [Fact]
    public void Handshake_AResumePresentingASecretMatchingNoHeldPeer_ReportsTheOneReservationItWasCheckedAgainst()
    {
        // Arrange — exactly one reservation is being held, so the count the record reports is knowable.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer impostor = new(accepter.Endpoint);
        impostor.Send(StreamSessionTestRecords.BuildResume(WrongSecretFor(held[0])));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        // Assert — Value0 says a reservation was there and the secret was wrong for it, which is what
        // tells this apart from the expired-Linger path where there is nothing left to check against.
        accepter.Sink.Received.Single(r => r.Id == DiagnosticId.ResumeUnmatched)
            .Value0.Should().Be(1);
    }

    [Fact]
    public void Handshake_AResumePresentingASecretMatchingNoHeldPeer_ReportsTheResumeRecordTypeItArrivedAs()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer impostor = new(accepter.Endpoint);
        impostor.Send(StreamSessionTestRecords.BuildResume(WrongSecretFor(held[0])));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        // Assert — Value1 separates a wrong secret in the record allowed to carry one from the Hello
        // path below, which ends the connection the same way for a different reason.
        accepter.Sink.Received.Single(r => r.Id == DiagnosticId.ResumeUnmatched)
            .Value1.Should().Be(ResumeRecordType);
    }

    [Fact]
    public void Handshake_AResumePresentingASecretMatchingNoHeldPeer_LogsNothingAgainstTheHeldPeerItDidNotMatch()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);
        accepter.Sink.Received.Clear();

        // Act
        using StreamSessionReactorHarness.RawStreamPeer impostor = new(accepter.Endpoint);
        impostor.Send(StreamSessionTestRecords.BuildResume(WrongSecretFor(held[0])));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        // Assert — a stranger's failed claim is not something the Peer it aimed at is charged with. The
        // held Peer goes on being held, which #123's own tests pin by resuming it afterwards.
        accepter.Sink.Received.Should().NotContain(r => r.SubjectKind == DiagnosticSubjectKind.Peer);
    }

    // =========================================================================================
    // Refusal path 2: a once-valid secret arriving after Linger expired — admitted afresh
    // =========================================================================================

    [Fact]
    public void Handshake_AOnceValidSecretPresentedAfterLingerExpired_ReportsThatNoReservationWasLeftToCheckAgainst()
    {
        // Arrange — the reservation is allowed to expire, so the Peer the secret named is closed and
        // its Slot released before the claim arrives.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);
        accepter.TimeProvider.Advance(_effectiveLinger);
        accepter.PumpAndDrainAll(2);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer late = new(accepter.Endpoint);
        late.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        // Assert — zero candidates: nothing was being held at all. That is the figure that tells an
        // operator this was a late reconnection rather than a wrong secret, which are the same refusal
        // on the wire and very different things to go and look at.
        accepter.Sink.Received.Single(r => r.Id == DiagnosticId.ResumeUnmatched)
            .Value0.Should().Be(0);
    }

    [Fact]
    public void AttemptFreshConnection_CarryingAOnceValidSecretAfterLingerExpired_IsAdmitted()
    {
        // Arrange — the same expired reservation. This is the reconfiguration path and the correct
        // one: the Session that secret named is gone, so what comes back negotiates fresh. The dial
        // carries the once-valid secret, which is what makes this the criterion's own case rather
        // than an unrelated fresh dial that happens to arrive afterwards.
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);
        accepter.TimeProvider.Advance(_effectiveLinger);
        accepter.PumpAndDrainAll(2);

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult outcome =
            accepter.AttemptFreshConnection(resumeSecret: held[0].ResumeSecret);

        // Assert — admitted, not refused as the reserved-Session claim it would have been while the
        // reservation was still held: there is no reservation left for it to be a claim on.
        outcome.Admitted.Should().BeTrue();
    }

    [Fact]
    public void AttemptFreshConnection_CarryingAOnceValidSecretAfterLingerExpired_BecomesANewPeerWithADifferentPeerId()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);
        accepter.TimeProvider.Advance(_effectiveLinger);
        accepter.PumpAndDrainAll(2);

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult outcome =
            accepter.AttemptFreshConnection(resumeSecret: held[0].ResumeSecret);

        // Assert — a new Peer, not the old one resumed. A PeerId carries its Slot's generation, and the
        // Slot was emptied and reused, so the two ids cannot be equal.
        outcome.Peer.Should().NotBeNull().And.NotBe(held[0].Peer);
    }

    [Fact]
    public void Handshake_AOnceValidSecretPresentedAfterLingerExpired_LogsNoResumeMatched()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 1);
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        accepter.InterruptHeldPeer(held[0]);
        accepter.TimeProvider.Advance(_effectiveLinger);
        accepter.PumpAndDrainAll(2);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer late = new(accepter.Endpoint);
        late.Send(StreamSessionTestRecords.BuildResume(held[0].ResumeSecret));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        // Assert — the secret was once real, which is exactly why it must not be honoured now: the
        // state it authorised access to no longer exists.
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.ResumeMatched);
    }

    // =========================================================================================
    // Refusal path 3: a Hello announcing a secret a reservation is holding
    // =========================================================================================

    [Fact]
    public void Handshake_AHelloAnnouncingASecretAReservationIsHolding_IsNotAdmittedEvenThoughASlotIsFree()
    {
        // Arrange — capacity 2 with one Peer interrupted, so one Slot is genuinely free: an ordinary
        // Hello here would be admitted, which is what makes this a refusal for what the record claims
        // rather than for capacity.
        using StreamSessionReactorHarness accepter = new(capacity: 2);
        StreamSessionReactorHarness.HeldPeer held = accepter.EstablishOnePeer();
        accepter.InterruptHeldPeer(held);

        // Act — a Hello, which re-announces version, MaxMessageSize, the send-idle interval and Linger,
        // carrying the secret that only a Resume may present.
        using StreamSessionReactorHarness.RawStreamPeer claimant = new(accepter.Endpoint);
        claimant.Send(StreamSessionTestRecords.BuildHello(resumeSecret: held.ResumeSecret));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events =
            accepter.PumpUntilDrained(_ => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        // Assert — a Resume has no field for parameters by construction (issue #115), and re-announcing
        // them would break the retention ring underneath the Session, so this is a protocol violation
        // rather than something to accept.
        events.Should().NotContain(e => e.Kind == SessionEventKind.PeerConnected);
    }

    [Fact]
    public void Handshake_AHelloAnnouncingASecretAReservationIsHolding_IsAnsweredWithNothingOnTheWire()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2);
        StreamSessionReactorHarness.HeldPeer held = accepter.EstablishOnePeer();
        accepter.InterruptHeldPeer(held);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer claimant = new(accepter.Endpoint);
        claimant.Send(StreamSessionTestRecords.BuildHello(resumeSecret: held.ResumeSecret));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        // Assert — no Hello establishing it and no Refusal either: the wire RefusalReason roster names
        // in-protocol declines a well-behaved dialler can act on, and a protocol violation is not one.
        claimant.TryReceiveWithinTimeout(1, out _).Should().BeFalse();
    }

    [Fact]
    public void Handshake_AHelloAnnouncingASecretAReservationIsHolding_LogsExactlyOneResumeUnmatchedAgainstAClaimedSource()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2);
        StreamSessionReactorHarness.HeldPeer held = accepter.EstablishOnePeer();
        accepter.InterruptHeldPeer(held);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer claimant = new(accepter.Endpoint);
        claimant.Send(StreamSessionTestRecords.BuildHello(resumeSecret: held.ResumeSecret));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        // Assert — refused, and not silently. The claimant never became a Peer, so what the record can
        // name is the origin it claimed.
        accepter.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.ResumeUnmatched)
            .Which.SubjectKind.Should().Be(DiagnosticSubjectKind.Source);
    }

    [Fact]
    public void Handshake_AHelloAnnouncingASecretAReservationIsHolding_ReportsTheHelloRecordTypeItArrivedAs()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2);
        StreamSessionReactorHarness.HeldPeer held = accepter.EstablishOnePeer();
        accepter.InterruptHeldPeer(held);

        // Act
        using StreamSessionReactorHarness.RawStreamPeer claimant = new(accepter.Endpoint);
        claimant.Send(StreamSessionTestRecords.BuildHello(resumeSecret: held.ResumeSecret));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));

        // Assert — Value1 is what separates this path from an ordinary wrong secret, since both end the
        // connection the same way and only this one is a protocol violation.
        accepter.Sink.Received.Single(r => r.Id == DiagnosticId.ResumeUnmatched)
            .Value1.Should().Be(HelloRecordType);
    }

    [Fact]
    public void Handshake_AHelloAnnouncingASecretAReservationIsHolding_LeavesTheHeldPeerResumableByThatSameSecret()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2);
        StreamSessionReactorHarness.HeldPeer held = accepter.EstablishOnePeer();
        accepter.InterruptHeldPeer(held);
        using (StreamSessionReactorHarness.RawStreamPeer claimant = new(accepter.Endpoint))
        {
            claimant.Send(StreamSessionTestRecords.BuildHello(resumeSecret: held.ResumeSecret));
            accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeUnmatched));
        }

        // Act — the party that can actually resume comes back, presenting the same secret in the record
        // that is allowed to carry it.
        using StreamSessionReactorHarness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held.ResumeSecret));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.ResumeMatched));

        // Assert — refusing the violation took nothing from the reservation it aimed at: the Slot, the
        // state and the secret are all still there for the record that may present it.
        accepter.Sink.Received.Should()
            .ContainSingle(r => r.Id == DiagnosticId.ResumeMatched)
            .Which.Peer.Should().Be(held.Peer);
    }

    [Fact]
    public void Handshake_AnOrdinaryHelloArrivingWhileAReservationIsHeld_IsStillAdmittedIntoTheFreeSlot()
    {
        // Arrange — the control for the test above. A fresh dialler announces the secret it minted for
        // the Session it is opening, so it reaches the same candidate scan and matches nothing.
        using StreamSessionReactorHarness accepter = new(capacity: 2);
        StreamSessionReactorHarness.HeldPeer held = accepter.EstablishOnePeer();
        accepter.InterruptHeldPeer(held);

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult outcome = accepter.AttemptFreshConnection();

        // Assert — the reserved-Session check refuses claims on a reservation, not every Hello that
        // arrives while one is held.
        outcome.Admitted.Should().BeTrue();
    }

    [Fact]
    public void Handshake_AnOrdinaryHelloArrivingWhileAReservationIsHeld_LogsNoResumeUnmatched()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(capacity: 2);
        StreamSessionReactorHarness.HeldPeer held = accepter.EstablishOnePeer();
        accepter.InterruptHeldPeer(held);

        // Act
        using StreamSessionReactorHarness.InboundAttemptResult outcome = accepter.AttemptFreshConnection();

        // Assert — a dialler that claimed nothing is not reported as having had a claim refused.
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.ResumeUnmatched);
    }

    /// <summary>
    /// Sixteen bytes that are not <paramref name="held"/>'s secret, asserted rather than assumed: the
    /// secret is drawn from real entropy, so a fixed pattern is only a wrong secret by overwhelming
    /// probability and this says so out loud.
    /// </summary>
    private static byte[] WrongSecretFor(StreamSessionReactorHarness.HeldPeer held)
    {
        byte[] wrong = Enumerable.Range(1, StreamSessionTestRecords.ResumeSecretLength)
            .Select(b => (byte)b)
            .ToArray();
        wrong.Should().NotEqual(held.ResumeSecret);
        return wrong;
    }
}
