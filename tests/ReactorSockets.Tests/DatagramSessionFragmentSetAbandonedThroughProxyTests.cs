using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #317 (parent #51, under slice-7 spec #46): the end-to-end proof that
/// <see cref="DiagnosticId.FragmentSetAbandoned"/> reports accurate held-against-expected figures
/// between two live Sessions. <see cref="DatagramSessionFragmentSetAbandonedTests"/> already proves the
/// figures are right, but through a hand-built client over a bare <see cref="RawReactor"/> driving the
/// receive window directly; this file proves the same figures survive a real two-Reactor path through a
/// <see cref="LossyProxy"/>, which is what stops them drifting into nonsense the next time the receive
/// window is touched.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a Message this wide.</b> A Reactor packs as many of a Message's Fragments into one outbound
/// Datagram as the 65 507-byte UDP maximum has room for, so an ordinary
/// <see cref="FragmentSizeDiagnosisFixture.SendLargeMessage"/> crosses as a single Datagram and a path
/// that loses it loses the entire set — issue #271's black hole, which leaves the receiving window
/// nothing held and no set to report against, and is exactly why that issue's criterion went unticked.
/// The case a consumer actually cares about is the one in between: a path that carries part of a set and
/// loses the rest. Reaching it through a real path needs a set that spans more than one Datagram, which
/// is what <see cref="FragmentSizeDiagnosisFixture.SendSpanningMessage"/> sends. Every loss here is then
/// an ordinary whole-Datagram loss, the only kind a network inflicts.
/// </para>
/// <para>
/// <b>The arithmetic this run's shape comes from.</b> At
/// <see cref="FragmentSizeDiagnosisFixture.FragmentSize"/> the spanning Message is 110 Fragments, at
/// sequences 0 through 109, and the receive window is exactly that wide — the floor for a Message this
/// size (ADR 0004). The first Datagram carries the 107 Fragments that fit; the second carries 107, 108
/// and 109, and the proxy never forwards it, so the accepter admits 0 through 106, holds them, and pins
/// its base at 107 behind a hole it can never pass. A second spanning Message then takes sequences 110
/// upward, and 110 resolves to the very ring slot sequence 0's Fragment still occupies — 110 slots on
/// from it — so admitting it abandons the first set with 107 of its 110 declared pieces held. That is
/// the "thirty-nine of forty" end of the range the Diagnostic exists to distinguish.
/// </para>
/// <para>
/// <b>Why the figures are read off the proxy.</b> Both numbers this file asserts come from the wire
/// rather than from these constants: the held figure from <see cref="LossyProxy.Forwarded"/>, the
/// distinct sequences of the first set the proxy actually put on the wire, and the expected figure from
/// the <c>fragmentCount</c> the set's own Fragment records declared. A hardcoded 107-of-110 would still
/// pass if the proxy silently forwarded something else, which is the drift this file exists to catch.
/// </para>
/// </remarks>
public class DatagramSessionFragmentSetAbandonedThroughProxyTests
{
    /// <summary>
    /// Both ends' <see cref="DatagramSessionOptions.MaxMessageSize"/> — exactly the spanning Message, so
    /// <see cref="DatagramSessionOptions.ReceiveWindowFloor"/> is its own Fragment count and the receive
    /// ring is exactly one set wide. That is what puts the second set's first Fragment on the first
    /// set's first slot rather than somewhere further into the run.
    /// </summary>
    private const int MaxMessageSize = FragmentSizeDiagnosisFixture.SpanningMessageLength;

    /// <summary>
    /// Both ends' <see cref="DatagramSessionOptions.RetentionBytes"/>: room for both spanning Messages
    /// at once, so the second is queued because this run asked for it rather than refused because the
    /// first is still unacknowledged.
    /// </summary>
    private const int RetentionBytes = 3 * FragmentSizeDiagnosisFixture.SpanningMessageLength;

    /// <summary>
    /// Rounds the first set is driven for on its own, before the second Message is queued. Long enough
    /// for its forwarded Fragments to land and be acknowledged, so the set is genuinely incomplete
    /// rather than merely not-yet-arrived when the second set overlaps it.
    /// </summary>
    private const int FirstSetRounds = 4;

    /// <summary>
    /// Rounds bounding the drive to the clobber after the second Message is queued. Wide enough to leave
    /// no doubt the abandonment is a genuine result rather than a near-miss, and bounded so a regression
    /// that stops it firing fails this test instead of hanging it.
    /// </summary>
    private const int OverlapRounds = 60;

    /// <summary>Rounds the completing run is driven for once the withheld Datagram is allowed through — ample for one retransmission to cross and the Message to be reassembled.</summary>
    private const int CompletionRounds = 40;

    [Fact]
    public void Pump_ASecondFragmentSetLapsTheRingOntoAnIncompleteFirstSet_EmitsFragmentSetAbandonedForThatPeer()
    {
        // Arrange & Act
        using FragmentSizeDiagnosisFixture fixture = DriveTwoOverlappingSets(out bool abandoned, out _);

        // Assert — the clobber actually happened within the budget, rather than the run merely running out.
        abandoned.Should().BeTrue(
            $"the second Fragment set's admission must lap the ring onto the first set's own slot well inside {OverlapRounds} rounds — a run that never reaches the clobber proves nothing about the figures");

        // Assert — exactly one abandonment, subjected to the dialler by both kind and id, as the accepter knows it.
        fixture.AccepterDiagnostics.Should().ContainSingle(r => r.Id == DiagnosticId.FragmentSetAbandoned)
            .Which.Should().Match<DiagnosticRecord>(r =>
                r.SubjectKind == DiagnosticSubjectKind.Peer && r.Peer.Equals(fixture.AccepterPeer),
                "an abandoned Fragment set is a fact about the Peer that sent it, so a consumer reading this on the receiving end must be told which Peer to act on");
    }

    [Fact]
    public void Pump_ASecondFragmentSetLapsTheRingOntoAnIncompleteFirstSet_ReportsTheFragmentsTheProxyForwardedAgainstTheCountTheSetDeclared()
    {
        // Arrange & Act
        using FragmentSizeDiagnosisFixture fixture = DriveTwoOverlappingSets(out _, out uint firstSetStart);
        int fragmentsForwarded = fixture.ForwardedFragmentSequencesOf(firstSetStart, fixture.FragmentsPerSpanningMessage).Count;
        ushort? countDeclared = fixture.DeclaredFragmentCountOf(firstSetStart, fixture.FragmentsPerSpanningMessage);

        // Assert — the path really did carry part of the set and lose the rest, which is the case in
        // between a whole set and a black hole that this file exists to cover.
        fragmentsForwarded.Should().BeInRange(1, fixture.FragmentsPerSpanningMessage - 1,
            "this run is only meaningful if the path carried some of the first set and lost the rest — a whole set is never abandoned, and a set with nothing forwarded leaves no residue to abandon");

        // Assert — held against expected, both read off the wire rather than restated here.
        fixture.AccepterDiagnostics.Should().ContainSingle(r => r.Id == DiagnosticId.FragmentSetAbandoned)
            .Which.Should().Match<DiagnosticRecord>(r =>
                r.Value0 == fragmentsForwarded && r.Value1 == countDeclared,
                "one held of forty and thirty-nine held of forty want different responses from whoever reads this, so the held figure must be exactly what the proxy forwarded of that set and the expected figure exactly what the set itself declared");
    }

    [Fact]
    public void Pump_AFragmentSetStillIncompleteWithNothingOverlappingIt_EmitsNoFragmentSetAbandoned()
    {
        // Arrange — one set, part of which the proxy never forwards, and no second set behind it.
        using FragmentSizeDiagnosisFixture fixture = NewSpanningFixture();
        WithholdTheTailOfTheNextSet(fixture);
        fixture.SendSpanningMessage().Should().Be(SendOutcome.Sent, "the Fragment set must actually be queued for this scenario to mean anything");

        // Act
        fixture.PumpRounds(FirstSetRounds + OverlapRounds);

        // Assert — an incomplete set nothing has clobbered is still live, not abandoned: this is what
        // makes the sibling runs' Diagnostic a report of the overlap rather than of the loss alone.
        fixture.AccepterDiagnostics.Should().NotContain(r => r.Id == DiagnosticId.FragmentSetAbandoned,
            "a set that is merely missing a piece may still be completed by a retransmission, so reporting it abandoned would teach a consumer to act on ordinary in-flight loss");
    }

    [Fact]
    public void Pump_APartialFragmentSetThatLaterCompletes_EmitsNoFragmentSetAbandoned()
    {
        // Arrange — the set's tail is withheld only while the rest of it crosses, then the path is
        // opened so the dialler's next retransmission of that tail completes the set.
        using FragmentSizeDiagnosisFixture fixture = NewSpanningFixture();
        WithholdTheTailOfTheNextSet(fixture);
        fixture.SendSpanningMessage().Should().Be(SendOutcome.Sent, "the Fragment set must actually be queued for this scenario to mean anything");
        fixture.PumpRounds(FirstSetRounds);
        fixture.DropFragmentsAtSequences();

        // Act
        bool delivered = fixture.PumpRoundsUntil(
            () => fixture.DeliveredPayloads.Any(p => p.Length == FragmentSizeDiagnosisFixture.SpanningMessageLength),
            CompletionRounds);

        // Assert — the set genuinely completed, so this is the slow-reassembly case rather than one that quietly stayed broken.
        delivered.Should().BeTrue(
            $"the withheld Fragments' next retransmission must cross and complete the Message inside {CompletionRounds} rounds, or this run proves nothing about a set that completes");

        // Assert — slow reassembly is not abandonment.
        fixture.AccepterDiagnostics.Should().NotContain(r => r.Id == DiagnosticId.FragmentSetAbandoned,
            "a set delivered late is a success, and a Diagnostic firing on it would teach a consumer to ignore the ones that mark a real loss");
    }

    [Fact]
    public void Pump_ASecondFragmentSetLapsTheRingOntoAnIncompleteFirstSet_LeavesBothSessionsOpen()
    {
        // Arrange & Act
        using FragmentSizeDiagnosisFixture fixture = DriveTwoOverlappingSets(out _, out _);

        // Assert — accounting, never a diagnosis: an abandoned set says what was lost and not why, so
        // neither end may close over it.
        fixture.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed,
            "the receiving end reported the abandonment, and reporting it may never be the same act as ending the Session it was reported about");
        fixture.DiallerEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed,
            "the sending end is likewise untouched — nothing about the accepter's own accounting reaches back across the path");
    }

    [Fact]
    public void Pump_ASecondFragmentSetLapsTheRingOntoAnIncompleteFirstSet_ReportsNoFragmentSizeUnviable()
    {
        // Arrange & Act
        using FragmentSizeDiagnosisFixture fixture = DriveTwoOverlappingSets(out _, out _);

        // Assert — partial loss is not a misconfigured fragment size, and must never be diagnosed as one.
        fixture.AccepterEvents.Concat(fixture.DiallerEvents).Should().NotContain(e => e.Reason == DisconnectReason.FragmentSizeUnviable,
            "the fragment size is viable here — Fragments of exactly this size crossed the path in both sets — so naming it as the cause would be a false diagnosis");
    }

    /// <summary>
    /// The shared arrangement the overlap runs read from: two spanning Messages sent through a proxy
    /// that never forwards the first set's tail Datagram, the second queued only once the first set has
    /// already been driven and is genuinely incomplete, then pumped until the accepter reports the
    /// abandonment or <see cref="OverlapRounds"/> is spent.
    /// </summary>
    /// <param name="abandoned">Whether the abandonment was reported before the budget ran out.</param>
    /// <param name="firstSetStart">The first sequence of the set that gets abandoned.</param>
    private static FragmentSizeDiagnosisFixture DriveTwoOverlappingSets(out bool abandoned, out uint firstSetStart)
    {
        FragmentSizeDiagnosisFixture fixture = NewSpanningFixture();
        try
        {
            firstSetStart = fixture.NextOrderedSequence;
            WithholdTheTailOfTheNextSet(fixture);
            fixture.SendSpanningMessage().Should().Be(SendOutcome.Sent, "the first Fragment set must actually be queued for this scenario to mean anything");
            fixture.PumpRounds(FirstSetRounds);

            fixture.SendSpanningMessage().Should().Be(SendOutcome.Sent, "the second Fragment set is what laps the ring onto the first, so it must actually be queued");
            abandoned = fixture.PumpRoundsUntil(
                () => fixture.AccepterDiagnostics.Any(r => r.Id == DiagnosticId.FragmentSetAbandoned),
                OverlapRounds);

            return fixture;
        }
        catch
        {
            fixture.Dispose();
            throw;
        }
    }

    /// <summary>
    /// Names the last Fragment of the set about to be sent as one the path never carries. Naming the
    /// last one rather than a chosen index is what keeps this run independent of how many Fragments
    /// happen to fit in a Datagram: whichever Datagram carries the set's final Fragment is the one lost,
    /// and how much of the set that leaves behind is then read back off the proxy rather than assumed.
    /// </summary>
    private static void WithholdTheTailOfTheNextSet(FragmentSizeDiagnosisFixture fixture) =>
        fixture.DropFragmentsAtSequences(fixture.NextOrderedSequence + (uint)fixture.FragmentsPerSpanningMessage - 1);

    private static FragmentSizeDiagnosisFixture NewSpanningFixture() =>
        new(_ => { }, maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes);
}
