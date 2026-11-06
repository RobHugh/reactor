using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// What a real two-Reactor path
/// through a <see cref="LossyProxy"/> does when part of a Fragment set is lost. A ring lap never
/// overwrites a held Fragment whose set can still complete, and a real sender only ever leaves sets
/// like that, so <see cref="DiagnosticId.FragmentSetAbandoned"/> never fires here.
/// <see cref="DatagramSessionFragmentSetAbandonedTests"/> proves its held-against-expected figures with a
/// hand-built client, the only kind that can leave a set uncompletable.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why a Message this wide.</b> A Reactor packs as many of a Message's Fragments into one outbound
/// Datagram as the 65 507-byte UDP maximum has room for, so an ordinary
/// <see cref="FragmentSizeDiagnosisFixture.SendLargeMessage"/> crosses as a single Datagram and a path
/// that loses it loses the entire set. That black hole leaves the receiving window nothing held and no
/// set to report against.
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
/// from it. The first set can still complete, so the accepter refuses 110 rather than overwrite it.
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
    /// Rounds the overlap runs drive after the second Message is queued: long enough for the second
    /// set's Fragments to reach the first set's slots and be refused there.
    /// </summary>
    private const int OverlapRounds = 60;

    /// <summary>Rounds the completing run is driven for once the withheld Datagram is allowed through — ample for one retransmission to cross and the Message to be reassembled.</summary>
    private const int CompletionRounds = 40;

    [Fact]
    public void Pump_AFragmentSetStillIncompleteWithNothingOverlappingIt_EmitsNoFragmentSetAbandoned()
    {
        // Arrange — one set, part of which the proxy never forwards, and no second set behind it.
        using FragmentSizeDiagnosisFixture fixture = NewSpanningFixture();
        WithholdTheTailOfTheNextSet(fixture);
        fixture.SendSpanningMessage().Should().Be(SendOutcome.Sent, "the Fragment set must actually be queued for this scenario to mean anything");

        // Act
        fixture.PumpRounds(FirstSetRounds + OverlapRounds);

        // Assert — an incomplete set is still live, not abandoned.
        fixture.AccepterDiagnostics.Should().NotContain(r => r.Id == DiagnosticId.FragmentSetAbandoned,
            "a set that is merely missing a Fragment may still be completed by a retransmission, so reporting it abandoned would teach a consumer to act on ordinary in-flight loss");
    }

    [Fact]
    public void Pump_ASecondFragmentSetLapsTheRingOntoAnIncompleteFirstSet_EmitsNoFragmentSetAbandoned()
    {
        // Arrange & Act
        using FragmentSizeDiagnosisFixture fixture = DriveTwoOverlappingSets();

        // Assert — the first set can still complete, so the lap is refused rather than overwriting it.
        fixture.AccepterDiagnostics.Should().NotContain(r => r.Id == DiagnosticId.FragmentSetAbandoned,
            "a real sender's loss never leaves a set that cannot complete, so a later set reaching its slots must wait rather than abandon it (issue #446)");
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
        using FragmentSizeDiagnosisFixture fixture = DriveTwoOverlappingSets();

        // Assert — a refused lap is not a diagnosis, so neither end may close over it.
        fixture.AccepterEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed,
            "the receiving end only refused records it can take again later, which may never be the same act as ending the Session");
        fixture.DiallerEvents.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed,
            "the sending end is likewise untouched — nothing about the accepter's own accounting reaches back across the path");
    }

    [Fact]
    public void Pump_ASecondFragmentSetLapsTheRingOntoAnIncompleteFirstSet_ReportsNoFragmentSizeUnviable()
    {
        // Arrange & Act
        using FragmentSizeDiagnosisFixture fixture = DriveTwoOverlappingSets();

        // Assert — partial loss is not a misconfigured fragment size, and must never be diagnosed as one.
        fixture.AccepterEvents.Concat(fixture.DiallerEvents).Should().NotContain(e => e.Reason == DisconnectReason.FragmentSizeUnviable,
            "the fragment size is viable here — Fragments of exactly this size crossed the path in both sets — so naming it as the cause would be a false diagnosis");
    }

    /// <summary>
    /// The shared arrangement the overlap runs read from: two spanning Messages sent through a proxy
    /// that never forwards the first set's tail Datagram, the second queued only once the first set has
    /// already been driven and is genuinely incomplete, then pumped for <see cref="OverlapRounds"/>.
    /// </summary>
    private static FragmentSizeDiagnosisFixture DriveTwoOverlappingSets()
    {
        FragmentSizeDiagnosisFixture fixture = NewSpanningFixture();
        try
        {
            WithholdTheTailOfTheNextSet(fixture);
            fixture.SendSpanningMessage().Should().Be(SendOutcome.Sent, "the first Fragment set must actually be queued for this scenario to mean anything");
            fixture.PumpRounds(FirstSetRounds);

            fixture.SendSpanningMessage().Should().Be(SendOutcome.Sent, "the second Fragment set is what laps the ring onto the first, so it must actually be queued");
            fixture.PumpRounds(OverlapRounds);

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
