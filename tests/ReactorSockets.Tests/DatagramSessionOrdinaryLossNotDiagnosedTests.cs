using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The negative half of the fragment-size stall signature's own test, judging
/// <see cref="FragmentStallSignature.StallRoundCount"/> against a path that is merely lossy rather than
/// size-limited. The same <see cref="FragmentSizeDiagnosisFixture"/> the positive
/// (<see cref="DatagramSessionFragmentSizeUnviableTests"/>) is driven through, but with the proxy's
/// standing rule replaced: instead of a size threshold that black-holes every Fragment forever, an
/// ordinary every-Nth loss rule takes Fragments and unfragmented Messages alike, indiscriminately. A path
/// losing a tenth of everything is a network, not a misconfiguration — diagnosing it as one would send a
/// consumer to change a setting that was never the problem, which is worse than reporting nothing at all.
/// </summary>
/// <remarks>
/// <para>
/// <b>Why every tenth relay, on both directions.</b> The issue frames the scenario as "a tenth of
/// everything", so <see cref="LossEveryNth"/> is 10 rather than a gentler value discovered by tuning
/// until the test passed. <see cref="LossyProxy.SetLossEveryNth"/> drops the Nth relay counted from
/// whenever it was set, deterministically rather than probabilistically (see its own remarks), so the
/// same tenth is lost every run — this test's own outcome cannot be a lucky seed. Both directions carry
/// the rule because <see cref="FragmentSizeDiagnosisFixture.PumpRound"/> puts every shape of traffic on
/// both: the dialler's large, fragmented Message and its small, unfragmented one travel
/// <see cref="RelayDirection.ClientToUpstream"/>, and the accepter's per-round keep-alive and every ack
/// travel <see cref="RelayDirection.UpstreamToClient"/>. A rule on only one direction would leave the
/// other free of loss and would not be the "Fragments and unfragmented Messages alike" scenario the issue
/// asks for.
/// </para>
/// <para>
/// <b>Why the round budget is what it is.</b> <see cref="FragmentStallSignature.StallRoundCount"/> — four
/// — is the number of consecutive retransmission rounds with no Fragment acked that the positive test
/// needs to fire the diagnosis. At one loss in ten, the chance every Fragment of a set is lost on any one
/// round is small, so a stall of that length essentially never accumulates before some Fragment gets
/// through and resets the window (<see cref="FragmentStallSignature.RecordFragmentAcked"/> latches
/// permanently once it does). <see cref="MaxDeliveryRounds"/> is set to <c>StallRoundCount</c> times a
/// wide margin — far more <see cref="FragmentSizeDiagnosisFixture.PumpRound"/> calls than four RTO
/// expiries could plausibly need even accounting for backoff — so a run that fails to deliver within
/// budget is a real failure to observe, not a budget too tight to give the retry loop a fair chance. After
/// delivery, the run keeps going for <see cref="MarginRounds"/> more rounds so the "stays quiet" assertions
/// cover more than the instant the large Message happened to land.
/// </para>
/// </remarks>
public class DatagramSessionOrdinaryLossNotDiagnosedTests
{
    /// <summary>
    /// Every Nth relay, in each direction, that the proxy drops. The issue's own framing —
    /// "a tenth of everything" — chooses this value; it is not tuned against this test's outcome.
    /// </summary>
    private const int LossEveryNth = 10;

    /// <summary>
    /// The bounded number of <see cref="FragmentSizeDiagnosisFixture.PumpRound"/> calls the large Message
    /// is given to complete in. A generous multiple of <see cref="FragmentStallSignature.StallRoundCount"/>
    /// so the run is a real test of quiet rather than one too short for a stall to have had any chance to
    /// show up.
    /// </summary>
    private const int MaxDeliveryRounds = FragmentStallSignature.StallRoundCount * 75;

    /// <summary>How many further rounds the run continues after delivery, so the quiet assertions cover more than the moment of delivery.</summary>
    private const int MarginRounds = 50;

    [Fact]
    public void Run_WithOrdinaryEveryNthLossOnBothDirections_NeverReportsFragmentSizeUnviable()
    {
        // Arrange
        using FragmentSizeDiagnosisFixture fixture = CreateFixture();

        // Act
        RunBoundedScenario(fixture);

        // Assert — every disconnect this run could possibly produce would carry a DisconnectReason;
        // ordinary loss must never be the size-boundary diagnosis, on either end.
        fixture.DiallerEvents.Should().NotContain(
            e => e.Reason == DisconnectReason.FragmentSizeUnviable,
            "a path that merely loses a tenth of its traffic is a network, not a fragment-size misconfiguration, and diagnosing it as one would send a consumer to change a setting that was never the problem");
        fixture.AccepterEvents.Should().NotContain(
            e => e.Reason == DisconnectReason.FragmentSizeUnviable,
            "FragmentStallSignature is tracked on the sending side — the dialler, which sends Fragments and awaits their acks — so checking both ends is belt-and-braces rather than the primary place the diagnosis could wrongly fire");
    }

    [Fact]
    public void Run_WithOrdinaryEveryNthLossOnBothDirections_SessionStaysConnectedWithNoDisconnectedEventDrained()
    {
        // Arrange
        using FragmentSizeDiagnosisFixture fixture = CreateFixture();

        // Act
        RunBoundedScenario(fixture);

        // Assert — no PeerClosed at all, for either end, across the whole bounded run: ordinary loss must
        // never close the Session, whatever reason it might otherwise have been closed with.
        fixture.DiallerEvents.Should().NotContain(
            e => e.Kind == SessionEventKind.PeerClosed,
            "ordinary every-Nth loss must never disconnect the Session — the large Message keeps retrying and eventually gets through, which is exactly what a live path recovering from ordinary loss looks like");
        fixture.AccepterEvents.Should().NotContain(
            e => e.Kind == SessionEventKind.PeerClosed,
            "ordinary every-Nth loss must never disconnect the Session — the large Message keeps retrying and eventually gets through, which is exactly what a live path recovering from ordinary loss looks like");
    }

    [Fact]
    public void Run_WithOrdinaryEveryNthLossOnBothDirections_LargeMessageIsDeliveredIntact()
    {
        // Arrange
        using FragmentSizeDiagnosisFixture fixture = CreateFixture();

        // Act
        bool delivered = RunBoundedScenario(fixture);

        // Assert — the large Message actually arrived within the round budget, and what arrived is
        // byte-for-byte what was sent: retransmission recovering from loss is only a real recovery if the
        // reassembled bytes match, not merely if something with the right length showed up.
        delivered.Should().BeTrue(
            $"{FragmentSizeDiagnosisFixture.LargeMessageLength} bytes lost at one relay in {LossEveryNth} on both directions must still complete through ordinary retransmission within {MaxDeliveryRounds} rounds");
        fixture.DeliveredPayloads.Should().ContainSingle(
            payload => payload.Length == FragmentSizeDiagnosisFixture.LargeMessageLength,
            "the fixture's dialler is the only sender, and it sent exactly one large Message")
            .Which.Should().Equal(
                Enumerable.Range(0, FragmentSizeDiagnosisFixture.LargeMessageLength).Select(i => (byte)i),
                "the reassembled Message must match what was actually sent, not merely arrive at the right length");
    }

    /// <summary>
    /// <see cref="DiagnosticId.FragmentSetAbandoned"/> fires only when a later record reaches the
    /// receive-window ring slot of a Fragment set that can never complete (issue #446) — one the base has
    /// passed entirely without it completing. Loss never leaves one; a sender's other records at the
    /// set's sequences or a Fragment larger than this end's slot can. This run does not
    /// observe the diagnostic fire. The assertions below are written to catch a disturbance if a
    /// regression ever makes it fire here, not to force it to.
    /// </summary>
    [Fact]
    public void Run_WithOrdinaryEveryNthLossOnBothDirections_AnyFragmentSetAbandonedDoesNotDisturbTheSession()
    {
        // Arrange
        using FragmentSizeDiagnosisFixture fixture = CreateFixture();

        // Act
        RunBoundedScenario(fixture);

        // Assert — FragmentSetAbandoned reports what was lost and is not itself a problem. What would be
        // a problem is that emitting it ever closed or otherwise disturbed the Session — it never
        // diagnoses why a set was lost, which is exactly why it cannot be the trigger for closing
        // anything (issue #268's own framing).
        fixture.AccepterEvents.Should().NotContain(
            e => e.Kind == SessionEventKind.PeerClosed,
            "FragmentSetAbandoned reports loss, it never diagnoses cause, so it must never be the reason a Session closes");
        fixture.DeliveredPayloads.Should().ContainSingle(
            payload => payload.Length == FragmentSizeDiagnosisFixture.LargeMessageLength,
            "even if some Fragment set was abandoned to ordinary loss along the way, the large Message's own retransmissions must still have completed delivery by the end of the run");
    }

    /// <summary>
    /// Across the whole negative run, no datagram beyond this fixture's own two Messages and
    /// the accepter's keep-alive ever reaches the wire. Ordinary loss recovers the large Message through
    /// retransmission alone — the same shared check the positive run uses
    /// (<see cref="DatagramSessionFragmentSizeStallDiagnosisTests"/>'s own equivalent Fact) proves that
    /// recovery never involved a probe, which is half of "we diagnose, we do not recover": a path that is
    /// merely lossy must recover exactly the way a genuinely misconfigured one is forbidden to.
    /// </summary>
    [Fact]
    public void Run_WithOrdinaryEveryNthLossOnBothDirections_NeverEmitsAProbe()
    {
        // Arrange
        using FragmentSizeDiagnosisFixture fixture = CreateFixture();

        // Act
        RunBoundedScenario(fixture);

        // Assert — shared with the positive run; see the method's own remarks for what a probe would look like here.
        fixture.AssertNoProbeWasObserved();
    }

    /// <summary>
    /// Across the whole negative run, both ends' configured fragment size is unchanged.
    /// Ordinary loss recovers the large Message through retransmission alone, never by touching the
    /// setting — the same shared check the positive run uses
    /// (<see cref="DatagramSessionFragmentSizeStallDiagnosisTests"/>'s own equivalent Fact), which is the
    /// other half of "we diagnose, we do not recover": a path that is merely lossy must recover exactly
    /// the way a genuinely misconfigured one is forbidden to.
    /// </summary>
    [Fact]
    public void Run_WithOrdinaryEveryNthLossOnBothDirections_LeavesTheConfiguredFragmentSizeUnchanged()
    {
        // Arrange
        using FragmentSizeDiagnosisFixture fixture = CreateFixture();

        // Act
        RunBoundedScenario(fixture);

        // Assert — shared with the positive run; see the method's own remarks for exactly what this reads back.
        fixture.AssertConfiguredFragmentSizeUnchanged(FragmentSizeDiagnosisFixture.FragmentSize);
    }

    /// <summary>Builds a fixture whose standing proxy rule is ordinary every-Nth loss on both directions, rather than the positive test's size threshold.</summary>
    private static FragmentSizeDiagnosisFixture CreateFixture() =>
        new(proxy =>
        {
            proxy.SetLossEveryNth(RelayDirection.ClientToUpstream, LossEveryNth);
            proxy.SetLossEveryNth(RelayDirection.UpstreamToClient, LossEveryNth);
        });

    /// <summary>
    /// Sends both shapes of Message the fixture supports — so the every-Nth rule is shown to take
    /// Fragments and unfragmented Messages alike, not just the large Message's Fragments — then pumps
    /// until the large Message is delivered or <see cref="MaxDeliveryRounds"/> is spent, and finally pumps
    /// <see cref="MarginRounds"/> further so every assertion covers more than the instant of delivery.
    /// Returns whether delivery happened within budget.
    /// </summary>
    private static bool RunBoundedScenario(FragmentSizeDiagnosisFixture fixture)
    {
        fixture.SendLargeMessage().Should().Be(SendOutcome.Sent, "the large Message must actually have been accepted for the run to prove anything about its delivery");
        fixture.SendSmallMessage().Should().Be(SendOutcome.Sent, "the small, unfragmented Message must ride the same lossy path as the Fragments, per the issue's own framing");

        bool delivered = fixture.PumpRoundsUntil(
            () => fixture.DeliveredPayloads.Any(payload => payload.Length == FragmentSizeDiagnosisFixture.LargeMessageLength),
            MaxDeliveryRounds);
        fixture.PumpRounds(MarginRounds);
        return delivered;
    }
}
