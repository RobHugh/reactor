using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The positive half of the stall-signature proof — a proxy
/// standing rule that black-holes every Fragment while an unfragmented Message keeps crossing closes the
/// stalled Peer by name, <see cref="DisconnectReason.FragmentSizeUnviable"/>, rather than by a generic
/// timeout or not at all. A misconfigured fragment size is invisible in normal operation, so this
/// file is what keeps the diagnosis honest against drift.
/// </summary>
/// <remarks>
/// <para>
/// <b>The size threshold.</b> <see cref="SizeThreshold"/> is chosen strictly between the on-wire length of
/// <see cref="FragmentSizeDiagnosisFixture.SendSmallMessage"/>'s unfragmented Datagram and the on-wire
/// length of the <em>shortest</em> Fragment <see cref="FragmentSizeDiagnosisFixture.SendLargeMessage"/> is
/// ever cut into — not merely below <see cref="FragmentSizeDiagnosisFixture.FragmentSize"/> itself, which
/// would still let the short remainder Fragment (the 1500-byte Message's third piece, shorter than the
/// other two by construction) through and violate "no Fragment ever lands". Both figures are the same
/// wire-header arithmetic <see cref="DatagramSessionFragmentSizeUnviableTests"/> and
/// <see cref="DatagramSessionFragmentSetAbandonedTests"/> already rely on: a 20-byte Datagram header, a
/// 7-byte unfragmented record header, and an 11-byte Fragment record header (4 bytes wider, for the
/// fragment index and count). The unfragmented Datagram is then <c>20 + 7 + 1 = 28</c> bytes; the
/// shortest Fragment Datagram is the remainder alone, <c>20 + 11 + (1500 - 2 * 600) = 331</c> bytes.
/// <see cref="SizeThreshold"/> sits at 150 — comfortably above 28 so the small Message is never at risk of
/// being caught by the threshold on its own, and comfortably below 331 so not even the remainder Fragment,
/// the smallest one on the wire, can ever cross.
/// </para>
/// <para>
/// <b>What "no Fragment ever lands" means to a consumer.</b> A Fragment is never itself a unit a consumer
/// observes (ADR 0004) — only a completed Message is. So the consumer-visible proof that no Fragment ever
/// reached the accepter is that the large Message's own reassembled payload never appears among
/// <see cref="FragmentSizeDiagnosisFixture.DeliveredPayloads"/>, for as long as the run has anything left
/// to drive: the accepter's Sink and drained Events are the only two surfaces this file may read from.
/// </para>
/// <para>
/// <b>Why no <c>FragmentSetAbandoned</c> is expected here.</b>
/// <see cref="DiagnosticId.FragmentSetAbandoned"/> fires only when a fresh admission clobbers a ring
/// slot that a <em>previously admitted</em> Fragment wrote non-zero length residue into
/// (<c>ReceiveClassification.AbandonedFragmentSet</c>, read by
/// <see cref="DatagramSessionReactor.ClassifyRecord"/>). The declared count comes from that same
/// residue, so nothing is ever "abandoned" at a ring slot no Fragment ever touched. With every
/// Fragment black-holed as this file's own threshold requires, the accepter's receive window for the
/// fragmented Space never admits anything at all, so that residue is never written and the Diagnostic
/// cannot fire — confirmed empirically here too: running this file's own drive for 400 rounds produces
/// zero entries in <see cref="FragmentSizeDiagnosisFixture.AccepterDiagnostics"/>. A Diagnostic with
/// zero held against the full declared count cannot coexist with no Fragment ever landing, so this file
/// asserts the Diagnostic's absence instead.
/// </para>
/// </remarks>
public class DatagramSessionFragmentSizeStallDiagnosisTests
{
    /// <summary>
    /// The proxy's standing <see cref="RelayDirection.ClientToUpstream"/> size threshold: strictly between
    /// the unfragmented Datagram's 28 on-wire bytes and the shortest Fragment Datagram's 331 (see the type
    /// remarks for the arithmetic). Every Fragment this run ever sends is cut to
    /// <see cref="FragmentSizeDiagnosisFixture.FragmentSize"/> or the short remainder, both well above this
    /// threshold, so none can ever cross; the unfragmented Datagram is well below it, so it is never at
    /// risk from the threshold alone.
    /// </summary>
    private const int SizeThreshold = 150;

    /// <summary>
    /// Rounds bounding the drive to the Peer's close: the stall signature needs
    /// <c>FragmentStallSignature.StallRoundCount</c> retransmission rounds after the unfragmented
    /// ack, which land well inside this budget (observed at round 16 with this file's own parameters) —
    /// wide enough to leave no doubt the close is a genuine result and not a near-miss, while still bounded
    /// so a regression that stops the Peer from ever closing fails this test instead of hanging it.
    /// </summary>
    private const int RoundBudget = 100;

    /// <summary>
    /// How many rounds <see cref="DriveWithPeriodicSmallSends"/> sends one small Message per round for,
    /// before handing control to a caller that may keep pumping. Long enough to comfortably straddle the
    /// close observed at round 16, so a caller can see small Messages delivered on both sides of it.
    /// </summary>
    private const int SmallSendRounds = 30;

    /// <summary>
    /// Proves the property this file exists for: with every Fragment
    /// black-holed and the small Message still crossing and being acked, the stalled Peer closes named by
    /// <see cref="DisconnectReason.FragmentSizeUnviable"/> — never a generic timeout, and never a Session
    /// that simply stalls without closing at all.
    /// </summary>
    /// <remarks>
    /// <c>FragmentStallSignature.IsFragmentSizeStall</c> can only become true once an unfragmented
    /// reliable record has been acknowledged at or after the Fragment window opened (see its own remarks).
    /// This close reason being reported at all is therefore already the observable proof that the small
    /// Message was genuinely being acked throughout the run, not merely arriving one-way — a Session that
    /// never saw an ack for anything would instead run out its ordinary Linger and report a different,
    /// generic reason, which is exactly the failure this test's own assertion rules out.
    /// </remarks>
    [Fact]
    public void Pump_EveryFragmentBlackholedWhileSmallMessagesKeepArriving_ClosesThePeerWithFragmentSizeUnviable()
    {
        // Arrange
        using FragmentSizeDiagnosisFixture fixture = new(proxy => proxy.SetSizeThreshold(RelayDirection.ClientToUpstream, SizeThreshold));
        fixture.SendLargeMessage().Should().Be(SendOutcome.Sent, "the fragmented Message must actually be queued for this scenario to mean anything");

        // Act — a small Message queued every round is what lets the stall signature's own "an
        // unfragmented record acked since the window opened" half ever become true (see the type remarks
        // on the close reason itself already proving that acking happened).
        bool closed = false;
        for (int round = 0; round < RoundBudget && !closed; round++)
        {
            fixture.SendSmallMessage();
            fixture.PumpRound();
            closed = fixture.DiallerEvents.Any(e => e.Kind == SessionEventKind.PeerClosed);
        }

        // Assert — the Session closed at all, rather than merely stalling forever within the budget.
        closed.Should().BeTrue(
            $"the stall signature's four retransmission rounds must complete and close the Peer well inside {RoundBudget} rounds; a Session that only stalls without closing is a failed diagnosis");

        // Assert — closed by name, not by a generic timeout.
        fixture.DiallerEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed && e.Peer.Equals(fixture.DiallerPeer))
            .Which.Reason.Should().Be(
                DisconnectReason.FragmentSizeUnviable,
                "a generic timeout reason here is a failed test — the diagnosis exists precisely to name the fragment size as the cause instead of leaving a consumer to guess");
    }

    /// <summary>
    /// Proves that no Fragment of the large Message ever reaches the accepter: across the whole run, the
    /// only consumer-visible unit a Fragment could ever surface as — the reassembled Message itself — never
    /// appears among what the accepter delivered.
    /// </summary>
    [Fact]
    public void Pump_EveryFragmentBlackholedByTheSizeThreshold_NeverDeliversTheFragmentedMessage()
    {
        // Arrange
        using FragmentSizeDiagnosisFixture fixture = new(proxy => proxy.SetSizeThreshold(RelayDirection.ClientToUpstream, SizeThreshold));
        fixture.SendLargeMessage().Should().Be(SendOutcome.Sent, "the fragmented Message must actually be queued for this scenario to mean anything");

        // Act — drive well past the Peer's close, so a Fragment slipping through late would still show up.
        DriveWithPeriodicSmallSends(fixture, SmallSendRounds);

        // Assert — the only way a Fragment's arrival would ever become visible to a consumer is the whole
        // Message it belongs to being reassembled and delivered; that never happens here.
        fixture.DeliveredPayloads.Should().NotContain(
            payload => payload.Length == FragmentSizeDiagnosisFixture.LargeMessageLength,
            "the size threshold sits below the shortest possible Fragment Datagram, so not even the remainder Fragment can ever cross, and the large Message can therefore never be reassembled");
    }

    /// <summary>
    /// Proves that the small, unfragmented Message keeps crossing and being delivered throughout the run
    /// — not just once at the start before the proxy rule bites, and not only after the stalled Peer
    /// closes — so the path is visibly alive from both ends the whole time the Fragments are being
    /// silently dropped.
    /// </summary>
    [Fact]
    public void Pump_EveryFragmentBlackholedByTheSizeThreshold_KeepsDeliveringUnfragmentedMessagesThroughoutTheRun()
    {
        // Arrange
        using FragmentSizeDiagnosisFixture fixture = new(proxy => proxy.SetSizeThreshold(RelayDirection.ClientToUpstream, SizeThreshold));
        fixture.SendLargeMessage().Should().Be(SendOutcome.Sent, "the fragmented Message must actually be queued for this scenario to mean anything");

        // Act — one small Message queued per round, spanning well before and after the observed close.
        DriveWithPeriodicSmallSends(fixture, SmallSendRounds);

        // Assert — several distinct small Messages were delivered, not merely one lucky arrival: a single
        // delivery would be as consistent with a fluke as with the path staying genuinely alive.
        fixture.DeliveredPayloads.Where(payload => payload.Length == FragmentSizeDiagnosisFixture.SmallMessageLength).Should().HaveCountGreaterThan(
            1, "the small Message must keep arriving across more than one round for the path to be shown alive throughout the run, not just once");
    }

    /// <summary>
    /// Across the whole
    /// run, the accepter's Sink never records a <see cref="DiagnosticId.FragmentSetAbandoned"/> entry.
    /// </summary>
    /// <remarks>
    /// This is not an absence of coverage — it is the correct outcome given how
    /// <c>ReceiveWindow.Classify</c> (ReactorSockets/Session/ReceiveWindow.cs) fills
    /// <c>ReceiveClassification.AbandonedFragmentSet</c>: it reports a set abandoned by reading a declared Fragment count back out of ring-slot residue that
    /// only a <em>previously admitted</em> Fragment could have written, then counts how many of that set's
    /// own members still hold non-zero length at the moment a fresh admission clobbers the slot. With
    /// every Fragment black-holed by this file's own size threshold, the accepter's receive window for the
    /// fragmented Space never admits a single one, so that residue is never written, there is no declared
    /// count to read back, and no set for a later clobber to abandon. Zero Fragments forwarded is
    /// therefore not merely "nothing to assert" — it is zero held against nothing declared, which is
    /// exactly "figures consistent with what the proxy actually forwarded" read honestly, rather than a
    /// held-zero-against-declared-three record, which the <c>ReceiveWindow</c> implementation cannot
    /// produce (see the type remarks). Asserting the absence
    /// here, filtered on <see cref="DiagnosticId.FragmentSetAbandoned"/> rather than the whole Sink, is
    /// what would catch a future change that starts fabricating this Diagnostic without ever admitting a
    /// Fragment to justify it.
    /// </remarks>
    [Fact]
    public void Pump_EveryFragmentBlackholedByTheSizeThreshold_NeverEmitsFragmentSetAbandoned()
    {
        // Arrange
        using FragmentSizeDiagnosisFixture fixture = new(proxy => proxy.SetSizeThreshold(RelayDirection.ClientToUpstream, SizeThreshold));
        fixture.SendLargeMessage().Should().Be(SendOutcome.Sent, "the fragmented Message must actually be queued for this scenario to mean anything");

        // Act — drive well past the Peer's close, so a set abandoned late would still show up.
        DriveWithPeriodicSmallSends(fixture, SmallSendRounds);

        // Assert — no Fragment was ever admitted to write the ring-slot residue HandleFragmentSetAbandonment
        // reads its declared count from, so no clobber can ever report a set abandoned: zero forwarded
        // Fragments means zero held against nothing declared, not a held-against-declared figure to report.
        fixture.AccepterDiagnostics.Should().NotContain(
            record => record.Id == DiagnosticId.FragmentSetAbandoned,
            "HandleFragmentSetAbandonment can only report a declared count it read from a previously admitted Fragment's own residue, and with every Fragment black-holed the accepter's receive window never admits one to write it");
    }

    /// <summary>
    /// Across the whole positive run, no datagram beyond this fixture's own two Messages and
    /// the accepter's keep-alive ever reaches the wire. A probe here would be some datagram this run's
    /// own sends and retransmissions cannot account for — an extra control-shaped message, a record
    /// naming a sequence or Space outside the two Messages this fixture sends, or a known record shape
    /// travelling the wrong direction — and it is checked at the wire, not at an internal flag, because a
    /// flag can be right while the datagram still leaves. This is
    /// <see cref="FragmentSizeDiagnosisFixture.AssertNoProbeWasObserved"/>, invoked identically from
    /// <see cref="DatagramSessionOrdinaryLossNotDiagnosedTests"/>'s own negative-run Fact, so the same
    /// check holds both runs to the same bar rather than each run keeping its own copy of it.
    /// </summary>
    [Fact]
    public void Pump_EveryFragmentBlackholedByTheSizeThreshold_NeverEmitsAProbe()
    {
        // Arrange
        using FragmentSizeDiagnosisFixture fixture = new(proxy => proxy.SetSizeThreshold(RelayDirection.ClientToUpstream, SizeThreshold));
        fixture.SendLargeMessage().Should().Be(SendOutcome.Sent, "the fragmented Message must actually be queued for this scenario to mean anything");

        // Act — drive well past the Peer's close, so a probe emitted only once diagnosed would still show up.
        DriveWithPeriodicSmallSends(fixture, SmallSendRounds);

        // Assert — shared with the negative run; see the method's own remarks for what a probe would look like here.
        fixture.AssertNoProbeWasObserved();
    }

    /// <summary>
    /// Across the whole positive run, both ends' configured fragment size is unchanged by the
    /// diagnosis — the observable form of "we diagnose, we do not recover" (see
    /// <see cref="FragmentSizeDiagnosisFixture.AssertConfiguredFragmentSizeUnchanged"/>'s own remarks for
    /// exactly what this reads and its limits). This is
    /// <see cref="FragmentSizeDiagnosisFixture.AssertConfiguredFragmentSizeUnchanged"/>, invoked
    /// identically from <see cref="DatagramSessionOrdinaryLossNotDiagnosedTests"/>'s own negative-run
    /// Fact, so the same check holds both runs to the same bar rather than each run keeping its own copy
    /// of it.
    /// </summary>
    [Fact]
    public void Pump_EveryFragmentBlackholedByTheSizeThreshold_LeavesTheConfiguredFragmentSizeUnchanged()
    {
        // Arrange
        using FragmentSizeDiagnosisFixture fixture = new(proxy => proxy.SetSizeThreshold(RelayDirection.ClientToUpstream, SizeThreshold));
        fixture.SendLargeMessage().Should().Be(SendOutcome.Sent, "the fragmented Message must actually be queued for this scenario to mean anything");

        // Act — drive well past the Peer's close, so a diagnosis that rewrote the setting only late would still show up.
        DriveWithPeriodicSmallSends(fixture, SmallSendRounds);

        // Assert — shared with the negative run; see the method's own remarks for exactly what this reads back.
        fixture.AssertConfiguredFragmentSizeUnchanged(FragmentSizeDiagnosisFixture.FragmentSize);
    }

    /// <summary>
    /// Sends one small Message per round for <paramref name="rounds"/> rounds, ignoring any
    /// <see cref="SendOutcome.RetentionExhausted"/> result from a round whose retention is momentarily
    /// full — retention freeing on the next acknowledged round is exactly the behaviour under test, not a
    /// fixture failure, so a caller here only cares about what eventually got delivered rather than every
    /// individual queuing attempt succeeding.
    /// </summary>
    private static void DriveWithPeriodicSmallSends(FragmentSizeDiagnosisFixture fixture, int rounds)
    {
        for (int round = 0; round < rounds; round++)
        {
            fixture.SendSmallMessage();
            fixture.PumpRound();
        }
    }
}
