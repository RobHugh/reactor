using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// <see cref="FragmentStallSignature"/> holds one Peer's raw material for the fragment-size stall
/// predicate (issue #269), so these tests construct it directly and drive its feeds by hand — the
/// type is public precisely so this can be pinned without an <c>InternalsVisibleTo</c> (ADR 0015), the
/// same reason <see cref="RetransmissionTimer"/> and <see cref="PumpIntervalObserver"/> are. Issue #301
/// covers the predicate itself: the one case it must fire, and the quiet cases — an acked Fragment,
/// nothing acked at all, and short of the round count — it must never fire in.
/// </summary>
public class FragmentStallSignatureTests
{
    [Fact]
    public void IsFragmentSizeStall_UnfragmentedAcksWithNoFragmentAckedPastStallRoundCount_IsTrue()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        FragmentStallSignature signature = new(timeProvider);
        signature.RecordFragmentSent();
        signature.RecordUnfragmentedRecordAcked();

        // Act
        for (int round = 0; round < FragmentStallSignature.StallRoundCount; round++)
        {
            timeProvider.Advance(TimeSpan.FromMilliseconds(10));
            signature.RecordRetransmissionRound();
        }

        // Assert
        signature.IsFragmentSizeStall.Should().BeTrue();
    }

    [Fact]
    public void IsFragmentSizeStall_AnyFragmentEverAcked_IsFalseHoweverManyRoundsFollow()
    {
        // Arrange — the path has proven it can carry a Fragment, so silence afterwards is not the size
        // boundary the signature looks for, no matter how much else is subsequently lost.
        FakeTimeProvider timeProvider = new();
        FragmentStallSignature signature = new(timeProvider);
        signature.RecordFragmentSent();
        signature.RecordUnfragmentedRecordAcked();
        signature.RecordFragmentAcked();

        // Act — a fresh Fragment window opens and goes unacked for far more than StallRoundCount rounds.
        signature.RecordFragmentSent();
        for (int round = 0; round < FragmentStallSignature.StallRoundCount * 3; round++)
        {
            timeProvider.Advance(TimeSpan.FromMilliseconds(10));
            signature.RecordUnfragmentedRecordAcked();
            signature.RecordRetransmissionRound();
        }

        // Assert
        signature.IsFragmentSizeStall.Should().BeFalse();
    }

    [Fact]
    public void IsFragmentSizeStall_NothingAtAllIsBeingAcked_IsFalse()
    {
        // Arrange — a dead path looks the same as a size boundary until an unfragmented ack proves the
        // Peer is reachable; that proof is exactly what is missing here, so this must stay quiet and
        // leave the diagnosis to the Session's own liveness machinery.
        FakeTimeProvider timeProvider = new();
        FragmentStallSignature signature = new(timeProvider);
        signature.RecordFragmentSent();

        // Act
        for (int round = 0; round < FragmentStallSignature.StallRoundCount * 3; round++)
        {
            timeProvider.Advance(TimeSpan.FromMilliseconds(10));
            signature.RecordRetransmissionRound();
        }

        // Assert
        signature.IsFragmentSizeStall.Should().BeFalse();
    }

    [Fact]
    public void IsFragmentSizeStall_OneRoundShortOfStallRoundCountAndThenAtIt_IsFalseThenTrue()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        FragmentStallSignature signature = new(timeProvider);
        signature.RecordFragmentSent();
        signature.RecordUnfragmentedRecordAcked();

        // Act — one round short of the threshold.
        for (int round = 0; round < FragmentStallSignature.StallRoundCount - 1; round++)
        {
            timeProvider.Advance(TimeSpan.FromMilliseconds(10));
            signature.RecordRetransmissionRound();
        }

        // Assert — ordinary loss still in flight, not yet the signature.
        signature.IsFragmentSizeStall.Should().BeFalse();

        // Act — the round that reaches the threshold.
        timeProvider.Advance(TimeSpan.FromMilliseconds(10));
        signature.RecordRetransmissionRound();

        // Assert
        signature.IsFragmentSizeStall.Should().BeTrue();
    }

    [Fact]
    public void IsFragmentSizeStall_AfterALateAckedFragmentAndFurtherUnackedRounds_StaysFalse()
    {
        // Arrange — reach the brink of firing, then a Fragment is acked late.
        FakeTimeProvider timeProvider = new();
        FragmentStallSignature signature = new(timeProvider);
        signature.RecordFragmentSent();
        signature.RecordUnfragmentedRecordAcked();
        for (int round = 0; round < FragmentStallSignature.StallRoundCount - 1; round++)
        {
            timeProvider.Advance(TimeSpan.FromMilliseconds(10));
            signature.RecordRetransmissionRound();
        }
        signature.RecordFragmentAcked();

        // Act — further rounds, plus a fresh unacked Fragment window, past the round count again.
        for (int round = 0; round < FragmentStallSignature.StallRoundCount * 3; round++)
        {
            timeProvider.Advance(TimeSpan.FromMilliseconds(10));
            signature.RecordRetransmissionRound();
        }
        signature.RecordFragmentSent();
        signature.RecordUnfragmentedRecordAcked();
        for (int round = 0; round < FragmentStallSignature.StallRoundCount * 3; round++)
        {
            timeProvider.Advance(TimeSpan.FromMilliseconds(10));
            signature.RecordRetransmissionRound();
        }

        // Assert — the latch left behind by the late ack means this can never fire again for this Peer.
        signature.IsFragmentSizeStall.Should().BeFalse();
    }

    [Fact]
    public void IsFragmentSizeStall_UnfragmentedAckPredatingTheFragmentWindow_IsFalse()
    {
        // Arrange — the ack must be at or after the window opened; one that predates it does not prove
        // the path is still carrying traffic for this window.
        FakeTimeProvider timeProvider = new();
        FragmentStallSignature signature = new(timeProvider);
        signature.RecordUnfragmentedRecordAcked();
        timeProvider.Advance(TimeSpan.FromMilliseconds(10));
        signature.RecordFragmentSent();

        // Act
        for (int round = 0; round < FragmentStallSignature.StallRoundCount; round++)
        {
            timeProvider.Advance(TimeSpan.FromMilliseconds(10));
            signature.RecordRetransmissionRound();
        }

        // Assert
        signature.IsFragmentSizeStall.Should().BeFalse();
    }

    [Fact]
    public void RecordRetransmissionRound_BeforeAnyFragmentSent_CountsNothing()
    {
        // Arrange
        FragmentStallSignature signature = new(new FakeTimeProvider());

        // Act
        signature.RecordRetransmissionRound();

        // Assert
        signature.RoundsSinceFirstUnackedFragment.Should().Be(0);
    }

    [Fact]
    public void RecordFragmentAcked_AfterSeveralRounds_ClearsTheRoundCountAndTheWindow()
    {
        // Arrange
        FakeTimeProvider timeProvider = new();
        FragmentStallSignature signature = new(timeProvider);
        signature.RecordFragmentSent();
        for (int round = 0; round < 2; round++)
        {
            timeProvider.Advance(TimeSpan.FromMilliseconds(10));
            signature.RecordRetransmissionRound();
        }

        // Act
        signature.RecordFragmentAcked();

        // Assert
        signature.HasAckedFragment.Should().BeTrue();
        signature.FirstUnackedFragmentSentTimestamp.Should().BeNull();
        signature.RoundsSinceFirstUnackedFragment.Should().Be(0);
    }

    [Fact]
    public void RecordFragmentSent_CalledAgainWhileAWindowIsAlreadyOpen_LeavesTheOriginalTimestamp()
    {
        // Arrange — a retransmit of a Fragment already sent must not reopen the window.
        FakeTimeProvider timeProvider = new();
        FragmentStallSignature signature = new(timeProvider);
        signature.RecordFragmentSent();
        long? firstSent = signature.FirstUnackedFragmentSentTimestamp;

        // Act
        timeProvider.Advance(TimeSpan.FromMilliseconds(50));
        signature.RecordFragmentSent();

        // Assert
        signature.FirstUnackedFragmentSentTimestamp.Should().Be(firstSent);
    }
}
