using System.Diagnostics;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// <see cref="FragmentGroup"/> is pure arithmetic over a Fragment's own sequence and index — extracted
/// for the same reason <see cref="FragmentPlan"/> is (ADR 0015, issue #254): these tests pin
/// <see cref="FragmentGroup.FirstSequenceOf"/> directly, including the <c>u32</c>-wrap identity that
/// <c>Pump_HostileFragmentGroupNonContiguousAcrossTheSequenceWrap_DiscardsWholeDatagramAndLeavesTheWindowUntouched</c>
/// (<see cref="DatagramSessionHostileFragmentDiscardTests"/>) only exercised indirectly.
///
/// <see cref="FragmentGroup.AssertContiguousSet"/> is pinned here too (issue #319): the send path only
/// ever hands it a set it just built correctly, so the gap and length-mismatch it guards against are
/// unreachable through <see cref="DatagramSessionReactor.Send"/> at any cost, and ADR 0015 pins them
/// directly instead. <see cref="Debug.Assert(bool, string)"/> failure does not throw by default — it
/// reports to <see cref="Trace.Listeners"/> and returns — so these tests install a
/// <see cref="ThrowingTraceListener"/> for the call and restore the original listeners afterwards.
/// </summary>
public class FragmentGroupTests
{
    /// <summary>
    /// Runs <paramref name="action"/> with <see cref="Trace.Listeners"/> replaced by one that turns a
    /// failed <see cref="Debug.Assert(bool, string)"/> into a thrown <see cref="DebugAssertFailedException"/>,
    /// restoring the original listeners afterwards regardless of outcome.
    /// </summary>
    private static void RunWithAssertFailuresThrowing(Action action)
    {
        TraceListener[] originalListeners = new TraceListener[Trace.Listeners.Count];
        Trace.Listeners.CopyTo(originalListeners, 0);
        Trace.Listeners.Clear();
        Trace.Listeners.Add(new ThrowingTraceListener());

        try
        {
            action();
        }
        finally
        {
            Trace.Listeners.Clear();
            Trace.Listeners.AddRange(originalListeners);
        }
    }

    /// <summary>Thrown by <see cref="ThrowingTraceListener"/> in place of the default assert-failure UI.</summary>
    private sealed class DebugAssertFailedException(string message) : Exception(message);

    /// <summary>Converts a failed <see cref="Trace.Fail(string, string)"/> into a thrown exception
    /// instead of the default listener's UI or console report, so a test can observe the failure.</summary>
    private sealed class ThrowingTraceListener : TraceListener
    {
        public override void Fail(string? message, string? detailMessage) =>
            throw new DebugAssertFailedException($"{message} {detailMessage}");

        public override void Write(string? message) { }

        public override void WriteLine(string? message) { }
    }

    [Fact]
    public void AssertContiguousSet_SequencesWithAGap_TripsTheContiguityAssertion()
    {
        // Arrange — three sequences claimed as one group, but the middle one skips 102.
        uint[] sequencesWithAGap = [100, 101, 103];

        // Act
        Action act = () => RunWithAssertFailuresThrowing(
            () => FragmentGroup.AssertContiguousSet(sequencesWithAGap, declaredFragmentCount: 3));

        // Assert
        act.Should().Throw<DebugAssertFailedException>()
            .WithMessage("*contiguous*");
    }

    [Fact]
    public void AssertContiguousSet_SetLengthDisagreesWithDeclaredCount_TripsTheLengthAssertion()
    {
        // Arrange — a contiguous set of 3, but the caller claims only 2 on the wire.
        uint[] contiguousSequences = [100, 101, 102];

        // Act
        Action act = () => RunWithAssertFailuresThrowing(
            () => FragmentGroup.AssertContiguousSet(contiguousSequences, declaredFragmentCount: 2));

        // Assert
        act.Should().Throw<DebugAssertFailedException>()
            .WithMessage("*fragment count*");
    }

    [Fact]
    public void FirstSequenceOf_IndexZero_IsTheSequenceItself()
    {
        // Arrange — a piece at fragment index 0 is, by definition, its own group's first sequence.
        uint sequence = 500;
        ushort fragmentIndex = 0;

        // Act
        uint firstSequence = FragmentGroup.FirstSequenceOf(sequence, fragmentIndex);

        // Assert
        firstSequence.Should().Be(500);
    }

    [Fact]
    public void FirstSequenceOf_SetAwayFromTheWrap_EveryPieceNamesTheSameFirstSequence()
    {
        // Arrange — a 4-piece group starting well clear of the u32 wrap.
        uint firstSequence = 10_000;
        ushort[] fragmentIndices = [0, 1, 2, 3];

        // Act — each piece's own sequence and index recovers the group's first sequence.
        uint[] resolved = fragmentIndices
            .Select(fragmentIndex => FragmentGroup.FirstSequenceOf(firstSequence + fragmentIndex, fragmentIndex))
            .ToArray();

        // Assert
        resolved.Should().AllBeEquivalentTo(firstSequence);
    }

    [Fact]
    public void FirstSequenceOf_SetStraddlingTheSequenceWrap_EveryPieceNamesTheSameFirstSequenceDespiteWrappingBackToSmallNumbers()
    {
        // Arrange — a group whose first sequence sits near uint.MaxValue, so its later pieces wrap
        // back around to small numbers rather than overflowing.
        uint firstSequence = uint.MaxValue - 1;
        uint[] sequencesInFragmentIndexOrder =
        [
            unchecked(firstSequence + 0), // uint.MaxValue - 1
            unchecked(firstSequence + 1), // uint.MaxValue
            unchecked(firstSequence + 2), // 0 — wrapped
            unchecked(firstSequence + 3), // 1 — wrapped
        ];

        // Act — naive (non-wrap-aware) subtraction would corrupt the wrapped pieces' answers;
        // unchecked u32 subtraction should recover the same first sequence for every one of them.
        uint[] resolved = sequencesInFragmentIndexOrder
            .Select((sequence, fragmentIndex) => FragmentGroup.FirstSequenceOf(sequence, (ushort)fragmentIndex))
            .ToArray();

        // Assert
        resolved.Should().AllBeEquivalentTo(firstSequence);
    }
}
