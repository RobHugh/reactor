using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// <see cref="FragmentGroup"/> is pure arithmetic over a Fragment's own sequence and index — extracted
/// for the same reason <see cref="FragmentPlan"/> is (ADR 0015, issue #254): these tests pin
/// <see cref="FragmentGroup.FirstSequenceOf"/> directly, including the <c>u32</c>-wrap identity that
/// <c>Pump_HostileFragmentGroupNonContiguousAcrossTheSequenceWrap_DiscardsWholeDatagramAndLeavesTheWindowUntouched</c>
/// (<see cref="DatagramSessionHostileFragmentDiscardTests"/>) only exercised indirectly.
/// </summary>
public class FragmentGroupTests
{
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
