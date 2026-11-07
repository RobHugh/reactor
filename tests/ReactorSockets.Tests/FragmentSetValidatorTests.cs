using FluentAssertions;
using Xunit;
using static ReactorSockets.Tests.PacketRecordBytes;

namespace ReactorSockets.Tests;

/// <summary>
/// <see cref="FragmentSetValidator"/> is pure over one record body, public for the same reason
/// <see cref="FragmentGroup"/> is (ADR 0015). These tests hand it record bytes built independently of the
/// production codec, which is internal. The reactor-level refusal of the same forgeries is pinned in
/// <see cref="DatagramSessionHostileFragmentDiscardTests"/>.
/// </summary>
public class FragmentSetValidatorTests
{
    [Fact]
    public void AreWellFormed_EmptyBody_ReturnsTrue()
    {
        // Arrange
        FragmentSetValidator validator = new();

        // Act
        bool wellFormed = validator.AreWellFormed([]);

        // Assert
        wellFormed.Should().BeTrue();
    }

    [Fact]
    public void AreWellFormed_OnlyUnfragmentedRecords_ReturnsTrue()
    {
        // Arrange
        FragmentSetValidator validator = new();
        byte[] body = [.. Record(ModeUnreliable, 1, [0x01]), .. Record(ModeReliableOrdered, 1, [0x01])];

        // Act
        bool wellFormed = validator.AreWellFormed(body);

        // Assert
        wellFormed.Should().BeTrue();
    }

    [Fact]
    public void AreWellFormed_PartialGroupWithAGapInItsIndices_ReturnsTrue()
    {
        // Arrange — selective retransmission packs indices 0 and 2 of a three-Fragment set together.
        FragmentSetValidator validator = new();
        byte[] body =
        [
            .. Fragment(ModeReliableOrdered, sequence: 10, index: 0, count: 3, payload: [0x01]),
            .. Fragment(ModeReliableOrdered, sequence: 12, index: 2, count: 3, payload: [0x01]),
        ];

        // Act
        bool wellFormed = validator.AreWellFormed(body);

        // Assert
        wellFormed.Should().BeTrue();
    }

    [Fact]
    public void AreWellFormed_GroupMembersDisagreeOnTheirCount_ReturnsFalse()
    {
        // Arrange
        FragmentSetValidator validator = new();
        byte[] body =
        [
            .. Fragment(ModeReliableOrdered, sequence: 10, index: 0, count: 3, payload: [0x01]),
            .. Fragment(ModeReliableOrdered, sequence: 11, index: 1, count: 4, payload: [0x01]),
        ];

        // Act
        bool wellFormed = validator.AreWellFormed(body);

        // Assert
        wellFormed.Should().BeFalse();
    }

    [Fact]
    public void AreWellFormed_GroupRepeatsAnIndex_ReturnsFalse()
    {
        // Arrange
        FragmentSetValidator validator = new();
        byte[] body =
        [
            .. Fragment(ModeReliableOrdered, sequence: 10, index: 0, count: 3, payload: [0x01]),
            .. Fragment(ModeReliableOrdered, sequence: 10, index: 0, count: 3, payload: [0x01]),
        ];

        // Act
        bool wellFormed = validator.AreWellFormed(body);

        // Assert
        wellFormed.Should().BeFalse();
    }

    [Fact]
    public void AreWellFormed_SameModeDeclaredRangesOverlapWithNoObservedCollision_ReturnsFalse()
    {
        // Arrange — 100..104 and 103..104 both claim 104, though only 100, 101 and 103 are present.
        FragmentSetValidator validator = new();
        byte[] body =
        [
            .. Fragment(ModeReliableOrdered, sequence: 100, index: 0, count: 5, payload: [0x01]),
            .. Fragment(ModeReliableOrdered, sequence: 101, index: 1, count: 5, payload: [0x01]),
            .. Fragment(ModeReliableOrdered, sequence: 103, index: 0, count: 2, payload: [0x01]),
        ];

        // Act
        bool wellFormed = validator.AreWellFormed(body);

        // Assert
        wellFormed.Should().BeFalse();
    }

    [Fact]
    public void AreWellFormed_AdjacentSameModeGroups_ReturnsTrue()
    {
        // Arrange — 100..101 and 102..103 touch but share no sequence.
        FragmentSetValidator validator = new();
        byte[] body =
        [
            .. Fragment(ModeReliableOrdered, sequence: 100, index: 0, count: 2, payload: [0x01]),
            .. Fragment(ModeReliableOrdered, sequence: 102, index: 0, count: 2, payload: [0x01]),
        ];

        // Act
        bool wellFormed = validator.AreWellFormed(body);

        // Assert
        wellFormed.Should().BeTrue();
    }

    [Fact]
    public void AreWellFormed_SameFirstSequenceInTwoModes_ReturnsTrue()
    {
        // Arrange — each mode is its own Space, so both claiming index 0 at sequence 100 is no conflict.
        FragmentSetValidator validator = new();
        byte[] body =
        [
            .. Fragment(ModeReliableOrdered, sequence: 100, index: 0, count: 2, payload: [0x01]),
            .. Fragment(ModeReliableUnordered, sequence: 100, index: 0, count: 3, payload: [0x01]),
        ];

        // Act
        bool wellFormed = validator.AreWellFormed(body);

        // Assert
        wellFormed.Should().BeTrue();
    }

    [Fact]
    public void AreWellFormed_OverlapAcrossTheWrapBesideADecoyGroup_ReturnsFalse()
    {
        // Arrange — 0xFFFFFFFE..1 overlaps 0..1. The decoy at 0x10 makes 0 the numerically smallest
        // first sequence, so a sweep anchored there would sort the overlapping pair to opposite ends.
        FragmentSetValidator validator = new();
        byte[] body =
        [
            .. Fragment(ModeReliableOrdered, sequence: 0xFFFF_FFFE, index: 0, count: 4, payload: [0x01]),
            .. Fragment(ModeReliableOrdered, sequence: 0, index: 0, count: 2, payload: [0x01]),
            .. Fragment(ModeReliableOrdered, sequence: 0x10, index: 0, count: 2, payload: [0x01]),
        ];

        // Act
        bool wellFormed = validator.AreWellFormed(body);

        // Assert
        wellFormed.Should().BeFalse();
    }

    [Fact]
    public void AreWellFormed_SameModeGroupsSpanningHalfTheSequenceSpace_ReturnsFalse()
    {
        // Arrange
        FragmentSetValidator validator = new();
        byte[] body =
        [
            .. Fragment(ModeReliableOrdered, sequence: 0, index: 0, count: 2, payload: [0x01]),
            .. Fragment(ModeReliableOrdered, sequence: 0x8000_0000, index: 0, count: 2, payload: [0x01]),
        ];

        // Act
        bool wellFormed = validator.AreWellFormed(body);

        // Assert
        wellFormed.Should().BeFalse();
    }

    [Fact]
    public void AreWellFormed_RecordThatDoesNotParse_ReturnsFalse()
    {
        // Arrange — a declared count of zero is refused by the record codec.
        FragmentSetValidator validator = new();
        byte[] body = Fragment(ModeReliableOrdered, sequence: 10, index: 0, count: 0, payload: [0x01]);

        // Act
        bool wellFormed = validator.AreWellFormed(body);

        // Assert
        wellFormed.Should().BeFalse();
    }

    [Fact]
    public void AreWellFormed_BodyLongerThanMaxBodySize_ReturnsFalse()
    {
        // Arrange
        FragmentSetValidator validator = new();
        byte[] body = new byte[FragmentSetValidator.MaxBodySize + 1];

        // Act
        bool wellFormed = validator.AreWellFormed(body);

        // Assert
        wellFormed.Should().BeFalse();
    }

    [Fact]
    public void AreWellFormed_ForgedBodyAfterAWellFormedOne_JudgesEachBodyOnItsOwn()
    {
        // Arrange — the scratch is reused, so a clean body must not leave state that clears a later forgery.
        FragmentSetValidator validator = new();
        byte[] clean =
        [
            .. Fragment(ModeReliableOrdered, sequence: 10, index: 0, count: 2, payload: [0x01]),
            .. Fragment(ModeReliableOrdered, sequence: 11, index: 1, count: 2, payload: [0x01]),
        ];
        byte[] forged =
        [
            .. Fragment(ModeReliableOrdered, sequence: 10, index: 0, count: 2, payload: [0x01]),
            .. Fragment(ModeReliableOrdered, sequence: 10, index: 0, count: 2, payload: [0x01]),
        ];
        validator.AreWellFormed(clean);

        // Act
        bool wellFormed = validator.AreWellFormed(forged);

        // Assert
        wellFormed.Should().BeFalse();
    }
}
