using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #258: <see cref="FragmentGroupCompleteness"/> is pure arithmetic over a window's own state —
/// capacity, base, base's ring slot, and its Fragment-length run — extracted for the same reason
/// <see cref="FragmentPlan"/> is (ADR 0015): no <see cref="ReceiveWindow"/>, Peer or Reactor is needed
/// to pin the ring-wrap and <c>u32</c>-wrap boundary conditions, so these tests build the length run
/// by hand and call <see cref="FragmentGroupCompleteness.IsComplete"/> and
/// <see cref="FragmentGroupCompleteness.TryGetRingSlot"/> directly.
/// </summary>
public class FragmentGroupCompletenessTests
{
    /// <summary>Builds an all-zero Fragment-length run of <paramref name="capacity"/> slots, two bytes each.</summary>
    private static byte[] NewLengthRun(int capacity) => new byte[capacity * 2];

    /// <summary>Builds an all-zero admission run of <paramref name="capacity"/> slots, one byte each.</summary>
    private static byte[] NewAdmittedRun(int capacity) => new byte[capacity];

    /// <summary>
    /// Marks ring slot <paramref name="ringSlot"/> as holding a Fragment of <paramref name="byteLength"/>
    /// bytes, and — since every sequence these tests hold is still at or ahead of the window's base —
    /// also marks it admitted, exactly as <see cref="ReceiveWindow.Classify(uint, out int)"/> would for
    /// a piece still ahead of a gap (issue #259: <see cref="FragmentGroupCompleteness.IsComplete"/>
    /// requires the admission bit too for a forward-of-base piece, to tell it apart from stale residue
    /// left by an abandoned occupant of the same physical slot).
    /// </summary>
    private static void Hold(byte[] lengthRun, byte[] admittedRun, int ringSlot, ushort byteLength)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(lengthRun.AsSpan(ringSlot * 2, 2), byteLength);
        admittedRun[ringSlot] = 1;
    }

    [Fact]
    public void IsComplete_FragmentsArriveInReverseOrder_ReportsIncompleteUntilTheLastOneLandsThenComplete()
    {
        // Arrange — a fresh window, base at 0, ring slot 0 is base's own slot, a 3-piece group
        // starting at sequence 0 so its ring slots are 0, 1, 2 in fragment-index order.
        const int capacity = 8;
        byte[] lengths = NewLengthRun(capacity);
        byte[] admitted = NewAdmittedRun(capacity);
        const uint groupFirstSequence = 0;
        const ushort fragmentCount = 3;

        // Act, Assert — index 2 lands first: still incomplete, since 0 and 1 are missing.
        Hold(lengths, admitted, ringSlot: 2, byteLength: 10);
        FragmentGroupCompleteness.IsComplete(admitted, lengths, capacity, windowBase: 0, baseSlot: 0, groupFirstSequence, fragmentCount)
            .Should().BeFalse("indices 0 and 1 have not arrived yet");

        // Act, Assert — index 1 lands next: still incomplete, since 0 is still missing.
        Hold(lengths, admitted, ringSlot: 1, byteLength: 10);
        FragmentGroupCompleteness.IsComplete(admitted, lengths, capacity, windowBase: 0, baseSlot: 0, groupFirstSequence, fragmentCount)
            .Should().BeFalse("index 0 has not arrived yet");

        // Act, Assert — index 0 lands last, completing the set.
        Hold(lengths, admitted, ringSlot: 0, byteLength: 10);
        FragmentGroupCompleteness.IsComplete(admitted, lengths, capacity, windowBase: 0, baseSlot: 0, groupFirstSequence, fragmentCount)
            .Should().BeTrue("every one of the group's three pieces is now held");
    }

    [Fact]
    public void IsComplete_AGroupOnlyPartlyPresent_ReportsIncomplete()
    {
        // Arrange — a 4-piece group with a hole at index 2.
        const int capacity = 8;
        byte[] lengths = NewLengthRun(capacity);
        byte[] admitted = NewAdmittedRun(capacity);
        Hold(lengths, admitted, ringSlot: 0, byteLength: 5);
        Hold(lengths, admitted, ringSlot: 1, byteLength: 5);
        Hold(lengths, admitted, ringSlot: 3, byteLength: 5);

        // Act
        bool complete = FragmentGroupCompleteness.IsComplete(
            admitted, lengths, capacity, windowBase: 0, baseSlot: 0, groupFirstSequence: 0, fragmentCount: 4);

        // Assert
        complete.Should().BeFalse("index 2 was never held");
    }

    [Fact]
    public void IsComplete_AGroupStraddlingTheWindowsRingWrap_IsDetectedAsComplete()
    {
        // Arrange — capacity 4, base at sequence 10 occupying ring slot 2, so ring slots run
        // 2, 3, 0, 1 for sequences 10, 11, 12, 13 — the group wraps the physical ring exactly once.
        const int capacity = 4;
        const uint windowBase = 10;
        const int baseSlot = 2;
        byte[] lengths = NewLengthRun(capacity);
        byte[] admitted = NewAdmittedRun(capacity);

        Hold(lengths, admitted, ringSlot: 2, byteLength: 1); // sequence 10
        Hold(lengths, admitted, ringSlot: 3, byteLength: 1); // sequence 11
        Hold(lengths, admitted, ringSlot: 0, byteLength: 1); // sequence 12
        Hold(lengths, admitted, ringSlot: 1, byteLength: 1); // sequence 13

        // Act
        bool complete = FragmentGroupCompleteness.IsComplete(
            admitted, lengths, capacity, windowBase, baseSlot, groupFirstSequence: windowBase, fragmentCount: 4);

        // Assert
        complete.Should().BeTrue("every ring slot the group wraps through holds a Fragment");
    }

    [Fact]
    public void IsComplete_AGroupStraddlingTheRingWrapMissingOnePiece_ReportsIncomplete()
    {
        // Arrange — same wrapping shape as above, but the piece that lands after the ring wrap
        // (sequence 12, ring slot 0) never arrives.
        const int capacity = 4;
        const uint windowBase = 10;
        const int baseSlot = 2;
        byte[] lengths = NewLengthRun(capacity);
        byte[] admitted = NewAdmittedRun(capacity);

        Hold(lengths, admitted, ringSlot: 2, byteLength: 1); // sequence 10
        Hold(lengths, admitted, ringSlot: 3, byteLength: 1); // sequence 11
        Hold(lengths, admitted, ringSlot: 1, byteLength: 1); // sequence 13

        // Act
        bool complete = FragmentGroupCompleteness.IsComplete(
            admitted, lengths, capacity, windowBase, baseSlot, groupFirstSequence: windowBase, fragmentCount: 4);

        // Assert
        complete.Should().BeFalse("sequence 12 (ring slot 0) was never held");
    }

    [Fact]
    public void IsComplete_AGroupStraddlingTheU32SequenceWrap_IsDetectedAsComplete()
    {
        // Arrange — the group's first sequence sits two below uint.MaxValue, so its four sequences are
        // uint.MaxValue - 1, uint.MaxValue, 0 and 1 — straddling the u32 wrap. Base is pinned at the
        // group's first sequence, ring slot 0.
        const int capacity = 8;
        const uint groupFirstSequence = uint.MaxValue - 1;
        byte[] lengths = NewLengthRun(capacity);
        byte[] admitted = NewAdmittedRun(capacity);

        Hold(lengths, admitted, ringSlot: 0, byteLength: 1); // uint.MaxValue - 1
        Hold(lengths, admitted, ringSlot: 1, byteLength: 1); // uint.MaxValue
        Hold(lengths, admitted, ringSlot: 2, byteLength: 1); // 0 (wrapped)
        Hold(lengths, admitted, ringSlot: 3, byteLength: 1); // 1 (wrapped)

        // Act
        bool complete = FragmentGroupCompleteness.IsComplete(
            admitted, lengths, capacity, windowBase: groupFirstSequence, baseSlot: 0, groupFirstSequence, fragmentCount: 4);

        // Assert
        complete.Should().BeTrue("every sequence across the u32 wrap holds a Fragment");
    }

    [Fact]
    public void IsComplete_AGroupWiderThanTheWindowCanSpan_ReportsIncompleteRatherThanThrowing()
    {
        // Arrange — a group declaring more pieces than the window has slots at all: its last sequence
        // is unreachable in either direction from the base.
        const int capacity = 4;
        byte[] lengths = NewLengthRun(capacity);
        byte[] admitted = NewAdmittedRun(capacity);
        for (int slot = 0; slot < capacity; slot++)
        {
            Hold(lengths, admitted, slot, byteLength: 1);
        }

        // Act
        Func<bool> act = () => FragmentGroupCompleteness.IsComplete(
            admitted, lengths, capacity, windowBase: 0, baseSlot: 0, groupFirstSequence: 0, fragmentCount: 6);

        // Assert
        act.Should().NotThrow();
        act().Should().BeFalse("a 6-piece group cannot fit in a 4-slot window, so its last two pieces are unaddressable");
    }

    [Fact]
    public void TryGetRingSlot_ASequenceUpToCapacitySlotsBehindBase_StillResolvesToItsSlot()
    {
        // Arrange — base has advanced to 4 (ring slot 0, wrapping from a capacity-4 window), so
        // sequence 0 — exactly Capacity behind — is the oldest sequence still addressable.
        const int capacity = 4;
        const uint windowBase = 4;
        const int baseSlot = 0;

        // Act
        bool found = FragmentGroupCompleteness.TryGetRingSlot(sequence: 0, capacity, windowBase, baseSlot, out int ringSlot);

        // Assert — sequence 0 shares its ring slot with sequence 4 (the base), since the ring is only
        // 4 slots wide.
        found.Should().BeTrue();
        ringSlot.Should().Be(0);
    }

    [Fact]
    public void TryGetRingSlot_ASequenceMoreThanCapacitySlotsBehindBase_IsNotFound()
    {
        // Arrange
        const int capacity = 4;
        const uint windowBase = 5;
        const int baseSlot = 1;

        // Act
        bool found = FragmentGroupCompleteness.TryGetRingSlot(sequence: 0, capacity, windowBase, baseSlot, out int ringSlot);

        // Assert
        found.Should().BeFalse("sequence 0 is five behind a base of 5, one slot further than the ring can address");
        ringSlot.Should().Be(-1);
    }

    [Fact]
    public void TryGetRingSlot_ASequenceAtOrBeyondTheWindowsFarEdge_IsNotFound()
    {
        // Arrange
        const int capacity = 4;
        const uint windowBase = 0;
        const int baseSlot = 0;

        // Act
        bool found = FragmentGroupCompleteness.TryGetRingSlot(sequence: 4, capacity, windowBase, baseSlot, out int ringSlot);

        // Assert
        found.Should().BeFalse("sequence 4 is at the far edge of a 4-slot window based at 0 — not yet reachable");
        ringSlot.Should().Be(-1);
    }
}
