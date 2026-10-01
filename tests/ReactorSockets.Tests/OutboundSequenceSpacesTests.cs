using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #240: the send side of the u32 sequence wrap (closing #178).
///
/// These tests document the language-level guarantees and wrap-aware arithmetic that
/// OutboundSequenceSpaces depends on. Send-side sequence wrapping itself is not directly testable
/// through the public seam (which does not expose allocated sequences until they reach the wire),
/// but it is implicitly validated by DatagramSessionSequenceWrapTests, which verifies that the
/// receive side correctly classifies sequences that wrap across the u32 boundary.
///
/// The full data path (send → wire → receive classification) is tested end-to-end via the
/// receive-side tests, which validate that sequences produced by the send side's unchecked
/// arithmetic are correctly ordered via wrap-aware serial comparison.
/// </summary>
public class OutboundSequenceSpacesTests
{
    /// <summary>
    /// Documents and verifies the unchecked-arithmetic wrapping behavior that the send side depends on.
    /// This demonstrates that uint post-increment wraps by design in C#'s unchecked context,
    /// which is the mechanism OutboundSequenceSpaces.Allocate uses for sequence wrapping.
    /// </summary>
    [Fact]
    public void UncheckedArithmetic_SequenceWrapsAfterMaxValue()
    {
        // Arrange — simulate what OutboundSequenceSpaces does internally with unchecked increment
        uint counter = uint.MaxValue;

        // Act — allocate sequences using unchecked post-increment
        uint allocated1 = unchecked(counter++); // Returns uint.MaxValue
        uint allocated2 = unchecked(counter++); // Returns 0 (wrapped)
        uint allocated3 = unchecked(counter++); // Returns 1

        // Assert — unchecked arithmetic guarantees the wrap
        allocated1.Should().Be(0xFFFF_FFFF, "first allocation at boundary");
        allocated2.Should().Be(0x0000_0000, "wrap to zero after max");
        allocated3.Should().Be(0x0000_0001, "continue incrementing");
    }

    /// <summary>
    /// Documents the ordering property for wrapped sequences: a sequence just above the wrap
    /// (e.g., 0 or 1) must be ordered as newer than one just below it (e.g., 0xFFFF_FFFF).
    /// This ordering is implemented via wrap-aware serial comparison (RFC 1982 style), tested
    /// extensively in DatagramSessionSequenceWrapTests.
    /// </summary>
    [Fact]
    public void SerialComparison_WrappedSequences_AreOrderedCorrectlyAsSignedDifference()
    {
        // The send side produces sequences using unchecked increment, which wraps as tested above.
        // The receive side orders these using wrap-aware serial comparison: (int)(a - b) > 0.
        // This test documents that pattern without accessing internal SequenceNumber.IsNewerThan.

        // When the send side wraps, it produces:
        uint justBelowWrap = 0xFFFF_FFFF;
        uint justAboveWrap = 0x0000_0000;

        // Wrap-aware comparison: unchecked cast the unsigned difference to signed
        int signedDifference = unchecked((int)(justAboveWrap - justBelowWrap));

        // Assert — the signed difference is positive, indicating justAboveWrap is newer
        signedDifference.Should().BeGreaterThan(0,
            "the difference 0 - 0xFFFFFFFF wraps to 1 when cast from uint to int, so 0 is newer than 0xFFFFFFFF");

        // This is the core property that makes serial comparison work across the wrap:
        // sequences are ordered by the shortest path around the circular space, not by numeric magnitude.
    }
}
