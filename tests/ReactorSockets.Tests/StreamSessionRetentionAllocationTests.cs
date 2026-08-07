using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 3.5.2 (issue #84): the retention arena is preallocated at <see cref="StreamSessionReactor"/>
/// construction time, sized <c>Capacity × RetentionBytes</c>, regardless of whether any Peer has
/// connected. Mirrors the allocation-measurement pattern in
/// <see cref="StreamSessionMalformedFramingTests"/>.
/// </summary>
public class StreamSessionRetentionAllocationTests
{
    [Fact]
    public void Construct_WithNoPeersConnected_AllocatesAtLeastCapacityTimesRetentionBytes()
    {
        // Arrange
        const int capacity = 64;
        const int retentionBytes = 4100;

        // Act
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        using StreamSessionReactorHarness harness = new(capacity: capacity, retentionBytes: retentionBytes);
        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();

        // Assert — the retention arena's backing buffer alone accounts for at least this many bytes,
        // preallocated up front rather than grown lazily as Peers connect.
        (allocatedAfter - allocatedBefore).Should().BeGreaterThanOrEqualTo((long)capacity * retentionBytes);
    }
}
