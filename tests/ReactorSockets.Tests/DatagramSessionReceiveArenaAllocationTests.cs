using System.Runtime.CompilerServices;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #171: the receive arena — the datagram Session profile's receive side of the memory bill,
/// <c>Capacity × 2 reliable Spaces × ReceiveWindowFragments × FragmentSize</c> — is preallocated once
/// at <see cref="DatagramSessionReactor"/> construction, regardless of whether any Peer has connected,
/// and released when the Reactor is disposed. Mirrors the allocation-measurement pattern in
/// <see cref="StreamSessionRetentionAllocationTests"/>, the send-side equivalent (issue #84).
/// </summary>
public class DatagramSessionReceiveArenaAllocationTests
{
    [Fact]
    public void Construct_WithNoPeersConnected_AllocatesAtLeastCapacityTimesTwoTimesReceiveWindowTimesFragmentSize()
    {
        // Arrange
        const int capacity = 16;
        const int fragmentSize = 1200;
        const int receiveWindowFragments = 25;

        // Act
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        using DatagramSessionReactorHarness harness = new(
            capacity: capacity,
            maxMessageSize: fragmentSize,
            fragmentSize: fragmentSize,
            receiveWindowFragments: receiveWindowFragments);
        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();

        // Assert — the receive arena's backing buffer alone accounts for at least this many bytes,
        // preallocated up front rather than grown lazily as data arrives. Only the two reliable Spaces
        // get window storage (the unreliable Spaces classify sequences but retain no payload), which
        // is why this is Capacity × 2 rather than Capacity × 4.
        long expectedReceiveBudgetBytes = (long)capacity * 2 * receiveWindowFragments * fragmentSize;
        (allocatedAfter - allocatedBefore).Should().BeGreaterThanOrEqualTo(expectedReceiveBudgetBytes);
    }

    [Fact]
    public void Construct_DoublingReceiveWindowFragments_AtLeastDoublesTheAllocationDelta()
    {
        // Arrange
        const int capacity = 16;
        const int fragmentSize = 1200;
        const int narrowWindow = 25;
        const int wideWindow = 50;

        // Act
        long beforeNarrow = GC.GetAllocatedBytesForCurrentThread();
        using (DatagramSessionReactorHarness narrow = new(
            capacity: capacity,
            maxMessageSize: fragmentSize,
            fragmentSize: fragmentSize,
            receiveWindowFragments: narrowWindow))
        {
            long allocatedForNarrow = GC.GetAllocatedBytesForCurrentThread() - beforeNarrow;

            long beforeWide = GC.GetAllocatedBytesForCurrentThread();
            using DatagramSessionReactorHarness wide = new(
                capacity: capacity,
                maxMessageSize: fragmentSize,
                fragmentSize: fragmentSize,
                receiveWindowFragments: wideWindow);
            long allocatedForWide = GC.GetAllocatedBytesForCurrentThread() - beforeWide;

            // Assert — doubling the configured window at least doubles the arena's own contribution:
            // the wide Reactor's construction allocates at least one more narrow-window's worth of
            // receive-arena bytes than the narrow Reactor's construction did.
            long oneWindowWorthOfBytes = (long)capacity * 2 * narrowWindow * fragmentSize;
            (allocatedForWide - allocatedForNarrow).Should().BeGreaterThanOrEqualTo(oneWindowWorthOfBytes);
        }
    }

    [Fact]
    public void Dispose_ReleasesTheReactorAndItsPreallocatedArenas()
    {
        // Arrange
        WeakReference reactorReference = ConstructAndDispose();

        // Act — nothing in this method still holds a strong reference to the Reactor once
        // ConstructAndDispose has returned, so a full collection reclaims it and everything it
        // preallocated, the receive arena included.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // Assert
        reactorReference.IsAlive.Should().BeFalse();
    }

    /// <summary>
    /// Isolated in its own non-inlined method so the local <see cref="DatagramSessionReactorHarness"/>
    /// and its Reactor are out of scope — and so not kept artificially alive by the JIT — by the time
    /// the caller forces a collection.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference ConstructAndDispose()
    {
        DatagramSessionReactorHarness harness = new(capacity: 16, receiveWindowFragments: 25);
        WeakReference reference = new(harness.Reactor);
        harness.Dispose();
        return reference;
    }
}
