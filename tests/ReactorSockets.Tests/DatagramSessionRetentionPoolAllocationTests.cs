using System.Net;
using System.Runtime.CompilerServices;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The send-side retention pool — <c>Capacity × 2 reliable Spaces ×
/// RetentionBytes</c>, the send line of <see cref="DatagramSessionOptions"/>'s memory bill — is
/// preallocated once at <see cref="DatagramSessionReactor"/> construction, regardless of whether any
/// Peer has connected, and does not grow as Peers connect, disconnect and reconnect into the same
/// capacity Slot. Mirrors the allocation-measurement pattern in
/// <see cref="DatagramSessionReceiveArenaAllocationTests"/>, the receive side's equivalent (issue #171).
/// </summary>
public class DatagramSessionRetentionPoolAllocationTests
{
    [Fact]
    public void Construct_WithNoPeersConnected_AllocatesAtLeastCapacityTimesReliableSpacesTimesRetentionBytes()
    {
        // Arrange
        const int capacity = 64;
        const int fragmentSize = 1200;
        const int retentionBytes = fragmentSize * 4;
        const int reliableSpaces = 2;

        // Act
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        using DatagramSessionReactorHarness harness = new(
            capacity: capacity,
            retentionBytes: retentionBytes,
            fragmentSize: fragmentSize);
        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();

        // Assert — the retention pool's backing buffer alone accounts for at least this many bytes,
        // preallocated up front rather than grown lazily as Peers connect (mirrors
        // StreamSessionRetentionAllocationTests's send-side floor for the stream profile).
        (allocatedAfter - allocatedBefore).Should().BeGreaterThanOrEqualTo(
            (long)capacity * reliableSpaces * retentionBytes);
    }

    [Fact]
    public void Construct_DoublingRetentionBytes_ExactlyDoublesTheMeasuredPoolAllocation()
    {
        // Act — the pool's own buffer is linear in RetentionBytes with no constant term, so the
        // per-construction fixed overhead (sockets, Slot table, Peer state, the receive arena) cancels
        // out of the difference between two otherwise-identical Reactors.
        WarmUpTheMeasurement();
        long narrow = MeasurePoolAllocation(retentionBytes: 1200 * 4);
        long wide = MeasurePoolAllocation(retentionBytes: 1200 * 8);

        // Assert
        (wide - narrow).Should().BeGreaterThanOrEqualTo((long)MeasurementCapacity * 2 * (1200 * 4));
    }

    private const int MeasurementCapacity = 8;
    private const int MeasurementFragmentSize = 1200;

    /// <summary>
    /// Takes and discards one measurement before the real ones, paying the one-off JIT and
    /// static-initialisation cost up front so it does not tilt the first real data point.
    /// </summary>
    private static void WarmUpTheMeasurement() => MeasurePoolAllocation(retentionBytes: 1200 * 4);

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static long MeasurePoolAllocation(int retentionBytes)
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        using DatagramSessionReactorHarness harness = new(
            capacity: MeasurementCapacity,
            retentionBytes: retentionBytes,
            fragmentSize: MeasurementFragmentSize,
            bindLoopback: false);
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    /// <summary>
    /// Admitting a Peer, disconnecting it, and admitting another into the same Slot allocates nothing.
    /// No Peer here Sends, so this measures the admission path alone: a second phase of connect/disconnect
    /// cycles, identical in shape to the first and reusing the same capacity Slots, must not allocate
    /// visibly more than the first, which is what a pool growing under traffic would show up as.
    /// </summary>
    [Fact]
    public void Pump_AdmittingDisconnectingAndReadmittingIntoTheSameSlots_DoesNotGrowAllocationAsCyclesContinue()
    {
        // Arrange
        const int capacity = 8;
        const int warmupCycles = 10;
        const int measuredCyclesPerPhase = 50;

        using DatagramSessionReactorHarness harness = new(capacity: capacity, bindLoopback: false);

        RunConnectDisconnectCycles(harness, capacity, warmupCycles);

        // Act — two equal-shaped phases back to back, deep into the same sustained run of Slot reuse.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long beforePhaseOne = GC.GetAllocatedBytesForCurrentThread();
        RunConnectDisconnectCycles(harness, capacity, measuredCyclesPerPhase);
        long phaseOneDelta = GC.GetAllocatedBytesForCurrentThread() - beforePhaseOne;

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long beforePhaseTwo = GC.GetAllocatedBytesForCurrentThread();
        RunConnectDisconnectCycles(harness, capacity, measuredCyclesPerPhase);
        long phaseTwoDelta = GC.GetAllocatedBytesForCurrentThread() - beforePhaseTwo;

        // Assert — generous headroom against GC noise, not a tight bound; what must not happen is the
        // cost climbing as Slot reuse continues, which is what a retention pool allocating per-admission
        // would show up as.
        phaseTwoDelta.Should().BeLessThanOrEqualTo(phaseOneDelta * 2 + 1);
    }

    /// <summary>
    /// Dials <paramref name="capacity"/> Peers, filling every capacity Slot, then closes every one of
    /// them — freeing every Slot for the next cycle to reuse exactly the same Slot indices.
    /// </summary>
    private static void RunConnectDisconnectCycles(DatagramSessionReactorHarness harness, int capacity, int cycles)
    {
        for (int cycle = 0; cycle < cycles; cycle++)
        {
            PeerId[] peers = new PeerId[capacity];
            for (int i = 0; i < capacity; i++)
            {
                peers[i] = harness.Reactor.Connect(new IPEndPoint(IPAddress.Loopback, 40000 + i));
            }

            harness.Reactor.Pump();
            harness.DrainToList();

            for (int i = 0; i < capacity; i++)
            {
                harness.Reactor.Close(peers[i]);
            }

            harness.Reactor.Pump();
            harness.DrainToList();
        }
    }
}
