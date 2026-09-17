using System.Buffers.Binary;
using System.Net;
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
    /// <summary>
    /// The window size the exact-match measurements below start from, and the Reactor shape they all
    /// share. Every measurement builds the same Reactor and fills every one of its Peer Slots, so the
    /// only thing that differs between two measurements is the configured window — which makes the
    /// difference between them exactly the receive side's own bytes, with no allowance for anything
    /// else construction happens to allocate.
    /// </summary>
    private const int MeasurementCapacity = 8;
    private const int ReliableSpaces = 2;
    private const int AllSpaces = 4;
    private const int MeasurementFragmentSize = 1200;
    private const int MeasurementWindowFragments = 32;

    /// <summary>
    /// Mirrors the internal <c>DatagramSessionOptions.FragmentLengthBytes</c> (#257) — this test file
    /// cannot reference it directly (no <c>InternalsVisibleTo</c>, per tests/CLAUDE.md), so it restates
    /// the same width the production bill is computed from.
    /// </summary>
    private const int FragmentLengthBytes = 2;

    /// <summary>
    /// Mirrors the internal <c>DatagramSessionOptions.FragmentGroupEntryBytes</c> (#297) — the six bytes
    /// per fragment slot that name the group a held Fragment belongs to, distinct from
    /// <see cref="PendingGroupEntryBytes"/>'s six bytes per waiting group.
    /// </summary>
    private const int FragmentGroupEntryBytes = 6;

    private const int PendingGroupEntryBytes = 6;

    [Fact]
    public void Construct_AndFillEveryPeerSlot_AllocatesExactlyTheOptionsStatedReceiveBudget()
    {
        // Act — two Reactors identical but for the window, each with every Peer Slot occupied.
        WarmUpTheMeasurement();
        long narrow = MeasureReceiveSideAllocation(MeasurementWindowFragments);
        long wide = MeasureReceiveSideAllocation(MeasurementWindowFragments * 2);

        // Assert — the receive budget is linear in the window with no constant term, so doubling the
        // window adds exactly one more narrow budget's worth of bytes. Exact, not a lower bound
        // (issue #174): every byte of per-Peer receive state now comes out of the arena, so occupying
        // all eight Slots takes nothing on top of what the options said the receive side would cost.
        (wide - narrow).Should().Be(StatedReceiveBudgetBytes(MeasurementWindowFragments));
    }

    [Fact]
    public void Construct_DoublingReceiveWindowFragments_ExactlyDoublesTheMeasuredAllocation()
    {
        // Act — three points on the same line, at one, two and three windows.
        WarmUpTheMeasurement();
        long single = MeasureReceiveSideAllocation(MeasurementWindowFragments);
        long doubled = MeasureReceiveSideAllocation(MeasurementWindowFragments * 2);
        long tripled = MeasureReceiveSideAllocation(MeasurementWindowFragments * 3);

        // Assert — each step of one window costs the same, which is what makes the measurement a
        // straight line through a window-independent fixed overhead (sockets, Slot table, Peer state).
        // Subtracting that overhead leaves the receive side alone, and doubling the window doubles it
        // exactly.
        long oneWindowOfBytes = doubled - single;
        (tripled - doubled).Should().Be(oneWindowOfBytes);

        long fixedOverhead = single - oneWindowOfBytes;
        (doubled - fixedOverhead).Should().Be(2 * (single - fixedOverhead));
    }

    [Fact]
    public void ReceiveBudgetBytes_CountsPayloadForTheTwoReliableSpacesAndClassificationForAllFourAndLengthForTheTwoReliableSpaces()
    {
        // Act
        long stated = StatedReceiveBudgetBytes(MeasurementWindowFragments);

        // Assert — the stated multiplier says what is actually allocated (issue #174, extended by
        // #257 and #297). Payload slots exist only for the two reliable Spaces, which retain fragments
        // until a Message is whole; the one-byte-per-slot admission record that lets a window classify a
        // sequence exists for all four, because all four classify; the two-byte-per-slot Fragment
        // length record and the six-byte-per-slot Fragment group-identity record exist only for the two
        // reliable Spaces, the only ones that ever hold a Fragment (ADR 0004); and the six-byte
        // waiting-group entries (#259) exist for those same two Spaces, half a window's worth each,
        // since a group is at least two Fragments wide.
        long payload = (long)MeasurementCapacity * ReliableSpaces
            * MeasurementWindowFragments * MeasurementFragmentSize;
        long classification = (long)MeasurementCapacity * AllSpaces
            * MeasurementWindowFragments;
        long length = (long)MeasurementCapacity * ReliableSpaces
            * MeasurementWindowFragments * FragmentLengthBytes;
        long fragmentGroups = (long)MeasurementCapacity * ReliableSpaces
            * MeasurementWindowFragments * FragmentGroupEntryBytes;
        long pendingGroups = (long)MeasurementCapacity * ReliableSpaces
            * (MeasurementWindowFragments / 2) * PendingGroupEntryBytes;
        stated.Should().Be(payload + classification + length + fragmentGroups + pendingGroups);
    }

    /// <summary>
    /// Takes and discards one measurement before the real ones.
    /// <see cref="GC.GetAllocatedBytesForCurrentThread"/> counts everything the thread allocates,
    /// including the one-off JIT and static-initialisation cost of the first construction, which
    /// would otherwise land entirely in the first data point and tilt the line. Discarding a
    /// measurement pays that cost up front so the points that are compared differ only by the window.
    /// </summary>
    private static void WarmUpTheMeasurement() => MeasureReceiveSideAllocation(MeasurementWindowFragments);

    /// <summary>
    /// Builds one Reactor of <paramref name="receiveWindowFragments"/> window, fills every one of its
    /// Peer Slots by dialling, and returns the bytes that took. Non-inlined and self-contained so the
    /// Reactor and its Peers are unreachable by the time the caller compares two measurements.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static long MeasureReceiveSideAllocation(int receiveWindowFragments)
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        using DatagramSessionReactorHarness harness = new(
            capacity: MeasurementCapacity,
            maxMessageSize: MeasurementFragmentSize,
            fragmentSize: MeasurementFragmentSize,
            receiveWindowFragments: receiveWindowFragments);

        for (int i = 0; i < MeasurementCapacity; i++)
        {
            harness.Reactor.Connect(new IPEndPoint(IPAddress.Loopback, 40000 + i));
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    /// <summary>
    /// What the options say the receive side of the memory bill costs for the measurement Reactor at
    /// <paramref name="receiveWindowFragments"/> — the figure every measurement above is compared
    /// against.
    /// </summary>
    private static long StatedReceiveBudgetBytes(int receiveWindowFragments) =>
        new DatagramSessionOptions
        {
            Reactor = new ReactorOptions { Capacity = MeasurementCapacity },
            MaxMessageSize = MeasurementFragmentSize,
            FragmentSize = MeasurementFragmentSize,
            ReceiveWindowFragments = receiveWindowFragments,
            RetentionBytes = MeasurementFragmentSize,
            SendIdleInterval = TimeSpan.FromSeconds(1),
            Linger = TimeSpan.FromSeconds(1),
        }.ReceiveBudgetBytes;

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

    /// <summary>
    /// Closes issue #171's outstanding acceptance criterion — "memory does not grow across a sustained
    /// run of inbound datagrams" — left unticked when the arena landed because nothing wrote into the
    /// receive path yet; #172 and #173 wired that up. Each cycle sends every odd offset in the window
    /// in one datagram, then every even offset in another — the same out-of-order, hole-filling pattern
    /// the direct <c>ReceiveWindow</c> unit test used before the type went internal (this ticket) — over
    /// the <c>Unreliable</c> Space, which classifies without retaining any payload, so this measures the
    /// window's own bookkeeping rather than reliable retention.
    /// </summary>
    [Fact]
    public void Pump_ASustainedRunOfOutOfOrderInboundDatagrams_DoesNotGrowAllocationAsTheRunContinues()
    {
        // Arrange
        const int capacity = 16;
        const int warmupCycles = 50;
        const int measuredCyclesPerPhase = 300;

        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: capacity, maxMessageSize: 1300);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        uint cycleBase = RunCycles(accepter, client, clientLink, sessionId, capacity, 0, warmupCycles);

        // Act — two equal-length phases run back to back, deep into the same sustained run. If the
        // window's bookkeeping grew with how many sequences it had already seen, the second phase —
        // identical in shape to the first, just further along — would allocate visibly more.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long beforePhaseOne = GC.GetAllocatedBytesForCurrentThread();
        cycleBase = RunCycles(accepter, client, clientLink, sessionId, capacity, cycleBase, measuredCyclesPerPhase);
        long phaseOneDelta = GC.GetAllocatedBytesForCurrentThread() - beforePhaseOne;

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long beforePhaseTwo = GC.GetAllocatedBytesForCurrentThread();
        RunCycles(accepter, client, clientLink, sessionId, capacity, cycleBase, measuredCyclesPerPhase);
        long phaseTwoDelta = GC.GetAllocatedBytesForCurrentThread() - beforePhaseTwo;

        // Assert — a per-datagram cost from the socket layer (L1, not this ticket's concern) is
        // expected and unavoidable; what must not happen is that cost climbing as the run goes on,
        // which is what an accumulating receive window would show up as. The multiple is generous
        // headroom against GC noise, not a tight bound.
        phaseTwoDelta.Should().BeLessThanOrEqualTo(phaseOneDelta * 2 + 1);
    }

    /// <summary>
    /// Closes #174's fifth and last criterion (#181): the sustained-run test above only shows the cost
    /// does not climb, and only for one arrival pattern. This drives two phases of identical datagram
    /// count and per-datagram shape — same header, same single unfragmented Unreliable-Space record —
    /// through the same live Session, differing only in the sequences the records carry: one phase
    /// strictly in order, the other a mix of duplicates, sequences below the base, and far-ahead jumps
    /// that force <c>SlideTo</c> to walk the full ring. A per-datagram L1 cost is expected in both and
    /// cancels in the comparison; what must not appear is a cost that depends on which branch
    /// <c>Classify</c> took.
    /// </summary>
    [Fact]
    public void Pump_APhaseOfDuplicatesBelowBaseSequencesAndFarAheadSlides_AllocatesTheSameAsAnInOrderPhaseOfEqualShape()
    {
        // Arrange
        const int windowCapacity = 16;
        const int phaseDatagramCount = windowCapacity * 4;

        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: windowCapacity, maxMessageSize: 1300);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);
        Source destination = Source.FromIPEndPoint(accepter.Endpoint);

        // A throwaway in-order phase pays the first-measurement JIT cost (per #174's measurement
        // discipline) and leaves the base far enough ahead of zero that a sequence from this phase is
        // safely below the base for every measured phase that follows.
        uint cursor = RunInOrderPhase(accepter, client, clientLink, sessionId, destination, 0, phaseDatagramCount);
        uint belowBaseSequence = cursor - 1;

        // Act
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long beforeInOrder = GC.GetAllocatedBytesForCurrentThread();
        cursor = RunInOrderPhase(accepter, client, clientLink, sessionId, destination, cursor, phaseDatagramCount);
        long inOrderDelta = GC.GetAllocatedBytesForCurrentThread() - beforeInOrder;

        PeerCounters beforeMixedCounters = accepter.Reactor.CountersOf(accepterPeer);
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long beforeMixed = GC.GetAllocatedBytesForCurrentThread();
        RunMixedPhase(accepter, client, clientLink, sessionId, destination, windowCapacity, cursor, belowBaseSequence);
        long mixedDelta = GC.GetAllocatedBytesForCurrentThread() - beforeMixed;
        PeerCounters afterMixedCounters = accepter.Reactor.CountersOf(accepterPeer);

        // Assert — the mixed phase must actually have taken every branch it claims to, not merely sent
        // sequences shaped to. A hole left at the base (see RunMixedPhase) is what keeps the "duplicate"
        // block's resends inside the window instead of retiring behind it.
        (afterMixedCounters.DuplicatePackets - beforeMixedCounters.DuplicatePackets).Should().BeGreaterThan(0);
        (afterMixedCounters.OutOfWindowPackets - beforeMixedCounters.OutOfWindowPackets).Should().BeGreaterThan(0);
        (afterMixedCounters.UnreliableLossPackets - beforeMixedCounters.UnreliableLossPackets).Should().BeGreaterThan(0);

        // Assert — same datagram count, same per-datagram shape, so the two deltas differ only by
        // whichever Classify branch each phase's sequences took. Exact equality, not a tolerance
        // (#181's third criterion): the measurement is warmed up above, so nothing but that branch
        // choice is left to explain a difference.
        mixedDelta.Should().Be(inOrderDelta);
    }

    /// <summary>
    /// Closes issue #257's allocation-free acceptance criterion: holding a Fragment (copying its
    /// payload into the receive arena's preallocated slot, recording its length) must not allocate any
    /// more than an ordinary admitted record does. Two phases of identical shape — a sustained run of
    /// two-piece <c>ReliableOrdered</c> Fragment sets, each piece its own datagram — run back to back;
    /// since every sequence admits in order (ADR 0004: a Fragment rides an ordinary sequence, so
    /// <c>AdvanceBase</c> keeps sliding the base forward exactly as it would for unfragmented traffic),
    /// the window never fills and the second phase must cost no more than the first. Every set here
    /// completes the moment its second piece lands (issue #258's <c>IsGroupComplete</c> runs on every
    /// held Fragment), so this also closes #258's allocation-free criterion for the completeness path —
    /// see also the dedicated reverse-order test below.
    /// </summary>
    [Fact]
    public void Pump_ASustainedRunOfReliableOrderedFragments_DoesNotGrowAllocationAsTheRunContinues()
    {
        // Arrange
        const int capacity = 16;
        const int warmupSets = 50;
        const int measuredSetsPerPhase = 300;

        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: capacity, maxMessageSize: 1300);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        uint cursor = RunInOrderFragmentPhase(accepter, client, clientLink, sessionId, 0, warmupSets);

        // Act — two equal-length phases run back to back, deep into the same sustained run.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long beforePhaseOne = GC.GetAllocatedBytesForCurrentThread();
        cursor = RunInOrderFragmentPhase(accepter, client, clientLink, sessionId, cursor, measuredSetsPerPhase);
        long phaseOneDelta = GC.GetAllocatedBytesForCurrentThread() - beforePhaseOne;

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long beforePhaseTwo = GC.GetAllocatedBytesForCurrentThread();
        RunInOrderFragmentPhase(accepter, client, clientLink, sessionId, cursor, measuredSetsPerPhase);
        long phaseTwoDelta = GC.GetAllocatedBytesForCurrentThread() - beforePhaseTwo;

        // Assert — the multiple is generous headroom against GC noise, not a tight bound, mirroring
        // the unfragmented sustained-run test above.
        phaseTwoDelta.Should().BeLessThanOrEqualTo(phaseOneDelta * 2 + 1);
    }

    /// <summary>
    /// Closes issue #258's allocation-free acceptance criterion directly: a sustained run of two-piece
    /// <c>ReliableOrdered</c> Fragment sets whose pieces arrive in <em>reverse</em> order — index 1
    /// before index 0 — so every set is held incomplete by its first datagram and only completes on
    /// its second. Two phases of identical shape run back to
    /// back; if <c>ReceiveWindow.IsGroupComplete</c> — reached through
    /// <see cref="DatagramSessionReactor"/>'s live receive path here — ever allocated, the second phase
    /// would cost visibly more than the first.
    /// </summary>
    [Fact]
    public void Pump_ASustainedRunOfReverseOrderCompletingFragmentSets_DoesNotGrowAllocationAsTheRunContinues()
    {
        // Arrange
        const int capacity = 16;
        const int warmupSets = 50;
        const int measuredSetsPerPhase = 300;

        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: capacity, maxMessageSize: 1300);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        uint cursor = RunReverseOrderCompletingFragmentPhase(accepter, client, clientLink, sessionId, 0, warmupSets);

        // Act — two equal-length phases run back to back, deep into the same sustained run.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long beforePhaseOne = GC.GetAllocatedBytesForCurrentThread();
        cursor = RunReverseOrderCompletingFragmentPhase(accepter, client, clientLink, sessionId, cursor, measuredSetsPerPhase);
        long phaseOneDelta = GC.GetAllocatedBytesForCurrentThread() - beforePhaseOne;

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long beforePhaseTwo = GC.GetAllocatedBytesForCurrentThread();
        RunReverseOrderCompletingFragmentPhase(accepter, client, clientLink, sessionId, cursor, measuredSetsPerPhase);
        long phaseTwoDelta = GC.GetAllocatedBytesForCurrentThread() - beforePhaseTwo;

        // Assert — the multiple is generous headroom against GC noise, not a tight bound, mirroring
        // the unfragmented sustained-run test above.
        phaseTwoDelta.Should().BeLessThanOrEqualTo(phaseOneDelta * 2 + 1);
    }

    /// <summary>
    /// Sends <paramref name="setCount"/> two-piece <c>ReliableOrdered</c> Fragment sets, one datagram
    /// per piece, sequences increasing by two per set starting at <paramref name="startingSequence"/>,
    /// index 1 sent before index 0 within each set — so the window admits the second piece first (held,
    /// incomplete) and completes the set only when the first piece lands. Returns the next unused
    /// sequence.
    /// </summary>
    private static uint RunReverseOrderCompletingFragmentPhase(
        DatagramSessionReactorHarness accepter,
        RawReactor client,
        LinkId clientLink,
        uint sessionId,
        uint startingSequence,
        int setCount)
    {
        Source destination = Source.FromIPEndPoint(accepter.Endpoint);
        uint sequence = startingSequence;
        for (int i = 0; i < setCount; i++)
        {
            byte[] second = BuildFragmentRecord(sequence + 1, fragmentIndex: 1, fragmentCount: 2, payload: [0x02]);
            byte[] datagramTwo = [.. BuildHeader(sessionId), .. second];
            ulong before = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, destination, datagramTwo);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);

            byte[] first = BuildFragmentRecord(sequence, fragmentIndex: 0, fragmentCount: 2, payload: [0x01]);
            byte[] datagramOne = [.. BuildHeader(sessionId), .. first];
            before = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, destination, datagramOne);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);

            accepter.DrainAndDiscard();
            sequence = unchecked(sequence + 2);
        }

        return sequence;
    }

    /// <summary>
    /// Sends <paramref name="setCount"/> two-piece <c>ReliableOrdered</c> Fragment sets, one datagram
    /// per piece, sequences increasing by two per set starting at <paramref name="startingSequence"/>.
    /// Each set completes the moment its second piece lands (issue #258), but delivering a completed
    /// set is issue #259's, so nothing is ever delivered regardless. Returns the next unused sequence.
    /// </summary>
    private static uint RunInOrderFragmentPhase(
        DatagramSessionReactorHarness accepter,
        RawReactor client,
        LinkId clientLink,
        uint sessionId,
        uint startingSequence,
        int setCount)
    {
        Source destination = Source.FromIPEndPoint(accepter.Endpoint);
        uint sequence = startingSequence;
        for (int i = 0; i < setCount; i++)
        {
            byte[] first = BuildFragmentRecord(sequence, fragmentIndex: 0, fragmentCount: 2, payload: [0x01]);
            byte[] datagramOne = [.. BuildHeader(sessionId), .. first];
            ulong before = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, destination, datagramOne);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);

            byte[] second = BuildFragmentRecord(sequence + 1, fragmentIndex: 1, fragmentCount: 2, payload: [0x02]);
            byte[] datagramTwo = [.. BuildHeader(sessionId), .. second];
            before = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, destination, datagramTwo);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);

            accepter.DrainAndDiscard();
            sequence = unchecked(sequence + 2);
        }

        return sequence;
    }

    /// <summary>
    /// Builds one <c>ReliableOrdered</c> Fragment record: the unfragmented shape's bytes with the
    /// fragment bit set, followed by a two-byte fragment index and a two-byte fragment count, then the
    /// payload.
    /// </summary>
    private static byte[] BuildFragmentRecord(uint sequence, ushort fragmentIndex, ushort fragmentCount, ReadOnlySpan<byte> payload)
    {
        const byte modeReliableOrdered = 0b0000_0011;
        const byte fragmentBit = 0b0000_0100;
        const int fragmentRecordSize = 11;
        byte[] record = new byte[fragmentRecordSize + payload.Length];
        record[0] = (byte)(modeReliableOrdered | fragmentBit);
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(7, 2), fragmentIndex);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(9, 2), fragmentCount);
        payload.CopyTo(record.AsSpan(fragmentRecordSize));
        return record;
    }

    /// <summary>
    /// Sends <paramref name="count"/> single-record Unreliable-Space datagrams, one per sequence
    /// starting at <paramref name="startingSequence"/> and increasing by one each time — every arrival
    /// admitted in order, so <c>Classify</c> never takes anything but the plain admit-and-advance
    /// branch. Returns the next unused sequence.
    /// </summary>
    private static uint RunInOrderPhase(
        DatagramSessionReactorHarness accepter,
        RawReactor client,
        LinkId clientLink,
        uint sessionId,
        Source destination,
        uint startingSequence,
        int count)
    {
        uint sequence = startingSequence;
        for (int i = 0; i < count; i++)
        {
            SendUnreliableRecord(accepter, client, clientLink, sessionId, destination, sequence);
            sequence = unchecked(sequence + 1);
        }

        return sequence;
    }

    /// <summary>
    /// Sends <c>4 × windowCapacity</c> single-record Unreliable-Space datagrams — the same count and
    /// shape <see cref="RunInOrderPhase"/> sends — split into four equal blocks that between them
    /// exercise every non-trivial <c>Classify</c> branch.
    /// </summary>
    /// <remarks>
    /// The first block admits offsets <c>1 .. windowCapacity - 1</c> from <paramref name="startingSequence"/>
    /// but never sends <paramref name="startingSequence"/> itself. That gap matters: <c>AdvanceBase</c>
    /// only moves the base past slots it finds already admitted, starting from the base's own slot, so
    /// leaving the base's slot empty pins the base at <paramref name="startingSequence"/> for the rest
    /// of the phase — the admitted slots above it stay admitted rather than being retired out from
    /// under the next block. Without that gap, a strictly in-order first block retires every slot it
    /// fills as it fills it (the same mechanism the in-order phase relies on), leaving nothing for a
    /// "duplicate" block to actually duplicate — a resend would land below the base instead, taking the
    /// same branch as the below-base block and never reaching <see cref="ReceiveWindowOutcome.Duplicate"/>.
    /// Each block is padded to <paramref name="windowCapacity"/> sends with an extra below-base resend
    /// so every block, and so the phase as a whole, matches the in-order phase's datagram count exactly.
    /// The last block's jumps start from the base's un-advanced position, each one landing
    /// <c>2 × windowCapacity</c> past the last, which keeps every jump's offset from the (already
    /// slid-forward) base at least <paramref name="windowCapacity"/> — enough to force <c>SlideTo</c> to
    /// walk the full ring again rather than take the plain single-slot advance.
    /// </remarks>
    private static void RunMixedPhase(
        DatagramSessionReactorHarness accepter,
        RawReactor client,
        LinkId clientLink,
        uint sessionId,
        Source destination,
        int windowCapacity,
        uint startingSequence,
        uint belowBaseSequence)
    {
        // Block 1 — new admits at offsets 1..windowCapacity-1, leaving the base's own slot (offset 0)
        // unfilled so the base does not advance past what this block just admitted.
        for (int offset = 1; offset < windowCapacity; offset++)
        {
            SendUnreliableRecord(accepter, client, clientLink, sessionId, destination, unchecked(startingSequence + (uint)offset));
        }

        SendUnreliableRecord(accepter, client, clientLink, sessionId, destination, belowBaseSequence);

        // Block 2 — duplicates of block 1's range. The base is still pinned at startingSequence, so
        // every one of these resends lands on a slot block 1 already marked admitted.
        for (int offset = 1; offset < windowCapacity; offset++)
        {
            SendUnreliableRecord(accepter, client, clientLink, sessionId, destination, unchecked(startingSequence + (uint)offset));
        }

        SendUnreliableRecord(accepter, client, clientLink, sessionId, destination, belowBaseSequence);

        // Block 3 — below-base resends only.
        for (int i = 0; i < windowCapacity; i++)
        {
            SendUnreliableRecord(accepter, client, clientLink, sessionId, destination, belowBaseSequence);
        }

        // Block 4 — far-ahead jumps. The base is still exactly startingSequence entering this block, so
        // the first jump's offset of windowCapacity already meets the "at or beyond the far edge"
        // threshold SlideTo requires.
        uint farCursor = startingSequence;
        for (int i = 0; i < windowCapacity; i++)
        {
            farCursor = unchecked(farCursor + (uint)(2 * windowCapacity));
            SendUnreliableRecord(accepter, client, clientLink, sessionId, destination, farCursor);
        }
    }

    /// <summary>
    /// Sends one unfragmented, empty-payload Unreliable-Space record for <paramref name="sequence"/>
    /// and pumps until the accepter has received it, draining the accepter's events afterwards so
    /// every call in a phase does exactly the same amount of bookkeeping regardless of which
    /// <c>Classify</c> branch the sequence takes.
    /// </summary>
    private static void SendUnreliableRecord(
        DatagramSessionReactorHarness accepter,
        RawReactor client,
        LinkId clientLink,
        uint sessionId,
        Source destination,
        uint sequence)
    {
        byte[] datagram = [.. BuildHeader(sessionId), .. BuildRecord(sequence)];
        ulong before = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, destination, datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);
        accepter.DrainAndDiscard();
    }

    /// <summary>
    /// Runs <paramref name="cycles"/> cycles of the odd-offsets-then-even-offsets pattern starting at
    /// <paramref name="startingCycleBase"/>, each cycle covering exactly <paramref name="capacity"/>
    /// sequences, and returns the next cycle's base.
    /// </summary>
    private static uint RunCycles(
        DatagramSessionReactorHarness accepter,
        RawReactor client,
        LinkId clientLink,
        uint sessionId,
        int capacity,
        uint startingCycleBase,
        int cycles)
    {
        uint cycleBase = startingCycleBase;
        Source destination = Source.FromIPEndPoint(accepter.Endpoint);

        for (int cycle = 0; cycle < cycles; cycle++)
        {
            List<byte> oddDatagram = [.. BuildHeader(sessionId)];
            for (int i = 1; i < capacity; i += 2)
            {
                oddDatagram.AddRange(BuildRecord(cycleBase + (uint)i));
            }

            ulong before = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, destination, [.. oddDatagram]);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);

            List<byte> evenDatagram = [.. BuildHeader(sessionId)];
            for (int i = 0; i < capacity; i += 2)
            {
                evenDatagram.AddRange(BuildRecord(cycleBase + (uint)i));
            }

            before = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, destination, [.. evenDatagram]);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);

            accepter.DrainAndDiscard();
            cycleBase += (uint)capacity;
        }

        return cycleBase;
    }

    private const byte ModeUnreliable = 0b0000_0000;
    private const int ControlDatagramSize = 256;
    private const uint ControlSession = 0;
    private const byte HelloType = 1;
    private const byte HelloWithCookieType = 3;
    private const int ControlBodyOffset = 5;
    private const int CookieLength = 32;
    private const int ResumeSecretLength = 16;
    private const uint ProtocolVersion = 1;
    private const uint DialerSessionId = 0xCAFE_BABE;
    private const uint DialerMaxMessageSize = 4096;
    private const ulong DialerSendIdleIntervalMilliseconds = 60_000;
    private const ulong DialerLingerMilliseconds = 30_000;
    private const int DatagramHeaderSize = 20;

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> — a bare
    /// <see cref="RawReactor"/> — and returns the wire session id <paramref name="accepter"/> assigned
    /// this Peer, mirroring <see cref="DatagramSessionSpaceRoutingTests"/>'s helper of the same name.
    /// </summary>
    private static uint EstablishLiveSessionId(DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink)
    {
        const ulong clientNonce = 0x0102_0304_0506_0708UL;
        Source destination = Source.FromIPEndPoint(accepter.Endpoint);

        byte[] hello = BuildHelloDatagram(clientNonce);
        client.SendTo(clientLink, destination, hello);
        byte[] cookieDatagram = ExchangeUntilReceived(client, accepter);

        ulong echoedNonce = BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset, 8));
        long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset + 8, 8)));
        byte[] cookie = cookieDatagram.AsSpan(ControlBodyOffset + 16, CookieLength).ToArray();
        echoedNonce.Should().Be(clientNonce);

        byte[] helloWithCookie = BuildHelloWithCookieDatagram(clientNonce, epoch, cookie);
        client.SendTo(clientLink, destination, helloWithCookie);
        byte[] acceptDatagram = ExchangeUntilReceived(client, accepter);

        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
    }

    /// <summary>
    /// Drains the <c>PeerConnected</c> event <see cref="EstablishLiveSessionId"/> leaves queued and
    /// returns its <see cref="PeerId"/>, mirroring <see cref="DatagramSessionSpaceRoutingTests"/>'s
    /// helper of the same name. Must be called before anything else drains the accepter's events.
    /// </summary>
    private static PeerId FindAccepterPeer(DatagramSessionReactorHarness accepter)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();
        return drained.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;
    }

    private static byte[] ExchangeUntilReceived(RawReactor client, DatagramSessionReactorHarness accepter)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            accepter.Reactor.Pump();
            client.Pump();
            foreach (RawEvent evt in client.Drain())
            {
                if (evt.Kind == RawEventKind.DatagramReceived)
                {
                    return evt.Payload.ToArray();
                }
            }

            if (deadline.Expired)
            {
                throw new InvalidOperationException("The handshake reply did not arrive within the Pump budget.");
            }

            PumpDeadline.Yield();
        }
    }

    private static byte[] BuildControlDatagram(byte controlType, ReadOnlySpan<byte> body)
    {
        byte[] datagram = new byte[ControlDatagramSize];
        BinaryPrimitives.WriteUInt32BigEndian(datagram, ControlSession);
        datagram[4] = controlType;
        body.CopyTo(datagram.AsSpan(ControlBodyOffset));
        return datagram;
    }

    private static byte[] BuildHelloDatagram(ulong clientNonce)
    {
        byte[] body = new byte[4 + 8];
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(0, 4), ProtocolVersion);
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(4, 8), clientNonce);
        return BuildControlDatagram(HelloType, body);
    }

    private static byte[] BuildHelloWithCookieDatagram(ulong clientNonce, long epoch, byte[] cookie)
    {
        byte[] body = new byte[4 + 8 + 8 + CookieLength + 4 + 4 + 8 + 8 + ResumeSecretLength];
        int offset = 0;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), ProtocolVersion);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), clientNonce);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), unchecked((ulong)epoch));
        offset += 8;
        cookie.CopyTo(body.AsSpan(offset, CookieLength));
        offset += CookieLength;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), DialerSessionId);
        offset += 4;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), DialerMaxMessageSize);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), DialerSendIdleIntervalMilliseconds);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), DialerLingerMilliseconds);
        offset += 8;
        return BuildControlDatagram(HelloWithCookieType, body);
    }

    /// <summary>Builds a 20-byte live-Session datagram header naming <paramref name="session"/> with both ack blocks zeroed.</summary>
    private static byte[] BuildHeader(uint session)
    {
        byte[] header = new byte[DatagramHeaderSize];
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0, 4), session);
        return header;
    }

    /// <summary>Builds one unfragmented, empty-payload Unreliable-Space packet record for <paramref name="sequence"/>.</summary>
    private static byte[] BuildRecord(uint sequence)
    {
        const int unfragmentedRecordSize = 7;
        byte[] record = new byte[unfragmentedRecordSize];
        record[0] = ModeUnreliable;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), 0);
        return record;
    }

    [Fact]
    public void Construct_ReceiveWindowBelowItsFloor_ThrowsThroughTheReactorWithTheExistingMessage()
    {
        // Arrange — a window of one fragment cannot hold a Message that occupies four, so the
        // options' floor refuses it. Asserted through the Reactor rather than the options object
        // alone (issue #171): the arena is taken at construction, so a combination that cannot be
        // afforded must fail before any socket is bound, not later when the first datagram arrives.
        const int fragmentSize = 1200;
        const int maxMessageSize = fragmentSize * 4;

        // Act
        Action construct = () => _ = new DatagramSessionReactorHarness(
            maxMessageSize: maxMessageSize,
            fragmentSize: fragmentSize,
            receiveWindowFragments: 1);

        // Assert
        construct.Should().Throw<ArgumentException>()
            .WithMessage($"*a receive window of 1 fragments cannot hold one {maxMessageSize}-byte Message*");
    }
}
