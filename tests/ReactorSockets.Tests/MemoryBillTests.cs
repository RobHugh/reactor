using System.Net;
using System.Runtime.CompilerServices;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #63: each profile's memory bill, as <c>docs/options.md</c> and the options types' remarks
/// state it, is evaluated against what construction actually allocates. Every measurement is a pair of
/// Reactors identical but for the knobs the formula names, each with every share-holder Slot occupied,
/// so the difference between them is exactly the formula's own bytes with nothing else to allow for —
/// the pattern <see cref="DatagramSessionReceiveArenaAllocationTests"/> established.
/// </summary>
public class MemoryBillTests
{
    private const int MeasurementCapacity = 4;

    /// <summary>
    /// The smallest of this many samples is the construction's own cost (issue #415): noise that is not
    /// the construction's own only ever adds bytes.
    /// </summary>
    private const int SamplesPerMeasurement = 5;

    /// <summary>The stream profile's <c>u32</c> length prefix, which sizes the Message-boundary ring.</summary>
    private const int LengthPrefixBytes = 4;

    /// <summary>
    /// The bookkeeping one datagram retention slot carries beside its payload: occupancy, sequence,
    /// length, fragment flag, fragment index, fragment count and control flag. Restated here rather than
    /// referenced, because the test project cannot see internals (tests/CLAUDE.md).
    /// </summary>
    private const int RetentionSlotBookkeepingBytes = 15;

    /// <summary>Mirrors the receive window's per-slot length and group-identity entries (#257, #297).</summary>
    private const int ReceiveSlotBookkeepingBytes = 2 + 6;

    /// <summary>Mirrors the six-byte waiting-group entry (#259).</summary>
    private const int PendingGroupEntryBytes = 6;

    [Fact]
    public void Construct_StreamProfile_AllocatesCapacityTimesRetentionPlusReceiveBufferPlusBoundaryRing()
    {
        // Arrange
        const int maxMessageSize = 1000;
        StreamSessionOptions small = StreamOptions(maxMessageSize, retentionBytes: 2000, receiveBufferSize: 1004);
        StreamSessionOptions large = StreamOptions(maxMessageSize, retentionBytes: 6000, receiveBufferSize: 5004);

        // Act
        Measure(() => SampleStream(small));
        Measure(() => SampleStream(large));
        long measured = Measure(() => SampleStream(large)) - Measure(() => SampleStream(small));

        // Assert — capacity × (retention + receive buffer), plus the boundary ring of one uint per four
        // retained bytes. No traffic term: every Slot is occupied and nothing has been sent.
        measured.Should().Be(StreamFormula(large) - StreamFormula(small));
    }

    [Fact]
    public void StreamBudgetProperties_AnyConfiguration_SumToTheDocumentedFormula()
    {
        // Arrange
        StreamSessionOptions options = StreamOptions(maxMessageSize: 1000, retentionBytes: 6001, receiveBufferSize: 5004);

        // Act
        long published = options.ReceiveBudgetBytes + options.SendBudgetBytes + options.BoundaryRingBudgetBytes;

        // Assert
        published.Should().Be(StreamFormula(options));
    }

    [Fact]
    public void Construct_DatagramProfile_AllocatesTheReceiveLineAndTheSendLine()
    {
        // Arrange
        const int fragmentSize = 1200;
        DatagramSessionOptions small = DatagramOptions(fragmentSize, windowFragments: 2, retentionBytes: fragmentSize);
        DatagramSessionOptions large = DatagramOptions(fragmentSize, windowFragments: 8, retentionBytes: 5 * fragmentSize);

        // Act
        Measure(() => SampleDatagram(small));
        Measure(() => SampleDatagram(large));
        long measured = Measure(() => SampleDatagram(large)) - Measure(() => SampleDatagram(small));

        // Assert
        measured.Should().Be(DatagramFormula(large) - DatagramFormula(small));
    }

    [Fact]
    public void DatagramBudgetProperties_AnyConfiguration_SumToTheDocumentedFormula()
    {
        // Arrange — retention that is not a whole number of fragments, so the truncation to whole slots
        // is part of what is checked.
        DatagramSessionOptions options = DatagramOptions(fragmentSize: 1200, windowFragments: 7, retentionBytes: 5000);

        // Act
        long published = options.ReceiveBudgetBytes + options.SendBudgetBytes;

        // Assert
        published.Should().Be(DatagramFormula(options));
    }

    [Fact]
    public void Construct_RawProfile_AllocatesTheScratchBufferPlusTheArena()
    {
        // Arrange
        ReactorOptions small = new() { Capacity = MeasurementCapacity, ArenaSize = 1000 };
        ReactorOptions large = new() { Capacity = MeasurementCapacity, ArenaSize = 9000 };

        // Act
        Measure(() => SampleRaw(small, scratchBufferSize: 1000));
        Measure(() => SampleRaw(large, scratchBufferSize: 5000));
        long measured = Measure(() => SampleRaw(large, scratchBufferSize: 5000))
            - Measure(() => SampleRaw(small, scratchBufferSize: 1000));

        // Assert — one scratch buffer and one arena, neither multiplied by capacity.
        measured.Should().Be((5000 + 9000) - (1000 + 1000));
    }

    /// <summary>
    /// The stream bill as documented: <c>Capacity × (RetentionBytes + receive buffer)</c>, plus the
    /// Message-boundary ring of one <c>uint</c> per <see cref="LengthPrefixBytes"/> retained bytes.
    /// </summary>
    private static long StreamFormula(StreamSessionOptions options)
    {
        long ringEntries = (options.RetentionBytes + LengthPrefixBytes - 1) / LengthPrefixBytes;
        return (long)options.Reactor.Capacity
            * (options.RetentionBytes + options.ResolvedReceiveBufferSize + (ringEntries * sizeof(uint)));
    }

    /// <summary>
    /// The datagram bill as documented. Receive: <c>Capacity × 2 × window × fragmentSize</c> of payload,
    /// with each payload slot's bookkeeping, one admission byte per slot for all four Spaces, and the
    /// waiting-group entries. Send: <c>Capacity × 2 × RetentionBytes</c>, in whole fragment slots, each
    /// with its bookkeeping.
    /// </summary>
    private static long DatagramFormula(DatagramSessionOptions options)
    {
        long capacity = options.Reactor.Capacity;
        long window = options.ResolvedReceiveWindowFragments;
        long fragmentSize = options.ResolvedFragmentSize;
        long pendingGroups = Math.Max(1, window / 2);

        long receive = (capacity * 2 * window * (fragmentSize + ReceiveSlotBookkeepingBytes))
            + (capacity * 4 * window)
            + (capacity * 2 * pendingGroups * PendingGroupEntryBytes);
        long send = capacity * 2 * (options.RetentionBytes / fragmentSize) * (fragmentSize + RetentionSlotBookkeepingBytes);
        return receive + send;
    }

    private static StreamSessionOptions StreamOptions(int maxMessageSize, int retentionBytes, int receiveBufferSize) =>
        new()
        {
            Reactor = new ReactorOptions { Capacity = MeasurementCapacity },
            MaxMessageSize = maxMessageSize,
            RetentionBytes = retentionBytes,
            ReceiveBufferSize = receiveBufferSize,
            SendIdleInterval = TimeSpan.FromSeconds(1),
            Linger = TimeSpan.FromSeconds(1),
        };

    private static DatagramSessionOptions DatagramOptions(int fragmentSize, int windowFragments, int retentionBytes) =>
        new()
        {
            Reactor = new ReactorOptions { Capacity = MeasurementCapacity },
            MaxMessageSize = fragmentSize,
            FragmentSize = fragmentSize,
            ReceiveWindowFragments = windowFragments,
            RetentionBytes = retentionBytes,
            SendIdleInterval = TimeSpan.FromSeconds(1),
            Linger = TimeSpan.FromSeconds(1),
        };

    private static long Measure(Func<long> sample)
    {
        long smallest = long.MaxValue;
        for (int i = 0; i < SamplesPerMeasurement; i++)
        {
            smallest = Math.Min(smallest, sample());
        }

        return smallest;
    }

    /// <summary>Builds a stream Reactor, dials every Slot full, and returns the bytes that took.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static long SampleStream(StreamSessionOptions options)
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        using StreamSessionReactor reactor = new(options);
        for (int i = 0; i < MeasurementCapacity; i++)
        {
            reactor.Connect(new IPEndPoint(IPAddress.Loopback, 40000 + i));
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    /// <summary>Builds a datagram Reactor, dials every Slot full, and returns the bytes that took.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static long SampleDatagram(DatagramSessionOptions options)
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        using DatagramSessionReactor reactor = new(options);
        for (int i = 0; i < MeasurementCapacity; i++)
        {
            reactor.Connect(new IPEndPoint(IPAddress.Loopback, 40000 + i));
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    /// <summary>Builds a Raw Reactor and returns the bytes that took. Raw holds nothing per Link.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static long SampleRaw(ReactorOptions options, int scratchBufferSize)
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        using RawReactor reactor = new(options, scratchBufferSize);
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }
}
