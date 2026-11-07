namespace ReactorSockets.Session;

/// <summary>
/// One packet record (ADR 0009) as <see cref="PacketRecordCodec.TryRead"/> decoded it: every field the
/// record carries, its payload, and how many bytes it occupies in the datagram. A ref struct rather than
/// a record struct because <see cref="Payload"/> is a view into the received datagram, not a copy, so the
/// value cannot outlive the bytes it was read from. Every member is meaningful only on a value a
/// successful read produced; <see langword="default"/> is what a failed read leaves behind.
/// </summary>
internal readonly ref struct PacketRecord
{
    /// <summary>Creates a decoded record. <see cref="PacketRecordCodec.TryRead"/> is the only caller.</summary>
    /// <param name="mode">The record's delivery mode.</param>
    /// <param name="sequence">The record's <c>u32</c> sequence number.</param>
    /// <param name="isFragment">Whether the record carries the fragment shape.</param>
    /// <param name="fragmentIndex">The fragment index, or 0 for an unfragmented record.</param>
    /// <param name="fragmentCount">The fragment count, or 0 for an unfragmented record.</param>
    /// <param name="isControl">Whether the record is a Ping or a Goodbye.</param>
    /// <param name="payload">The record's payload bytes, a view into the datagram.</param>
    /// <param name="size">The bytes the record occupies, header plus payload.</param>
    public PacketRecord(
        PacketRecordMode mode,
        uint sequence,
        bool isFragment,
        ushort fragmentIndex,
        ushort fragmentCount,
        bool isControl,
        ReadOnlySpan<byte> payload,
        int size)
    {
        Mode = mode;
        Sequence = sequence;
        IsFragment = isFragment;
        FragmentIndex = fragmentIndex;
        FragmentCount = fragmentCount;
        IsControl = isControl;
        Payload = payload;
        Size = size;
    }

    /// <summary>The record's delivery mode.</summary>
    public PacketRecordMode Mode { get; }

    /// <summary>The record's <c>u32</c> sequence number.</summary>
    public uint Sequence { get; }

    /// <summary>
    /// Whether this record carries the fragment shape. <see langword="false"/> for an unfragmented
    /// record, which has no <see cref="FragmentIndex"/> or <see cref="FragmentCount"/>.
    /// </summary>
    public bool IsFragment { get; }

    /// <summary>
    /// The record's <c>u16</c> fragment index when <see cref="IsFragment"/> is <see langword="true"/>,
    /// always less than <see cref="FragmentCount"/>. 0 and meaningless for an unfragmented record.
    /// </summary>
    public ushort FragmentIndex { get; }

    /// <summary>
    /// The record's <c>u16</c> fragment count when <see cref="IsFragment"/> is <see langword="true"/>,
    /// always at least 2. 0 and meaningless for an unfragmented record.
    /// </summary>
    public ushort FragmentCount { get; }

    /// <summary>
    /// Whether this record is an in-Session control record, a Ping or a Goodbye. When
    /// <see langword="true"/> the record is always unfragmented, has an empty <see cref="Payload"/>, and
    /// has a <see cref="Mode"/> of <c>ReliableUnordered</c> (a Ping) or <c>ReliableOrdered</c> (a
    /// Goodbye).
    /// </summary>
    public bool IsControl { get; }

    /// <summary>
    /// The record's payload bytes, sliced from the datagram the record was read from. Its length is the
    /// record's declared <c>u16</c> length field.
    /// </summary>
    public ReadOnlySpan<byte> Payload { get; }

    /// <summary>
    /// The total bytes this record occupies in the datagram, its header plus its payload, so a reader
    /// can slice past it to the next record.
    /// </summary>
    public int Size { get; }
}
