namespace ReactorSockets.Session;

/// <summary>
/// What a packet record of a given shape must look like, and whether this slice acts on it — stated
/// once, as a table, so that the parser and the sender both consult the same statement rather than two
/// copies that can drift apart.
/// </summary>
/// <param name="IsProcessed">
/// Whether this slice does anything with a record of this shape. The fragment shape is on the wire
/// format (ADR 0009) but not implemented until slice 7, so it is <see langword="false"/> here and
/// rejected exactly as an unrecognised shape would be — the wire is not the place to advertise an
/// unfinished feature.
/// </param>
/// <param name="RecordSize">
/// The exact number of bytes a record of this shape occupies, taken from
/// <see cref="DatagramSessionProtocol"/> rather than restated.
/// </param>
internal readonly record struct PacketRecordShape(bool IsProcessed, int RecordSize);

/// <summary>
/// The packet record roster as a single table (the house pattern <see cref="ControlDatagramLegality"/>
/// and <see cref="RawLinkLegality"/> set). Both shapes ADR 0009 defines are spelled out and the
/// fall-through refuses, so a shape added without a decision here is rejected rather than parsed
/// against whatever length the last arm happened to name.
/// </summary>
internal static class PacketRecordLegality
{
    /// <summary>
    /// The shape of a packet record whose flags byte is <paramref name="flags"/>, decided by
    /// <see cref="DatagramSessionProtocol.FragmentBit"/> — the only bit in the flags byte that changes
    /// which shape a record is.
    /// </summary>
    public static PacketRecordShape ShapeOf(byte flags) => (flags & DatagramSessionProtocol.FragmentBit) switch
    {
        // The unfragmented shape: flags(1) + seq(4) + length(2). The only shape this slice processes.
        0 => new PacketRecordShape(true, DatagramSessionProtocol.UnfragmentedRecordSize),

        // The fragment shape: the unfragmented shape's bytes followed by fragIndex(2) + fragCount(2).
        // On the roster because ADR 0009 defines it; not implemented until slice 7, and therefore
        // rejected here rather than half-parsed — the two parse shapes exist as a contract from the
        // start even though only one of them does anything yet.
        DatagramSessionProtocol.FragmentBit => new PacketRecordShape(false, DatagramSessionProtocol.FragmentRecordSize),

        // The discard arm refuses. Masking flags against FragmentBit only ever produces one of the two
        // values named above, but the compiler cannot prove that, so this arm stays reachable in its
        // eyes — which is exactly the fail-closed shape ControlDatagramLegality.ShapeOf and
        // RawLinkLegality.RefusalFor both follow for an outcome no row names.
        _ => new PacketRecordShape(false, 0),
    };
}
