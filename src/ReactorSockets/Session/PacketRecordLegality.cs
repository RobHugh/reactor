namespace ReactorSockets.Session;

/// <summary>
/// What a packet record of a given shape must look like, and whether this profile acts on it — stated
/// once, as a table, so that the parser and the sender both consult the same statement rather than two
/// copies that can drift apart.
/// </summary>
/// <param name="IsProcessed">
/// Whether this profile reads a record of this shape at all. Both wire shapes ADR 0009 defines are
/// <see langword="true"/> here; only a shape not on the roster falls through to the discard arm and
/// is rejected exactly as an unrecognised shape would be — the wire is not the place to advertise an
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
/// against whatever length the last arm happened to name. The fragment shape's row also carries the
/// mode restriction ADR 0004 puts on it, since which of the two shapes a record may be is decided by
/// the same flags byte and belongs in the same table rather than in a second test the parser would have
/// to remember to make.
/// </summary>
internal static class PacketRecordLegality
{
    /// <summary>
    /// The shape of a packet record whose flags byte is <paramref name="flags"/>, decided by
    /// <see cref="DatagramSessionProtocol.FragmentBit"/> — the only bit in the flags byte that changes
    /// which shape a record is — and, for the fragment shape, by the two mode bits that say whether a
    /// record of that shape may exist at all.
    /// </summary>
    public static PacketRecordShape ShapeOf(byte flags) => (flags & DatagramSessionProtocol.FragmentBit) switch
    {
        // The unfragmented shape: flags(1) + seq(4) + length(2). A complete Message.
        0 => new PacketRecordShape(true, DatagramSessionProtocol.UnfragmentedRecordSize),

        // The fragment shape: the unfragmented shape's bytes followed by fragIndex(2) + fragCount(2).
        // One piece of a larger Message; what those two extra fields mean is judged elsewhere
        // (issue #253), not here — this table only says the shape is read. Reliable modes only: you may
        // only fragment what you can retransmit (ADR 0004), so the fragment shape in an unreliable mode
        // names a record no conforming sender can produce and this profile has no reassembly path for.
        DatagramSessionProtocol.FragmentBit when !IsUnreliableMode(flags) =>
            new PacketRecordShape(true, DatagramSessionProtocol.FragmentRecordSize),

        // The discard arm refuses, and now carries a shape a peer can actually put on the wire: the
        // fragment shape in an unreliable mode. Masking flags against FragmentBit only ever produces
        // one of the two values named above, so the arm was previously unreachable in fact though not
        // in the compiler's eyes — the fail-closed shape ControlDatagramLegality.ShapeOf and
        // RawLinkLegality.RefusalFor both follow for an outcome no row names.
        _ => new PacketRecordShape(false, 0),
    };

    /// <summary>
    /// Whether <paramref name="record"/> is refused by <see cref="PacketRecordCodec.TryRead"/>
    /// specifically because its flags byte names a Fragment in an unreliable mode, rather than for any
    /// of the other reasons a read fails. <see langword="false"/> for a record whose spare bits are
    /// set, mirroring <see cref="PacketRecordCodec.TryRead"/>'s own precedence: a spare bit is judged
    /// before the flags byte is read for anything else, so a record breaking both rules is reported as
    /// the violation that actually refused it.
    /// </summary>
    /// <param name="record">
    /// The datagram's bytes from the record's first byte onward, exactly as handed to
    /// <see cref="PacketRecordCodec.TryRead"/>. An empty span is <see langword="false"/>: there is no
    /// flags byte to have named anything.
    /// </param>
    public static bool RefusedAsUnreliableFragment(ReadOnlySpan<byte> record) =>
        !record.IsEmpty
        && (record[0] & DatagramSessionProtocol.SpareBitsMask) == 0
        && (record[0] & DatagramSessionProtocol.FragmentBit) != 0
        && IsUnreliableMode(record[0]);

    /// <summary>
    /// Whether <paramref name="flags"/>'s two mode bits name one of the two unreliable modes — the two
    /// ADR 0004 caps at a single Datagram, and so the two that may never carry the fragment shape.
    /// </summary>
    private static bool IsUnreliableMode(byte flags) =>
        (flags & DatagramSessionProtocol.ModeMask)
            is DatagramSessionProtocol.ModeUnreliable or DatagramSessionProtocol.ModeUnreliableSequenced;

    /// <summary>
    /// Whether a record already read as far as its mode, fragment shape, and length may legally carry
    /// <see cref="DatagramSessionProtocol.ControlBit"/>. Exactly two combinations qualify —
    /// unfragmented, zero length, and either <c>ReliableUnordered</c> (a Ping) or
    /// <c>ReliableOrdered</c> (a Goodbye) — so the Space alone tells the two apart. Stated once here so
    /// <see cref="PacketRecordCodec.TryRead"/> consults this table for the control bit's legality the
    /// same way it already consults <see cref="ShapeOf"/> for a record's shape, rather than restating
    /// the three-way condition inline.
    /// </summary>
    /// <param name="mode">The record's decoded delivery mode.</param>
    /// <param name="isFragment">Whether the record carries the fragment shape.</param>
    /// <param name="length">The record's declared payload length.</param>
    public static bool IsLegalControlRecord(PacketRecordMode mode, bool isFragment, ushort length) =>
        mode is PacketRecordMode.ReliableUnordered or PacketRecordMode.ReliableOrdered && !isFragment && length == 0;
}
