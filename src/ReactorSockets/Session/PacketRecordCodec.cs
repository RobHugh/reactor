using System.Buffers.Binary;

namespace ReactorSockets.Session;

/// <summary>
/// The wire value of a packet record's two mode bits (ADR 0009), named rather than left as a raw
/// byte so a caller reading <see cref="PacketRecordCodec.TryRead"/>'s result does not have to
/// re-derive what each bit pattern means. Not the public <c>DeliveryMode</c> enum #44 owns: that
/// enum is this Session's user-facing vocabulary, and building it is out of scope here. This one is
/// scoped to <see cref="PacketRecordCodec"/> and exists only so #163's parse loop and #164 have a
/// symbol to switch on instead of the four <see cref="DatagramSessionProtocol"/> mode constants.
/// </summary>
internal enum PacketRecordMode : byte
{
    /// <summary>The wire value of the <c>Unreliable</c> mode's two mode bits.</summary>
    Unreliable = DatagramSessionProtocol.ModeUnreliable,

    /// <summary>The wire value of the <c>UnreliableSequenced</c> mode's two mode bits.</summary>
    UnreliableSequenced = DatagramSessionProtocol.ModeUnreliableSequenced,

    /// <summary>The wire value of the <c>ReliableUnordered</c> mode's two mode bits.</summary>
    ReliableUnordered = DatagramSessionProtocol.ModeReliableUnordered,

    /// <summary>The wire value of the <c>ReliableOrdered</c> mode's two mode bits.</summary>
    ReliableOrdered = DatagramSessionProtocol.ModeReliableOrdered,
}

/// <summary>
/// The single place a packet record (ADR 0009) is read out of a datagram. A datagram is a container
/// of as many records as fit, so this class reads exactly one record positioned at the start of the
/// span it is given and reports how many bytes it consumed — the caller (#163's parse loop)
/// re-slices past that count to read the next one. This subtask covers reading only; writing a
/// record belongs to whichever ticket first needs to send one.
/// </summary>
/// <remarks>
/// Allocation-free and total by construction, the same discipline <see cref="ControlDatagramCodec"/>
/// follows for control datagrams: every member works over <see cref="ReadOnlySpan{T}"/>, no path
/// throws, and every length is checked against what remains before it is read, never after. Unknown
/// flag bits are a protocol violation rather than something to tolerate (ADR 0009) — a spare bit set
/// or the fragment bit set both fail the read outright, the same as a record too short to hold its
/// declared shape or a length that would run past the datagram. Multi-byte integers are big-endian,
/// matched by every reader here rather than left to the host's native order.
/// </remarks>
internal static class PacketRecordCodec
{
    /// <summary>
    /// Reads one packet record from the start of <paramref name="record"/>, which is positioned at
    /// the record's first byte and extends to the end of the bytes remaining in the datagram —
    /// callers do not pass a separate remaining-length parameter because <paramref name="record"/>'s
    /// own length already is that count. Fails without reading past <paramref name="record"/> when:
    /// any spare bit (<see cref="DatagramSessionProtocol.SpareBitsMask"/>) is set; the fragment bit
    /// is set, since the fragment shape is rejected until slice 7 implements it
    /// (<see cref="PacketRecordLegality"/>); fewer bytes remain than the record's shape needs; or
    /// <paramref name="length"/> would exceed the bytes left in the datagram after the record header.
    /// </summary>
    /// <param name="record">
    /// The datagram's bytes from this record's first byte onward. Its length is the bytes remaining
    /// in the datagram, which is also what bounds <paramref name="length"/>.
    /// </param>
    /// <param name="mode">The record's delivery mode, valid only when this method returns <see langword="true"/>.</param>
    /// <param name="sequence">The record's <c>u32</c> sequence number, valid only when this method returns <see langword="true"/>.</param>
    /// <param name="length">The record's declared payload length in bytes, valid only when this method returns <see langword="true"/>.</param>
    /// <param name="bytesConsumed">
    /// The total bytes this record occupies in the datagram — its header plus its payload — so the
    /// caller can slice past it to read the next record. Valid only when this method returns
    /// <see langword="true"/>.
    /// </param>
    public static bool TryRead(
        ReadOnlySpan<byte> record,
        out PacketRecordMode mode,
        out uint sequence,
        out ushort length,
        out int bytesConsumed)
    {
        mode = default;
        sequence = default;
        length = default;
        bytesConsumed = default;

        // Nothing to read the shape from: a record needs at least its flags byte before the legality
        // table can say how long it is.
        if (record.IsEmpty)
        {
            return false;
        }

        // A spare bit set is a protocol violation, not something to mask off. With exact version
        // matching, leniency buys nothing and would cost the ability to ever trust a bit's absence.
        byte flags = record[0];
        if ((flags & DatagramSessionProtocol.SpareBitsMask) != 0)
        {
            return false;
        }

        // The fragment shape rejects here, through the table rather than through a second bit test, so
        // the shape a record claims and the bytes it is read against can never disagree.
        PacketRecordShape shape = PacketRecordLegality.ShapeOf(flags);
        if (!shape.IsProcessed || record.Length < shape.RecordSize)
        {
            return false;
        }

        // The declared length is bounded against what is actually left before a single payload byte is
        // touched, so a length a stranger chose can never move a read past the received bytes.
        ushort candidateLength = BinaryPrimitives.ReadUInt16BigEndian(record[DatagramSessionProtocol.RecordLengthOffset..]);
        if (candidateLength > record.Length - shape.RecordSize)
        {
            return false;
        }

        mode = (PacketRecordMode)(flags & DatagramSessionProtocol.ModeMask);
        sequence = BinaryPrimitives.ReadUInt32BigEndian(record[DatagramSessionProtocol.RecordSequenceOffset..]);
        length = candidateLength;
        bytesConsumed = shape.RecordSize + candidateLength;
        return true;
    }
}
