using System.Buffers.Binary;

namespace ReactorSockets.Session;

/// <summary>
/// The wire value of a packet record's two mode bits (ADR 0009), named rather than left as a raw
/// byte so a caller reading <see cref="PacketRecordCodec.TryRead"/>'s result does not have to
/// re-derive what each bit pattern means. Not the public <see cref="ReactorSockets.DeliveryMode"/>
/// enum (issue #207): that enum is this Session's user-facing vocabulary. This one is
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
/// The single place a packet record (ADR 0009) is read from or written to a datagram. A datagram is a
/// container of as many records as fit, so this class reads or writes exactly one record positioned at
/// the start of the span it is given and reports how many bytes it consumed or produced — the caller
/// (#163's parse loop for reading, <see cref="DatagramRecordPacker"/> for writing) re-slices past that
/// count to move on to the next one.
/// </summary>
/// <remarks>
/// Allocation-free and total by construction, the same discipline <see cref="ControlDatagramCodec"/>
/// follows for control datagrams: every member works over <see cref="ReadOnlySpan{T}"/> or
/// <see cref="Span{T}"/>, no path throws for an ordinary and expected outcome, and every length is
/// checked against what remains before it is read or written, never after. Unknown flag bits are a
/// protocol violation rather than something to tolerate (ADR 0009) — a spare bit set fails the read
/// outright, the same as a record too short to hold its declared shape or a length that would run past
/// the datagram. Two shapes are on the wire (<see cref="PacketRecordLegality"/>): an unfragmented
/// record, a complete Message; and a Fragment, one piece of a larger Message, which carries a fragment
/// index and fragment count no unfragmented record has room for or needs. <see cref="TryWrite"/> does
/// not yet mirror that on the way out: it only ever writes the unfragmented shape, since nothing sends
/// Fragments until slice 7 gives the fragment shape a writer. Multi-byte integers are big-endian,
/// matched by every reader and writer here rather than left to the host's native order.
/// </remarks>
internal static class PacketRecordCodec
{
    /// <summary>
    /// Reads one packet record from the start of <paramref name="record"/>, which is positioned at
    /// the record's first byte and extends to the end of the bytes remaining in the datagram —
    /// callers do not pass a separate remaining-length parameter because <paramref name="record"/>'s
    /// own length already is that count. Reads either wire shape (<see cref="PacketRecordLegality"/>):
    /// an unfragmented record, whose <paramref name="isFragment"/> comes back <see langword="false"/>
    /// with <paramref name="fragmentIndex"/> and <paramref name="fragmentCount"/> left unset, since a
    /// complete Message has no fragment to index into; or a Fragment, whose two extra fields are read
    /// and reported alongside it. This method reads the shape only — it does not judge whether a
    /// Fragment's index and count describe a legal set (issue #253). Fails without reading past
    /// <paramref name="record"/> when: any spare bit (<see cref="DatagramSessionProtocol.SpareBitsMask"/>)
    /// is set; fewer bytes remain than the record's shape needs; or <paramref name="length"/> would
    /// exceed the bytes left in the datagram after the record header.
    /// </summary>
    /// <param name="record">
    /// The datagram's bytes from this record's first byte onward. Its length is the bytes remaining
    /// in the datagram, which is also what bounds <paramref name="length"/>.
    /// </param>
    /// <param name="mode">The record's delivery mode, valid only when this method returns <see langword="true"/>.</param>
    /// <param name="sequence">The record's <c>u32</c> sequence number, valid only when this method returns <see langword="true"/>.</param>
    /// <param name="length">The record's declared payload length in bytes, valid only when this method returns <see langword="true"/>.</param>
    /// <param name="isFragment">
    /// Whether this record carries the fragment shape, valid only when this method returns
    /// <see langword="true"/>. <see langword="false"/> for an unfragmented record.
    /// </param>
    /// <param name="fragmentIndex">
    /// The record's <c>u16</c> fragment index, valid only when this method returns
    /// <see langword="true"/> and <paramref name="isFragment"/> is <see langword="true"/>. Unset — and
    /// meaningless — for an unfragmented record.
    /// </param>
    /// <param name="fragmentCount">
    /// The record's <c>u16</c> fragment count, valid only when this method returns
    /// <see langword="true"/> and <paramref name="isFragment"/> is <see langword="true"/>. Unset — and
    /// meaningless — for an unfragmented record.
    /// </param>
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
        out bool isFragment,
        out ushort fragmentIndex,
        out ushort fragmentCount,
        out int bytesConsumed)
    {
        mode = default;
        sequence = default;
        length = default;
        isFragment = default;
        fragmentIndex = default;
        fragmentCount = default;
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

        // The shape a record claims comes from the table, not from a second bit test here, so the
        // shape and the bytes it is read against can never disagree.
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

        // The fragment fields exist only on the longer shape, so they are read only when the flags
        // byte claims it — reading them off an unfragmented record would read into its payload.
        isFragment = (flags & DatagramSessionProtocol.FragmentBit) != 0;
        if (isFragment)
        {
            fragmentIndex = BinaryPrimitives.ReadUInt16BigEndian(record[DatagramSessionProtocol.RecordFragmentIndexOffset..]);
            fragmentCount = BinaryPrimitives.ReadUInt16BigEndian(record[DatagramSessionProtocol.RecordFragmentCountOffset..]);
        }

        bytesConsumed = shape.RecordSize + candidateLength;
        return true;
    }

    /// <summary>
    /// Writes one unfragmented packet record — flags byte, <c>u32</c> sequence, <c>u16</c> length,
    /// then the payload itself — to the start of <paramref name="destination"/>. The counterpart to
    /// <see cref="TryRead"/>, used by <see cref="DatagramRecordPacker"/> to pack a Peer's Sent records
    /// into an outbound datagram. Only the unfragmented shape is written; the fragment shape has no
    /// writer until slice 7 gives it one, matching <see cref="PacketRecordLegality"/> only ever
    /// processing the unfragmented shape today.
    /// </summary>
    /// <param name="destination">
    /// The bytes to write into, positioned at the record's first byte. Never partially written: a
    /// <paramref name="destination"/> too short to hold this record's header plus
    /// <paramref name="payload"/> fails without writing anything, the same as a caller finding the
    /// remaining space in a datagram already spent.
    /// </param>
    /// <param name="mode">The record's delivery mode, written into the flags byte's two mode bits. No other bit in the flags byte is ever set — never a spare bit, never the fragment bit.</param>
    /// <param name="sequence">The record's <c>u32</c> sequence number.</param>
    /// <param name="payload">
    /// The record's payload bytes, copied verbatim after the record's fixed header. Its length becomes
    /// the record's <c>u16</c> length field.
    /// </param>
    /// <param name="bytesWritten">
    /// The total bytes written — the record's header plus <paramref name="payload"/> — so the caller
    /// can advance past it. Valid only when this method returns <see langword="true"/>.
    /// </param>
    /// <exception cref="ArgumentException">
    /// <paramref name="payload"/> is longer than <see cref="ushort.MaxValue"/>. The wire format's
    /// length field cannot represent it, and a payload this size is always chosen by our own code, so
    /// this is our bug, not a remote sender's — unlike a short <paramref name="destination"/>, which is
    /// an ordinary and expected outcome reported by a <see langword="false"/> return.
    /// </exception>
    public static bool TryWrite(
        Span<byte> destination,
        PacketRecordMode mode,
        uint sequence,
        ReadOnlySpan<byte> payload,
        out int bytesWritten)
    {
        bytesWritten = default;

        if (payload.Length > ushort.MaxValue)
        {
            throw new ArgumentException(
                $"A packet record's payload cannot exceed {ushort.MaxValue} bytes.",
                nameof(payload));
        }

        int recordSize = DatagramSessionProtocol.UnfragmentedRecordSize + payload.Length;
        if (destination.Length < recordSize)
        {
            return false;
        }

        destination[0] = (byte)mode;
        BinaryPrimitives.WriteUInt32BigEndian(destination[DatagramSessionProtocol.RecordSequenceOffset..], sequence);
        BinaryPrimitives.WriteUInt16BigEndian(destination[DatagramSessionProtocol.RecordLengthOffset..], (ushort)payload.Length);
        payload.CopyTo(destination[DatagramSessionProtocol.UnfragmentedRecordSize..]);

        bytesWritten = recordSize;
        return true;
    }
}
