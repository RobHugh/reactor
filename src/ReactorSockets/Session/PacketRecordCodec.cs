using System.Buffers.Binary;

namespace ReactorSockets.Session;

/// <summary>
/// The wire value of a packet record's two mode bits (ADR 0009), named rather than left as a raw
/// byte so a caller reading <see cref="PacketRecordCodec.TryRead"/>'s result does not have to
/// re-derive what each bit pattern means. Not the public <see cref="ReactorSockets.DeliveryMode"/>
/// enum: that enum is this Session's user-facing vocabulary. This one is scoped to
/// <see cref="PacketRecordCodec"/> and exists only so the datagram parse loop has a symbol to switch on
/// instead of the four <see cref="DatagramSessionProtocol"/> mode constants.
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
/// the start of the span it is given and reports how many bytes it consumed or produced. The caller (<see cref="PacketRecordReader"/> for
/// reading, <see cref="DatagramRecordPacker"/> for writing) re-slices past that count to move on to the
/// next one.
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
/// index and fragment count no unfragmented record has room for or needs. <see cref="TryWrite(Span{byte}, PacketRecordMode, uint, ReadOnlySpan{byte}, out int)"/>
/// now mirrors that on the way out with one overload per shape — a complete Message has no index or count
/// to give, so the unfragmented overload simply carries no parameters for them rather than accepting
/// values it would ignore. Multi-byte integers are big-endian, matched by every reader and writer here
/// rather than left to the host's native order.
/// </remarks>
internal static class PacketRecordCodec
{
    /// <summary>
    /// Reads one packet record from the start of <paramref name="record"/>, which is positioned at
    /// the record's first byte and extends to the end of the bytes remaining in the datagram, so its
    /// own length bounds the record's declared length. Reads either wire shape
    /// (<see cref="PacketRecordLegality"/>): an unfragmented record, a complete Message with no fragment
    /// index or count; or a Fragment, whose two extra fields are read alongside it. For a Fragment this
    /// method also judges whether its index and count describe a set that could ever legally exist: an
    /// index at or beyond its count names a piece outside the set it claims; a count of zero has no
    /// valid index at all; and a count of one is refused too, because a one-piece Message is
    /// unfragmented and should never have carried the fragment shape. It judges
    /// <see cref="DatagramSessionProtocol.ControlBit"/> as well: the bit is legal only on a record that is
    /// otherwise unfragmented, zero length, and either <c>ReliableUnordered</c> (a Ping) or
    /// <c>ReliableOrdered</c> (a Goodbye), as <see cref="PacketRecordLegality.IsLegalControlRecord"/>
    /// states. Fails without reading past <paramref name="record"/> when: any spare bit
    /// (<see cref="DatagramSessionProtocol.SpareBitsMask"/>) is set; the flags byte names the Fragment
    /// shape in one of the two unreliable modes, which ADR 0004 forbids and
    /// <see cref="PacketRecordLegality.ShapeOf"/> therefore refuses; fewer bytes remain than the record's
    /// shape needs; the declared length would exceed the bytes left after the record header; a
    /// Fragment's index and count form one of the impossible combinations above; or the control bit is
    /// set on a record that is not a legal Ping or Goodbye. Every one of these fails the same way, so the
    /// caller has one failure case to handle. A caller that wants to tell the unreliable-Fragment refusal
    /// apart from the rest asks <see cref="PacketRecordLegality.RefusedAsUnreliableFragment"/> after the
    /// fact.
    /// </summary>
    /// <param name="record">
    /// The datagram's bytes from this record's first byte onward. Its length is the bytes remaining
    /// in the datagram.
    /// </param>
    /// <param name="decoded">
    /// The record read, with its payload sliced from <paramref name="record"/>. Valid only when this
    /// method returns <see langword="true"/>, and <see langword="default"/> otherwise.
    /// </param>
    public static bool TryRead(ReadOnlySpan<byte> record, out PacketRecord decoded)
    {
        decoded = default;

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
        ushort length = BinaryPrimitives.ReadUInt16BigEndian(record[DatagramSessionProtocol.RecordLengthOffset..]);
        if (length > record.Length - shape.RecordSize)
        {
            return false;
        }

        PacketRecordMode mode = (PacketRecordMode)(flags & DatagramSessionProtocol.ModeMask);
        uint sequence = BinaryPrimitives.ReadUInt32BigEndian(record[DatagramSessionProtocol.RecordSequenceOffset..]);

        // The fragment fields exist only on the longer shape, so they are read only when the flags
        // byte claims it. Reading them off an unfragmented record would read into its payload.
        bool isFragment = (flags & DatagramSessionProtocol.FragmentBit) != 0;
        ushort fragmentIndex = 0;
        ushort fragmentCount = 0;
        if (isFragment)
        {
            fragmentIndex = BinaryPrimitives.ReadUInt16BigEndian(record[DatagramSessionProtocol.RecordFragmentIndexOffset..]);
            fragmentCount = BinaryPrimitives.ReadUInt16BigEndian(record[DatagramSessionProtocol.RecordFragmentCountOffset..]);

            // A count of zero has no index that could be valid, an index at or beyond its count names a
            // piece outside the set it claims to belong to, and a count of one is a complete Message that
            // should never have carried the fragment shape. Issue #253 refuses all three rather than let
            // a stranger's impossible metadata reach the contiguity logic downstream.
            if (fragmentCount <= 1 || fragmentIndex >= fragmentCount)
            {
                return false;
            }
        }

        // The control bit (#339) is legal on exactly two combinations, stated once in
        // PacketRecordLegality.IsLegalControlRecord (#341). Every other combination carrying the bit is
        // refused, since SpareBitsMask does not cover it and the bit would otherwise mean something
        // unproven.
        bool isControl = (flags & DatagramSessionProtocol.ControlBit) != 0;
        if (isControl && !PacketRecordLegality.IsLegalControlRecord(mode, isFragment, length))
        {
            return false;
        }

        decoded = new PacketRecord(
            mode,
            sequence,
            isFragment,
            fragmentIndex,
            fragmentCount,
            isControl,
            record.Slice(shape.RecordSize, length),
            shape.RecordSize + length);
        return true;
    }

    /// <summary>
    /// Writes one unfragmented packet record — flags byte, <c>u32</c> sequence, <c>u16</c> length,
    /// then the payload itself — to the start of <paramref name="destination"/>. The counterpart to
    /// <see cref="TryRead"/>'s unfragmented shape, used by <see cref="DatagramRecordPacker"/> to pack a
    /// Peer's Sent records into an outbound datagram. A complete Message needs no fragment index or
    /// fragment count — it is not a piece of anything larger — so this overload has no parameters for
    /// them; the overload that takes a fragment index and count writes the other shape.
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

    /// <summary>
    /// Writes one in-Session control record — flags byte with <see
    /// cref="DatagramSessionProtocol.ControlBit"/> set alongside the <c>ReliableUnordered</c> mode
    /// bits, <c>u32</c> sequence, then a <c>u16</c> length of zero — to the start of <paramref
    /// name="destination"/>. The unfragmented shape carries a control record: seven bytes, no payload,
    /// no fragment index or count. A control record is always <c>ReliableUnordered</c> — its first use
    /// is the idle-triggered Ping, which the parent issue restricts to that mode — so this method takes
    /// no mode parameter, unlike
    /// <see cref="TryWrite(Span{byte}, PacketRecordMode, uint, ReadOnlySpan{byte}, out int)"/>.
    /// <see cref="TryRead"/> now accepts exactly this shape back: ReliableUnordered, unfragmented, zero
    /// length, control bit set.
    /// </summary>
    /// <param name="destination">
    /// The bytes to write into, positioned at the record's first byte. Never partially written: a
    /// <paramref name="destination"/> shorter than <see cref="DatagramSessionProtocol.UnfragmentedRecordSize"/>
    /// fails without writing anything, the same as a caller finding the remaining space in a datagram
    /// already spent.
    /// </param>
    /// <param name="sequence">The record's <c>u32</c> sequence number, drawn from the same
    /// <c>ReliableUnordered</c> space as any other record in that mode.</param>
    /// <param name="bytesWritten">
    /// The total bytes written — always <see cref="DatagramSessionProtocol.UnfragmentedRecordSize"/>,
    /// seven, since a control record never carries a payload — so the caller can advance past it.
    /// Valid only when this method returns <see langword="true"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the record fit in <paramref name="destination"/> and was written.
    /// <see langword="false"/> if it did not fit — nothing is written in that case.
    /// </returns>
    public static bool TryWritePingRecord(Span<byte> destination, uint sequence, out int bytesWritten) =>
        TryWriteZeroLengthControlRecord(destination, PacketRecordMode.ReliableUnordered, sequence, out bytesWritten);

    /// <summary>
    /// Writes one Goodbye record — flags byte with <see cref="DatagramSessionProtocol.ControlBit"/> set
    /// alongside the <c>ReliableOrdered</c> mode bits, <c>u32</c> sequence, then a <c>u16</c> length of
    /// zero — to the start of <paramref name="destination"/>. The same seven-byte shape
    /// <see cref="TryWritePingRecord"/> writes for a Ping, in the other reliable Space: the Space alone
    /// tells the two apart, and <see cref="TryRead"/> accepts exactly this shape back.
    /// </summary>
    /// <param name="destination">
    /// The bytes to write into, positioned at the record's first byte. Never partially written: a
    /// <paramref name="destination"/> shorter than <see cref="DatagramSessionProtocol.UnfragmentedRecordSize"/>
    /// fails without writing anything.
    /// </param>
    /// <param name="sequence">The record's <c>u32</c> sequence number, drawn from the <c>ReliableOrdered</c> space.</param>
    /// <param name="bytesWritten">
    /// The total bytes written — always <see cref="DatagramSessionProtocol.UnfragmentedRecordSize"/>.
    /// Valid only when this method returns <see langword="true"/>.
    /// </param>
    /// <returns>
    /// <see langword="true"/> if the record fit in <paramref name="destination"/> and was written.
    /// <see langword="false"/> if it did not fit — nothing is written in that case.
    /// </returns>
    public static bool TryWriteGoodbyeRecord(Span<byte> destination, uint sequence, out int bytesWritten) =>
        TryWriteZeroLengthControlRecord(destination, PacketRecordMode.ReliableOrdered, sequence, out bytesWritten);

    /// <summary>
    /// Writes the seven-byte zero-length control shape shared by a Ping and a Goodbye, differing only in
    /// the <paramref name="mode"/> bits beside <see cref="DatagramSessionProtocol.ControlBit"/>. Writes
    /// nothing and returns <see langword="false"/> when <paramref name="destination"/> is too short.
    /// </summary>
    private static bool TryWriteZeroLengthControlRecord(Span<byte> destination, PacketRecordMode mode, uint sequence, out int bytesWritten)
    {
        bytesWritten = default;

        if (destination.Length < DatagramSessionProtocol.UnfragmentedRecordSize)
        {
            return false;
        }

        destination[0] = (byte)((byte)mode | DatagramSessionProtocol.ControlBit);
        BinaryPrimitives.WriteUInt32BigEndian(destination[DatagramSessionProtocol.RecordSequenceOffset..], sequence);
        BinaryPrimitives.WriteUInt16BigEndian(destination[DatagramSessionProtocol.RecordLengthOffset..], 0);

        bytesWritten = DatagramSessionProtocol.UnfragmentedRecordSize;
        return true;
    }

    /// <summary>
    /// Writes one Fragment record — flags byte with the fragment bit set, <c>u32</c> sequence,
    /// <c>u16</c> length, <c>u16</c> fragment index, <c>u16</c> fragment count, then the payload itself
    /// — to the start of <paramref name="destination"/>. The counterpart to <see cref="TryRead"/>'s
    /// Fragment shape: a Fragment is one piece of a larger Message, so unlike the unfragmented overload
    /// it carries the two extra fields a receiver needs to place this piece and know when every piece
    /// has arrived — fields a complete Message has no use for and never pays for.
    /// </summary>
    /// <param name="destination">
    /// The bytes to write into, positioned at the record's first byte. Never partially written: a
    /// <paramref name="destination"/> too short to hold this record's header plus
    /// <paramref name="payload"/> fails without writing anything, the same as a caller finding the
    /// remaining space in a datagram already spent.
    /// </param>
    /// <param name="mode">The record's delivery mode, written into the flags byte's two mode bits. No other bit in the flags byte is ever set except <see cref="DatagramSessionProtocol.FragmentBit"/> — never a spare bit.</param>
    /// <param name="sequence">The record's <c>u32</c> sequence number.</param>
    /// <param name="fragmentIndex">
    /// This piece's <c>u16</c> position within its Message, written verbatim. Must be less than
    /// <paramref name="fragmentCount"/>; see the exception below.
    /// </param>
    /// <param name="fragmentCount">
    /// The Message's <c>u16</c> total piece count, written verbatim. Must be at least 2; see the
    /// exception below.
    /// </param>
    /// <remarks>
    /// <para>
    /// <strong>Neither fragment field has a path that varies with its magnitude</strong>, so "index and
    /// count at their maximum" holds by ADR 0015's criterion 3 rather than by a test: the code is
    /// already such that the invariant is trivially true. Both parameters are <see cref="ushort"/>, so
    /// the type bounds the domain; both are written by a single fixed-width big-endian primitive that
    /// is total over that domain; and the only two branches reading them —
    /// <paramref name="fragmentCount"/> against 1 and <paramref name="fragmentIndex"/> against
    /// <paramref name="fragmentCount"/> — are relational rather than magnitude-dependent, and are
    /// pinned at the boundaries that actually separate their arms. A test at 65535 would therefore
    /// exercise the same path as a test at 2 and assert the compiler's type bounds rather than this
    /// method's behaviour.
    /// </para>
    /// <para>
    /// The reading half of the boundary is a different matter and <em>is</em> covered, because a
    /// hostile peer can put any <c>u16</c> on the wire whatever this writer would have chosen:
    /// <c>DatagramSessionDataDatagramParseTests.Pump_FragmentRecordsAtTheValidBoundaries_AllParseSuccessfully</c>
    /// drives a count of 65535 through <see cref="TryRead"/>.
    /// </para>
    /// </remarks>
    /// <param name="payload">
    /// The record's payload bytes, copied verbatim after the record's fixed header. Its length becomes
    /// the record's <c>u16</c> length field.
    /// </param>
    /// <param name="bytesWritten">
    /// The total bytes written — the record's header plus <paramref name="payload"/> — so the caller
    /// can advance past it. Valid only when this method returns <see langword="true"/>.
    /// </param>
    /// <exception cref="ArgumentException">
    /// <paramref name="payload"/> is longer than <see cref="ushort.MaxValue"/>; or
    /// <paramref name="fragmentCount"/> is 0 or 1; or <paramref name="fragmentIndex"/> is greater than
    /// or equal to <paramref name="fragmentCount"/>. All three are refused the same way <see
    /// cref="TryRead"/> refuses them on the way in — a count of zero has no valid index, a count of one
    /// describes a Message that is whole in one piece and should never carry the fragment shape, and an
    /// index at or beyond its count names a piece outside the set it claims. The difference from a
    /// short <paramref name="destination"/>, which is an ordinary and expected outcome reported by a
    /// <see langword="false"/> return, is who chose the bad value: a remote peer chooses what arrives
    /// over the wire, but this method's caller chooses what it hands to the writer, so metadata that
    /// could never legally round-trip is this library's own bug, not a remote sender's — the same
    /// distinction the payload-length check above already draws.
    /// </exception>
    public static bool TryWrite(
        Span<byte> destination,
        PacketRecordMode mode,
        uint sequence,
        ushort fragmentIndex,
        ushort fragmentCount,
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

        if (fragmentCount <= 1)
        {
            throw new ArgumentException(
                $"A Fragment's count must be at least 2; {fragmentCount} describes a Message that is " +
                "whole in one piece and should never carry the fragment shape.",
                nameof(fragmentCount));
        }

        if (fragmentIndex >= fragmentCount)
        {
            throw new ArgumentException(
                $"A Fragment's index ({fragmentIndex}) must be less than its count ({fragmentCount}).",
                nameof(fragmentIndex));
        }

        int recordSize = DatagramSessionProtocol.FragmentRecordSize + payload.Length;
        if (destination.Length < recordSize)
        {
            return false;
        }

        destination[0] = (byte)((byte)mode | DatagramSessionProtocol.FragmentBit);
        BinaryPrimitives.WriteUInt32BigEndian(destination[DatagramSessionProtocol.RecordSequenceOffset..], sequence);
        BinaryPrimitives.WriteUInt16BigEndian(destination[DatagramSessionProtocol.RecordLengthOffset..], (ushort)payload.Length);
        BinaryPrimitives.WriteUInt16BigEndian(destination[DatagramSessionProtocol.RecordFragmentIndexOffset..], fragmentIndex);
        BinaryPrimitives.WriteUInt16BigEndian(destination[DatagramSessionProtocol.RecordFragmentCountOffset..], fragmentCount);
        payload.CopyTo(destination[DatagramSessionProtocol.FragmentRecordSize..]);

        bytesWritten = recordSize;
        return true;
    }
}
