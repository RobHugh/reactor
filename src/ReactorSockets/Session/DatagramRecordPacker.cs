namespace ReactorSockets.Session;

/// <summary>
/// Packs one Peer's Sent records into the bytes of one outbound data datagram (ADR 0009): the 20-byte
/// header — session id plus both reliable spaces' ack blocks — followed by as many packet records as
/// the caller writes and fit. This is the packing step only. Nothing sends the bytes it produces and
/// nothing yet drives it from a Pump; deciding which of a Peer's Sent records to hand it, and starting
/// a new datagram once records stop fitting, is #216's. Splitting a whole Pump's worth of records
/// across multiple datagrams by capacity is #45's.
/// </summary>
/// <remarks>
/// A ref struct wrapping the caller's own destination buffer, so packing one datagram allocates
/// nothing: <see cref="TryPackRecord"/> writes straight into the span passed to the constructor
/// through <see cref="PacketRecordCodec.TryWrite(Span{byte}, PacketRecordMode, uint, ReadOnlySpan{byte}, out int)"/>,
/// and <see cref="TryPackFragmentRecord"/> writes the fragment shape through
/// <see cref="PacketRecordCodec.TryWrite(Span{byte}, PacketRecordMode, uint, ushort, ushort, ReadOnlySpan{byte}, out int)"/>
/// (issue #245) — the caller picks whichever shape one record needs; <see cref="TryPackControlRecord"/>
/// writes the seven-byte control shape through <see cref="PacketRecordCodec.TryWriteControlRecord"/>
/// (issue #338). <see cref="BytesWritten"/> and <see cref="RecordsPacked"/> are running counts shared
/// by all three, rather than anything collected afterward. The header is written once, at construction, through
/// <see cref="DatagramHeaderCodec.WriteHeader"/> — both ack blocks always appear, even when the Peer
/// has received nothing, matching that codec's own unconditional write. A record that does not fit in
/// the remaining space is left entirely unwritten and the packing method returns
/// <see langword="false"/>; the caller decides what to do with a record that did not fit — hold it for
/// the next datagram, or something else — this type has no opinion on that and never asks.
/// </remarks>
internal ref struct DatagramRecordPacker
{
    private readonly Span<byte> _destination;

    /// <summary>
    /// Writes the 20-byte header into <paramref name="destination"/> and prepares to pack records
    /// after it.
    /// </summary>
    /// <param name="destination">
    /// The outbound datagram buffer, owned by the caller for the packer's whole lifetime. Its length
    /// bounds how many records <see cref="TryPackRecord"/> can fit; nothing beyond
    /// <see cref="BytesWritten"/> is ever touched.
    /// </param>
    /// <param name="session">The session id this datagram belongs to.</param>
    /// <param name="reliableOrderedAckBase">The <c>ReliableOrdered</c> space's cumulative ack base sequence.</param>
    /// <param name="reliableOrderedAckBits">The <c>ReliableOrdered</c> space's ack bitfield, above the base.</param>
    /// <param name="reliableUnorderedAckBase">The <c>ReliableUnordered</c> space's cumulative ack base sequence.</param>
    /// <param name="reliableUnorderedAckBits">The <c>ReliableUnordered</c> space's ack bitfield, above the base.</param>
    /// <exception cref="ArgumentException">
    /// <paramref name="destination"/> is shorter than <see cref="DatagramSessionProtocol.DatagramHeaderSize"/>,
    /// propagated from <see cref="DatagramHeaderCodec.WriteHeader"/>. Sizing the outbound buffer is
    /// the caller's own responsibility, so a buffer too short even for the header is our bug, not a
    /// remote peer's.
    /// </exception>
    public DatagramRecordPacker(
        Span<byte> destination,
        uint session,
        uint reliableOrderedAckBase,
        uint reliableOrderedAckBits,
        uint reliableUnorderedAckBase,
        uint reliableUnorderedAckBits)
    {
        DatagramHeaderCodec.WriteHeader(
            destination,
            session,
            reliableOrderedAckBase,
            reliableOrderedAckBits,
            reliableUnorderedAckBase,
            reliableUnorderedAckBits);

        _destination = destination;
        BytesWritten = DatagramSessionProtocol.DatagramHeaderSize;
        RecordsPacked = 0;
    }

    /// <summary>
    /// The total bytes written so far: the 20-byte header plus every record <see cref="TryPackRecord"/>
    /// has successfully packed. Never counts a record <see cref="TryPackRecord"/> refused.
    /// </summary>
    public int BytesWritten { get; private set; }

    /// <summary>The number of records <see cref="TryPackRecord"/> has successfully packed.</summary>
    public int RecordsPacked { get; private set; }

    /// <summary>
    /// Appends one unfragmented packet record — mode, sequence and payload — immediately after the
    /// bytes already packed, through
    /// <see cref="PacketRecordCodec.TryWrite(Span{byte}, PacketRecordMode, uint, ReadOnlySpan{byte}, out int)"/>.
    /// Only the record's two mode bits are ever set in its flags byte: no spare bit, no fragment bit.
    /// The counterpart for a Fragment is <see cref="TryPackFragmentRecord"/>.
    /// </summary>
    /// <param name="mode">The record's delivery mode.</param>
    /// <param name="sequence">The record's <c>u32</c> sequence number.</param>
    /// <param name="payload">The record's payload bytes, copied verbatim.</param>
    /// <returns>
    /// <see langword="true"/> if the record fit in the destination buffer's remaining space and was
    /// written, advancing <see cref="BytesWritten"/> and <see cref="RecordsPacked"/>.
    /// <see langword="false"/> if it did not fit — nothing is written in that case, the destination's
    /// bytes past <see cref="BytesWritten"/> are left exactly as they were, and the record is left
    /// entirely for the caller to pack into a later datagram.
    /// </returns>
    public bool TryPackRecord(PacketRecordMode mode, uint sequence, ReadOnlySpan<byte> payload)
    {
        if (!PacketRecordCodec.TryWrite(_destination[BytesWritten..], mode, sequence, payload, out int bytesWritten))
        {
            return false;
        }

        BytesWritten += bytesWritten;
        RecordsPacked++;
        return true;
    }

    /// <summary>
    /// Appends one Fragment record — mode, sequence, fragment index, fragment count and payload —
    /// immediately after the bytes already packed, through
    /// <see cref="PacketRecordCodec.TryWrite(Span{byte}, PacketRecordMode, uint, ushort, ushort, ReadOnlySpan{byte}, out int)"/>.
    /// The counterpart to <see cref="TryPackRecord"/> for a Message <see cref="DatagramSessionReactor.Send"/>
    /// cut into Fragments (issue #245): the fragment bit is set alongside the mode bits, and no spare
    /// bit is ever set.
    /// </summary>
    /// <param name="mode">The record's delivery mode.</param>
    /// <param name="sequence">The record's <c>u32</c> sequence number.</param>
    /// <param name="fragmentIndex">This piece's <c>u16</c> position within its Message.</param>
    /// <param name="fragmentCount">The Message's <c>u16</c> total piece count.</param>
    /// <param name="payload">The record's payload bytes, copied verbatim.</param>
    /// <returns>
    /// <see langword="true"/> if the record fit in the destination buffer's remaining space and was
    /// written, advancing <see cref="BytesWritten"/> and <see cref="RecordsPacked"/>.
    /// <see langword="false"/> if it did not fit — nothing is written in that case, the destination's
    /// bytes past <see cref="BytesWritten"/> are left exactly as they were, and the record is left
    /// entirely for the caller to pack into a later datagram.
    /// </returns>
    public bool TryPackFragmentRecord(PacketRecordMode mode, uint sequence, ushort fragmentIndex, ushort fragmentCount, ReadOnlySpan<byte> payload)
    {
        if (!PacketRecordCodec.TryWrite(_destination[BytesWritten..], mode, sequence, fragmentIndex, fragmentCount, payload, out int bytesWritten))
        {
            return false;
        }

        BytesWritten += bytesWritten;
        RecordsPacked++;
        return true;
    }

    /// <summary>
    /// Appends one in-Session control record — seven bytes: <c>ReliableUnordered</c> mode bits plus
    /// <see cref="DatagramSessionProtocol.ControlBit"/>, a <c>u32</c> sequence, and a <c>u16</c> length
    /// of zero — immediately after the bytes already packed, through
    /// <see cref="PacketRecordCodec.TryWriteControlRecord"/>. Prefactor only (issue #338): nothing
    /// calls this yet — the idle-triggered Ping (#53) that will is a later issue's work.
    /// </summary>
    /// <param name="sequence">The record's <c>u32</c> sequence number, from the <c>ReliableUnordered</c> space.</param>
    /// <returns>
    /// <see langword="true"/> if the record fit in the destination buffer's remaining space and was
    /// written, advancing <see cref="BytesWritten"/> and <see cref="RecordsPacked"/>.
    /// <see langword="false"/> if it did not fit — nothing is written in that case, the destination's
    /// bytes past <see cref="BytesWritten"/> are left exactly as they were, and the record is left
    /// entirely for the caller to pack into a later datagram.
    /// </returns>
    public bool TryPackControlRecord(uint sequence)
    {
        if (!PacketRecordCodec.TryWriteControlRecord(_destination[BytesWritten..], sequence, out int bytesWritten))
        {
            return false;
        }

        BytesWritten += bytesWritten;
        RecordsPacked++;
        return true;
    }
}
