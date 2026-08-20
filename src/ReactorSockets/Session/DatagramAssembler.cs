using System.Buffers.Binary;

namespace ReactorSockets.Session;

/// <summary>
/// The single place an outbound live-Session datagram (ADR 0009) is written: the 20-byte header,
/// then as many unfragmented packet records as fit. Mirrors <see cref="DatagramHeaderCodec"/> and
/// <see cref="PacketRecordCodec"/> from the write side — what this class writes is exactly what those
/// two read back, so the two ends of the wire format stay one contract instead of two.
/// </summary>
/// <remarks>
/// Allocation-free by construction, the same discipline <see cref="ControlDatagramCodec"/> follows:
/// every member works over a caller-supplied <see cref="Span{T}"/>, writing directly into the
/// outbound buffer at a caller-supplied offset rather than staging a record anywhere else. A record
/// that would not fit in what remains of the buffer is not written — <see cref="TryAppendRecord"/>
/// reports <see langword="false"/> and leaves the buffer exactly as it was, rather than writing a
/// truncated record. The caller (its own queue, its own retry policy) decides what happens to a
/// payload that did not fit; that decision, and any coalescing across Pumps, is out of scope here.
/// Multi-byte integers are big-endian, matching every other codec in this namespace. Only the
/// unfragmented record shape is written — the fragment shape stays undefined on the write side until
/// slice 7 implements it, the same boundary <see cref="PacketRecordLegality"/> draws on the read side.
/// </remarks>
internal static class DatagramAssembler
{
    /// <summary>
    /// Writes the 20-byte datagram header — session id, then the <c>ReliableOrdered</c> and
    /// <c>ReliableUnordered</c> ack blocks — into <paramref name="destination"/> at offset 0, and
    /// returns the number of bytes it occupies so the caller can chain straight into
    /// <see cref="TryAppendRecord"/> at that offset. Both ack blocks are written unconditionally: the
    /// wire format has no field to say one is absent, so a Session with nothing yet to acknowledge
    /// still gets a zeroed block rather than a shorter header.
    /// </summary>
    /// <remarks>
    /// The bytes themselves are <see cref="DatagramHeaderCodec"/>'s, not written a second time here.
    /// What this method adds is the running offset an assembly needs: the codec's job ends at the
    /// header, and the caller's next question is always where the first record starts.
    /// </remarks>
    public static int WriteHeader(
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
        return DatagramSessionProtocol.DatagramHeaderSize;
    }

    /// <summary>
    /// Appends one unfragmented packet record — flags byte, sequence, payload length, then the
    /// payload itself — at <paramref name="offset"/> in <paramref name="destination"/>. Fails without
    /// writing anything when the record would not fit in what remains of the buffer, so the caller
    /// always knows precisely which payloads were taken and which were not, rather than finding out
    /// from a truncated datagram.
    /// </summary>
    /// <param name="destination">The outbound send buffer, already holding the header this Session's datagram opens with.</param>
    /// <param name="offset">Where in <paramref name="destination"/> this record starts — the running total of bytes already written.</param>
    /// <param name="mode">The record's delivery mode; written as the flags byte's two mode bits with the fragment bit and spare bits left clear.</param>
    /// <param name="sequence">The record's <c>u32</c> sequence number.</param>
    /// <param name="payload">The record's payload bytes, copied as-is after the record's 7-byte header.</param>
    /// <param name="bytesWritten">
    /// The total bytes this record occupies — its header plus its payload — valid only when this
    /// method returns <see langword="true"/>, so the caller can advance its own offset by exactly
    /// this much before appending the next record.
    /// </param>
    /// <exception cref="ArgumentException">
    /// <paramref name="payload"/> is longer than <see cref="ushort.MaxValue"/> bytes. The record's
    /// length field is 16 bits; a payload that cannot fit it is our own bug — the caller is
    /// responsible for keeping Messages within what the wire format can declare — not something a
    /// remote peer could ever cause on the write side.
    /// </exception>
    public static bool TryAppendRecord(
        Span<byte> destination,
        int offset,
        PacketRecordMode mode,
        uint sequence,
        ReadOnlySpan<byte> payload,
        out int bytesWritten)
    {
        if (payload.Length > ushort.MaxValue)
        {
            throw new ArgumentException(
                $"A packet record payload cannot exceed {ushort.MaxValue} bytes.",
                nameof(payload));
        }

        int recordSize = DatagramSessionProtocol.UnfragmentedRecordSize + payload.Length;
        if (offset + recordSize > destination.Length)
        {
            bytesWritten = 0;
            return false;
        }

        Span<byte> record = destination.Slice(offset, recordSize);
        record[0] = (byte)mode;
        BinaryPrimitives.WriteUInt32BigEndian(record[DatagramSessionProtocol.RecordSequenceOffset..], sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record[DatagramSessionProtocol.RecordLengthOffset..], (ushort)payload.Length);
        payload.CopyTo(record[DatagramSessionProtocol.UnfragmentedRecordSize..]);

        bytesWritten = recordSize;
        return true;
    }
}
