using System.Buffers.Binary;

namespace ReactorSockets.Session;

/// <summary>
/// The single place the 20-byte live-Session datagram header (ADR 0009) is encoded and decoded: the
/// session id, then the <c>ReliableOrdered</c> ack block, then the <c>ReliableUnordered</c> ack block.
/// Only this class knows where those fields live inside <see cref="DatagramSessionProtocol.DatagramHeaderSize"/>
/// bytes, so a layout change touches one file instead of every caller that peeks at an offset.
/// </summary>
/// <remarks>
/// Allocation-free by construction: every member works over <see cref="Span{T}"/> or
/// <see cref="ReadOnlySpan{T}"/> supplied by the caller. Multi-byte integers are big-endian
/// throughout, matching <see cref="ControlDatagramCodec"/>. Both ack blocks are always written and
/// always read, even when nothing has been received yet — a presence flag would save a few bytes on a
/// 1200-byte datagram at the cost of turning the header into more than one parse shape, and this
/// codec exists precisely so there is only one. Reading is total: a datagram shorter than the header
/// fails to parse rather than throwing, allocating, or reading past what the caller supplied.
/// </remarks>
internal static class DatagramHeaderCodec
{
    /// <summary>
    /// Writes the 20-byte header — session id, then the <c>ReliableOrdered</c> and
    /// <c>ReliableUnordered</c> ack blocks — into <paramref name="destination"/>. Both ack blocks are
    /// written unconditionally, whatever base and bits are passed, because the wire format has no
    /// field to say one is absent.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// <paramref name="destination"/> is shorter than <see cref="DatagramSessionProtocol.DatagramHeaderSize"/>.
    /// The caller sizes the outbound buffer itself, so a short buffer here is our own bug, not
    /// something a remote sender could ever cause.
    /// </exception>
    public static void WriteHeader(
        Span<byte> destination,
        uint session,
        uint reliableOrderedAckBase,
        uint reliableOrderedAckBits,
        uint reliableUnorderedAckBase,
        uint reliableUnorderedAckBits)
    {
        if (destination.Length < DatagramSessionProtocol.DatagramHeaderSize)
        {
            throw new ArgumentException(
                $"A datagram header buffer must be at least {DatagramSessionProtocol.DatagramHeaderSize} bytes.",
                nameof(destination));
        }

        BinaryPrimitives.WriteUInt32BigEndian(destination, session);
        WriteAckBlock(
            destination.Slice(DatagramSessionProtocol.ReliableOrderedAckBlockOffset),
            reliableOrderedAckBase,
            reliableOrderedAckBits);
        WriteAckBlock(
            destination.Slice(DatagramSessionProtocol.ReliableUnorderedAckBlockOffset),
            reliableUnorderedAckBase,
            reliableUnorderedAckBits);
    }

    /// <summary>Writes one ack block: a cumulative base sequence followed by its bitfield.</summary>
    private static void WriteAckBlock(Span<byte> destination, uint ackBase, uint ackBits)
    {
        BinaryPrimitives.WriteUInt32BigEndian(destination, ackBase);
        BinaryPrimitives.WriteUInt32BigEndian(destination[sizeof(uint)..], ackBits);
    }

    /// <summary>
    /// Reads the 20-byte header from <paramref name="datagram"/>. Fails, rather than throwing, when
    /// <paramref name="datagram"/> is shorter than <see cref="DatagramSessionProtocol.DatagramHeaderSize"/> —
    /// a datagram this short comes from the network, not our own code, so a malformed one is an
    /// ordinary and expected outcome rather than a bug.
    /// </summary>
    public static bool TryReadHeader(
        ReadOnlySpan<byte> datagram,
        out uint session,
        out uint reliableOrderedAckBase,
        out uint reliableOrderedAckBits,
        out uint reliableUnorderedAckBase,
        out uint reliableUnorderedAckBits)
    {
        if (datagram.Length < DatagramSessionProtocol.DatagramHeaderSize)
        {
            session = default;
            reliableOrderedAckBase = default;
            reliableOrderedAckBits = default;
            reliableUnorderedAckBase = default;
            reliableUnorderedAckBits = default;
            return false;
        }

        session = BinaryPrimitives.ReadUInt32BigEndian(datagram);
        ReadAckBlock(
            datagram[DatagramSessionProtocol.ReliableOrderedAckBlockOffset..],
            out reliableOrderedAckBase,
            out reliableOrderedAckBits);
        ReadAckBlock(
            datagram[DatagramSessionProtocol.ReliableUnorderedAckBlockOffset..],
            out reliableUnorderedAckBase,
            out reliableUnorderedAckBits);
        return true;
    }

    /// <summary>Reads one ack block: a cumulative base sequence followed by its bitfield.</summary>
    private static void ReadAckBlock(ReadOnlySpan<byte> source, out uint ackBase, out uint ackBits)
    {
        ackBase = BinaryPrimitives.ReadUInt32BigEndian(source);
        ackBits = BinaryPrimitives.ReadUInt32BigEndian(source[sizeof(uint)..]);
    }
}
