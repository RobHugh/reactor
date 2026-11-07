using System.Buffers.Binary;

namespace ReactorSockets.Tests;

/// <summary>
/// Builds data-datagram headers and packet records byte by byte, independently of the production codec,
/// which is internal and out of reach from this test assembly (ADR 0015). The constants restate the
/// wire format so a codec change that moves a field fails the tests that use these.
/// </summary>
internal static class PacketRecordBytes
{
    /// <summary>A data datagram header: a <c>u32</c> session id, then two eight-byte ack blocks.</summary>
    public const int DatagramHeaderSize = 20;

    /// <summary>An unfragmented record: flags, a <c>u32</c> sequence, a <c>u16</c> payload length.</summary>
    public const int UnfragmentedRecordSize = 7;

    /// <summary>A Fragment record: the unfragmented shape plus a <c>u16</c> index and a <c>u16</c> count.</summary>
    public const int FragmentRecordSize = 11;

    /// <summary>The <c>Unreliable</c> mode's two mode bits.</summary>
    public const byte ModeUnreliable = 0b0000_0000;

    /// <summary>The <c>ReliableUnordered</c> mode's two mode bits.</summary>
    public const byte ModeReliableUnordered = 0b0000_0010;

    /// <summary>The <c>ReliableOrdered</c> mode's two mode bits.</summary>
    public const byte ModeReliableOrdered = 0b0000_0011;

    /// <summary>The flag bit marking a Fragment record.</summary>
    public const byte FragmentBit = 0b0000_0100;

    /// <summary>The flag bit marking a control record such as a Goodbye.</summary>
    public const byte ControlBit = 0b0000_1000;

    /// <summary>Builds a data datagram header naming <paramref name="session"/> with both ack blocks zeroed.</summary>
    public static byte[] Header(uint session)
    {
        byte[] header = new byte[DatagramHeaderSize];
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0, 4), session);
        return header;
    }

    /// <summary>Builds one unfragmented record carrying <paramref name="payload"/>.</summary>
    public static byte[] Record(byte modeBits, uint sequence, ReadOnlySpan<byte> payload)
    {
        byte[] record = new byte[UnfragmentedRecordSize + payload.Length];
        record[0] = modeBits;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        payload.CopyTo(record.AsSpan(UnfragmentedRecordSize));
        return record;
    }

    /// <summary>Builds one Fragment record: index <paramref name="index"/> of a set declaring <paramref name="count"/>.</summary>
    public static byte[] Fragment(byte modeBits, uint sequence, ushort index, ushort count, ReadOnlySpan<byte> payload)
    {
        byte[] record = new byte[FragmentRecordSize + payload.Length];
        record[0] = (byte)(modeBits | FragmentBit);
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(7, 2), index);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(9, 2), count);
        payload.CopyTo(record.AsSpan(FragmentRecordSize));
        return record;
    }

    /// <summary>Builds a Goodbye-shaped record: <c>ReliableOrdered</c> with the control bit and no payload.</summary>
    public static byte[] Goodbye(uint sequence) => Record(ModeReliableOrdered | ControlBit, sequence, []);
}
