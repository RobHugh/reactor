using System.Buffers.Binary;

namespace ReactorSockets.Tests;

/// <summary>
/// One reliable Space's ack block (ADR 0009): a cumulative base sequence followed by the 32-bit
/// bitfield of the sequences above it.
/// </summary>
/// <param name="Base">The cumulative base sequence — every sequence at or below this one is acknowledged.</param>
/// <param name="Bits">The bitfield of sequences above <paramref name="Base"/> that are also acknowledged.</param>
internal readonly record struct AckBlock(uint Base, uint Bits);

/// <summary>
/// One packet record (ADR 0009) decoded from a datagram, in the shape a test wants to assert on
/// rather than as hand-indexed offsets: the record's raw flags byte alongside the delivery mode those
/// flags carry, its sequence, its declared payload length, and the payload itself.
/// </summary>
/// <param name="Flags">
/// The record's raw flags byte, unmasked — a test proving the two mode bits, the fragment bit or the
/// spare bits reads them straight from here rather than re-deriving them from <paramref name="Mode"/>.
/// </param>
/// <param name="Mode">
/// The record's delivery mode, taken from the flags byte's two mode bits. <see cref="DeliveryMode"/>'s
/// values are defined to match the wire's mode bits exactly (ADR 0009), so this is a direct cast of
/// <paramref name="Flags"/> masked to those two bits — never a lookup table that could drift from the
/// values the enum already documents.
/// </param>
/// <param name="Sequence">The record's <c>u32</c> sequence number.</param>
/// <param name="Length">The record's declared payload length in bytes, as read from the wire.</param>
/// <param name="Payload">
/// The record's payload bytes, copied out of the source datagram so they outlive it — a later
/// assertion can hold onto this array after the byte array it was decoded from is gone.
/// </param>
internal sealed record DecodedPacketRecord(byte Flags, DeliveryMode Mode, uint Sequence, ushort Length, byte[] Payload);

/// <summary>
/// One live-Session datagram (ADR 0009) decoded into named values: the header's session id and both
/// reliable Spaces' ack blocks, then every packet record it carried, in the order they appeared on the
/// wire.
/// </summary>
/// <param name="Session">The wire session id the header named.</param>
/// <param name="ReliableOrderedAck">The <c>ReliableOrdered</c> Space's ack block.</param>
/// <param name="ReliableUnorderedAck">The <c>ReliableUnordered</c> Space's ack block.</param>
/// <param name="Records">Every packet record the datagram carried, in wire order.</param>
internal sealed record DecodedDatagram(
    uint Session,
    AckBlock ReliableOrderedAck,
    AckBlock ReliableUnorderedAck,
    IReadOnlyList<DecodedPacketRecord> Records);

/// <summary>
/// The outcome of <see cref="DatagramDecoder.TryDecode"/>: either the datagram it decoded, or the
/// reason it could not be decoded. Named-value pass/fail, the same shape <see cref="Result"/> uses for
/// the production surface, extended with the decoded value a decode needs to hand back on success.
/// </summary>
internal readonly record struct DatagramDecodeResult
{
    private DatagramDecodeResult(bool isSuccess, DecodedDatagram? datagram, string? error)
    {
        IsSuccess = isSuccess;
        Datagram = datagram;
        Error = error;
    }

    /// <summary>Whether the decode succeeded.</summary>
    public bool IsSuccess { get; }

    /// <summary>The decoded datagram, or <see langword="null"/> when <see cref="IsSuccess"/> is <see langword="false"/>.</summary>
    public DecodedDatagram? Datagram { get; }

    /// <summary>The failure reason, or <see langword="null"/> when <see cref="IsSuccess"/> is <see langword="true"/>.</summary>
    public string? Error { get; }

    /// <summary>Builds a successful result carrying the decoded datagram.</summary>
    public static DatagramDecodeResult Success(DecodedDatagram datagram) => new(true, datagram, null);

    /// <summary>Builds a failed result naming <paramref name="error"/> as the reason the decode was refused.</summary>
    public static DatagramDecodeResult Failure(string error) => new(false, null, error);
}

/// <summary>
/// An independent reader of the live-Session datagram wire format (ADR 0009): the 20-byte header
/// (session id, then the <c>ReliableOrdered</c> and <c>ReliableUnordered</c> ack blocks), followed by
/// as many packet records (flags byte, <c>u32</c> sequence, <c>u16</c> length, then the payload) as fit.
/// Exists so a test proving the wire format asserts against a second, independent parse of the bytes a
/// Session actually sent, rather than against the same production codec that wrote them —
/// <see cref="ReactorSockets.Session.DatagramHeaderCodec"/> and
/// <see cref="ReactorSockets.Session.PacketRecordCodec"/> are <see langword="internal"/> to the library
/// and out of reach from this test assembly (ADR 0015), and mirroring their logic here would only prove
/// the writer agrees with itself.
/// </summary>
/// <remarks>
/// Decoding is strict, not lenient: a datagram shorter than the header, or a record whose header or
/// declared payload would run past the bytes remaining, fails the whole decode via
/// <see cref="DatagramDecodeResult.Failure"/> rather than returning whatever records were read before
/// the truncation. Multi-byte integers are read big-endian, matching every writer in
/// <c>src/ReactorSockets/Session</c>. This decoder does not enforce the fragment bit or the spare bits
/// the write side never sets — those are a write-side legality concern already proven where the writer
/// lives; a datagram a test hand-builds with those bits set decodes here and reports them in
/// <see cref="DecodedPacketRecord.Flags"/> for the test itself to assert on.
/// </remarks>
internal static class DatagramDecoder
{
    private const int DatagramHeaderSize = 20;
    private const int ReliableOrderedAckBlockOffset = sizeof(uint);
    private const int ReliableUnorderedAckBlockOffset = ReliableOrderedAckBlockOffset + AckBlockSize;
    private const int AckBlockSize = sizeof(uint) + sizeof(uint);

    private const int UnfragmentedRecordSize = sizeof(byte) + sizeof(uint) + sizeof(ushort);
    private const int RecordSequenceOffset = sizeof(byte);
    private const int RecordLengthOffset = RecordSequenceOffset + sizeof(uint);
    private const byte ModeMask = 0b0000_0011;

    /// <summary>
    /// Decodes <paramref name="datagram"/> into its header and packet records. Fails the whole decode,
    /// rather than returning what was read so far, when <paramref name="datagram"/> is shorter than the
    /// 20-byte header, or when a record's fixed header or its declared payload length would run past
    /// the bytes remaining in <paramref name="datagram"/>.
    /// </summary>
    /// <param name="datagram">The captured datagram bytes to decode, exactly as observed on the wire.</param>
    public static DatagramDecodeResult TryDecode(byte[] datagram)
    {
        if (datagram.Length < DatagramHeaderSize)
        {
            return DatagramDecodeResult.Failure(
                $"Datagram is {datagram.Length} bytes, shorter than the {DatagramHeaderSize}-byte header.");
        }

        uint session = BinaryPrimitives.ReadUInt32BigEndian(datagram);
        AckBlock reliableOrderedAck = ReadAckBlock(datagram, ReliableOrderedAckBlockOffset);
        AckBlock reliableUnorderedAck = ReadAckBlock(datagram, ReliableUnorderedAckBlockOffset);

        List<DecodedPacketRecord> records = [];
        int offset = DatagramHeaderSize;
        while (offset < datagram.Length)
        {
            if (offset + UnfragmentedRecordSize > datagram.Length)
            {
                return DatagramDecodeResult.Failure(
                    $"Record at offset {offset} needs {UnfragmentedRecordSize} bytes for its header but only " +
                    $"{datagram.Length - offset} remain.");
            }

            byte flags = datagram[offset];
            uint sequence = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(offset + RecordSequenceOffset));
            ushort length = BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + RecordLengthOffset));

            int payloadOffset = offset + UnfragmentedRecordSize;
            if (payloadOffset + length > datagram.Length)
            {
                return DatagramDecodeResult.Failure(
                    $"Record at offset {offset} declares a {length}-byte payload but only " +
                    $"{datagram.Length - payloadOffset} bytes remain.");
            }

            byte[] payload = new byte[length];
            Array.Copy(datagram, payloadOffset, payload, 0, length);
            records.Add(new DecodedPacketRecord(flags, (DeliveryMode)(flags & ModeMask), sequence, length, payload));

            offset = payloadOffset + length;
        }

        return DatagramDecodeResult.Success(new DecodedDatagram(session, reliableOrderedAck, reliableUnorderedAck, records));
    }

    /// <summary>Reads one ack block — a cumulative base sequence followed by its bitfield — at <paramref name="offset"/> in <paramref name="datagram"/>.</summary>
    private static AckBlock ReadAckBlock(byte[] datagram, int offset)
    {
        uint ackBase = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(offset));
        uint ackBits = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(offset + sizeof(uint)));
        return new AckBlock(ackBase, ackBits);
    }
}
