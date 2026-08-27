using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #229: <see cref="DatagramDecoder.TryDecode"/> against byte arrays built by hand in each test,
/// proving the decoder itself reads the live-Session wire format (ADR 0009) correctly and refuses
/// strictly rather than returning a partial result. Every input here is constructed independently of
/// the decoder's own offsets, using local helpers scoped to this file — the point is that the decoder's
/// reading and a test's writing agree only because both start from the same documented format, not
/// because one was copied from the other.
/// </summary>
public class DatagramDecoderTests
{
    private const int DatagramHeaderSize = 20;

    private const byte ModeUnreliable = 0b0000_0000;
    private const byte ModeUnreliableSequenced = 0b0000_0001;
    private const byte ModeReliableUnordered = 0b0000_0010;
    private const byte ModeReliableOrdered = 0b0000_0011;
    private const byte FragmentBit = 0b0000_0100;
    private const byte OneSpareBit = 0b0000_1000;

    [Fact]
    public void TryDecode_WellFormedHeaderWithNoRecords_DecodesSessionAndBothAckBlocks()
    {
        // Arrange
        byte[] datagram = BuildHeader(
            session: 0x1234_5678,
            reliableOrderedAckBase: 10,
            reliableOrderedAckBits: 0x0000_00FF,
            reliableUnorderedAckBase: 20,
            reliableUnorderedAckBits: 0xFFFF_0000);

        // Act
        DatagramDecodeResult result = DatagramDecoder.TryDecode(datagram);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Datagram!.Session.Should().Be(0x1234_5678u);
        result.Datagram!.ReliableOrderedAck.Should().Be(new AckBlock(10, 0x0000_00FF));
        result.Datagram!.ReliableUnorderedAck.Should().Be(new AckBlock(20, 0xFFFF_0000));
        result.Datagram!.Records.Should().BeEmpty();
    }

    [Fact]
    public void TryDecode_SingleRecord_ExposesFlagsModeSequenceLengthAndPayload()
    {
        // Arrange
        byte[] payload = [0xAA, 0xBB, 0xCC];
        byte[] datagram =
        [
            .. BuildHeader(1, 0, 0, 0, 0),
            .. BuildRecord(ModeReliableOrdered, sequence: 42, payload: payload),
        ];

        // Act
        DatagramDecodeResult result = DatagramDecoder.TryDecode(datagram);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Datagram!.Records.Should().HaveCount(1);
        DecodedPacketRecord record = result.Datagram!.Records[0];
        record.Flags.Should().Be(ModeReliableOrdered);
        record.Mode.Should().Be(DeliveryMode.ReliableOrdered);
        record.Sequence.Should().Be(42u);
        record.Length.Should().Be(3);
        record.Payload.Should().Equal(payload);
    }

    [Fact]
    public void TryDecode_SeveralRecords_DecodesEachInWireOrder()
    {
        // Arrange
        byte[] datagram =
        [
            .. BuildHeader(1, 0, 0, 0, 0),
            .. BuildRecord(ModeUnreliable, sequence: 1, payload: [0x01]),
            .. BuildRecord(ModeUnreliableSequenced, sequence: 2, payload: []),
            .. BuildRecord(ModeReliableUnordered, sequence: 3, payload: [0x02, 0x03]),
            .. BuildRecord(ModeReliableOrdered, sequence: 4, payload: [0x04, 0x05, 0x06]),
        ];

        // Act
        DatagramDecodeResult result = DatagramDecoder.TryDecode(datagram);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Datagram!.Records.Select(r => r.Mode).Should().ContainInOrder(
            DeliveryMode.Unreliable,
            DeliveryMode.UnreliableSequenced,
            DeliveryMode.ReliableUnordered,
            DeliveryMode.ReliableOrdered);
        result.Datagram!.Records.Select(r => r.Sequence).Should().ContainInOrder(1u, 2u, 3u, 4u);
    }

    [Theory]
    [InlineData(ModeUnreliable, DeliveryMode.Unreliable)]
    [InlineData(ModeUnreliableSequenced, DeliveryMode.UnreliableSequenced)]
    [InlineData(ModeReliableUnordered, DeliveryMode.ReliableUnordered)]
    [InlineData(ModeReliableOrdered, DeliveryMode.ReliableOrdered)]
    public void TryDecode_EachModeBitPattern_MapsToItsDeliveryMode(byte wireMode, DeliveryMode expected)
    {
        // Arrange
        byte[] datagram =
        [
            .. BuildHeader(1, 0, 0, 0, 0),
            .. BuildRecord(wireMode, sequence: 1, payload: []),
        ];

        // Act
        DatagramDecodeResult result = DatagramDecoder.TryDecode(datagram);

        // Assert
        result.Datagram!.Records[0].Mode.Should().Be(expected);
    }

    [Fact]
    public void TryDecode_EmptyPayloadRecord_DecodesZeroLengthPayload()
    {
        // Arrange
        byte[] datagram =
        [
            .. BuildHeader(1, 0, 0, 0, 0),
            .. BuildRecord(ModeUnreliable, sequence: 7, payload: []),
        ];

        // Act
        DatagramDecodeResult result = DatagramDecoder.TryDecode(datagram);

        // Assert
        result.Datagram!.Records[0].Length.Should().Be(0);
        result.Datagram!.Records[0].Payload.Should().BeEmpty();
    }

    [Fact]
    public void TryDecode_RecordFlagsCarryFragmentOrSpareBits_ReportsTheRawByteWithoutFailing()
    {
        // Arrange — the decoder does not enforce write-side legality; it reports whatever bits a
        // hand-built datagram carries so a test of the flags byte itself can assert on them.
        byte rawFlags = (byte)(ModeReliableOrdered | FragmentBit | OneSpareBit);
        byte[] datagram =
        [
            .. BuildHeader(1, 0, 0, 0, 0),
            .. BuildRecord(rawFlags, sequence: 9, payload: []),
        ];

        // Act
        DatagramDecodeResult result = DatagramDecoder.TryDecode(datagram);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Datagram!.Records[0].Flags.Should().Be(rawFlags);
        result.Datagram!.Records[0].Mode.Should().Be(DeliveryMode.ReliableOrdered);
    }

    [Fact]
    public void TryDecode_PayloadMutatedAfterDecode_LeavesDecodedPayloadUnchanged()
    {
        // Arrange
        byte[] payload = [0x11, 0x22, 0x33];
        byte[] datagram =
        [
            .. BuildHeader(1, 0, 0, 0, 0),
            .. BuildRecord(ModeUnreliable, sequence: 1, payload: payload),
        ];

        // Act
        DatagramDecodeResult result = DatagramDecoder.TryDecode(datagram);
        Array.Clear(datagram, 0, datagram.Length);

        // Assert
        result.Datagram!.Records[0].Payload.Should().Equal(0x11, 0x22, 0x33);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(DatagramHeaderSize - 1)]
    public void TryDecode_DatagramShorterThanHeader_FailsTheDecode(int length)
    {
        // Arrange
        byte[] datagram = new byte[length];

        // Act
        DatagramDecodeResult result = DatagramDecoder.TryDecode(datagram);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Datagram.Should().BeNull();
        result.Error.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public void TryDecode_TrailingBytesShorterThanARecordHeader_FailsTheDecode()
    {
        // Arrange — a well-formed record followed by 3 stray bytes, too few for another record header.
        byte[] datagram =
        [
            .. BuildHeader(1, 0, 0, 0, 0),
            .. BuildRecord(ModeUnreliable, sequence: 1, payload: []),
            0x01, 0x02, 0x03,
        ];

        // Act
        DatagramDecodeResult result = DatagramDecoder.TryDecode(datagram);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Datagram.Should().BeNull();
    }

    [Fact]
    public void TryDecode_RecordDeclaresPayloadLongerThanBytesRemaining_FailsTheDecode()
    {
        // Arrange — the record header claims a 10-byte payload but only 2 bytes follow it.
        byte[] header = BuildHeader(1, 0, 0, 0, 0);
        byte[] shortRecord = [ModeUnreliable, 0, 0, 0, 1, 0, 10, 0xAA, 0xBB];
        byte[] datagram = [.. header, .. shortRecord];

        // Act
        DatagramDecodeResult result = DatagramDecoder.TryDecode(datagram);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Datagram.Should().BeNull();
        result.Error.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public void TryDecode_SecondRecordRunsPastDatagramEnd_FailsWholeDecodeRatherThanReturningFirstRecord()
    {
        // Arrange — a well-formed first record, then a second record's header truncated mid-way.
        byte[] datagram =
        [
            .. BuildHeader(1, 0, 0, 0, 0),
            .. BuildRecord(ModeUnreliable, sequence: 1, payload: [0xAA]),
            ModeReliableOrdered, 0, 0, 0,
        ];

        // Act
        DatagramDecodeResult result = DatagramDecoder.TryDecode(datagram);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Datagram.Should().BeNull();
    }

    /// <summary>Builds a 20-byte datagram header from named fields, independent of <see cref="DatagramDecoder"/>'s own offsets.</summary>
    private static byte[] BuildHeader(
        uint session,
        uint reliableOrderedAckBase,
        uint reliableOrderedAckBits,
        uint reliableUnorderedAckBase,
        uint reliableUnorderedAckBits)
    {
        byte[] header = new byte[DatagramHeaderSize];
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0), session);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(4), reliableOrderedAckBase);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(8), reliableOrderedAckBits);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(12), reliableUnorderedAckBase);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(16), reliableUnorderedAckBits);
        return header;
    }

    /// <summary>Builds one unfragmented packet record — flags, sequence, length, payload — from named fields.</summary>
    private static byte[] BuildRecord(byte flags, uint sequence, byte[] payload)
    {
        byte[] record = new byte[7 + payload.Length];
        record[0] = flags;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5), (ushort)payload.Length);
        payload.CopyTo(record.AsSpan(7));
        return record;
    }
}
