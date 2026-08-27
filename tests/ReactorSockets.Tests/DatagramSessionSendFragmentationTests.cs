using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #245: <see cref="DatagramSessionReactor.Send"/> cutting a reliable Message wider than one
/// Fragment into <c>ceil(size / fragmentSize)</c> Fragments, each an ordinary reliable packet with its
/// own sequence number. Every test here drives a live Session through <see cref="WireTap"/> and decodes
/// the captured bytes by hand — the same proof style <see cref="DatagramSessionWireFormatTests"/> uses —
/// rather than through <see cref="DatagramDecoder"/>, which only understands the unfragmented record
/// shape (issue #229) and is left alone here (out of this ticket's scope).
/// </summary>
public class DatagramSessionSendFragmentationTests
{
    private const int DatagramHeaderSize = 20;
    private const int UnfragmentedRecordSize = 7;
    private const int FragmentRecordSize = 11;
    private const byte ModeMask = 0b0000_0011;
    private const byte FragmentBit = 0b0000_0100;
    private const byte SpareBitsMask = 0b1111_1000;

    private const int FragmentSize = 576;
    private const int MaxMessageSize = 2000; // ceil(2000 / 576) = 4 Fragments: three full, one remainder.

    public static TheoryData<DeliveryMode> ReliableModes => new()
    {
        DeliveryMode.ReliableOrdered,
        DeliveryMode.ReliableUnordered,
    };

    [Theory]
    [MemberData(nameof(ReliableModes))]
    public void Send_MaxMessageSizeMessage_IsCutIntoCeilFragmentCount(DeliveryMode mode)
    {
        // Arrange / Act
        (List<DecodedRecord> records, byte[] message) = SendAndDecodeFragments(mode);

        // Assert
        int expectedFragmentCount = (MaxMessageSize + FragmentSize - 1) / FragmentSize;
        records.Should().HaveCount(expectedFragmentCount, "a MaxMessageSize Message cuts into ceil(size / fragmentSize) Fragments");
        records.Sum(r => r.Length).Should().Be(message.Length, "every byte of the Message is carried by exactly one Fragment");
    }

    [Theory]
    [MemberData(nameof(ReliableModes))]
    public void Send_MaxMessageSizeMessage_FragmentSequencesAreContiguousAndAscending(DeliveryMode mode)
    {
        // Arrange / Act
        (List<DecodedRecord> records, _) = SendAndDecodeFragments(mode);

        // Assert — contiguity is what lets a receiver group Fragments by firstSeq = seq - fragmentIndex
        // (ADR 0004), so this is asserted directly on the wire sequence numbers, not merely inferred.
        List<uint> sequences = records.Select(r => r.Sequence).ToList();
        sequences.Should().BeInAscendingOrder();
        for (int i = 1; i < sequences.Count; i++)
        {
            sequences[i].Should().Be(sequences[i - 1] + 1, "Fragments of one Message occupy contiguous sequence numbers");
        }
    }

    [Theory]
    [MemberData(nameof(ReliableModes))]
    public void Send_MaxMessageSizeMessage_EachFragmentCarriesItsOwnIndexAndTheCountMatchesWhatWasRetained(DeliveryMode mode)
    {
        // Arrange / Act
        (List<DecodedRecord> records, _) = SendAndDecodeFragments(mode);

        // Assert
        for (int i = 0; i < records.Count; i++)
        {
            records[i].IsFragment.Should().BeTrue("a Message cut into more than one piece always carries the fragment shape");
            records[i].FragmentIndex.Should().Be((ushort)i, "Fragments are queued, sequenced and packed in cut order");
            records[i].FragmentCount.Should().Be((ushort)records.Count, "the count on the wire matches the number of Fragments actually retained and sent");
        }
    }

    [Theory]
    [MemberData(nameof(ReliableModes))]
    public void Send_MaxMessageSizeMessage_LastFragmentCarriesTheRemainderAndEarlierOnesAreFull(DeliveryMode mode)
    {
        // Arrange / Act
        (List<DecodedRecord> records, byte[] message) = SendAndDecodeFragments(mode);

        // Assert
        for (int i = 0; i < records.Count - 1; i++)
        {
            records[i].Length.Should().Be(FragmentSize, "every Fragment but the last carries a full fragment size of payload");
        }

        int remainder = message.Length - (FragmentSize * (records.Count - 1));
        records[^1].Length.Should().Be((ushort)remainder, "the last Fragment carries whatever is left over");
        records[^1].Length.Should().BeLessThanOrEqualTo((ushort)FragmentSize);
    }

    [Theory]
    [MemberData(nameof(ReliableModes))]
    public void Send_MessageThatFitsInOneFragment_IsSentUnfragmentedWithTheFragmentBitClear(DeliveryMode mode)
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, maxMessageSize: MaxMessageSize, retentionBytes: MaxMessageSize * 2, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness accepter = new(maxMessageSize: MaxMessageSize, retentionBytes: MaxMessageSize * 2, fragmentSize: FragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        byte[] message = Enumerable.Range(0, FragmentSize).Select(i => (byte)i).ToArray();

        // Act — exactly one Fragment's worth of payload: still cut into 1 piece, which is unfragmented.
        dialler.Reactor.Send(diallerPeer, mode, message);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        List<DecodedRecord> records = DecodeRecords(FirstDataDatagram(tap));

        // Assert
        records.Should().HaveCount(1);
        records[0].IsFragment.Should().BeFalse("a Message that fits in one Fragment is sent unfragmented");
        (records[0].Flags & FragmentBit).Should().Be((byte)0, "the fragment bit is clear on an unfragmented record");
        records[0].Length.Should().Be((ushort)message.Length);
        records[0].Payload.Should().Equal(message);
    }

    [Theory]
    [MemberData(nameof(ReliableModes))]
    public void Send_FragmentRecord_RoundTripsModeSequenceLengthIndexAndCountWithNoSpareBitSet(DeliveryMode mode)
    {
        // Arrange / Act
        (List<DecodedRecord> records, byte[] message) = SendAndDecodeFragments(mode);

        // Assert — every Fragment on the wire decodes back to the mode it was Sent under, a sequence,
        // a length, a fragment index and count, and never a spare bit.
        for (int i = 0; i < records.Count; i++)
        {
            DecodedRecord record = records[i];
            record.Mode.Should().Be(mode);
            record.Sequence.Should().Be((uint)i);
            record.FragmentIndex.Should().Be((ushort)i);
            record.FragmentCount.Should().Be((ushort)records.Count);
            (record.Flags & SpareBitsMask).Should().Be((byte)0, "no spare bit is ever set on a record this library writes");

            int offset = i * FragmentSize;
            int length = Math.Min(FragmentSize, message.Length - offset);
            record.Payload.Should().Equal(message.AsSpan(offset, length).ToArray());
        }
    }

    /// <summary>
    /// Sends a <see cref="MaxMessageSize"/>-byte Message under <paramref name="mode"/> through a
    /// freshly connected Session pair, pumps it across a <see cref="WireTap"/>, and decodes the one
    /// captured data Datagram's records by hand.
    /// </summary>
    private static (List<DecodedRecord> Records, byte[] Message) SendAndDecodeFragments(DeliveryMode mode)
    {
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, maxMessageSize: MaxMessageSize, retentionBytes: MaxMessageSize * 2, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness accepter = new(maxMessageSize: MaxMessageSize, retentionBytes: MaxMessageSize * 2, fragmentSize: FragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        byte[] message = Enumerable.Range(0, MaxMessageSize).Select(i => (byte)(i % 256)).ToArray();

        SendOutcome outcome = dialler.Reactor.Send(diallerPeer, mode, message);
        outcome.Should().Be(SendOutcome.Sent, "retention was sized generously enough to hold every Fragment of one MaxMessageSize Message");
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        List<DecodedRecord> records = DecodeRecords(FirstDataDatagram(tap));
        return (records, message);
    }

    /// <summary>The first datagram travelling from the dialler whose session id names a live Session rather than the reserved control session id.</summary>
    private static byte[] FirstDataDatagram(WireTap tap) =>
        tap.PayloadsInDirection(TapDirection.DiallerToAccepter).First(payload => BinaryPrimitives.ReadUInt32BigEndian(payload.AsSpan(0, 4)) != 0);

    /// <summary>
    /// Walks every packet record after the 20-byte header, reading either wire shape by hand (ADR 0009):
    /// the unfragmented 7-byte header, or the 11-byte Fragment header carrying an index and count, chosen
    /// by the flags byte's fragment bit — the same choice <see cref="PacketRecordCodec.TryRead"/> makes,
    /// reimplemented independently here so a bug in the production reader could not hide itself from this
    /// test.
    /// </summary>
    private static List<DecodedRecord> DecodeRecords(byte[] datagram)
    {
        List<DecodedRecord> records = [];
        int offset = DatagramHeaderSize;
        while (offset < datagram.Length)
        {
            byte flags = datagram[offset];
            uint sequence = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(offset + 1, 4));
            ushort length = BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + 5, 2));
            bool isFragment = (flags & FragmentBit) != 0;

            ushort fragmentIndex = 0;
            ushort fragmentCount = 0;
            int headerSize = UnfragmentedRecordSize;
            if (isFragment)
            {
                fragmentIndex = BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + 7, 2));
                fragmentCount = BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + 9, 2));
                headerSize = FragmentRecordSize;
            }

            int payloadOffset = offset + headerSize;
            byte[] payload = datagram.AsSpan(payloadOffset, length).ToArray();

            records.Add(new DecodedRecord(flags, (DeliveryMode)(flags & ModeMask), sequence, length, isFragment, fragmentIndex, fragmentCount, payload));
            offset = payloadOffset + length;
        }

        return records;
    }

    /// <summary>One packet record decoded by hand off a captured live-Session datagram.</summary>
    private sealed record DecodedRecord(
        byte Flags,
        DeliveryMode Mode,
        uint Sequence,
        ushort Length,
        bool IsFragment,
        ushort FragmentIndex,
        ushort FragmentCount,
        byte[] Payload);
}
