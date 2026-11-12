using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A Peer sends at the smaller of the two ends' Fragment sizes, fixed when the Peer reaches Established
/// (ADR 0024 point 4), and a reliable <see cref="DatagramSessionReactor.Send"/> whose Fragment count at
/// that size exceeds the remote receive window is refused with <see cref="SendOutcome.RemoteMessageTooLarge"/>
/// (point 5). Every test drives a live Session through <see cref="WireTap"/> and decodes the captured
/// records by hand.
/// </summary>
public class DatagramSessionFragmentSizeNegotiationTests
{
    private const int DatagramHeaderSize = 20;
    private const int UnfragmentedRecordSize = 7;
    private const int FragmentRecordSize = 11;
    private const byte FragmentBit = 0b0000_0100;

    /// <summary>One packet record read off the wire: its sequence, whether it is a Fragment, and its payload length.</summary>
    private readonly record struct WireRecord(uint Sequence, bool IsFragment, int PayloadLength);

    /// <summary>Every packet record in every data datagram the tap relayed from the dialler, in capture order.</summary>
    private static List<WireRecord> DiallerRecords(WireTap tap)
    {
        List<WireRecord> records = [];
        foreach (byte[] datagram in tap.PayloadsInDirection(TapDirection.DiallerToAccepter))
        {
            if (BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(0, 4)) == 0)
            {
                continue;
            }

            int offset = DatagramHeaderSize;
            while (offset < datagram.Length)
            {
                bool isFragment = (datagram[offset] & FragmentBit) != 0;
                uint sequence = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(offset + 1, 4));
                ushort length = BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + 5, 2));
                records.Add(new WireRecord(sequence, isFragment, length));
                offset += (isFragment ? FragmentRecordSize : UnfragmentedRecordSize) + length;
            }
        }

        return records;
    }

    /// <summary>Connects a dialler with its own Fragment size to an accepter with its own, both with one Peer's worth of capacity and room to retain several Messages.</summary>
    private static (DatagramSessionReactorHarness Dialler, DatagramSessionReactorHarness Accepter, WireTap Tap, PeerId DiallerPeer) Connect(
        int diallerFragmentSize, int accepterFragmentSize, int maxMessageSize)
    {
        DatagramSessionReactorHarness dialler = new(
            capacity: 1, bindLoopback: false, maxMessageSize: maxMessageSize, retentionBytes: maxMessageSize * 4, fragmentSize: diallerFragmentSize);
        DatagramSessionReactorHarness accepter = new(
            capacity: 1, maxMessageSize: maxMessageSize, retentionBytes: maxMessageSize * 4, fragmentSize: accepterFragmentSize);
        WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);
        return (dialler, accepter, tap, diallerPeer);
    }

    [Theory]
    [InlineData(DeliveryMode.ReliableOrdered)]
    [InlineData(DeliveryMode.ReliableUnordered)]
    public void Send_SenderFragmentSizeLargerThanReceivers_CutsAtTheReceiversSize(DeliveryMode mode)
    {
        // Arrange — 2300 bytes at the receiver's 600 is 4 Fragments (600, 600, 600, 500). The receiver's
        // window floor is ceil(2400 / 600) = 4, so the Message fits.
        (DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, WireTap tap, PeerId peer) = Connect(900, 600, 2400);
        using DatagramSessionReactorHarness diallerOwner = dialler;
        using DatagramSessionReactorHarness accepterOwner = accepter;
        using WireTap tapOwner = tap;
        byte[] message = Enumerable.Range(0, 2300).Select(i => (byte)i).ToArray();

        // Act
        SendOutcome outcome = dialler.Reactor.Send(peer, mode, message);
        List<byte[]> received = [];
        for (int round = 0; round < 6; round++)
        {
            DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);
            received.AddRange(accepter.DrainPayloads());
        }

        // Assert
        outcome.Should().Be(SendOutcome.Sent);
        List<WireRecord> records = DiallerRecords(tap);
        records.Select(r => r.PayloadLength).Should().Equal(
            [600, 600, 600, 500], "the sender cuts at min(900, 600), not at its own 900");
        records.Should().OnlyContain(r => r.IsFragment);
        received.Should().BeEquivalentTo(new[] { message }, "the receiver accepts every Fragment and reassembles the Message");
    }

    [Fact]
    public void Send_EightHundredFiftyByteMessageToASmallerFragmentSizePeer_IsCutIntoTwoFragments()
    {
        // Arrange — 850 fits the sender's own 900, so a cut at the local size would send it whole.
        (DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, WireTap tap, PeerId peer) = Connect(900, 600, 2400);
        using DatagramSessionReactorHarness diallerOwner = dialler;
        using DatagramSessionReactorHarness accepterOwner = accepter;
        using WireTap tapOwner = tap;
        byte[] message = Enumerable.Range(0, 850).Select(i => (byte)i).ToArray();

        // Act
        SendOutcome outcome = dialler.Reactor.Send(peer, DeliveryMode.ReliableOrdered, message);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Assert
        outcome.Should().Be(SendOutcome.Sent);
        DiallerRecords(tap).Should().Equal(
            [new WireRecord(0, true, 600), new WireRecord(1, true, 250)],
            "an 850-byte Message is wider than the receiver's 600-byte Fragment and is sent as 600 + 250");
    }

    [Theory]
    [InlineData(DeliveryMode.ReliableOrdered)]
    [InlineData(DeliveryMode.ReliableUnordered)]
    public void Send_FragmentCountAtTheSendSizeOverTheRemoteWindow_ReturnsRemoteMessageTooLargeAndRetainsNothing(DeliveryMode mode)
    {
        // Arrange — the receiver (900, MaxMessageSize 4000) has a window of ceil(4000 / 900) = 5, which
        // the options confirm. The sender cuts at 600: 4000 bytes is 7 Fragments, 3000 bytes is 5.
        (DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, WireTap tap, PeerId peer) = Connect(600, 900, 4000);
        using DatagramSessionReactorHarness diallerOwner = dialler;
        using DatagramSessionReactorHarness accepterOwner = accepter;
        using WireTap tapOwner = tap;
        accepter.Options.ResolvedReceiveWindowFragments.Should().Be(5);

        // Act
        SendOutcome tooLarge = dialler.Reactor.Send(peer, mode, new byte[4000]);
        SendOutcome fits = dialler.Reactor.Send(peer, mode, new byte[3000]);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Assert — the refused Message spent no sequence: the accepted one starts at sequence 0 and its
        // five Fragments are 0 to 4, so nothing was sequenced or retained for the 4000-byte Message.
        tooLarge.Should().Be(SendOutcome.RemoteMessageTooLarge);
        fits.Should().Be(SendOutcome.Sent);
        DiallerRecords(tap).Select(r => r.Sequence).Should().Equal(
            [0u, 1u, 2u, 3u, 4u], "the refusal sequenced nothing, so the next Message takes the first sequences");
    }

    [Theory]
    [InlineData(DeliveryMode.Unreliable)]
    [InlineData(DeliveryMode.UnreliableSequenced)]
    public void Send_UnreliableMessageWiderThanTheRemoteFragmentSize_ReturnsMessageTooLarge(DeliveryMode mode)
    {
        // Arrange — 601 bytes fits the sender's own 900 but not the receiver's 600, and the Peer's send
        // Fragment size is the smaller of the two.
        (DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, WireTap tap, PeerId peer) = Connect(900, 600, 2400);
        using DatagramSessionReactorHarness diallerOwner = dialler;
        using DatagramSessionReactorHarness accepterOwner = accepter;
        using WireTap tapOwner = tap;

        // Act
        SendOutcome outcome = dialler.Reactor.Send(peer, mode, new byte[601]);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Assert
        outcome.Should().Be(SendOutcome.MessageTooLarge, "the single-Datagram cap is min(900, 600)");
        DiallerRecords(tap).Should().BeEmpty("a refused Message puts nothing on the wire");
    }

    [Theory]
    [InlineData(DeliveryMode.Unreliable)]
    [InlineData(DeliveryMode.UnreliableSequenced)]
    public void Send_UnreliableMessageExactlyTheRemoteFragmentSize_IsSentWholeAndDelivered(DeliveryMode mode)
    {
        // Arrange
        (DatagramSessionReactorHarness dialler, DatagramSessionReactorHarness accepter, WireTap tap, PeerId peer) = Connect(900, 600, 2400);
        using DatagramSessionReactorHarness diallerOwner = dialler;
        using DatagramSessionReactorHarness accepterOwner = accepter;
        using WireTap tapOwner = tap;
        byte[] message = Enumerable.Range(0, 600).Select(i => (byte)i).ToArray();

        // Act
        SendOutcome outcome = dialler.Reactor.Send(peer, mode, message);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);
        List<byte[]> received = accepter.DrainPayloads();

        // Assert
        outcome.Should().Be(SendOutcome.Sent);
        DiallerRecords(tap).Should().Equal(
            [new WireRecord(0, false, 600)], "an unreliable Message is sent whole in one record");
        received.Should().BeEquivalentTo(new[] { message });
    }
}
