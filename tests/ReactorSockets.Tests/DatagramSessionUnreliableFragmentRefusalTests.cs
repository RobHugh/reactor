using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The refusal that gives fragmentation its shape.
/// <see cref="DeliveryMode.Unreliable"/> and <see cref="DeliveryMode.UnreliableSequenced"/> are capped
/// at a single Datagram and <see cref="DatagramSessionReactor.Send"/> answers
/// <see cref="SendOutcome.MessageTooLarge"/> above it, because you may only fragment what you can
/// retransmit (ADR 0004): a ten-Fragment unreliable Message on a path losing 1% of Datagrams arrives
/// about 90% of the time, having spent ten Datagrams of bandwidth on a coin flip.
/// </summary>
/// <remarks>
/// The refusal has to leave no trace, so the two halves of that promise are asserted separately: no
/// retention taken, and no sequence number burned. The sequence half is only visible on the wire —
/// no member of this Reactor surfaces a sequence number — so it is read off a captured Datagram at
/// the byte offsets <see cref="DatagramSessionWireFormatTests"/> pins, rather than inferred.
/// </remarks>
/// <remarks>
/// A Send under any mode is refused with <see cref="SendOutcome.NotEstablished"/> until the handshake
/// completes (issue #332), so every test below pumps a real handshake to completion first.
/// </remarks>
public class DatagramSessionUnreliableFragmentRefusalTests
{
    private const int DatagramHeaderSize = 20;
    private const int UnfragmentedRecordSize = 7;
    private const int RecordSequenceOffset = 1;
    private const int RecordLengthOffset = 5;
    private const byte FragmentBit = 0b0000_0100;
    private const byte ModeMask = 0b0000_0011;

    [Theory]
    [InlineData(DeliveryMode.Unreliable)]
    [InlineData(DeliveryMode.UnreliableSequenced)]
    public void Send_AMaxMessageSizeMessageInAnUnreliableMode_ReturnsMessageTooLargeRatherThanRetentionExhausted(DeliveryMode mode)
    {
        // Arrange — MaxMessageSize deliberately several fragments wide, and retention deliberately
        // ample, so the only honest answer is the permanent refusal rather than the transient one.
        const int fragmentSize = 600;
        const int maxMessageSize = 4 * fragmentSize;
        using DatagramSessionReactorHarness harness = new(
            bindLoopback: false,
            maxMessageSize: maxMessageSize,
            retentionBytes: 16 * fragmentSize,
            fragmentSize: fragmentSize);
        using DatagramSessionReactorHarness remote = new();
        PeerId peer = harness.Reactor.Connect(remote.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(harness, remote);
        byte[] maximumMessage = new byte[maxMessageSize];

        // Act
        SendOutcome outcome = harness.Reactor.Send(peer, mode, maximumMessage);

        // Assert
        outcome.Should().Be(SendOutcome.MessageTooLarge);
        outcome.Should().NotBe(
            SendOutcome.RetentionExhausted,
            "retention is ample here, so the refusal is permanent rather than backpressure a caller could retry past");
    }

    [Theory]
    [InlineData(DeliveryMode.Unreliable)]
    [InlineData(DeliveryMode.UnreliableSequenced)]
    public void Send_ARefusedOversizedUnreliableMessage_TakesNoRetention(DeliveryMode mode)
    {
        // Arrange — retention pinned to a single slot per reliable Space. If a refused unreliable Send
        // had taken retention, the reliable Send that follows it would find that slot already spent.
        const int fragmentSize = 600;
        using DatagramSessionReactorHarness harness = new(
            bindLoopback: false,
            maxMessageSize: fragmentSize,
            retentionBytes: fragmentSize,
            fragmentSize: fragmentSize);
        using DatagramSessionReactorHarness remote = new();
        PeerId peer = harness.Reactor.Connect(remote.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(harness, remote);
        byte[] tooWide = new byte[fragmentSize + 1];

        // Act
        SendOutcome refused = harness.Reactor.Send(peer, mode, tooWide);
        SendOutcome reliable = harness.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [1, 2, 3]);

        // Assert
        refused.Should().Be(SendOutcome.MessageTooLarge);
        reliable.Should().Be(SendOutcome.Sent, "the refused unreliable Send took no retention slot");
    }

    [Theory]
    [InlineData(DeliveryMode.Unreliable)]
    [InlineData(DeliveryMode.UnreliableSequenced)]
    public void Send_ARefusedOversizedUnreliableMessage_TakesNoSequenceFromItsSpace(DeliveryMode mode)
    {
        // Arrange — the sequence half of the no-trace promise, read off the wire because nothing on
        // the public seam reports a sequence number. Each mode has its own Space, each based at 0, so
        // the first record this mode ever puts on the wire must carry sequence 0. Had the refusal
        // burned one, it would carry 1.
        const int fragmentSize = 600;
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, maxMessageSize: fragmentSize, retentionBytes: fragmentSize, fragmentSize: fragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            maxMessageSize: fragmentSize, retentionBytes: fragmentSize, fragmentSize: fragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        byte[] tooWide = new byte[fragmentSize + 1];

        // Act
        SendOutcome refused = dialler.Reactor.Send(diallerPeer, mode, tooWide);
        SendOutcome accepted = dialler.Reactor.Send(diallerPeer, mode, [1, 2, 3]);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        byte[] datagram = FirstDataDatagram(tap, TapDirection.DiallerToAccepter);
        uint sequence = BinaryPrimitives.ReadUInt32BigEndian(
            datagram.AsSpan(DatagramHeaderSize + RecordSequenceOffset, 4));

        // Assert
        refused.Should().Be(SendOutcome.MessageTooLarge);
        accepted.Should().Be(SendOutcome.Sent);
        sequence.Should().Be(0U, "the refused Send allocated nothing, so the accepted one is this Space's first sequence");
    }

    [Theory]
    [InlineData(DeliveryMode.Unreliable, (byte)0b0000_0000)]
    [InlineData(DeliveryMode.UnreliableSequenced, (byte)0b0000_0001)]
    public void Send_AMessageExactlyAtTheOneDatagramCap_IsAcceptedAndLeavesUnfragmented(DeliveryMode mode, byte modeBits)
    {
        // Arrange — the cap is "wider than one Datagram", so a Message that exactly fills one is
        // carried, and carried as a single unfragmented record with the fragment bit clear.
        const int fragmentSize = 600;
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, maxMessageSize: fragmentSize, retentionBytes: fragmentSize, fragmentSize: fragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            maxMessageSize: fragmentSize, retentionBytes: fragmentSize, fragmentSize: fragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        byte[] exactlyAtCap = new byte[fragmentSize];
        for (int index = 0; index < exactlyAtCap.Length; index++)
        {
            exactlyAtCap[index] = (byte)index;
        }

        // Act
        SendOutcome outcome = dialler.Reactor.Send(diallerPeer, mode, exactlyAtCap);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        byte[] datagram = FirstDataDatagram(tap, TapDirection.DiallerToAccepter);
        byte flags = datagram[DatagramHeaderSize];
        ushort declaredLength = BinaryPrimitives.ReadUInt16BigEndian(
            datagram.AsSpan(DatagramHeaderSize + RecordLengthOffset, 2));

        // Assert
        outcome.Should().Be(SendOutcome.Sent);
        (flags & FragmentBit).Should().Be(0, "a Message that fits one Datagram is never cut, so the fragment bit stays clear");
        (flags & ModeMask).Should().Be(modeBits, "the record carries the mode it was Sent under");
        declaredLength.Should().Be(fragmentSize, "the whole Message travels as one record's payload");
        datagram.Length.Should().Be(
            DatagramHeaderSize + UnfragmentedRecordSize + fragmentSize,
            "one 7-byte unfragmented record header plus the payload, and nothing else");
        datagram.AsSpan(DatagramHeaderSize + UnfragmentedRecordSize, fragmentSize).ToArray()
            .Should().Equal(exactlyAtCap);
    }

    private static byte[] FirstDataDatagram(WireTap tap, TapDirection direction) =>
        tap.PayloadsInDirection(direction).First(payload => BinaryPrimitives.ReadUInt32BigEndian(payload.AsSpan(0, 4)) != 0);
}
