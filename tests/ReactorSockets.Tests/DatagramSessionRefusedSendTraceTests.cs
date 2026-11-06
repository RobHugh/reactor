using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 7.2.4 (issue #244): what a refused <see cref="DatagramSessionReactor.Send"/> is allowed to
/// leave behind, and the span-copy contract that lets a caller reuse its buffer the instant
/// <c>Send</c> returns. A <see cref="SendOutcome.RetentionExhausted"/> refusal spends nothing: no
/// record is queued, no retention is taken, and no sequence number is claimed from the Space.
/// </summary>
/// <remarks>
/// A burned sequence becomes visible only when a <em>later</em> record in the same Space reaches the
/// wire carrying a number one too high, so
/// <see cref="Send_AcceptedAfterAnAckFreesTheSlotARefusalFoundFull_CarriesTheNextSequenceSoTheRefusalBurnedNone"/>
/// exhausts a Space, is refused, lets an ack free the slot, and reads the next accepted record's
/// sequence off the wire.
/// </remarks>
public class DatagramSessionRefusedSendTraceTests
{
    private const int FragmentSize = 600;

    [Fact]
    public void Send_RefusedAsRetentionExhausted_QueuesNothingSoTheWireCarriesOnlyTheAcceptedRecord()
    {
        // Arrange — one retention slot per reliable Space, so the second ReliableOrdered Send is
        // certain to be refused while the first is certain to have been retained.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, maxMessageSize: FragmentSize, retentionBytes: FragmentSize, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            maxMessageSize: FragmentSize, retentionBytes: FragmentSize, fragmentSize: FragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Act
        SendOutcome accepted = dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [1, 2, 3]);
        SendOutcome refused = dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [4, 5, 6]);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Assert
        accepted.Should().Be(SendOutcome.Sent);
        refused.Should().Be(SendOutcome.RetentionExhausted, "the Space holds exactly one slot and the first Send took it");

        DecodedDatagram datagram = DecodeFirstDataDatagram(tap);
        datagram.Records.Should().ContainSingle("the refused Send queued no record to carry");
        datagram.Records[0].Mode.Should().Be(DeliveryMode.ReliableOrdered);
        datagram.Records[0].Sequence.Should().Be(0U, "the accepted Send is this Space's first allocation");
        datagram.Records[0].Payload.Should().Equal([1, 2, 3], "the record on the wire is the one that was accepted");
    }

    [Fact]
    public void Send_AcceptedAfterAnAckFreesTheSlotARefusalFoundFull_CarriesTheNextSequenceSoTheRefusalBurnedNone()
    {
        // Arrange — one retention slot per reliable Space. Linger is pushed far out so no RTO fires
        // and resends sequence 0 mid-test.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, maxMessageSize: FragmentSize, retentionBytes: FragmentSize, fragmentSize: FragmentSize,
            linger: TimeSpan.FromHours(1));
        using DatagramSessionReactorHarness accepter = new(
            maxMessageSize: FragmentSize, retentionBytes: FragmentSize, fragmentSize: FragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Sent);
        SendOutcome refused = dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [2]);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // The accepter's reply carries the ack for sequence 0, and the dialler's next Pump reads it
        // and frees the slot.
        accepter.Reactor.Send(accepterPeer, DeliveryMode.Unreliable, [9]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);
        dialler.Reactor.Pump();

        // Act
        SendOutcome afterAck = dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [3]);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Assert
        refused.Should().Be(SendOutcome.RetentionExhausted, "the Space holds exactly one slot and the first Send took it");
        afterAck.Should().Be(SendOutcome.Sent, "the ack for sequence 0 freed the only slot this Space has");

        DecodedPacketRecord record = DecodeDataDatagrams(tap)
            .SelectMany(datagram => datagram.Records)
            .Should().ContainSingle(r => r.Mode == DeliveryMode.ReliableOrdered && r.Payload.SequenceEqual(new byte[] { 3 }))
            .Subject;
        record.Sequence.Should().Be(1U, "the refusal claimed no sequence, so the next accepted Send takes the one after 0");
    }

    [Theory]
    [InlineData(DeliveryMode.Unreliable)]
    [InlineData(DeliveryMode.UnreliableSequenced)]
    [InlineData(DeliveryMode.ReliableUnordered)]
    public void Send_AfterAReliableOrderedRefusal_LeavesEveryOtherSpaceAtItsOwnFirstSequence(DeliveryMode mode)
    {
        // Arrange — the refusal happens in ReliableOrdered's Space. Every other Space must still be
        // untouched, so the first record each of them puts on the wire carries sequence zero.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, maxMessageSize: FragmentSize, retentionBytes: FragmentSize, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            maxMessageSize: FragmentSize, retentionBytes: FragmentSize, fragmentSize: FragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Sent);
        SendOutcome refused = dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [2]);

        // Act
        SendOutcome elsewhere = dialler.Reactor.Send(diallerPeer, mode, [3]);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Assert
        refused.Should().Be(SendOutcome.RetentionExhausted);
        elsewhere.Should().Be(SendOutcome.Sent, "the refusal was ReliableOrdered's Space alone");

        DecodedDatagram datagram = DecodeFirstDataDatagram(tap);
        DecodedPacketRecord record = datagram.Records.Should().ContainSingle(r => r.Mode == mode).Subject;
        record.Sequence.Should().Be(0U, "this Space has allocated nothing before, and the refusal was not its to pay for");
    }

    [Theory]
    [InlineData(DeliveryMode.Unreliable)]
    [InlineData(DeliveryMode.UnreliableSequenced)]
    [InlineData(DeliveryMode.ReliableUnordered)]
    [InlineData(DeliveryMode.ReliableOrdered)]
    public void Send_WhoseCallerMutatesTheBufferAfterItReturns_PutsTheBytesAsTheyWereOnTheWire(DeliveryMode mode)
    {
        // Arrange — the span-copy contract: Send copies, so the caller owns its buffer again the
        // instant Send returns. The buffer is overwritten before the Pump that flushes it, which is
        // the window a borrowed span would show through.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, maxMessageSize: FragmentSize, retentionBytes: FragmentSize, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            maxMessageSize: FragmentSize, retentionBytes: FragmentSize, fragmentSize: FragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        byte[] buffer = [10, 20, 30, 40];

        // Act
        SendOutcome outcome = dialler.Reactor.Send(diallerPeer, mode, buffer);
        Array.Fill<byte>(buffer, 0xFF);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Assert
        outcome.Should().Be(SendOutcome.Sent);

        DecodedDatagram datagram = DecodeFirstDataDatagram(tap);
        DecodedPacketRecord record = datagram.Records.Should().ContainSingle(r => r.Mode == mode).Subject;
        record.Payload.Should().Equal([10, 20, 30, 40], "Send copied the caller's span rather than borrowing it");
    }

    /// <summary>
    /// The first datagram the dialler sent that is not a control datagram, decoded. A control
    /// datagram carries session id zero; a Session datagram carries the receiver-assigned id, which is
    /// never zero, exactly as <see cref="DatagramSessionUnreliableFragmentRefusalTests"/> separates them.
    /// </summary>
    private static DecodedDatagram DecodeFirstDataDatagram(WireTap tap)
    {
        byte[] payload = tap.PayloadsInDirection(TapDirection.DiallerToAccepter)
            .First(p => BinaryPrimitives.ReadUInt32BigEndian(p.AsSpan(0, 4)) != 0);

        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(payload);
        decoded.IsSuccess.Should().BeTrue(decoded.Error);
        return decoded.Datagram!;
    }

    /// <summary>
    /// Every datagram the dialler sent that is not a control datagram, decoded, in the order sent.
    /// </summary>
    private static List<DecodedDatagram> DecodeDataDatagrams(WireTap tap)
    {
        List<DecodedDatagram> datagrams = [];
        foreach (byte[] payload in tap.PayloadsInDirection(TapDirection.DiallerToAccepter)
            .Where(p => BinaryPrimitives.ReadUInt32BigEndian(p.AsSpan(0, 4)) != 0))
        {
            DatagramDecodeResult decoded = DatagramDecoder.TryDecode(payload);
            decoded.IsSuccess.Should().BeTrue(decoded.Error);
            datagrams.Add(decoded.Datagram!);
        }

        return datagrams;
    }
}
