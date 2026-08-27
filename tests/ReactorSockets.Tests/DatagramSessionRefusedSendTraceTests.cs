using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 7.2.4 (issue #244): what a refused <see cref="DatagramSessionReactor.Send"/> is allowed to
/// leave behind, and the span-copy contract that lets a caller reuse its buffer the instant
/// <c>Send</c> returns. A <see cref="SendOutcome.RetentionExhausted"/> refusal spends nothing: no
/// record is queued, no retention is taken, and no sequence number is claimed from the Space, because
/// the sequence is read with <c>Peek</c> and only claimed once retention has committed.
/// </summary>
/// <remarks>
/// The no-sequence-burned half is asserted here as far as the public seam reaches, which is not all
/// the way. A burned sequence only becomes visible when a <em>later</em> record in the same Space
/// reaches the wire carrying a number one too high, and no later record can: nothing on any live path
/// calls <c>Core.RetentionPool.Release</c> yet (issue #187), so a Space that has refused once stays
/// exhausted for the rest of that Peer's life and never sends again. What is reachable is asserted
/// below — the refusal puts nothing on the wire, and every other Space still bases at zero — and the
/// unreachable half was answered the way ADR 0015 prescribes for an invariant the seam cannot reach:
/// the code was changed until it is trivially true, rather than the seam being opened to observe it.
/// When #187 lands, the direct assertion belongs here: exhaust a Space, refuse, let an ack free a
/// slot, and read the next record's sequence off the wire.
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
}
