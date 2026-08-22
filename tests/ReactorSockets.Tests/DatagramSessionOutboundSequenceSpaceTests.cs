using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.7.10 (issue #237): the four outbound sequence Spaces per Peer
/// (<see cref="ReactorSockets.Session.OutboundSequenceSpaces"/>), proved through the public seam for
/// the first time — the test obligation #169 left behind when it shipped the type with no tests at
/// all, because the type is internal (ADR 0015 forbids <c>InternalsVisibleTo</c>) and the datagram
/// profile had no public <c>Send</c> to drive an allocation through. Both gaps are closed now: <see
/// cref="DatagramSessionReactor.Send"/> exists, and <see cref="WireTap"/> plus <see
/// cref="DatagramDecoder"/> can read the sequence stamped on each observed record. Every assertion
/// below reads a sequence off a <see cref="DecodedPacketRecord"/> decoded from a captured datagram —
/// never from the internal allocator itself.
/// </summary>
public class DatagramSessionOutboundSequenceSpaceTests
{
    private static readonly DeliveryMode[] _allModes =
    [
        DeliveryMode.Unreliable,
        DeliveryMode.UnreliableSequenced,
        DeliveryMode.ReliableUnordered,
        DeliveryMode.ReliableOrdered,
    ];

    [Fact]
    public void Send_OnceInEachOfTheFourModesBeforeTheFirstPump_StampsSequenceZeroOnEveryRecord()
    {
        // Arrange — retention sized for the two reliable modes' single record each; the two unreliable
        // modes are never retained regardless.
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, retentionBytes: 1200 * 4);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Act — one Send per mode, queued before any flushing Pump, then a single round flushes all
        // four as one coalesced Datagram.
        foreach (DeliveryMode mode in _allModes)
        {
            dialler.Reactor.Send(diallerPeer, mode, [1]).Should().Be(SendOutcome.Sent);
        }

        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Assert — the one datagram this round produced carries one record per mode, each stamped with
        // sequence zero: the first allocation in every one of the four Spaces is zero.
        byte[] onlyArrival = tap.PayloadsInDirection(TapDirection.DiallerToAccepter).Last();
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(onlyArrival);
        decoded.IsSuccess.Should().BeTrue(decoded.Error);

        Dictionary<DeliveryMode, uint> sequenceByMode = decoded.Datagram!.Records.ToDictionary(r => r.Mode, r => r.Sequence);
        sequenceByMode.Keys.Should().BeEquivalentTo(_allModes, "one record for each of the four Spaces was Sent");
        sequenceByMode.Values.Should().OnlyContain(sequence => sequence == 0, "every Space's first allocation is zero");
    }

    [Theory]
    [InlineData(DeliveryMode.Unreliable)]
    [InlineData(DeliveryMode.UnreliableSequenced)]
    [InlineData(DeliveryMode.ReliableUnordered)]
    [InlineData(DeliveryMode.ReliableOrdered)]
    public void Send_ThreeTimesInOneModeBeforeThePump_StampsSuccessiveRecordsWithSequencesIncreasingByOne(DeliveryMode mode)
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, retentionBytes: 1200 * 4);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Act — three Sends in the same mode, queued before any flushing Pump.
        dialler.Reactor.Send(diallerPeer, mode, [1]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, mode, [2, 2]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, mode, [3, 3, 3]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Assert — the one datagram carries three records, in Send order, with sequences 0, 1, 2.
        byte[] onlyArrival = tap.PayloadsInDirection(TapDirection.DiallerToAccepter).Last();
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(onlyArrival);
        decoded.IsSuccess.Should().BeTrue(decoded.Error);

        decoded.Datagram!.Records.Select(r => r.Sequence).Should().Equal(
            [0u, 1u, 2u], "three successive allocations in the same Space increase by exactly one each time");
    }

    [Fact]
    public void Send_AfterAdvancingOneSpaceByThreeAllocations_LeavesEveryOtherSpacesNextSequenceAtZero()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, retentionBytes: 1200 * 4);
        using DatagramSessionReactorHarness accepter = new();
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Act — advance ReliableOrdered's Space by three allocations, flushed in its own round first.
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [2]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [3]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Then Send once in each of the other three modes, in a second round.
        dialler.Reactor.Send(diallerPeer, DeliveryMode.Unreliable, [4]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.UnreliableSequenced, [5]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableUnordered, [6]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Assert — the second round's datagram carries the three untouched modes, each still at sequence
        // zero: allocating three times in ReliableOrdered's Space moved nothing else's next sequence.
        byte[] secondArrival = tap.PayloadsInDirection(TapDirection.DiallerToAccepter).Last();
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(secondArrival);
        decoded.IsSuccess.Should().BeTrue(decoded.Error);

        Dictionary<DeliveryMode, uint> sequenceByMode = decoded.Datagram!.Records.ToDictionary(r => r.Mode, r => r.Sequence);
        sequenceByMode.Keys.Should().BeEquivalentTo(
            [DeliveryMode.Unreliable, DeliveryMode.UnreliableSequenced, DeliveryMode.ReliableUnordered],
            "the second round Sent once to each of the three Spaces ReliableOrdered's advance never touched");
        sequenceByMode.Values.Should().OnlyContain(
            sequence => sequence == 0,
            "advancing ReliableOrdered's Space by three allocations must not move any other Space's next sequence");
    }

    [Fact]
    public void Send_ToASecondPeerAfterAdvancingTheFirstPeersSpace_StampsSequenceZeroOnTheSecondPeersFirstRecord()
    {
        // Arrange — two independent dialler-to-accepter dialogues, each relayed through its own tap so
        // the accepter admits two distinct Peers (the pattern DatagramSessionPerPeerDatagramTests uses,
        // since one WireTap tracks only one dialler at a time).
        using DatagramSessionReactorHarness accepter = new();
        using DatagramSessionReactorHarness dialler1 = new(bindLoopback: false, retentionBytes: 1200 * 4);
        using DatagramSessionReactorHarness dialler2 = new(bindLoopback: false, retentionBytes: 1200 * 4);
        using WireTap tap1 = new(accepter.Endpoint, dialler1.TimeProvider);
        using WireTap tap2 = new(accepter.Endpoint, dialler2.TimeProvider);

        PeerId diallerPeer1 = dialler1.Reactor.Connect(tap1.PublicEndPoint);
        PeerId diallerPeer2 = dialler2.Reactor.Connect(tap2.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler1, tap1, accepter);
        DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler2, tap2, accepter);

        // Act — Peer 1 sends three times in ReliableOrdered, advancing its own Space to next-sequence 3.
        dialler1.Reactor.Send(diallerPeer1, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Sent);
        dialler1.Reactor.Send(diallerPeer1, DeliveryMode.ReliableOrdered, [2]).Should().Be(SendOutcome.Sent);
        dialler1.Reactor.Send(diallerPeer1, DeliveryMode.ReliableOrdered, [3]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler1, tap1, accepter);

        // Peer 2, an entirely separate Peer, sends for the first time in the same mode.
        dialler2.Reactor.Send(diallerPeer2, DeliveryMode.ReliableOrdered, [9]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler2, tap2, accepter);

        // Assert — Peer 2's own first ReliableOrdered record is sequence zero, unaffected by how far
        // Peer 1's own Space has advanced: the two Peers' Space sets are independent.
        byte[] peer2Arrival = tap2.PayloadsInDirection(TapDirection.DiallerToAccepter).Last();
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(peer2Arrival);
        decoded.IsSuccess.Should().BeTrue(decoded.Error);

        decoded.Datagram!.Records.Should().ContainSingle()
            .Which.Sequence.Should().Be(0, "Peer 2's first Send in this Space is sequence zero however much Peer 1 has Sent");
    }
}
