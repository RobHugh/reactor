using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The property the four-Space decision exists for. With a
/// <c>ReliableOrdered</c> packet dropped, so that Space is stalled behind a hole, the other three modes
/// keep delivering across many Pumps — a bulk transfer and a latency-sensitive stream to the same Peer
/// are genuinely independent — and the stalled Space recovers with nothing lost once its retransmission
/// arrives.
/// </summary>
/// <remarks>
/// The second test is the counter-proof, and it is what stops the first from passing for reasons that
/// have nothing to do with the decision: it runs the same script with all four Messages Sent
/// <c>ReliableOrdered</c>, which is precisely the four Spaces collapsed into one, and the same drop then
/// stalls every one of them. Collapsing the Spaces is expressed by what the test Sends rather than by
/// reaching into the Reactor, so no <c>InternalsVisibleTo</c> and no seam for a test is needed
/// (ADR 0015).
/// </remarks>
public class DatagramSessionSpaceIndependenceTests
{
    /// <summary>
    /// Comfortably past the RTO, whose ceiling is the harness's one-second Linger, so the deadline has
    /// certainly passed however the estimator has moved.
    /// </summary>
    private static readonly TimeSpan _pastTheRetransmitDeadline = TimeSpan.FromSeconds(3);

    /// <summary>Wide enough to hold the run these tests park above an open hole; the default four fragments is not.</summary>
    private const int WideReceiveWindow = 16;

    /// <summary>Room for every reliable record these tests keep in flight at once (1200 bytes is the default fragment size).</summary>
    private const int RetentionForSeveralInFlight = 1200 * 12;

    /// <summary>How many Pumps the other three Spaces are watched across — many, not one, since one round could pass on timing alone.</summary>
    private const int RoundsOfIndependentTraffic = 10;

    /// <summary>
    /// Retention for the counter-proof, where every Message travels in the one Space and none is
    /// acknowledged while the hole stays open, so all thirty-one records are retained at once.
    /// </summary>
    private const int RetentionForOneCollapsedSpace = 1200 * 40;

    [Fact]
    public void Pump_WithAReliableOrderedPacketDropped_TheOtherThreeSpacesKeepDeliveringAndTheStalledOneRecovers()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, retentionBytes: RetentionForSeveralInFlight);
        using DatagramSessionReactorHarness receiver = new(receiveWindowFragments: WideReceiveWindow);
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        byte[] stalled = [0xFF];

        // Act — one ReliableOrdered packet is dropped, stalling that Space behind the hole it leaves.
        proxy.DropNext(RelayDirection.ClientToUpstream);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, stalled).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);

        // The other three modes are then driven across many Pumps, one Message each per round, while
        // that hole stays open. No time is advanced yet, so nothing is retransmitted and the hole stays
        // open for every one of these rounds.
        List<byte[]> unorderedDelivered = [];
        List<byte[]> sequencedDelivered = [];
        List<byte[]> unreliableDelivered = [];
        List<byte[]> orderedDeliveredWhileStalled = [];

        for (int round = 0; round < RoundsOfIndependentTraffic; round++)
        {
            byte[] unordered = [1, (byte)round];
            byte[] sequenced = [2, (byte)round];
            byte[] unreliable = [3, (byte)round];

            sender.Reactor.Send(peer, DeliveryMode.ReliableUnordered, unordered).Should().Be(SendOutcome.Sent);
            sender.Reactor.Send(peer, DeliveryMode.UnreliableSequenced, sequenced).Should().Be(SendOutcome.Sent);
            sender.Reactor.Send(peer, DeliveryMode.Unreliable, unreliable).Should().Be(SendOutcome.Sent);
            DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);

            foreach (byte[] payload in receiver.DrainPayloads())
            {
                switch (payload[0])
                {
                    case 1:
                        unorderedDelivered.Add(payload);
                        break;
                    case 2:
                        sequencedDelivered.Add(payload);
                        break;
                    case 3:
                        unreliableDelivered.Add(payload);
                        break;
                    default:
                        orderedDeliveredWhileStalled.Add(payload);
                        break;
                }
            }

            sender.DrainAndDiscard();
        }

        // Assert — every round of the other three Spaces delivered while the fourth was stalled.
        unorderedDelivered.Should().HaveCount(
            RoundsOfIndependentTraffic, "ReliableUnordered has no ordered queue to be held by another Space's hole");
        sequencedDelivered.Should().HaveCount(
            RoundsOfIndependentTraffic, "UnreliableSequenced kept delivering its newest through the stall");
        unreliableDelivered.Should().HaveCount(
            RoundsOfIndependentTraffic, "Unreliable kept delivering through the stall");
        orderedDeliveredWhileStalled.Should().BeEmpty("the stalled Space is genuinely stalled, so the test is proving something");

        // Act — the deadline passes and the stalled Space recovers on its retransmission.
        sender.TimeProvider.Advance(_pastTheRetransmitDeadline);
        List<byte[]> recovered = [];
        for (int round = 0; round < RoundsOfIndependentTraffic; round++)
        {
            DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);
            recovered.AddRange(receiver.DrainPayloads());
            sender.DrainAndDiscard();
            PumpDeadline.Yield();
        }

        // Assert
        recovered.Should().ContainSingle("the stalled Message arrives once the hole fills, and nothing is lost")
            .Which.Should().Equal(stalled);
    }

    [Fact]
    public void Pump_WithAllFourMessagesInOneSpace_TheSameDropStallsEveryOneOfThem()
    {
        // Arrange — the counter-proof. Sending all four Messages ReliableOrdered is the four Spaces
        // collapsed into one: one sequence numbering, one receive window, one ordered queue. If that
        // collapse did not matter, the three Messages behind the drop would still deliver.
        using DatagramSessionReactorHarness sender = new(bindLoopback: false, retentionBytes: RetentionForOneCollapsedSpace);
        using DatagramSessionReactorHarness receiver = new(receiveWindowFragments: WideReceiveWindow);
        using LossyProxy proxy = new(receiver.Endpoint, sender.TimeProvider);
        PeerId peer = sender.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(sender, proxy, receiver);

        // Act — the same drop as the test above, then the same traffic, differing only in that every
        // Message now travels in the dropped Message's Space.
        proxy.DropNext(RelayDirection.ClientToUpstream);
        sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [0xFF]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);

        List<byte[]> delivered = [];
        for (int round = 0; round < RoundsOfIndependentTraffic; round++)
        {
            sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [1, (byte)round]).Should().Be(SendOutcome.Sent);
            sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [2, (byte)round]).Should().Be(SendOutcome.Sent);
            sender.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [3, (byte)round]).Should().Be(SendOutcome.Sent);
            DatagramSessionReactorHarness.PumpRoundThroughProxy(sender, proxy, receiver);
            delivered.AddRange(receiver.DrainPayloads());
            sender.DrainAndDiscard();
        }

        // Assert — the independence assertion the first test makes fails here: nothing at all delivers
        // while the one hole stays open, which is exactly what the four Spaces buy by not sharing.
        delivered.Should().BeEmpty(
            "with one Space for everything, one dropped packet stalls every Message behind it");
    }
}
