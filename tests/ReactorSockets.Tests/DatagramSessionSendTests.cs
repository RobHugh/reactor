using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.5.8 (issue #207): <see cref="DatagramSessionReactor.Send"/> resolves the Peer, routes
/// the chosen <see cref="DeliveryMode"/> to its own Space, retains — or does not — and reports the
/// outcome. Nothing here reaches the wire: packing a record, fragmenting a Message and flushing a
/// Peer's coalesced Datagram are #44's and #45's, so every assertion below is about the outcome
/// <see cref="DatagramSessionReactor.Send"/> reports and the retention state behind it, never about a
/// Datagram actually observed on a socket.
/// </summary>
/// <remarks>
/// An "acks release retention" recovery scenario is not asserted here: nothing on any live path calls
/// <c>Core.RetentionPool.Release</c> yet, the same gap
/// <see cref="DatagramSessionAckBlockTests"/> and <see cref="RetransmissionExpirySweepTests"/> already
/// document for this profile's outbound send — an ack genuinely releasing a slot needs a record to
/// have actually reached the wire and been acknowledged, which does not happen until #45's coalescing
/// flush exists. What is reachable through the public seam, and asserted below, is that a full
/// Space's outcome is <see cref="SendOutcome.RetentionExhausted"/> rather than an exception or a silent
/// drop, and that the other reliable Space and both unreliable modes are entirely unaffected by it.
/// </remarks>
public class DatagramSessionSendTests
{
    [Theory]
    [InlineData(DeliveryMode.Unreliable)]
    [InlineData(DeliveryMode.UnreliableSequenced)]
    [InlineData(DeliveryMode.ReliableUnordered)]
    [InlineData(DeliveryMode.ReliableOrdered)]
    public void Send_ToAFreshlyDialledPeerUnderEveryMode_ReturnsSent(DeliveryMode mode)
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new(bindLoopback: false);
        using DatagramSessionReactorHarness remote = new();
        PeerId peer = harness.Reactor.Connect(remote.Endpoint);

        // Act
        SendOutcome outcome = harness.Reactor.Send(peer, mode, [1, 2, 3]);

        // Assert
        outcome.Should().Be(SendOutcome.Sent);
    }

    [Fact]
    public void Send_ReliableOrderedUntilItsSpaceIsFull_ReturnsRetentionExhaustedAndLeavesReliableUnorderedUnaffected()
    {
        // Arrange — two slots per reliable Space (RetentionBytes == 2 fragments), so the third
        // ReliableOrdered Send finds its own Space full while ReliableUnordered's is untouched. This is
        // #43's own interference proof, exercised through Send rather than the pool directly.
        const int fragmentSize = 600;
        using DatagramSessionReactorHarness harness = new(
            bindLoopback: false, fragmentSize: fragmentSize, retentionBytes: fragmentSize * 2, maxMessageSize: fragmentSize);
        using DatagramSessionReactorHarness remote = new();
        PeerId peer = harness.Reactor.Connect(remote.Endpoint);
        byte[] message = [1, 2, 3];

        // Act
        SendOutcome first = harness.Reactor.Send(peer, DeliveryMode.ReliableOrdered, message);
        SendOutcome second = harness.Reactor.Send(peer, DeliveryMode.ReliableOrdered, message);
        SendOutcome third = harness.Reactor.Send(peer, DeliveryMode.ReliableOrdered, message);
        SendOutcome unorderedStillWorks = harness.Reactor.Send(peer, DeliveryMode.ReliableUnordered, message);

        // Assert
        first.Should().Be(SendOutcome.Sent);
        second.Should().Be(SendOutcome.Sent);
        third.Should().Be(SendOutcome.RetentionExhausted);
        unorderedStillWorks.Should().Be(SendOutcome.Sent);
    }

    [Fact]
    public void Send_ReliableUnorderedUntilItsSpaceIsFull_ReturnsRetentionExhaustedAndLeavesReliableOrderedUnaffected()
    {
        // Arrange — the reverse of the test above: exhausting ReliableUnordered's own Space must leave
        // ReliableOrdered's untouched, proving #43's interference guard holds in both directions.
        const int fragmentSize = 600;
        using DatagramSessionReactorHarness harness = new(
            bindLoopback: false, fragmentSize: fragmentSize, retentionBytes: fragmentSize * 2, maxMessageSize: fragmentSize);
        using DatagramSessionReactorHarness remote = new();
        PeerId peer = harness.Reactor.Connect(remote.Endpoint);
        byte[] message = [1, 2, 3];

        // Act
        SendOutcome first = harness.Reactor.Send(peer, DeliveryMode.ReliableUnordered, message);
        SendOutcome second = harness.Reactor.Send(peer, DeliveryMode.ReliableUnordered, message);
        SendOutcome third = harness.Reactor.Send(peer, DeliveryMode.ReliableUnordered, message);
        SendOutcome orderedStillWorks = harness.Reactor.Send(peer, DeliveryMode.ReliableOrdered, message);

        // Assert
        first.Should().Be(SendOutcome.Sent);
        second.Should().Be(SendOutcome.Sent);
        third.Should().Be(SendOutcome.RetentionExhausted);
        orderedStillWorks.Should().Be(SendOutcome.Sent);
    }

    [Fact]
    public void Send_OnePeerExhaustedInReliableOrdered_LeavesAnotherPeersSendsUnaffected()
    {
        // Arrange — one slot per reliable Space, so a Peer's second ReliableOrdered Send already finds
        // its own Space full. Retention is per Peer per Space (#43), so a second, distinct Peer must be
        // entirely unaffected by the first Peer's exhaustion.
        const int fragmentSize = 600;
        using DatagramSessionReactorHarness harness = new(
            bindLoopback: false, fragmentSize: fragmentSize, retentionBytes: fragmentSize, maxMessageSize: fragmentSize);
        using DatagramSessionReactorHarness firstRemote = new();
        using DatagramSessionReactorHarness secondRemote = new();
        PeerId firstPeer = harness.Reactor.Connect(firstRemote.Endpoint);
        PeerId secondPeer = harness.Reactor.Connect(secondRemote.Endpoint);
        byte[] message = [1, 2, 3];

        // Act
        SendOutcome firstPeerFirstSend = harness.Reactor.Send(firstPeer, DeliveryMode.ReliableOrdered, message);
        SendOutcome firstPeerExhausted = harness.Reactor.Send(firstPeer, DeliveryMode.ReliableOrdered, message);
        SendOutcome secondPeerSend = harness.Reactor.Send(secondPeer, DeliveryMode.ReliableOrdered, message);

        // Assert
        firstPeerFirstSend.Should().Be(SendOutcome.Sent);
        firstPeerExhausted.Should().Be(SendOutcome.RetentionExhausted);
        secondPeerSend.Should().Be(SendOutcome.Sent);
    }

    [Fact]
    public void Send_UnreliableAtHighRateWithRetentionAtItsMinimum_NeverConsumesRetention()
    {
        // Arrange — one slot per reliable Space, so a single ReliableOrdered Send would already exhaust
        // it; a long run of Unreliable and UnreliableSequenced Sends must all still succeed, because
        // neither consumes retention at all (#43). RetentionHighWaterBytes is monotonic and never falls,
        // so it staying at zero after this run also certifies retained bytes never rose above zero at
        // any point during it.
        const int fragmentSize = 600;
        using DatagramSessionReactorHarness harness = new(
            bindLoopback: false, fragmentSize: fragmentSize, retentionBytes: fragmentSize, maxMessageSize: fragmentSize);
        using DatagramSessionReactorHarness remote = new();
        PeerId peer = harness.Reactor.Connect(remote.Endpoint);
        byte[] message = [9, 9, 9];

        // Act
        List<SendOutcome> outcomes = [];
        for (int i = 0; i < 100; i++)
        {
            outcomes.Add(harness.Reactor.Send(peer, DeliveryMode.Unreliable, message));
            outcomes.Add(harness.Reactor.Send(peer, DeliveryMode.UnreliableSequenced, message));
        }

        PeerCounters counters = harness.Reactor.CountersOf(peer);

        // Assert
        outcomes.Should().AllSatisfy(outcome => outcome.Should().Be(SendOutcome.Sent));
        counters.RetentionHighWaterBytes.Should().Be(0UL, "neither unreliable mode ever consumes retention (#43)");
    }

    [Fact]
    public void Send_ToAPeerIdThisReactorNeverIssued_ReturnsClosedRatherThanThrowing()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new();

        // Act
        SendOutcome outcome = harness.Reactor.Send(
            new PeerId(Origin: 0, Index: 0, Generation: 999), DeliveryMode.ReliableOrdered, [1]);

        // Assert
        outcome.Should().Be(SendOutcome.Closed);
    }

    [Fact]
    public void Send_ToAPeerAlreadyClosed_ReturnsClosedRatherThanThrowing()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new(bindLoopback: false);
        using DatagramSessionReactorHarness remote = new();
        PeerId peer = harness.Reactor.Connect(remote.Endpoint);
        harness.Reactor.Close(peer);

        // Act
        SendOutcome outcome = harness.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [1]);

        // Assert
        outcome.Should().Be(SendOutcome.Closed);
    }

    [Theory]
    [InlineData(DeliveryMode.Unreliable)]
    [InlineData(DeliveryMode.UnreliableSequenced)]
    [InlineData(DeliveryMode.ReliableUnordered)]
    [InlineData(DeliveryMode.ReliableOrdered)]
    public void Send_AMessageWiderThanOneFragment_ReturnsMessageTooLargeInEveryMode(DeliveryMode mode)
    {
        // Arrange — subtask 6.6.2 (issue #215): a Message too wide for one Datagram is a checked
        // outcome the caller can branch on, in every one of the four modes, not an exception.
        const int fragmentSize = 600;
        using DatagramSessionReactorHarness harness = new(
            bindLoopback: false, fragmentSize: fragmentSize, retentionBytes: fragmentSize, maxMessageSize: fragmentSize);
        using DatagramSessionReactorHarness remote = new();
        PeerId peer = harness.Reactor.Connect(remote.Endpoint);
        byte[] tooWide = new byte[fragmentSize + 1];

        // Act
        SendOutcome outcome = harness.Reactor.Send(peer, mode, tooWide);

        // Assert
        outcome.Should().Be(SendOutcome.MessageTooLarge);
    }

    [Fact]
    public void Send_AMessageExactlyAtTheFragmentSizeCap_ReturnsSent()
    {
        // Arrange — the cap is "wider than one Datagram", so a Message that exactly fills one must
        // still be accepted (issue #215).
        const int fragmentSize = 600;
        using DatagramSessionReactorHarness harness = new(
            bindLoopback: false, fragmentSize: fragmentSize, retentionBytes: fragmentSize, maxMessageSize: fragmentSize);
        using DatagramSessionReactorHarness remote = new();
        PeerId peer = harness.Reactor.Connect(remote.Endpoint);
        byte[] exactlyAtCap = new byte[fragmentSize];

        // Act
        SendOutcome outcome = harness.Reactor.Send(peer, DeliveryMode.ReliableOrdered, exactlyAtCap);

        // Assert
        outcome.Should().Be(SendOutcome.Sent);
    }

    [Fact]
    public void Send_ARefusedOversizedMessage_TakesNoRetentionSlotFromItsSpace()
    {
        // Arrange — issue #215's "no retention is taken" half of the no-footprint promise. Retention
        // is directly observable through the public seam (a full Space answers RetentionExhausted, the
        // same proof #43's own interference tests already use); a rejected oversized Send must not have
        // spent the Space's one slot before the two ordinary Sends that follow it.
        const int fragmentSize = 600;
        using DatagramSessionReactorHarness harness = new(
            bindLoopback: false, fragmentSize: fragmentSize, retentionBytes: fragmentSize, maxMessageSize: fragmentSize);
        using DatagramSessionReactorHarness remote = new();
        PeerId peer = harness.Reactor.Connect(remote.Endpoint);
        byte[] tooWide = new byte[fragmentSize + 1];
        byte[] message = [1, 2, 3];

        // Act
        SendOutcome refused = harness.Reactor.Send(peer, DeliveryMode.ReliableOrdered, tooWide);
        SendOutcome first = harness.Reactor.Send(peer, DeliveryMode.ReliableOrdered, message);
        SendOutcome second = harness.Reactor.Send(peer, DeliveryMode.ReliableOrdered, message);

        // Assert
        refused.Should().Be(SendOutcome.MessageTooLarge);
        first.Should().Be(SendOutcome.Sent, "the refused Send took no retention slot, so the Space's one slot is still free");
        second.Should().Be(SendOutcome.RetentionExhausted, "the Space genuinely holds only one slot");
    }

    [Fact]
    public void Send_ARefusedOversizedMessage_TakesNoSequenceFromItsSpace()
    {
        // Arrange — issue #215's "no sequence is allocated" half, proven the only way the public seam
        // allows: no member of this Reactor ever surfaces a sequence number, so this drives the record
        // all the way to a second, connected Reactor and reads the outcome off ITS receive window
        // instead. With ReceiveWindowFragments pinned to 1, that window admits only the exact sequence
        // it is currently based at (0 for a fresh Peer) and refuses anything past it as
        // OutOfWindowPackets — the receive window never advances past the pinned base 1 slot lets it
        // hold, so a correct Send that allocates sequence 0 is silently admitted (Admitted raises no
        // counter), while a bug that let the refused Send steal sequence 0 would arrive as sequence 1
        // and be refused as out of window, moving PeerCounters.OutOfWindowPackets off zero. This is
        // #216's flush and #215's no-footprint promise proven together, because nothing shows a
        // sequence number without a Datagram genuinely leaving the wire.
        const int fragmentSize = 600;
        using DatagramSessionReactorHarness sender = new(
            bindLoopback: false,
            fragmentSize: fragmentSize,
            retentionBytes: fragmentSize,
            maxMessageSize: fragmentSize,
            receiveWindowFragments: 1);
        using DatagramSessionReactorHarness receiver = new(
            fragmentSize: fragmentSize,
            retentionBytes: fragmentSize,
            maxMessageSize: fragmentSize,
            receiveWindowFragments: 1);

        PeerId senderPeer = sender.Reactor.Connect(receiver.Endpoint);
        (var _, var receiverEvents) = DatagramSessionReactorHarness.PumpBothUntilConnected(sender, receiver);
        PeerId receiverPeer = receiverEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

        byte[] tooWide = new byte[fragmentSize + 1];
        byte[] message = [1, 2, 3];

        // Act
        SendOutcome refused = sender.Reactor.Send(senderPeer, DeliveryMode.ReliableOrdered, tooWide);
        SendOutcome accepted = sender.Reactor.Send(senderPeer, DeliveryMode.ReliableOrdered, message);
        sender.Reactor.Pump();
        receiver.PumpUntil(() => receiver.Reactor.Counters.DatagramsReceived > 0);

        PeerCounters receiverCounters = receiver.Reactor.CountersOf(receiverPeer);

        // Assert
        refused.Should().Be(SendOutcome.MessageTooLarge);
        accepted.Should().Be(SendOutcome.Sent);
        receiverCounters.OutOfWindowPackets.Should().Be(
            0UL, "the accepted Send must have carried sequence 0, the only sequence this pinned window ever admits");
    }
}
