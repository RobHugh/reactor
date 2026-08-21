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
    public void Send_UnreliableAtHighRateWithRetentionAtItsMinimum_NeverConsumesRetention()
    {
        // Arrange — one slot per reliable Space, so a single ReliableOrdered Send would already exhaust
        // it; a long run of Unreliable and UnreliableSequenced Sends must all still succeed, because
        // neither consumes retention at all (#43).
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

        // Assert
        outcomes.Should().AllSatisfy(outcome => outcome.Should().Be(SendOutcome.Sent));
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

    [Fact]
    public void Send_AMessageWiderThanOneFragment_Throws()
    {
        // Arrange
        const int fragmentSize = 600;
        using DatagramSessionReactorHarness harness = new(
            bindLoopback: false, fragmentSize: fragmentSize, retentionBytes: fragmentSize, maxMessageSize: fragmentSize);
        using DatagramSessionReactorHarness remote = new();
        PeerId peer = harness.Reactor.Connect(remote.Endpoint);
        byte[] tooWide = new byte[fragmentSize + 1];

        // Act
        Action send = () => harness.Reactor.Send(peer, DeliveryMode.ReliableOrdered, tooWide);

        // Assert
        send.Should().Throw<ArgumentException>().WithParameterName("message");
    }
}
