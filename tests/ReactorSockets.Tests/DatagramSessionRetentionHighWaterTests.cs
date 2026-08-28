using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.5.6 (issue #205): <see cref="PeerCounters.RetentionHighWaterBytes"/> reports the most
/// bytes one Peer ever held retained at once, summed across both of that Peer's reliable Spaces, and
/// never falls. Subtask 6.5.7 (issue #206) is asserted here too, in the one place its Slot-reuse
/// claim shows: a Slot's retention outlives whichever Peer occupies it, so a Peer admitted into a
/// Slot a previous Peer left fragments in must still start empty.
/// </summary>
/// <remarks>
/// These are <see cref="DatagramSessionReactor.Send"/>'s (issue #207) to prove, and could not be
/// written before it existed: retention had no public seam that filled it. The counter is still only
/// half-observable — nothing on any live path calls <c>Core.RetentionPool.Release</c>, so the
/// fill-release-fill cycle #205 names cannot be driven from here, the same gap
/// <see cref="DatagramSessionSendTests"/> already records. What is reachable is that the figure
/// starts at zero, rises with what is retained, sums across both reliable Spaces, and does not fall
/// once a Space is exhausted and further Sends are refused.
/// </remarks>
public class DatagramSessionRetentionHighWaterTests
{
    private const int FragmentSize = 600;

    [Fact]
    public void CountersOf_AFreshlyConnectedPeerThatHasSentNothing_ReadsZeroRetentionHighWaterBytes()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new(bindLoopback: false);
        using DatagramSessionReactorHarness remote = new();
        PeerId peer = harness.Reactor.Connect(remote.Endpoint);

        // Act
        PeerCounters counters = harness.Reactor.CountersOf(peer);

        // Assert
        counters.RetentionHighWaterBytes.Should().Be(0UL);
    }

    [Fact]
    public void CountersOf_AfterAReliableSend_ReadsTheRetainedByteLengthRatherThanTheWholeSlot()
    {
        // Arrange — the counter is denominated in the bytes actually retained, not in slots taken:
        // a three-byte Message occupies a whole fragment-sized slot but retains three bytes.
        using DatagramSessionReactorHarness harness = new(bindLoopback: false, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness remote = new();
        PeerId peer = harness.Reactor.Connect(remote.Endpoint);

        // A reliable Send is refused unless the Peer has learned the remote's announced
        // MaxMessageSize (issue #284), so the handshake must actually complete first.
        DatagramSessionReactorHarness.PumpBothUntilConnected(harness, remote);

        // Act
        harness.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [1, 2, 3]);

        // Assert
        harness.Reactor.CountersOf(peer).RetentionHighWaterBytes.Should().Be(3UL);
    }

    [Fact]
    public void CountersOf_AfterReliableSendsInBothSpaces_ReadsTheSumAcrossBothOfThatPeersSpaces()
    {
        // Arrange — the bound that refuses a Send is per Space, but the counter answers a question
        // about one Peer's memory (#43), so the two Spaces' retained bytes are summed here.
        using DatagramSessionReactorHarness harness = new(bindLoopback: false, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness remote = new();
        PeerId peer = harness.Reactor.Connect(remote.Endpoint);

        // A reliable Send is refused unless the Peer has learned the remote's announced
        // MaxMessageSize (issue #284), so the handshake must actually complete first.
        DatagramSessionReactorHarness.PumpBothUntilConnected(harness, remote);

        // Act
        harness.Reactor.Send(peer, DeliveryMode.ReliableOrdered, new byte[10]);
        harness.Reactor.Send(peer, DeliveryMode.ReliableUnordered, new byte[7]);

        // Assert
        harness.Reactor.CountersOf(peer).RetentionHighWaterBytes.Should().Be(17UL);
    }

    [Fact]
    public void CountersOf_AsSuccessiveReliableSendsRetain_RisesWithEachOneRatherThanReportingOnlyTheLast()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new(
            bindLoopback: false, fragmentSize: FragmentSize, retentionBytes: FragmentSize * 3, maxMessageSize: FragmentSize);
        using DatagramSessionReactorHarness remote = new();
        PeerId peer = harness.Reactor.Connect(remote.Endpoint);

        // A reliable Send is refused unless the Peer has learned the remote's announced
        // MaxMessageSize (issue #284), so the handshake must actually complete first.
        DatagramSessionReactorHarness.PumpBothUntilConnected(harness, remote);

        // Act
        List<ulong> readings = [];
        for (int i = 0; i < 3; i++)
        {
            harness.Reactor.Send(peer, DeliveryMode.ReliableOrdered, new byte[100]);
            readings.Add(harness.Reactor.CountersOf(peer).RetentionHighWaterBytes);
        }

        // Assert
        readings.Should().ContainInOrder(100UL, 200UL, 300UL);
    }

    [Fact]
    public void CountersOf_AfterTheSpaceIsExhaustedAndFurtherSendsAreRefused_HoldsAtThePeakRatherThanFalling()
    {
        // Arrange — two slots per reliable Space, so the third ReliableOrdered Send is refused. A
        // refusal retains nothing, and the figure is monotonic (#205), so it must neither rise for
        // the refused Send nor fall back below what was retained before it.
        using DatagramSessionReactorHarness harness = new(
            bindLoopback: false, fragmentSize: FragmentSize, retentionBytes: FragmentSize * 2, maxMessageSize: FragmentSize);
        using DatagramSessionReactorHarness remote = new();
        PeerId peer = harness.Reactor.Connect(remote.Endpoint);

        // A reliable Send is refused unless the Peer has learned the remote's announced
        // MaxMessageSize (issue #284), so the handshake must actually complete first.
        DatagramSessionReactorHarness.PumpBothUntilConnected(harness, remote);

        harness.Reactor.Send(peer, DeliveryMode.ReliableOrdered, new byte[50]);
        harness.Reactor.Send(peer, DeliveryMode.ReliableOrdered, new byte[50]);
        ulong peak = harness.Reactor.CountersOf(peer).RetentionHighWaterBytes;

        // Act
        SendOutcome refused = harness.Reactor.Send(peer, DeliveryMode.ReliableOrdered, new byte[50]);

        // Assert
        refused.Should().Be(SendOutcome.RetentionExhausted);
        harness.Reactor.CountersOf(peer).RetentionHighWaterBytes.Should().Be(peak);
    }

    [Fact]
    public void Send_ByAPeerAdmittedIntoASlotAPreviousPeerLeftRetentionIn_SucceedsBecauseTheSlotsRetentionWasReleasedWithIt()
    {
        // Arrange — capacity one, one slot per reliable Space, so the second Connect is certain to
        // reuse the first Peer's Slot and would find its single ReliableOrdered slot still occupied
        // if a Slot's retention were not cleared when a new Peer is admitted into it (#206).
        using DatagramSessionReactorHarness harness = new(
            bindLoopback: false, capacity: 1, fragmentSize: FragmentSize, retentionBytes: FragmentSize, maxMessageSize: FragmentSize);
        using DatagramSessionReactorHarness remote = new();
        PeerId first = harness.Reactor.Connect(remote.Endpoint);

        // A reliable Send is refused unless the Peer has learned the remote's announced
        // MaxMessageSize (issue #284), so each Peer here must complete a real handshake before it Sends.
        DatagramSessionReactorHarness.PumpBothUntilConnected(harness, remote);
        harness.Reactor.Send(first, DeliveryMode.ReliableOrdered, new byte[40]);
        harness.Reactor.Close(first);
        harness.PumpAndDrainAll(1);

        // Act
        PeerId second = harness.Reactor.Connect(remote.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(harness, remote);
        SendOutcome outcome = harness.Reactor.Send(second, DeliveryMode.ReliableOrdered, new byte[40]);

        // Assert
        outcome.Should().Be(SendOutcome.Sent);
    }

    [Fact]
    public void CountersOf_APeerAdmittedIntoAReusedSlot_ReadsOnlyItsOwnRetainedBytesNotThePreviousOccupants()
    {
        // Arrange — the previous occupant retained more than this one will, so a high-water figure
        // carried across the reuse would be visible as the larger of the two.
        using DatagramSessionReactorHarness harness = new(
            bindLoopback: false, capacity: 1, fragmentSize: FragmentSize, retentionBytes: FragmentSize, maxMessageSize: FragmentSize);
        using DatagramSessionReactorHarness remote = new();
        PeerId first = harness.Reactor.Connect(remote.Endpoint);

        // A reliable Send is refused unless the Peer has learned the remote's announced
        // MaxMessageSize (issue #284), so each Peer here must complete a real handshake before it Sends.
        DatagramSessionReactorHarness.PumpBothUntilConnected(harness, remote);
        harness.Reactor.Send(first, DeliveryMode.ReliableOrdered, new byte[400]);
        harness.Reactor.Close(first);
        harness.PumpAndDrainAll(1);

        // Act
        PeerId second = harness.Reactor.Connect(remote.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(harness, remote);
        harness.Reactor.Send(second, DeliveryMode.ReliableOrdered, new byte[40]);

        // Assert
        harness.Reactor.CountersOf(second).RetentionHighWaterBytes.Should().Be(40UL);
    }

    [Fact]
    public void CountersOf_APeerAdmittedIntoAReusedSlotThatHasSentNothing_ReadsZeroRatherThanThePreviousOccupantsPeak()
    {
        // Arrange
        using DatagramSessionReactorHarness harness = new(
            bindLoopback: false, capacity: 1, fragmentSize: FragmentSize, retentionBytes: FragmentSize, maxMessageSize: FragmentSize);
        using DatagramSessionReactorHarness remote = new();
        PeerId first = harness.Reactor.Connect(remote.Endpoint);

        // A reliable Send is refused unless the Peer has learned the remote's announced
        // MaxMessageSize (issue #284), so the handshake must actually complete first.
        DatagramSessionReactorHarness.PumpBothUntilConnected(harness, remote);
        harness.Reactor.Send(first, DeliveryMode.ReliableOrdered, new byte[400]);
        harness.Reactor.Close(first);
        harness.PumpAndDrainAll(1);

        // Act
        PeerId second = harness.Reactor.Connect(remote.Endpoint);

        // Assert
        harness.Reactor.CountersOf(second).RetentionHighWaterBytes.Should().Be(0UL);
    }

    [Fact]
    public void Send_ByAPeerInASlotAnotherPeerExhausted_IsUnaffectedByTheOtherSlotsOccupancy()
    {
        // Arrange — retention is per Peer per Space (#43), so filling Slot 0's ReliableOrdered Space
        // to exhaustion says nothing about Slot 1's. Distinct from
        // DatagramSessionSendTests' own two-Peer test in what it reads: the counter, not the outcome.
        using DatagramSessionReactorHarness harness = new(
            bindLoopback: false, capacity: 2, fragmentSize: FragmentSize, retentionBytes: FragmentSize, maxMessageSize: FragmentSize);
        using DatagramSessionReactorHarness firstRemote = new();
        using DatagramSessionReactorHarness secondRemote = new();
        PeerId first = harness.Reactor.Connect(firstRemote.Endpoint);
        PeerId second = harness.Reactor.Connect(secondRemote.Endpoint);

        // A reliable Send is refused unless its Peer has learned the remote's announced
        // MaxMessageSize (issue #284), so both Peers here must complete a real handshake before they Send.
        DatagramSessionReactorHarness.PumpBothUntilConnected(harness, firstRemote);
        DatagramSessionReactorHarness.PumpBothUntilConnected(harness, secondRemote);
        harness.Reactor.Send(first, DeliveryMode.ReliableOrdered, new byte[300]);
        harness.Reactor.Send(first, DeliveryMode.ReliableOrdered, new byte[300]).Should().Be(SendOutcome.RetentionExhausted);

        // Act
        harness.Reactor.Send(second, DeliveryMode.ReliableOrdered, new byte[20]);

        // Assert
        harness.Reactor.CountersOf(second).RetentionHighWaterBytes.Should().Be(20UL);
    }
}
