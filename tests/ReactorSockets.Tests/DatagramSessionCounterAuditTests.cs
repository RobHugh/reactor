using FluentAssertions;
using Xunit;
using Harness = ReactorSockets.Tests.DatagramSessionReactorHarness;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #61: the Counter surface audited on the datagram Session profile. Every field rises where
/// expected and nowhere else, <see cref="ReactorCounters.AdmissionDeferrals"/> stays at zero for a
/// whole exchange, a declined Hello is an exact <see cref="ReactorCounters.AdmissionDiscards"/>, a
/// spent read share is counted on the Peer, and a snapshot is a value.
/// </summary>
/// <remarks>
/// "Moved" is read by reflection (<see cref="CounterFields"/>), so a field added later and moved by
/// this traffic fails these tests until someone decides it should move here. A Resume lowering no
/// field is <see cref="DatagramSessionResumeCountersTests"/>. The packet figures this exchange leaves
/// alone are each driven by <see cref="DatagramSessionCounterExchangeTests"/>.
/// </remarks>
public class DatagramSessionCounterAuditTests
{
    [Fact]
    public void Pump_OneMessageEachWay_MovesTheDatagramFiguresAndTheArenaHighWaterAtReactorScopeOnly()
    {
        // Arrange — the Reactor baseline is taken before the dial, so the handshake is inside the
        // window: no admission figure and no unattributed-datagram figure moves at any point.
        using Harness accepter = new();
        using Harness dialler = new(bindLoopback: false);
        ReactorCounters before = accepter.Reactor.Counters;
        (PeerId diallerPeer, PeerId accepterPeer) = Connect(dialler, accepter);

        // Act
        ExchangeOneMessageEachWay(dialler, diallerPeer, accepter, accepterPeer);

        // Assert
        CounterFields.Moved(before, accepter.Reactor.Counters).Should().BeEquivalentTo(
            nameof(ReactorCounters.DatagramsSent),
            nameof(ReactorCounters.DatagramsReceived),
            nameof(ReactorCounters.ArenaHighWaterBytes));
        accepter.Reactor.Counters.ArenaHighWaterBytes.Should().Be(40, "one 40-byte Message was the most any Frame delivered");
    }

    [Fact]
    public void Pump_OneMessageEachWay_MovesThePeersTrafficFiguresAndNoLossOrRepairFigure()
    {
        // Arrange
        using Harness accepter = new();
        using Harness dialler = new(bindLoopback: false);
        (PeerId diallerPeer, PeerId accepterPeer) = Connect(dialler, accepter);
        PeerCounters before = accepter.Reactor.CountersOf(accepterPeer);

        // Act
        ExchangeOneMessageEachWay(dialler, diallerPeer, accepter, accepterPeer);

        // Assert
        CounterFields.Moved(before, accepter.Reactor.CountersOf(accepterPeer)).Should().BeEquivalentTo(
            nameof(PeerCounters.MessagesReceived),
            nameof(PeerCounters.RetentionHighWaterBytes),
            nameof(PeerCounters.BytesSent),
            nameof(PeerCounters.BytesReceived),
            nameof(PeerCounters.SocketWrites));
    }

    [Fact]
    public void Pump_TwoHellosPastCapacity_CountsTwoDiscardsAndNeverADeferral()
    {
        // Arrange — a datagram Session has nowhere to leave a declined Hello waiting, so the work is
        // let go. The unit is pieces of work, so two Hellos read two, where a Deferral would read one.
        // AdmissionDeferrals must never move on this profile.
        using Harness accepter = new(capacity: 1);
        using Harness first = new(bindLoopback: false);
        using Harness second = new(bindLoopback: false);
        using Harness third = new(bindLoopback: false);
        Connect(first, accepter);
        ReactorCounters before = accepter.Reactor.Counters;

        // Act
        second.Reactor.Connect(accepter.Endpoint);
        third.Reactor.Connect(accepter.Endpoint);
        Harness.PumpBothUntil(second, accepter, () =>
        {
            third.Reactor.Pump();
            return accepter.Reactor.Counters.AdmissionDiscards - before.AdmissionDiscards >= 2;
        });

        // Assert
        ReactorCounters after = accepter.Reactor.Counters;
        (after.AdmissionDiscards - before.AdmissionDiscards).Should().Be(2);
        after.AdmissionDeferrals.Should().Be(0);
    }

    [Fact]
    public void Pump_ThreeMessagesInOneFramePastThePeersReadShare_CountsOneReadShareExhaustion()
    {
        // Arrange — capacity 2 and a 4096-byte arena give a 2048-byte read share. Two 1000-byte
        // Messages fit it; the third arrives in the same Frame and finds the share spent.
        using Harness accepter = new(capacity: 2, arenaSize: 4096);
        using Harness dialler = new(bindLoopback: false);
        (PeerId diallerPeer, PeerId accepterPeer) = Connect(dialler, accepter);
        for (int i = 0; i < 3; i++)
        {
            dialler.Reactor.Send(diallerPeer, DeliveryMode.Unreliable, new byte[1000]).Should().Be(SendOutcome.Sent);
        }

        // Act
        dialler.Reactor.Pump();
        accepter.PumpUntil(() => accepter.Reactor.CountersOf(accepterPeer).ReadShareExhaustions > 0);

        // Assert — unlike the stream profile, nothing is kept for a retry: the refused Unreliable
        // Message is gone.
        accepter.PumpAndDrainAll(3);
        accepter.Reactor.CountersOf(accepterPeer).ReadShareExhaustions.Should().Be(1);
        accepter.Reactor.CountersOf(accepterPeer).MessagesReceived.Should().Be(2);
        accepter.Reactor.Counters.ArenaHighWaterBytes.Should().Be(2000, "two 1000-byte Messages filled the share in one Frame");
    }

    [Fact]
    public void Counters_SnapshotsTakenBeforeMoreTraffic_AreValuesTheReactorDoesNotChange()
    {
        // Arrange
        using Harness accepter = new();
        using Harness dialler = new(bindLoopback: false);
        (PeerId diallerPeer, PeerId accepterPeer) = Connect(dialler, accepter);
        ReactorCounters reactorSnapshot = accepter.Reactor.Counters;
        PeerCounters peerSnapshot = accepter.Reactor.CountersOf(accepterPeer);
        IReadOnlyDictionary<string, ulong> reactorValues = CounterFields.Read(reactorSnapshot);
        IReadOnlyDictionary<string, ulong> peerValues = CounterFields.Read(peerSnapshot);

        // Act
        ExchangeOneMessageEachWay(dialler, diallerPeer, accepter, accepterPeer);

        // Assert — the Reactor moved, and the snapshots taken before it did not.
        accepter.Reactor.Counters.Should().NotBe(reactorSnapshot);
        accepter.Reactor.CountersOf(accepterPeer).Should().NotBe(peerSnapshot);
        CounterFields.Read(reactorSnapshot).Should().Equal(reactorValues);
        CounterFields.Read(peerSnapshot).Should().Equal(peerValues);
    }

    /// <summary>Dials <paramref name="accepter"/> from <paramref name="dialler"/> and returns each end's Peer once both are connected.</summary>
    private static (PeerId DiallerPeer, PeerId AccepterPeer) Connect(Harness dialler, Harness accepter)
    {
        PeerId diallerPeer = dialler.Reactor.Connect(accepter.Endpoint);
        (_, List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> accepterEvents) =
            Harness.PumpBothUntilConnected(dialler, accepter);
        PeerId accepterPeer = accepterEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;
        return (diallerPeer, accepterPeer);
    }

    /// <summary>Sends a 40-byte Message from the dialler and a 24-byte one back, and pumps until both arrive.</summary>
    private static void ExchangeOneMessageEachWay(Harness dialler, PeerId diallerPeer, Harness accepter, PeerId accepterPeer)
    {
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, new byte[40]).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(accepterPeer, DeliveryMode.ReliableOrdered, new byte[24]).Should().Be(SendOutcome.Sent);
        Harness.PumpBothUntil(dialler, accepter, () =>
            accepter.Reactor.CountersOf(accepterPeer).MessagesReceived == 1
            && dialler.Reactor.CountersOf(diallerPeer).MessagesReceived == 1);
    }
}
