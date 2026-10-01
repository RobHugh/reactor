using FluentAssertions;
using Xunit;
using Harness = ReactorSockets.Tests.StreamSessionReactorHarness;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #61: the Counter surface audited on the stream Session profile. Every field rises where
/// expected and nowhere else, <see cref="ReactorCounters.AdmissionDiscards"/> stays at zero for a whole
/// exchange, <see cref="ReactorCounters.AdmissionDeferrals"/> counts declinings rather than
/// connections, a Resume lowers nothing, and a snapshot is a value.
/// </summary>
/// <remarks>
/// "Moved" is read by reflection (<see cref="CounterFields"/>), so a field added later and moved by
/// this traffic fails these tests until someone decides it should move here.
/// </remarks>
public class StreamSessionCounterAuditTests
{
    [Fact]
    public void Pump_OneMessageEachWay_MovesOnlyTheArenaHighWaterAtReactorScope()
    {
        // Arrange — the Reactor baseline is taken before the dial, so the handshake is inside the
        // window: no datagram figure and no admission figure moves at any point of a stream exchange.
        using Harness accepter = new();
        using Harness dialler = new(bindLoopback: false);
        ReactorCounters before = accepter.Reactor.Counters;
        (PeerId diallerPeer, PeerId accepterPeer) = Connect(dialler, accepter);

        // Act
        ExchangeOneMessageEachWay(dialler, diallerPeer, accepter, accepterPeer);

        // Assert
        CounterFields.Moved(before, accepter.Reactor.Counters)
            .Should().BeEquivalentTo(nameof(ReactorCounters.ArenaHighWaterBytes));
        accepter.Reactor.Counters.ArenaHighWaterBytes.Should().Be(40, "one 40-byte Message was the most any Frame delivered");
    }

    [Fact]
    public void Pump_OneMessageEachWay_MovesThePeersTrafficFiguresAndNoPacketFigure()
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
    public void CountersOf_AfterAnExchangeSettles_EachEndReceivedExactlyTheBytesTheOtherSent()
    {
        // Arrange — the byte figures count every byte on the Peer's connection, handshake records
        // included, so once nothing is in flight what one end wrote is what the other end read.
        using Harness accepter = new();
        using Harness dialler = new(bindLoopback: false);
        (PeerId diallerPeer, PeerId accepterPeer) = Connect(dialler, accepter);

        // Act
        ExchangeOneMessageEachWay(dialler, diallerPeer, accepter, accepterPeer);
        Harness.PumpBothUntil(dialler, accepter, () =>
            accepter.Reactor.CountersOf(accepterPeer).BytesReceived == dialler.Reactor.CountersOf(diallerPeer).BytesSent
            && dialler.Reactor.CountersOf(diallerPeer).BytesReceived == accepter.Reactor.CountersOf(accepterPeer).BytesSent);

        // Assert
        PeerCounters accepterCounters = accepter.Reactor.CountersOf(accepterPeer);
        PeerCounters diallerCounters = dialler.Reactor.CountersOf(diallerPeer);
        accepterCounters.BytesReceived.Should().Be(diallerCounters.BytesSent)
            .And.BeGreaterThan(StreamSessionTestRecords.HandshakeRecordSize + 40UL);
        diallerCounters.BytesReceived.Should().Be(accepterCounters.BytesSent)
            .And.BeGreaterThan(StreamSessionTestRecords.HandshakeRecordSize + 24UL);
    }

    [Fact]
    public void Pump_ThreeConnectionsWaitingBehindOneEndpoint_CountsOneDeferralAndNoDiscard()
    {
        // Arrange — the unit is declinings, not connections: one endpoint declines its whole backlog
        // with one declining, so three waiting connections must still read one.
        using Harness accepter = new(admissionCap: 1);
        using Harness.RawStreamPeer first = new(accepter.Endpoint);
        using Harness.RawStreamPeer second = new(accepter.Endpoint);
        using Harness.RawStreamPeer third = new(accepter.Endpoint);
        ReactorCounters before = accepter.Reactor.Counters;

        // Act
        accepter.PumpUntil(() => accepter.Reactor.Counters.AdmissionDeferrals > before.AdmissionDeferrals);

        // Assert
        ReactorCounters after = accepter.Reactor.Counters;
        (after.AdmissionDeferrals - before.AdmissionDeferrals).Should().Be(1);
        CounterFields.Moved(before, after).Should().BeEquivalentTo(nameof(ReactorCounters.AdmissionDeferrals));
    }

    [Fact]
    public void Pump_ThreeMessagesInOneReadPastThePeersReadShare_CountsOneReadShareExhaustion()
    {
        // Arrange — capacity 4 and a 200-byte arena give a 50-byte read share. Two 20-byte Messages
        // fit it; the third is held back to the next Pump.
        using Harness accepter = new(
            capacity: 4, maxMessageSize: 64, retentionBytes: 68, receiveBufferSize: 100, arenaSize: 200);
        Harness.HeldPeer held = accepter.EstablishOnePeer();
        using Harness.RawStreamPeer counterparty = held.Counterparty;
        byte[] frame = StreamSessionTestRecords.BuildFrame(StreamSessionTestRecords.PayloadOf(20, 1));

        // Act
        counterparty.Send([.. frame, .. frame, .. frame]);
        accepter.PumpUntil(() => accepter.Reactor.CountersOf(held.Peer).MessagesReceived == 3);

        // Assert
        accepter.Reactor.CountersOf(held.Peer).ReadShareExhaustions.Should().Be(1);
        accepter.Reactor.Counters.ArenaHighWaterBytes.Should().Be(40, "two 20-byte Messages filled the share in one Frame");
    }

    [Fact]
    public void CountersOf_AcrossAResume_LowersNoFieldAndChargesTheResumeRecordToThePeer()
    {
        // Arrange
        using Harness accepter = new(capacity: 1);
        Harness.HeldPeer held = accepter.EstablishOnePeer();
        held.Counterparty.Send(StreamSessionTestRecords.BuildFrame(StreamSessionTestRecords.PayloadOf(8, 1)));
        accepter.Reactor.Send(held.Peer, StreamSessionTestRecords.PayloadOf(8, 2)).Should().Be(SendOutcome.Sent);
        accepter.PumpUntil(() => accepter.Reactor.CountersOf(held.Peer).MessagesReceived == 1);
        PeerCounters before = accepter.Reactor.CountersOf(held.Peer);
        accepter.InterruptHeldPeer(held);
        held.Counterparty.Dispose();

        // Act
        using Harness.RawStreamPeer resumed = new(accepter.Endpoint);
        resumed.Send(StreamSessionTestRecords.BuildResume(held.ResumeSecret, deliveryCount: 1));
        accepter.PumpUntil(() => accepter.Reactor.CountersOf(held.Peer).Resumes > before.Resumes);

        // Assert
        PeerCounters after = accepter.Reactor.CountersOf(held.Peer);
        CounterFields.Read(before).Where(field => field.Value > 0).Select(field => field.Key).Should().Contain(
            [nameof(PeerCounters.BytesSent), nameof(PeerCounters.BytesReceived), nameof(PeerCounters.SocketWrites),
                nameof(PeerCounters.MessagesReceived), nameof(PeerCounters.RetentionHighWaterBytes)],
            "traffic came first, so the Resume has history to lose");
        CounterFields.Fell(before, after).Should().BeEmpty();
        (after.Resumes - before.Resumes).Should().Be(1);
        (after.BytesReceived - before.BytesReceived).Should().BeGreaterThanOrEqualTo(
            (ulong)StreamSessionTestRecords.HandshakeRecordSize, "the matched Resume record was read from this Peer's new connection");
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
        dialler.Reactor.Send(diallerPeer, StreamSessionTestRecords.PayloadOf(40, 1)).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(accepterPeer, StreamSessionTestRecords.PayloadOf(24, 2)).Should().Be(SendOutcome.Sent);
        Harness.PumpBothUntil(dialler, accepter, () =>
            accepter.Reactor.CountersOf(accepterPeer).MessagesReceived == 1
            && dialler.Reactor.CountersOf(diallerPeer).MessagesReceived == 1);
    }
}
