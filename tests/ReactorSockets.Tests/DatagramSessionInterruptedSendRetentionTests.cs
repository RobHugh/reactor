using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #369 (parent #55): once a Peer is <see cref="SessionEventKind.PeerInterrupted"/>, reliable
/// <see cref="DatagramSessionReactor.Send"/> keeps succeeding — the same rule the stream profile applies
/// (<see cref="StreamSessionInterruptedRetentionTests"/>) — until the Peer's reliable Space has no free
/// retention slot left, at which point <c>Send</c> returns <see cref="SendOutcome.RetentionExhausted"/>
/// while the Peer stays Interrupted: closing would discard a Session that might have resumed a moment
/// later, and dropping the oldest retained Fragments would silently break reliable delivery where the
/// consumer is least able to observe it.
/// </summary>
/// <remarks>
/// Drives a real <see cref="DatagramSessionReactor"/> pair through a <see cref="LossyProxy"/> on a
/// shared <see cref="Microsoft.Extensions.Time.Testing.FakeTimeProvider"/> (mirroring
/// <see cref="DatagramSessionInterruptedPeerIdleTests"/>), interrupts the dialler by cutting both
/// relaying directions and crossing the receive-idle deadline, then Sends single-Fragment
/// <see cref="DeliveryMode.ReliableOrdered"/> Messages one at a time — Pumping between each so
/// <see cref="FrameArena"/>'s per-share-holder budget never has to hold two reliable records in the same
/// Frame — until the Space's exact retention-slot count is exhausted. Only <c>ReliableOrdered</c> is
/// exercised, not <c>ReliableUnordered</c> too: the same short <c>SendIdleInterval</c> this file needs to
/// reach the receive-idle deadline quickly also arms the Ping heartbeat
/// (<see cref="DatagramSessionReactor"/>'s send-idle Ping is always <c>ReliableUnordered</c>), which would
/// silently spend one of that Space's own retention slots before the test ever calls <c>Send</c> and
/// throw off the exact boundary this file asserts. <see cref="RetentionBytes"/> is sized from
/// <see cref="FragmentSize"/> so the boundary Send is asserted exactly, not merely "eventually".
/// </remarks>
public class DatagramSessionInterruptedSendRetentionTests
{
    private const int FragmentSize = 576;
    private const int SlotsPerSpace = 3;
    private const int RetentionBytes = FragmentSize * SlotsPerSpace;
    private const int MaxMessageSize = FragmentSize * 10;

    private const DeliveryMode Mode = DeliveryMode.ReliableOrdered;

    private static readonly TimeSpan _sendIdleInterval = TimeSpan.FromSeconds(1);

    private const int MissedHeartbeats = 3;

    /// <summary>Far longer than the deadline this file crosses, so nothing here is mistaken for a Linger expiry.</summary>
    private static readonly TimeSpan _linger = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Connects a dialler/accepter pair through a <see cref="LossyProxy"/>, then interrupts the dialler's
    /// view of its Peer: cuts both relaying directions and crosses the receive-idle deadline exactly
    /// once, asserting the single <see cref="SessionEventKind.PeerInterrupted"/> this produces.
    /// </summary>
    private static PeerId ConnectAndInterrupt(
        DatagramSessionReactorHarness dialler, LossyProxy proxy, DatagramSessionReactorHarness accepter)
    {
        PeerId peer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        proxy.StopRelayingBothDirections();

        TimeSpan deadline = _sendIdleInterval * MissedHeartbeats;
        DatagramSessionReactorHarness.SettleThenAdvance(dialler, deadline);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = dialler.PumpAndDrainAll(1);
        events.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer.Equals(peer));

        return peer;
    }

    /// <summary>Sends exactly <see cref="SlotsPerSpace"/> single-Fragment Messages, Pumping and draining between each, asserting each returns <see cref="SendOutcome.Sent"/>.</summary>
    private static void FillTheSpaceExactly(DatagramSessionReactorHarness dialler, PeerId peer)
    {
        for (int i = 0; i < SlotsPerSpace; i++)
        {
            dialler.Reactor.Send(peer, Mode, new byte[FragmentSize]).Should().Be(SendOutcome.Sent);
            dialler.Reactor.Pump();
            dialler.DrainToList();
        }
    }

    // =========================================================================================
    // Send keeps returning Sent, exactly SlotsPerSpace times, while the Space still has room
    // =========================================================================================

    [Fact]
    public void Send_OnInterruptedPeerWithRoomInTheSpace_ReturnsSentUntilExactlyTheSlotCount()
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes,
            fragmentSize: FragmentSize, sendIdleInterval: _sendIdleInterval, linger: _linger,
            missedHeartbeats: MissedHeartbeats);
        using DatagramSessionReactorHarness accepter = new(
            maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize,
            sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId peer = ConnectAndInterrupt(dialler, proxy, accepter);

        // Act — one single-Fragment reliable Message per Send, Pumping between each so the arena never
        // has to hold two reliable records in the same Frame.
        List<SendOutcome> outcomes = [];
        for (int i = 0; i < SlotsPerSpace; i++)
        {
            outcomes.Add(dialler.Reactor.Send(peer, Mode, new byte[FragmentSize]));
            dialler.Reactor.Pump();
            dialler.DrainToList();
        }

        // Assert — every Send fits: the Space's exact slot count, no more, no fewer.
        outcomes.Should().AllSatisfy(o => o.Should().Be(SendOutcome.Sent));
    }

    // =========================================================================================
    // The (SlotsPerSpace + 1)-th Send returns RetentionExhausted, exactly at the boundary
    // =========================================================================================

    [Fact]
    public void Send_OnInterruptedPeerOnceTheSpaceIsFull_ReturnsRetentionExhaustedAtTheExactBoundary()
    {
        // Arrange — fill the Space exactly.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes,
            fragmentSize: FragmentSize, sendIdleInterval: _sendIdleInterval, linger: _linger,
            missedHeartbeats: MissedHeartbeats);
        using DatagramSessionReactorHarness accepter = new(
            maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize,
            sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId peer = ConnectAndInterrupt(dialler, proxy, accepter);
        FillTheSpaceExactly(dialler, peer);

        // Act — the Space has no free slot left.
        SendOutcome outcome = dialler.Reactor.Send(peer, Mode, new byte[FragmentSize]);

        // Assert
        outcome.Should().Be(SendOutcome.RetentionExhausted, "the Space's free-slot count is exactly SlotsPerSpace, already spent");
    }

    // =========================================================================================
    // The Peer stays Interrupted: no PeerClosed after the Space fills, and a further Send still
    // returns RetentionExhausted rather than Closed
    // =========================================================================================

    [Fact]
    public void Send_RepeatedlyAgainstAFullSpaceOnAnInterruptedPeer_KeepsReturningRetentionExhaustedWithoutClosing()
    {
        // Arrange — fill the Space exactly.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes,
            fragmentSize: FragmentSize, sendIdleInterval: _sendIdleInterval, linger: _linger,
            missedHeartbeats: MissedHeartbeats);
        using DatagramSessionReactorHarness accepter = new(
            maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize,
            sendIdleInterval: _sendIdleInterval, linger: _linger, missedHeartbeats: MissedHeartbeats);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId peer = ConnectAndInterrupt(dialler, proxy, accepter);
        FillTheSpaceExactly(dialler, peer);

        // Act — several further Sends against the same full Space, each Pumped and drained in turn so a
        // PeerClosed hiding behind an undrained Frame cannot escape this assertion (Pump clears
        // undrained pending Events at its start).
        List<SendOutcome> outcomes = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 5; i++)
        {
            outcomes.Add(dialler.Reactor.Send(peer, Mode, new byte[FragmentSize]));
            dialler.Reactor.Pump();
            drained.AddRange(dialler.DrainToList());
        }

        // Assert — every attempt refused the same way, and the Peer never closed.
        outcomes.Should().AllSatisfy(o => o.Should().Be(SendOutcome.RetentionExhausted));
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);

        // Assert — RetentionExhausted is distinct from Closed: a later Send is refused for the same
        // reason as before, never reclassified once the Peer has sat Interrupted for a while.
        SendOutcome laterOutcome = dialler.Reactor.Send(peer, Mode, new byte[FragmentSize]);
        laterOutcome.Should().Be(SendOutcome.RetentionExhausted);
        laterOutcome.Should().NotBe(SendOutcome.Closed);
    }
}
