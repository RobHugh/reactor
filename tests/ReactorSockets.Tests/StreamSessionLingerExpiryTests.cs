using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Linger, not the loss of the transport, is what closes an
/// <see cref="SessionEventKind.PeerInterrupted"/> Peer (issue #119). Every test here drives a real
/// <see cref="StreamSessionReactor"/> against a hand-built raw counterparty
/// (<see cref="LingerPeer"/>, mirroring <see cref="StreamSessionInterruptedTests"/>'s
/// <c>InterruptedPeer</c>) through the public L2 surface only (tests/CLAUDE.md, ADR 0015), driven
/// entirely by <see cref="StreamSessionReactorHarness.TimeProvider"/> — no test here sleeps to observe
/// timer behaviour. The effective Linger under test is the harness default of 1 second on both ends
/// (<see cref="StreamSessionReactorHarness"/>'s own <c>Linger</c> and <see cref="StreamSessionTestRecords.BuildHello"/>'s
/// default <c>lingerMs</c>), so the negotiated minimum (ADR 0011) is 1 second everywhere below.
/// </summary>
public class StreamSessionLingerExpiryTests
{
    private static readonly TimeSpan _effectiveLinger = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Establishes a Session, then kills the counterparty's transport abruptly (a reset, exactly as
    /// <see cref="StreamSessionInterruptedTests"/> does) and pumps until <see cref="SessionEventKind.PeerInterrupted"/>
    /// drains, leaving the accepter's Reactor holding an Interrupted Peer whose Linger has not yet
    /// started elapsing from the caller's point of view.
    /// </summary>
    private static PeerId EstablishAndInterrupt(StreamSessionReactorHarness accepter, LingerPeer peer)
    {
        PeerId id = StreamSessionTestRecords.EstablishSession(accepter, peer.Send);
        peer.Receive(StreamSessionTestRecords.HandshakeRecordSize);

        peer.ResetAbruptly();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerInterrupted); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted);
        return id;
    }

    // =========================================================================================
    // Advancing time past the effective Linger closes an Interrupted Peer, naming Linger expiry
    // =========================================================================================

    [Fact]
    public void Pump_AfterTheEffectiveLingerElapsesOnAnInterruptedPeer_ClosesItWithLingerExpired()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using LingerPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishAndInterrupt(accepter, peer);

        // Act — the full effective Linger elapses.
        accepter.TimeProvider.Advance(_effectiveLinger);
        accepter.Reactor.Pump();

        // Assert
        accepter.DrainToList().Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerClosed && e.Reason == DisconnectReason.LingerExpired
                && e.Peer == id);
    }

    // =========================================================================================
    // Not closed at just under the effective Linger, asserted at both sides of the boundary
    // =========================================================================================

    [Fact]
    public void Pump_JustUnderTheEffectiveLinger_DoesNotCloseTheInterruptedPeerAndTheFollowingMillisecondDoes()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using LingerPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishAndInterrupt(accepter, peer);

        // Act — one millisecond short of the deadline: the Peer must still be Interrupted, not closed.
        accepter.TimeProvider.Advance(_effectiveLinger - TimeSpan.FromMilliseconds(1));
        accepter.Reactor.Pump();

        // Assert
        accepter.DrainToList().Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        Action stillReadable = () => accepter.Reactor.CountersOf(id);
        stillReadable.Should().NotThrow("the Peer is still held, just short of Linger expiry");

        // Act — the last millisecond closes the deadline.
        accepter.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
        accepter.Reactor.Pump();

        // Assert
        accepter.DrainToList().Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerClosed && e.Reason == DisconnectReason.LingerExpired
                && e.Peer == id);
    }

    // =========================================================================================
    // A Close on an Interrupted Peer whose Goodbye can never reach a dead socket completes at Linger
    // expiry rather than retrying forever — the bound issue #92 recorded as missing
    // =========================================================================================

    [Fact]
    public void Close_OnAnInterruptedPeerWhoseGoodbyeCanNeverReachTheWire_CompletesAtLingerExpiryRatherThanRetryingForever()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using LingerPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishAndInterrupt(accepter, peer);

        // Act — Close sets LocalGoodbyeSent and queues the Goodbye append, but FlushRetention's
        // IsInterrupted guard means that append can never reach a socket that no longer exists.
        accepter.Reactor.Close(id);

        // A handful of Pumps well short of Linger — the retry limbo issue #92 recorded as unbounded —
        // must never close the Peer on its own.
        for (int i = 0; i < 5; i++)
        {
            accepter.Reactor.Pump();
            accepter.DrainToList().Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        }

        // Act — the effective Linger elapses.
        accepter.TimeProvider.Advance(_effectiveLinger);
        accepter.Reactor.Pump();

        // Assert — Linger expiry is what ends it, not the Close call itself, and it still names Linger
        // expiry rather than the Close the consumer originally asked for.
        accepter.DrainToList().Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerClosed && e.Reason == DisconnectReason.LingerExpired
                && e.Peer == id);
    }

    // =========================================================================================
    // Per-Peer counters remain readable up to the moment of closure, and the PeerId is dead from
    // PeerClosed onwards
    // =========================================================================================

    [Fact]
    public void CountersOf_AfterLingerExpiryClosesThePeer_ThrowsBecauseThePeerIdIsNowDead()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using LingerPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishAndInterrupt(accepter, peer);

        // Readable right up to the moment of closure.
        Action readableBeforeClosure = () => accepter.Reactor.CountersOf(id);
        readableBeforeClosure.Should().NotThrow();

        // Act
        accepter.TimeProvider.Advance(_effectiveLinger);
        accepter.Reactor.Pump();
        accepter.DrainToList().Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed);

        // Assert — the PeerId is dead from PeerClosed onwards, exactly as that event already promises.
        Action act = () => accepter.Reactor.CountersOf(id);
        act.Should().Throw<ArgumentException>();
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// whose transport can be killed abruptly. Mirrors <see cref="StreamSessionInterruptedTests"/>'s
    /// private <c>InterruptedPeer</c>, trimmed to what this file needs.
    /// </summary>
    private sealed class LingerPeer : IDisposable
    {
        private readonly Socket _socket;

        public LingerPeer(IPEndPoint destination)
        {
            _socket = new Socket(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                ReceiveTimeout = 5000,
            };
            _socket.Connect(destination);
        }

        /// <summary>Sends every byte of <paramref name="bytes"/> in a single write.</summary>
        public void Send(byte[] bytes) => _socket.Send(bytes);

        /// <summary>Blocks until exactly <paramref name="count"/> bytes have been read off the socket.</summary>
        public void Receive(int count)
        {
            byte[] buffer = new byte[count];
            int totalRead = 0;
            while (totalRead < count)
            {
                int read = _socket.Receive(buffer.AsSpan(totalRead));
                if (read == 0)
                {
                    throw new InvalidOperationException("The Reactor closed the connection before sending everything expected.");
                }

                totalRead += read;
            }
        }

        /// <summary>
        /// Kills the connection abruptly: <c>SO_LINGER</c> set to zero, then closed, which forces the
        /// OS to send a reset instead of an ordinary FIN — the same technique
        /// <see cref="StreamSessionInterruptedTests"/> uses to reach Interrupted deterministically.
        /// </summary>
        public void ResetAbruptly()
        {
            _socket.LingerState = new LingerOption(true, 0);
            _socket.Close();
        }

        public void Dispose() => _socket.Dispose();
    }
}
