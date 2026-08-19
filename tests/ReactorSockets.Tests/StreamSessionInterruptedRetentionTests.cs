using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 4.3.2 (issue #120): <see cref="StreamSessionReactor.Send(PeerId, ReadOnlySpan{byte})"/> against
/// an <see cref="SessionEventKind.PeerInterrupted"/> Peer (issue #119). ADR 0010's backpressure rule is
/// the point under test — <c>Send</c> keeps succeeding and retaining while the ring has room, and once
/// it fills, <c>Send</c> returns <see cref="SendOutcome.RetentionExhausted"/> and the Peer stays
/// Interrupted rather than closing, indefinitely and without growing memory. Every test here drives a
/// real <see cref="StreamSessionReactor"/> against a hand-built raw counterparty through the public L2
/// surface only (tests/CLAUDE.md, ADR 0015). Mirrors <see cref="StreamSessionInterruptedTests"/>'s
/// <c>InterruptedPeer</c> and <see cref="StreamSessionSendRetentionTests"/>'s exact-fit retention sizing.
/// </summary>
public class StreamSessionInterruptedRetentionTests
{
    private const int MaxMessageSize = 64;
    private const int RetentionBytes = MaxMessageSize + 4;

    private static PeerId EstablishAndInterrupt(StreamSessionReactorHarness accepter, InterruptedPeer peer)
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
    // Send succeeds while the ring has room, and the Message is retained rather than discarded
    // =========================================================================================

    [Fact]
    public void Send_OnInterruptedPeerWithRoomInTheRing_ReturnsSentAndRetainsTheMessage()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes);
        using InterruptedPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishAndInterrupt(accepter, peer);

        // Act — the ring is bounded to fit exactly one maximum-size Message, so this fills it exactly
        // and still fits.
        SendOutcome outcome = accepter.Reactor.Send(id, new byte[MaxMessageSize]);

        // Assert — retained, not discarded: the high-water mark now reflects the framed bytes.
        outcome.Should().Be(SendOutcome.Sent);
        accepter.Reactor.CountersOf(id).RetentionHighWaterBytes.Should().Be(RetentionBytes);
    }

    // =========================================================================================
    // Once the ring is full, Send returns RetentionExhausted and the Peer stays Interrupted
    // =========================================================================================

    [Fact]
    public void Send_OnInterruptedPeerOnceTheRingIsFull_ReturnsRetentionExhaustedAndThePeerStaysInterrupted()
    {
        // Arrange — the first Send fills the ring exactly.
        using StreamSessionReactorHarness accepter = new(maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes);
        using InterruptedPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishAndInterrupt(accepter, peer);
        accepter.Reactor.Send(id, new byte[MaxMessageSize]).Should().Be(SendOutcome.Sent);

        // Act — a second Send, of any size, has nowhere left to go.
        SendOutcome outcome = accepter.Reactor.Send(id, [1]);

        // Assert — the refusal is RetentionExhausted, and draining events after a further Pump finds no
        // PeerClosed: the full ring never closes the Peer (ADR 0010 explicitly rejects that).
        outcome.Should().Be(SendOutcome.RetentionExhausted);
        accepter.Reactor.Pump();
        accepter.DrainToList().Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    // =========================================================================================
    // Continuing to Send against a full ring keeps refusing indefinitely, never closes the Peer,
    // and never grows the ring
    // =========================================================================================

    [Fact]
    public void Send_RepeatedlyAgainstAFullRingOnAnInterruptedPeer_KeepsReturningRetentionExhaustedWithoutClosingOrGrowing()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes);
        using InterruptedPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishAndInterrupt(accepter, peer);
        accepter.Reactor.Send(id, new byte[MaxMessageSize]).Should().Be(SendOutcome.Sent);
        ulong highWaterAfterFill = accepter.Reactor.CountersOf(id).RetentionHighWaterBytes;

        // Act — five further Sends against the same full ring, each pumped in turn.
        List<SendOutcome> outcomes = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 5; i++)
        {
            outcomes.Add(accepter.Reactor.Send(id, [(byte)i]));
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert — every attempt refused the same way, the Peer never closed, and the high-water mark
        // — the ring's memory footprint — never moved, so nothing grew to accommodate the pressure.
        outcomes.Should().AllSatisfy(o => o.Should().Be(SendOutcome.RetentionExhausted));
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        accepter.Reactor.CountersOf(id).RetentionHighWaterBytes.Should().Be(highWaterAfterFill);
    }

    // =========================================================================================
    // The retention high-water counter reflects the interruption's fill
    // =========================================================================================

    [Fact]
    public void CountersOf_AfterAnInterruptedPeersRingFills_RetentionHighWaterBytesReflectsTheFill()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes);
        using InterruptedPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishAndInterrupt(accepter, peer);

        // Act
        accepter.Reactor.Send(id, new byte[MaxMessageSize]).Should().Be(SendOutcome.Sent);

        // Assert — the framed Message is MaxMessageSize plus its 4-byte length prefix, exactly this
        // ring's bound.
        accepter.Reactor.CountersOf(id).RetentionHighWaterBytes.Should().Be(RetentionBytes);
    }

    // =========================================================================================
    // RetentionExhausted while Interrupted is the same enum member a live Peer's full ring returns —
    // distinguishable from Closed, never conflated with it
    // =========================================================================================

    [Fact]
    public void Send_OnInterruptedPeerOnceTheRingIsFull_IsDistinctFromTheClosedOutcomeAGoodbyeProduces()
    {
        // Arrange — one Peer interrupted with a full ring, a second Peer closed by its own Goodbye.
        using StreamSessionReactorHarness accepter = new(maxMessageSize: MaxMessageSize, retentionBytes: RetentionBytes);
        using InterruptedPeer interruptedPeer = new(accepter.Endpoint);
        PeerId interruptedId = EstablishAndInterrupt(accepter, interruptedPeer);
        accepter.Reactor.Send(interruptedId, new byte[MaxMessageSize]).Should().Be(SendOutcome.Sent);

        using InterruptedPeer closedPeer = new(accepter.Endpoint);
        PeerId closedId = StreamSessionTestRecords.EstablishSession(accepter, closedPeer.Send);
        closedPeer.Receive(StreamSessionTestRecords.HandshakeRecordSize);
        accepter.Reactor.Close(closedId);

        // Act
        SendOutcome interruptedOutcome = accepter.Reactor.Send(interruptedId, [1]);
        SendOutcome closedOutcome = accepter.Reactor.Send(closedId, [1]);

        // Assert — a full ring on an Interrupted Peer and a Goodbye already sent are refused for
        // different reasons, and a consumer can tell them apart at the return value alone.
        interruptedOutcome.Should().Be(SendOutcome.RetentionExhausted);
        closedOutcome.Should().Be(SendOutcome.Closed);
        interruptedOutcome.Should().NotBe(closedOutcome);
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// whose transport can be killed abruptly. Mirrors <see cref="StreamSessionInterruptedTests"/>'s
    /// private <c>InterruptedPeer</c>, trimmed to what this file needs.
    /// </summary>
    private sealed class InterruptedPeer : IDisposable
    {
        private readonly Socket _socket;

        public InterruptedPeer(IPEndPoint destination)
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
