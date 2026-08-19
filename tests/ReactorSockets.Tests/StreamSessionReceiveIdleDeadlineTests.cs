using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 4.2.2 (issue #117, #26's second idle timer), revised by issue #119: over TCP only receiving
/// is evidence a Peer's transport is still there — the kernel accepts written bytes into a buffer
/// whether or not anything is at the far end. A Peer from which nothing at all has been received for
/// <see cref="PeerSlot.RemoteSendIdleInterval"/> x <see cref="StreamSessionOptions.ResolvedMissedHeartbeats"/>
/// — the interval the peer itself announced in its Hello, never this end's own
/// <see cref="StreamSessionOptions.SendIdleInterval"/> — moves the Peer to
/// <see cref="SessionEventKind.PeerInterrupted"/> rather than closing it: its transport is gone, but
/// its identity and undelivered Messages are still worth holding for a Resume (ADR 0010), and a dead
/// receive-idle timer is exactly as good a sign of that as a reset or a failed write. Every test here
/// drives a real <see cref="StreamSessionReactor"/> against a hand-built raw counterparty
/// (<see cref="RawPeer"/>, mirroring <see cref="StreamSessionSendIdleHeartbeatTests"/>'s
/// <c>HeartbeatPeer</c>) through the public L2 surface only (tests/CLAUDE.md, ADR 0015), driven
/// entirely by <see cref="StreamSessionReactorHarness.TimeProvider"/> — no test here sleeps to observe
/// timer behaviour.
/// </summary>
public class StreamSessionReceiveIdleDeadlineTests
{
    /// <summary>The peer's own announced send-idle interval used by every test here unless overridden.</summary>
    private const ulong DefaultPeerSendIdleMs = 1000;

    /// <summary>
    /// Sends a Hello via <paramref name="peer"/> announcing <paramref name="peerSendIdleMs"/> as its own
    /// send-idle interval, pumps <paramref name="accepter"/> until <see cref="SessionEventKind.PeerConnected"/>
    /// drains, then reads the accepter's own Hello reply off the wire so a later read on the same socket
    /// never mistakes it for something else. Mirrors <see cref="StreamSessionTestRecords.EstablishSession"/>,
    /// widened only to let a test announce a peer send-idle interval other than the default — the one
    /// customisation issue #117's AC2 needs and the shared helper does not offer.
    /// </summary>
    private static PeerId EstablishSession(
        StreamSessionReactorHarness accepter, RawPeer peer, ulong peerSendIdleMs = DefaultPeerSendIdleMs, TimeSpan? budget = null)
    {
        peer.Send(StreamSessionTestRecords.BuildHello(sendIdleMs: peerSendIdleMs));
        PumpDeadline deadline = PumpDeadline.Start(budget);
        while (true)
        {
            accepter.Reactor.Pump();
            foreach ((SessionEventKind Kind, PeerId Peer, DisconnectReason Reason) evt in accepter.DrainToList())
            {
                if (evt.Kind == SessionEventKind.PeerConnected)
                {
                    peer.Receive(StreamSessionTestRecords.HandshakeRecordSize);
                    return evt.Peer;
                }
            }

            if (deadline.Expired)
            {
                throw new InvalidOperationException("The Session never established within the Pump budget.");
            }

            PumpDeadline.Yield();
        }
    }

    // =========================================================================================
    // AC1 — a Peer that stops responding entirely is declared lost after peerSendIdle x
    // MissedHeartbeats, and not before
    // =========================================================================================

    [Fact]
    public void Pump_PeerThatStopsRespondingEntirely_IsInterruptedOnceTheReceiveIdleDeadlineElapsesAndNotBefore()
    {
        // Arrange — default MissedHeartbeats (3) x the peer's own 1-second announced interval, so the
        // deadline is 3 seconds.
        using StreamSessionReactorHarness accepter = new();
        using RawPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        TimeSpan deadline = TimeSpan.FromMilliseconds(DefaultPeerSendIdleMs * 3);

        // Act — one Pump short of the deadline: the peer must still be alive.
        accepter.TimeProvider.Advance(deadline - TimeSpan.FromMilliseconds(1));
        accepter.Reactor.Pump();

        // Assert
        accepter.DrainToList().Should().NotContain(
            e => e.Kind == SessionEventKind.PeerClosed || e.Kind == SessionEventKind.PeerInterrupted,
            "the receive-idle deadline has not fully elapsed yet");

        // Act — the last millisecond closes the deadline.
        accepter.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
        accepter.Reactor.Pump();

        // Assert — moved to Interrupted, not closed (issue #119): the transport is gone, but the Peer
        // itself is held for a Resume, and CountersOf still answers for it.
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted)
            .Which.Peer.Should().Be(id);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        Action countersOf = () => accepter.Reactor.CountersOf(id);
        countersOf.Should().NotThrow("the Peer is Interrupted, not gone — CountersOf must still answer for it");
    }

    // =========================================================================================
    // AC2 — the deadline is computed from the interval the peer announced, not this end's own
    // SendIdleInterval
    // =========================================================================================

    [Fact]
    public void Pump_PeerAnnouncingAShortIntervalWhileThisEndsOwnIntervalIsLong_IsInterruptedOffThePeersOwnInterval()
    {
        // Arrange — this end's own SendIdleInterval is 10 seconds, far longer than the deadline below.
        // If the receive-idle deadline were (wrongly) computed from this end's own SendIdleInterval
        // instead of the peer's announced one, nothing would move to Interrupted for another ~9.4
        // seconds — so a move within the first 600 milliseconds can only mean the peer's own
        // 200-millisecond interval drove the deadline.
        using StreamSessionReactorHarness accepter = new(sendIdleInterval: TimeSpan.FromSeconds(10));
        using RawPeer peer = new(accepter.Endpoint);
        const ulong peerSendIdleMs = 200;
        PeerId id = EstablishSession(accepter, peer, peerSendIdleMs);
        TimeSpan deadline = TimeSpan.FromMilliseconds(peerSendIdleMs * 3);

        // Act — one Pump short of the peer-derived deadline.
        accepter.TimeProvider.Advance(deadline - TimeSpan.FromMilliseconds(1));
        accepter.Reactor.Pump();

        // Assert
        accepter.DrainToList().Should().NotContain(
            e => e.Kind == SessionEventKind.PeerClosed || e.Kind == SessionEventKind.PeerInterrupted,
            "the peer-derived deadline has not fully elapsed yet");

        // Act — the last millisecond closes the peer-derived deadline.
        accepter.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
        accepter.Reactor.Pump();

        // Assert — Interrupted well before this end's own 10-second SendIdleInterval could ever be the
        // cause.
        accepter.DrainToList().Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == id,
            "the deadline must be driven by the peer's own announced interval, not this end's SendIdleInterval");
    }

    // =========================================================================================
    // AC3 — any received record resets the deadline, including a bare heartbeat carrying no
    // Messages
    // =========================================================================================

    [Fact]
    public void Pump_ABareHeartbeatWithNoMessages_ResetsTheReceiveIdleDeadline()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using RawPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        TimeSpan deadline = TimeSpan.FromMilliseconds(DefaultPeerSendIdleMs * 3);

        // Act — well before the deadline, the peer sends nothing but a bare delivery-counter record —
        // no Messages at all — which must still count as received activity.
        accepter.TimeProvider.Advance(deadline - TimeSpan.FromSeconds(1));
        peer.Send(StreamSessionTestRecords.BuildDeliveryCounterRecord(0));
        accepter.Reactor.Pump();
        accepter.DrainToList().Should().NotContain(
            e => e.Kind == SessionEventKind.PeerClosed || e.Kind == SessionEventKind.PeerInterrupted);

        // Act — advance to one millisecond short of a full deadline measured from that bare heartbeat.
        // Measured from establishment instead, this point is already past the original deadline, so a
        // Reactor that failed to reset on the bare heartbeat would already have interrupted the Peer here.
        accepter.TimeProvider.Advance(deadline - TimeSpan.FromMilliseconds(1));
        accepter.Reactor.Pump();

        // Assert — still alive: the deadline is measured from the bare heartbeat, not from establishment.
        accepter.DrainToList().Should().NotContain(
            e => e.Kind == SessionEventKind.PeerClosed || e.Kind == SessionEventKind.PeerInterrupted,
            "the bare heartbeat reset the deadline, so a full interval has not yet passed since it arrived");

        // Act — the last millisecond closes the deadline measured from the bare heartbeat.
        accepter.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
        accepter.Reactor.Pump();

        // Assert — Interrupted, not closed (issue #119).
        accepter.DrainToList().Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == id);
    }

    // =========================================================================================
    // AC4 — a non-default MissedHeartbeats moves the deadline proportionally
    // =========================================================================================

    [Fact]
    public void Pump_ANonDefaultMissedHeartbeatsCount_MovesTheReceiveIdleDeadlineProportionally()
    {
        // Arrange — five missed heartbeats instead of the default three, so the deadline is five times
        // the peer's announced 1-second interval rather than three times it.
        using StreamSessionReactorHarness accepter = new(missedHeartbeats: 5);
        using RawPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        TimeSpan deadline = TimeSpan.FromMilliseconds(DefaultPeerSendIdleMs * 5);

        // Act — one Pump short of the proportionally longer deadline: the default-count deadline (3
        // seconds) has long since passed by this point, so surviving here can only mean the configured
        // count of 5 is what is actually governing.
        accepter.TimeProvider.Advance(deadline - TimeSpan.FromMilliseconds(1));
        accepter.Reactor.Pump();

        // Assert
        accepter.DrainToList().Should().NotContain(
            e => e.Kind == SessionEventKind.PeerClosed || e.Kind == SessionEventKind.PeerInterrupted,
            "the deadline scales with the configured MissedHeartbeats count, not the default");

        // Act — the last millisecond closes the proportionally longer deadline.
        accepter.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
        accepter.Reactor.Pump();

        // Assert — Interrupted, not closed (issue #119).
        accepter.DrainToList().Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == id);
    }

    // =========================================================================================
    // AC5 — no DiagnosticId member is added by this ticket
    // =========================================================================================

    [Fact]
    public void DiagnosticId_HasNoMemberForTheReceiveIdleDeadline()
    {
        // Arrange
        string[] names = Enum.GetNames<DiagnosticId>();

        // Assert — the receive-idle-to-Interrupted transition (issue #117, revised by #119) is already
        // an Event (SessionEventKind.PeerInterrupted); Info is reserved for lifecycle that never became
        // one (ADR 0013), so this ticket adds no DiagnosticId member for it.
        names.Should().NotContain(
            n => n.Contains("ReceiveIdle", StringComparison.OrdinalIgnoreCase)
                || n.Contains("MissedHeartbeat", StringComparison.OrdinalIgnoreCase)
                || n.Contains("Interrupted", StringComparison.OrdinalIgnoreCase),
            "the receive-idle deadline (issue #117) and Interrupted (issue #119) are surfaced only through SessionEventKind, never a DiagnosticId");
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// that drives the handshake and otherwise sends only what a given test tells it to — including
    /// sending nothing at all, which is what most of this file's tests are about. Mirrors
    /// <see cref="StreamSessionSendIdleHeartbeatTests"/>'s <c>HeartbeatPeer</c>.
    /// </summary>
    private sealed class RawPeer : IDisposable
    {
        private readonly Socket _socket;

        public RawPeer(IPEndPoint destination)
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
        public byte[] Receive(int count)
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

            return buffer;
        }

        public void Dispose() => _socket.Dispose();
    }
}
