using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 4.2.3 (issue #118, #26's third and last idle-timer sub-issue): pins the reason
/// <see cref="StreamSessionReactor.EnforceIdleTimers"/> runs two independent checks per Peer rather than
/// one. A heartbeat triggered by receive idleness alone deadlocks two quiet peers: A's probe resets B's
/// receive-idle deadline, so B — judging its own need to send off what it has received — stays silent,
/// so A eventually hears nothing from a peer that is in fact healthy and declares it dead. The send-idle
/// timer (issue #114) must therefore fire unconditionally, never consulting
/// <see cref="PeerSlot.LastReceiveTimestamp"/>. This file adds no production behaviour — #114 and #117
/// already built both timers — it exists purely so a later refactor that collapses the two checks into
/// one fails a test rather than passing the whole suite silently.
/// </summary>
public class StreamSessionIdleTimerIndependenceTests
{
    // =========================================================================================
    // AC1 (mandatory) — two quiet Sessions, both real Reactors, both pumping: neither ever
    // declares the other dead, because each side's send-idle heartbeat reaches the other
    // unconditionally on what that side itself received.
    // =========================================================================================

    [Fact]
    public void Pump_TwoQuietSessionsBothPumpingWellPastSeveralIntervals_NeitherSideIsEverDeclaredDead()
    {
        // Arrange — two real Sessions, wired socket-to-socket, both left otherwise silent: neither ever
        // calls Send. The only traffic either side ever puts on the wire is its own send-idle heartbeat
        // (#114). If that heartbeat were instead suppressed by what was received (the mutation this test
        // exists to catch), each side would go quiet the moment it heard from the other, and the peer
        // that fell silent first would eventually be declared dead by the far end's receive-idle timer
        // (#117) despite being perfectly healthy — the deadlock issue #118 exists to defend against.
        using StreamSessionReactorHarness accepter = new();
        using StreamSessionReactorHarness dialler = new(bindLoopback: false);
        dialler.Reactor.Connect(accepter.Endpoint);
        EstablishBothSessions(dialler, accepter);

        // Act — advance both clocks in lockstep by slightly more than one SendIdleInterval each round,
        // pumping both sides every round, for far more rounds than either the SendIdleInterval or the
        // receive-idle deadline (peerSendIdle x MissedHeartbeats = 1s x 3 = 3s at these defaults) would
        // need to fire on their own — 10 rounds of ~1.1 seconds is roughly 11 seconds, well past three
        // full receive-idle deadlines. Each round's advance clears the send-idle interval, so both sides
        // keep emitting a heartbeat every round; that heartbeat is itself received activity for the other
        // side, so neither side's receive-idle deadline (3 seconds) is ever reached — as long as the
        // send-idle timer really is unconditional.
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> accepterEvents = [];
        TimeSpan increment = TimeSpan.FromMilliseconds(1100);
        for (int round = 0; round < 10; round++)
        {
            dialler.TimeProvider.Advance(increment);
            accepter.TimeProvider.Advance(increment);

            dialler.Reactor.Pump();
            diallerEvents.AddRange(dialler.DrainToList());

            accepter.Reactor.Pump();
            accepterEvents.AddRange(accepter.DrainToList());
        }

        // Assert — neither side ever closed the other.
        diallerEvents.Should().NotContain(
            e => e.Kind == SessionEventKind.PeerClosed,
            "the dialler's own send-idle heartbeat must keep reaching the accepter unconditionally, " +
            "so the accepter's receive-idle timer never has cause to fire");
        accepterEvents.Should().NotContain(
            e => e.Kind == SessionEventKind.PeerClosed,
            "the accepter's own send-idle heartbeat must keep reaching the dialler unconditionally, " +
            "so the dialler's receive-idle timer never has cause to fire");
    }

    /// <summary>
    /// Dials <paramref name="dialler"/> (already told to <see cref="StreamSessionReactor.Connect"/>) into
    /// <paramref name="accepter"/> and pumps both alternately until each has drained its own
    /// <see cref="SessionEventKind.PeerConnected"/>, draining everything else along the way so the test
    /// body starts from a clean Frame on both sides.
    /// </summary>
    private static void EstablishBothSessions(
        StreamSessionReactorHarness dialler, StreamSessionReactorHarness accepter, int maxRounds = 20)
    {
        bool diallerConnected = false;
        bool accepterConnected = false;

        for (int round = 0; round < maxRounds && !(diallerConnected && accepterConnected); round++)
        {
            dialler.Reactor.Pump();
            foreach ((SessionEventKind Kind, PeerId Peer, DisconnectReason Reason) evt in dialler.DrainToList())
            {
                diallerConnected |= evt.Kind == SessionEventKind.PeerConnected;
            }

            accepter.Reactor.Pump();
            foreach ((SessionEventKind Kind, PeerId Peer, DisconnectReason Reason) evt in accepter.DrainToList())
            {
                accepterConnected |= evt.Kind == SessionEventKind.PeerConnected;
            }
        }

        if (!diallerConnected || !accepterConnected)
        {
            throw new InvalidOperationException("Both Sessions never established within the Pump budget.");
        }
    }

    // =========================================================================================
    // AC2/AC3 — the lateness tolerance: a peer whose own loop stalls (scheduling jitter, or in
    // .NET a collection pause) is not declared dead for a stall slightly longer than one
    // send-idle interval, but is declared dead once the stall passes peerSendIdle x
    // MissedHeartbeats — so the tolerance is real, not merely an unreachable deadline.
    // =========================================================================================

    [Fact]
    public void Pump_PeerStallingSlightlyLongerThanOneSendIdleInterval_SurvivesAtTheDefaultMissedHeartbeatsCount()
    {
        // Arrange — default MissedHeartbeats (3), so the receive-idle deadline is 3 seconds; the peer's
        // announced interval is the default 1 second.
        using StreamSessionReactorHarness accepter = new();
        using RawPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);

        // Act — the peer's own loop stalls (e.g. a GC pause) for slightly longer than one send-idle
        // interval: it neither sends its own heartbeat nor anything else. This alone must not be enough
        // to declare it dead — a count of 1 (fire the instant one interval passes) would leave no room
        // for exactly this kind of jitter, which is why the default is 3.
        accepter.TimeProvider.Advance(TimeSpan.FromMilliseconds(1100));
        accepter.Reactor.Pump();

        // Assert — still alive: one interval of lateness is well inside the tolerance.
        accepter.DrainToList().Should().NotContain(
            e => e.Kind == SessionEventKind.PeerClosed,
            "a single interval of lateness is exactly the jitter the MissedHeartbeats count exists to absorb");

        // Act — the peer's loop resumes and sends its overdue heartbeat.
        peer.Send(StreamSessionTestRecords.BuildDeliveryCounterRecord(0));
        accepter.Reactor.Pump();

        // Assert — the resumed heartbeat is accepted normally; the Session is unaffected by the stall.
        accepter.DrainToList().Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    [Fact]
    public void Pump_PeerStallingPastPeerSendIdleTimesMissedHeartbeats_IsInterrupted()
    {
        // Arrange — pairs with the test above: proves the tolerance has an actual ceiling rather than
        // the deadline being unreachable in practice.
        using StreamSessionReactorHarness accepter = new();
        using RawPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);
        TimeSpan deadline = TimeSpan.FromMilliseconds(1000 * 3);

        // Act — the stall continues well past the full deadline; the peer never resumes.
        accepter.TimeProvider.Advance(deadline);
        accepter.Reactor.Pump();

        // Assert — moved to Interrupted once the stall exceeds peerSendIdle x MissedHeartbeats (issue
        // #119 revises this from the close issue #117 originally landed).
        accepter.DrainToList().Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted);
    }

    /// <summary>Mirrors <see cref="StreamSessionReceiveIdleDeadlineTests"/>'s own <c>EstablishSession</c>.</summary>
    private static PeerId EstablishSession(StreamSessionReactorHarness accepter, RawPeer peer, TimeSpan? budget = null)
    {
        peer.Send(StreamSessionTestRecords.BuildHello());
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

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// that drives the handshake and otherwise sends only what a given test tells it to. Mirrors
    /// <see cref="StreamSessionReceiveIdleDeadlineTests"/>'s <c>RawPeer</c>.
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
