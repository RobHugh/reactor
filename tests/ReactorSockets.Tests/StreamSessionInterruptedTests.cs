using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 4.3.1 (issue #119): <c>Interrupted</c>, the state of a Session whose transport has been lost
/// but whose Peer identity and undelivered Messages are still held, pending a Resume. ADR 0010's rule
/// governs every test here — <b>a FIN is not a Goodbye</b>, so a collapsing half-open pipe, a reset, or
/// a local write failure all move an established Peer to <see cref="SessionEventKind.PeerInterrupted"/>
/// rather than closing it, while a Goodbye followed by a FIN still reaches
/// <see cref="SessionEventKind.PeerClosing"/>/<see cref="SessionEventKind.PeerClosed"/> exactly as
/// before this ticket and never raises <see cref="SessionEventKind.PeerInterrupted"/>. Every test here
/// drives a real <see cref="StreamSessionReactor"/> against a hand-built raw counterparty
/// (<see cref="InterruptedPeer"/>) through the public L2 surface only (tests/CLAUDE.md, ADR 0015).
/// Mirrors <see cref="StreamSessionGoodbyeTests"/>'s <c>GoodbyePeer</c> and
/// <see cref="StreamSessionReceiveIdleDeadlineTests"/>'s <c>RawPeer</c>, the house pattern for a raw
/// TCP counterparty.
/// </summary>
public class StreamSessionInterruptedTests
{
    private static byte[] BuildFrame(byte[] payload) => StreamSessionTestRecords.BuildFrame(payload);

    private static byte[] BuildGoodbye() => StreamSessionTestRecords.BuildPrefix(uint.MaxValue);

    private static PeerId EstablishSession(StreamSessionReactorHarness accepter, InterruptedPeer peer, int maxPumps = 20)
    {
        PeerId id = StreamSessionTestRecords.EstablishSession(accepter, peer.Send, maxPumps);
        peer.Receive(StreamSessionTestRecords.HandshakeRecordSize);
        return id;
    }

    // =========================================================================================
    // The transport killed abruptly — a reset — moves the Peer to Interrupted, not a close
    // =========================================================================================

    [Fact]
    public void Pump_TransportKilledAbruptlyWithAReset_MovesThePeerToInterruptedNotClosed()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using InterruptedPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        // Act — a socket closed with linger zero forces a reset rather than an ordinary FIN.
        peer.ResetAbruptly();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind is SessionEventKind.PeerInterrupted or SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert — Interrupted, never a close, and the same PeerId (generation included) the Session
        // established under.
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted)
            .Which.Peer.Should().Be(id);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    // =========================================================================================
    // FIN with no Goodbye — the counterparty shuts down its write half and nothing else — moves the
    // Peer to Interrupted (ADR 0010: a FIN is not a Goodbye)
    // =========================================================================================

    [Fact]
    public void Pump_CounterpartyShutsDownItsWriteHalfWithNoGoodbye_MovesThePeerToInterrupted()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using InterruptedPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        // Act — an ordinary graceful FIN, nothing buffered ahead of it and no Goodbye sentinel.
        peer.CloseGracefully();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind is SessionEventKind.PeerInterrupted or SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted)
            .Which.Peer.Should().Be(id);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    // =========================================================================================
    // Goodbye then FIN: Closing/Closed as before this ticket, never Interrupted
    // =========================================================================================

    [Fact]
    public void Pump_CounterpartySendsGoodbyeThenFin_ClosesWithPeerGoodbyeAndNeverRaisesInterrupted()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using InterruptedPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        // Act — the far end announces it is leaving on purpose, then closes its write half. Nothing was
        // ever outstanding on either side, so this end closes directly with PeerGoodbye.
        peer.Send(BuildGoodbye());
        peer.CloseGracefully();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert — closed with PeerGoodbye, and Interrupted is never observed for this Peer at any
        // point in the sequence.
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed && e.Peer == id)
            .Which.Reason.Should().Be(DisconnectReason.PeerGoodbye);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerInterrupted,
            "a Goodbye forbids a Resume (ADR 0010) — the FIN behind it must never be read as an interruption");
    }

    // =========================================================================================
    // Identity and counters survive the interruption: PeerId (generation included) is unchanged,
    // CountersOf still answers, and per-Peer counters stay non-zero and unreset
    // =========================================================================================

    [Fact]
    public void Pump_AfterInterruption_PeerIdIsUnchangedAndCountersOfKeepsAnsweringWithTheSameNonZeroFigures()
    {
        // Arrange — one Message delivered before the transport is lost, so MessagesReceived is
        // provably non-zero going into the interruption.
        using StreamSessionReactorHarness accepter = new();
        using InterruptedPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        byte[] message = StreamSessionTestRecords.PayloadOf(16, 5);
        peer.Send(BuildFrame(message));
        accepter.PumpUntil(() => accepter.Reactor.CountersOf(id).MessagesReceived == 1);
        accepter.DrainToList();
        ulong messagesReceivedBeforeInterruption = accepter.Reactor.CountersOf(id).MessagesReceived;

        // Act — the transport is lost with a reset.
        peer.ResetAbruptly();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerInterrupted); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert — the Peer this event names is the very same PeerId (generation included) the Session
        // established under, and CountersOf still answers for it with the count carried across the
        // interruption unreset, never dropped back to zero (ADR 0013: a counter's lifetime is the
        // Peer's, not the transport's).
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted)
            .Which.Peer.Should().Be(id);
        messagesReceivedBeforeInterruption.Should().Be(1);
        accepter.Reactor.CountersOf(id).MessagesReceived.Should().Be(messagesReceivedBeforeInterruption);
    }

    // =========================================================================================
    // No DiagnosticId member is added by this ticket
    // =========================================================================================

    [Fact]
    public void DiagnosticId_HasNoMemberForInterrupted()
    {
        // Arrange
        string[] names = Enum.GetNames<DiagnosticId>();

        // Assert — the transition to Interrupted is already an Event
        // (SessionEventKind.PeerInterrupted); Info is reserved for lifecycle that never became one
        // (ADR 0013), so this ticket adds no DiagnosticId member for it.
        names.Should().NotContain(
            n => n.Contains("Interrupt", StringComparison.OrdinalIgnoreCase),
            "Interrupted (issue #119) is surfaced only through SessionEventKind, never a DiagnosticId");
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// whose transport can be killed abruptly (<see cref="ResetAbruptly"/>) or ended gracefully with no
    /// Goodbye (<see cref="CloseGracefully"/>) — the two ways ADR 0010 says a Session reaches
    /// Interrupted from the far end. Mirrors <see cref="StreamSessionGoodbyeTests"/>'s
    /// <c>GoodbyePeer</c>, widened with the abrupt-reset method that ticket had no need for.
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

        /// <summary>
        /// Ends the connection gracefully — a bare FIN with nothing further on the wire, and no Goodbye
        /// sentinel unless a test sent one first.
        /// </summary>
        public void CloseGracefully() => _socket.Shutdown(SocketShutdown.Send);

        /// <summary>
        /// Kills the connection abruptly: <c>SO_LINGER</c> set to zero, then closed, which forces the
        /// OS to send a reset instead of an ordinary FIN — the "socket closed with linger zero" half of
        /// issue #119's first acceptance criterion.
        /// </summary>
        public void ResetAbruptly()
        {
            _socket.LingerState = new LingerOption(true, 0);
            _socket.Close();
        }

        public void Dispose() => _socket.Dispose();
    }
}
