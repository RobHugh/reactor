using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 4.5.3 (issue #125): the dialling side's own re-dial of a lost transport, during a
/// <see cref="StreamSessionReactor.Pump"/>, on the schedule <see cref="RedialSchedule"/> (issue #111)
/// derives from the observed Pump interval (issue #112) and this Peer's own effective Linger — never a
/// separate attempt-count knob. Every test here drives a real dialling <see cref="StreamSessionReactor"/>
/// through the public L2 surface only (tests/CLAUDE.md, ADR 0015) against a hand-built raw counterparty
/// that <em>listens</em> rather than dials (<see cref="RawStreamListener"/>) — the mirror image of every
/// other stream Session test file's raw counterparty, needed here because this Reactor is the one doing
/// the dialling. Every test is driven entirely by <see cref="StreamSessionReactorHarness.TimeProvider"/>
/// — no test here sleeps, and no attempt's actual jittered delay is ever asserted exactly: a test only
/// ever advances the clock by at least the schedule's own ceiling for that attempt, which is due
/// regardless of where <see cref="RedialSchedule"/>'s jitter happened to land.
/// </summary>
public class StreamSessionRedialTests
{
    private static readonly TimeSpan _harnessLinger = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Dials <paramref name="dialer"/> at <paramref name="listener"/>, completes the handshake by hand
    /// (this Reactor's own Hello, then a raw Hello reply announcing <paramref name="resumeSecret"/>), and
    /// returns the established <see cref="PeerId"/> paired with the raw socket now holding the far end,
    /// leaving <paramref name="dialer"/>'s clock advanced by a small, known amount between the two Pumps
    /// the handshake takes — the cadence <see cref="StreamSessionReactor.ObservedPumpInterval"/> reads at
    /// the moment a later interruption begins this Peer's re-dial schedule.
    /// </summary>
    private static (PeerId Peer, RawStreamListener.Accepted Far) EstablishDialledSession(
        StreamSessionReactorHarness dialer, RawStreamListener listener, byte[] resumeSecret)
    {
        PeerId peer = dialer.Reactor.Connect(listener.Endpoint);
        RawStreamListener.Accepted far = listener.Accept();

        dialer.Reactor.Pump();
        byte[] hello = far.Receive(StreamSessionTestRecords.HandshakeRecordSize);
        hello[0].Should().Be(StreamSessionTestRecords.HelloRecordType);

        dialer.TimeProvider.Advance(_cadence);
        far.Send(StreamSessionTestRecords.BuildHello(resumeSecret: resumeSecret));

        PeerId? connected = null;
        for (int i = 0; i < 20 && connected is null; i++)
        {
            dialer.Reactor.Pump();
            foreach ((SessionEventKind Kind, PeerId Peer, DisconnectReason Reason) evt in dialer.DrainToList())
            {
                if (evt.Kind == SessionEventKind.PeerConnected)
                {
                    connected = evt.Peer;
                }
            }
        }

        connected.Should().Be(peer, "the dial must establish before a test can interrupt it");
        return (peer, far);
    }

    /// <summary>The known cadence <see cref="EstablishDialledSession"/> advances the clock by mid-handshake.</summary>
    private static readonly TimeSpan _cadence = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Interrupts <paramref name="dialer"/>'s established Peer by resetting <paramref name="far"/>
    /// abruptly, advancing the clock by <see cref="_cadence"/> first so the Pump that observes the reset
    /// also reports <see cref="_cadence"/> as <see cref="StreamSessionReactor.ObservedPumpInterval"/> —
    /// the floor <see cref="RedialSchedule"/>'s first delay is derived from. Pumps until
    /// <see cref="SessionEventKind.PeerInterrupted"/> drains and returns everything drained on the way.
    /// </summary>
    private static List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> InterruptDialledPeer(
        StreamSessionReactorHarness dialer, RawStreamListener.Accepted far, PeerId peer)
    {
        far.ResetAbruptly();

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        bool interrupted() => drained.Any(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == peer);
        for (int i = 0; i < 20 && !interrupted(); i++)
        {
            dialer.TimeProvider.Advance(_cadence);
            dialer.Reactor.Pump();
            drained.AddRange(dialer.DrainToList());
        }

        interrupted().Should().BeTrue("the reset must be observed as an interruption within the Pump budget");
        return drained;
    }

    // =========================================================================================
    // AC1 / AC6 — an Interrupted dialling-side Peer re-dials without being asked, targeting the
    // endpoint originally dialled, during ordinary Pump calls with FakeTimeProvider advanced
    // =========================================================================================

    [Fact]
    public void Pump_InterruptedDiallingPeer_ReDialsTheOriginalEndpointWithoutTheConsumerAsking()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new();
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 9);
        (PeerId peer, RawStreamListener.Accepted far) = EstablishDialledSession(dialer, listener, resumeSecret);
        InterruptDialledPeer(dialer, far, peer);
        far.Dispose();

        // Act — advance past the first delay's ceiling (min(observed cadence, effective Linger), so
        // min(50ms, 1s) = 50ms here) and pump: a fresh dial to the same listener must land.
        dialer.TimeProvider.Advance(_cadence);
        dialer.Reactor.Pump();

        // Assert — the listener this Reactor originally dialled receives a second connection, on its
        // own, with no further consumer action between the reset and now.
        listener.TryAccept(out RawStreamListener.Accepted redial, timeoutMs: 2000).Should().BeTrue(
            "an Interrupted dialling Peer must re-dial its original endpoint without being asked");
        redial.Dispose();
    }

    // =========================================================================================
    // A re-dial that connects sends a Resume record naming the secret the far end issued, handing
    // off to the resume match on the listening side (#123) — never closes, and stops the schedule
    // =========================================================================================

    [Fact]
    public void Pump_ReDialThatConnects_SendsTheResumeRecordAndRaisesNoFurtherAttempts()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new();
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 3);
        (PeerId peer, RawStreamListener.Accepted far) = EstablishDialledSession(dialer, listener, resumeSecret);
        InterruptDialledPeer(dialer, far, peer);
        far.Dispose();

        // Act — the first attempt connects (the listener is still up); one more Pump lets PollConnect
        // observe it and send the Resume record.
        dialer.TimeProvider.Advance(_cadence);
        dialer.Reactor.Pump();
        listener.TryAccept(out RawStreamListener.Accepted redialed, timeoutMs: 2000).Should().BeTrue();
        dialer.Reactor.Pump();

        // Assert — the record presented is a Resume naming exactly the secret this end's far end
        // issued at the original Hello, not a fresh Hello.
        byte[] record = redialed.Receive(StreamSessionTestRecords.HandshakeRecordSize);
        record[0].Should().Be(StreamSessionTestRecords.ResumeRecordType);
        record.AsSpan(1, StreamSessionTestRecords.ResumeSecretLength).ToArray().Should().Equal(resumeSecret);

        // Assert — never closed by any of this: the whole point of a re-dial is that it never touches
        // ClosePeer on the way to a successful hand-off.
        dialer.DrainToList().Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);

        // Act — mid-schedule success stops the schedule: advance well past several further delays and
        // pump repeatedly. No third connection ever arrives.
        for (int i = 0; i < 5; i++)
        {
            dialer.TimeProvider.Advance(TimeSpan.FromSeconds(1));
            dialer.Reactor.Pump();
        }

        listener.TryAccept(out _, timeoutMs: 200).Should().BeFalse(
            "a re-dial that already succeeded must raise no further attempts");
        redialed.Dispose();
    }

    // =========================================================================================
    // A re-dial attempt that fails — nothing listening at the endpoint any more — never closes the
    // Peer, which stays Interrupted until Linger
    // =========================================================================================

    [Fact]
    public void Pump_ReDialAttemptThatFails_LeavesThePeerInterruptedRatherThanClosingIt()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new();
        IPEndPoint originalEndpoint;
        PeerId peer;
        RawStreamListener.Accepted far;
        using (RawStreamListener listener = new())
        {
            byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 5);
            (peer, far) = EstablishDialledSession(dialer, listener, resumeSecret);
            originalEndpoint = listener.Endpoint;
            InterruptDialledPeer(dialer, far, peer);
            far.Dispose();
        }

        // Act — the listener is gone (disposed, so nothing answers that port); several attempts across
        // several Pumps must each resolve as a failed connect, never a close.
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 4; i++)
        {
            dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(200));
            dialer.Reactor.Pump();
            drained.AddRange(dialer.DrainToList());
        }

        // Assert
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        Action stillReadable = () => dialer.Reactor.CountersOf(peer);
        stillReadable.Should().NotThrow("a failed re-dial attempt leaves the Peer Interrupted, not closed");
        _ = originalEndpoint;
    }

    // =========================================================================================
    // AC — re-dialling stops at the effective Linger: the Peer closes with LingerExpired, never an
    // attempt-count reason
    // =========================================================================================

    [Fact]
    public void Pump_ReDiallingPastTheEffectiveLinger_ClosesWithLingerExpiredNotAnAttemptCountReason()
    {
        // Arrange
        using StreamSessionReactorHarness dialer = new();
        PeerId peer;
        RawStreamListener.Accepted far;
        using (RawStreamListener listener = new())
        {
            byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 7);
            (peer, far) = EstablishDialledSession(dialer, listener, resumeSecret);
            InterruptDialledPeer(dialer, far, peer);
            far.Dispose();
        }

        // Act — jump straight past the effective Linger (1 second, the harness's fixed value) in one
        // leap: EnforceIdleTimers' Linger check runs ahead of ProcessRedials in the same Pump, so this
        // must close the Peer this same call rather than let a redial due at the same instant win.
        dialer.TimeProvider.Advance(_harnessLinger + TimeSpan.FromMilliseconds(1));
        dialer.Reactor.Pump();

        // Assert
        dialer.DrainToList().Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerClosed && e.Peer == peer)
            .Which.Reason.Should().Be(DisconnectReason.LingerExpired);
    }

    // =========================================================================================
    // No re-dial from the listening side: it has no address to dial, and Linger alone governs it
    // exactly as before this ticket
    // =========================================================================================

    [Fact]
    public void Pump_InterruptedListeningSidePeer_NeverReDialsAndOnlyLingerClosesIt()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        List<StreamSessionReactorHarness.HeldPeer> held = accepter.FillToCapacity();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained =
            accepter.InterruptHeldPeer(held[0]);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);

        // Act — advance generously, in several steps well short of Linger, well past where several
        // re-dial attempts would have landed had this side (wrongly) scheduled any.
        for (int i = 0; i < 4; i++)
        {
            accepter.TimeProvider.Advance(TimeSpan.FromMilliseconds(200));
            accepter.Reactor.Pump();
            accepter.DrainToList().Should().BeEmpty(
                "a listening-side Interrupted Peer has no address to re-dial and nothing to report before Linger");
        }

        // Act — the same Linger deadline as before this ticket, nothing earlier and nothing later.
        accepter.TimeProvider.Advance(_harnessLinger - TimeSpan.FromMilliseconds(800) + TimeSpan.FromMilliseconds(1));
        accepter.Reactor.Pump();

        // Assert
        accepter.DrainToList().Should().ContainSingle(
            e => e.Kind == SessionEventKind.PeerClosed && e.Peer == held[0].Peer)
            .Which.Reason.Should().Be(DisconnectReason.LingerExpired);
    }

    // =========================================================================================
    // No re-dial after a local Goodbye: LocalGoodbyeSent is checked even when the append itself was
    // deferred (retention exhausted) and never actually reached PendingCloseReasonAfterRetentionDrains
    // =========================================================================================

    [Fact]
    public void Pump_TransportLostAfterCloseWithADeferredGoodbyeAppend_NeverReDials()
    {
        // Arrange — retention sized to hold exactly one maximum Message and nothing more, so Close's own
        // Goodbye append finds RetentionOutcome.Exhausted and LocalGoodbyeSent is set well before
        // PendingCloseReasonAfterRetentionDrains ever is (mirrors StreamSessionGoodbyeTests' own
        // deferred-append scenario, on the dialling side this time).
        const int maxMessageSize = 16;
        const int retentionBytes = maxMessageSize + 4;
        using StreamSessionReactorHarness dialer = new(maxMessageSize: maxMessageSize, retentionBytes: retentionBytes);
        using RawStreamListener listener = new();
        byte[] resumeSecret = StreamSessionTestRecords.PayloadOf(StreamSessionTestRecords.ResumeSecretLength, 1);
        (PeerId peer, RawStreamListener.Accepted far) = EstablishDialledSession(dialer, listener, resumeSecret);

        byte[] message = StreamSessionTestRecords.PayloadOf(maxMessageSize, 4);
        dialer.Reactor.Send(peer, message).Should().Be(SendOutcome.Sent);
        dialer.Reactor.Close(peer);
        dialer.Reactor.Pump();
        far.Receive(StreamSessionTestRecords.BuildFrame(message).Length);
        dialer.DrainToList().Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed,
            "the Goodbye append is exhausted and deferred, not yet committed");

        // Act — the transport is lost before the deferred Goodbye ever lands: LocalGoodbyeSent is true,
        // PendingCloseReasonAfterRetentionDrains is still null, and InterruptPeer — not ClosePeer —
        // is what runs.
        far.ResetAbruptly();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerInterrupted); i++)
        {
            dialer.TimeProvider.Advance(_cadence);
            dialer.Reactor.Pump();
            drained.AddRange(dialer.DrainToList());
        }

        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == peer);

        // Advance well past where a re-dial would have fired had one (wrongly) been scheduled.
        for (int i = 0; i < 4; i++)
        {
            dialer.TimeProvider.Advance(TimeSpan.FromMilliseconds(200));
            dialer.Reactor.Pump();
        }

        listener.TryAccept(out _, timeoutMs: 200).Should().BeFalse(
            "a Peer this end already said Goodbye to must never be re-dialled, even though the append itself was still deferred");
    }
}

/// <summary>
/// A plain BCL <see cref="TcpListener"/>, standing in for the far end of a Session this Reactor
/// dialled — the mirror image of every other stream Session test file's raw counterparty
/// (<c>RawStreamPeer</c>, <c>RawPeer</c>, <c>InterruptedPeer</c>, ...), which all connect outward to a
/// Reactor under test. Here the Reactor under test is the one dialling, so this listens instead.
/// </summary>
internal sealed class RawStreamListener : IDisposable
{
    private readonly TcpListener _listener;

    public RawStreamListener()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
    }

    /// <summary>The endpoint a Reactor under test dials to reach this listener.</summary>
    public IPEndPoint Endpoint => (IPEndPoint)_listener.LocalEndpoint;

    /// <summary>Blocks up to 5 seconds for a connection to arrive, throwing if none does.</summary>
    public Accepted Accept(int timeoutMs = 5000)
    {
        if (!TryAccept(out Accepted accepted, timeoutMs))
        {
            throw new InvalidOperationException("No connection arrived within the timeout.");
        }

        return accepted;
    }

    /// <summary>
    /// Answers whether a connection arrived within <paramref name="timeoutMs"/> rather than throwing or
    /// blocking indefinitely — used both to observe a re-dial actually happening and to prove one did
    /// not, across repeated <see cref="StreamSessionReactor.Pump"/> calls the caller drives.
    /// </summary>
    public bool TryAccept(out Accepted accepted, int timeoutMs = 2000)
    {
        if (_listener.Server.Poll(timeoutMs * 1000, SelectMode.SelectRead))
        {
            accepted = new Accepted(_listener.AcceptSocket());
            return true;
        }

        accepted = null!;
        return false;
    }

    public void Dispose() => _listener.Stop();

    /// <summary>One connection this listener accepted, standing in for the far end of a dialled Session.</summary>
    public sealed class Accepted : IDisposable
    {
        private readonly Socket _socket;

        public Accepted(Socket socket)
        {
            _socket = socket;
            _socket.ReceiveTimeout = 5000;
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
                    throw new InvalidOperationException(
                        "The dialling Reactor closed the connection before sending everything expected.");
                }

                totalRead += read;
            }

            return buffer;
        }

        /// <summary>
        /// Kills the connection abruptly: <c>SO_LINGER</c> set to zero, then closed, which forces the OS
        /// to send a reset instead of an ordinary FIN — what interrupts the dialling Reactor's Peer.
        /// </summary>
        public void ResetAbruptly()
        {
            _socket.LingerState = new LingerOption(true, 0);
            _socket.Close();
        }

        public void Dispose() => _socket.Dispose();
    }
}
