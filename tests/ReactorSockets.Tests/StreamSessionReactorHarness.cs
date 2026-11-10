using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;

namespace ReactorSockets.Tests;

/// <summary>
/// The fixture the stream Session tests are written against (see tests/CLAUDE.md): a real
/// <see cref="StreamSessionReactor"/> on loopback, a <see cref="FakeTimeProvider"/>, and a recording
/// Sink. L0 and L1 are internal, so this — the public surface a consumer has — is the only seam.
/// Mirrors <see cref="DatagramSessionReactorHarness"/>, the house pattern for this profile.
/// </summary>
internal sealed class StreamSessionReactorHarness : IDisposable
{
    public StreamSessionReactorHarness(
        int capacity = 4,
        int? maxPeersPerAddress = null,
        bool defaultMaxPeersPerAddress = false,
        int? peerLimitIPv6PrefixLength = null,
        int? admissionCap = null,
        int maxMessageSize = 4096,
        int retentionBytes = 4100,
        int? receiveBufferSize = null,
        int? sendBufferSize = null,
        int? missedHeartbeats = null,
        int? arenaSize = null,
        TimeSpan? sendIdleInterval = null,
        DiagnosticLevel minimumLevel = DiagnosticLevel.Trace,
        IReactorDiagnostics? sinkOverride = null,
        bool withoutDiagnostics = false,
        bool bindLoopback = true,
        IReadOnlyList<IPEndPoint>? endpoints = null,
        RandomNumberGenerator? entropy = null,
        TimeSpan? linger = null,
        FakeTimeProvider? timeProvider = null)
    {
        Capacity = capacity;
        SendIdleInterval = sendIdleInterval ?? TimeSpan.FromSeconds(1);
        Linger = linger ?? TimeSpan.FromSeconds(1);
        TimeProvider = timeProvider ?? new FakeTimeProvider();
        IReactorDiagnostics? installedSink = withoutDiagnostics ? null : sinkOverride ?? Sink;
        Entropy = entropy ?? RandomNumberGenerator.Create();
        Reactor = new StreamSessionReactor(new StreamSessionOptions
        {
            Reactor = new ReactorOptions
            {
                Capacity = capacity,
                AdmissionCap = admissionCap,
                ArenaSize = arenaSize,
                TimeProvider = TimeProvider,
                Sink = installedSink,
                MinimumLevel = installedSink is null ? null : minimumLevel,
            },
            MaxMessageSize = maxMessageSize,
            RetentionBytes = retentionBytes,
            // No limit unless asked: every counterparty in a test shares the loopback address.
            MaxPeersPerAddress = defaultMaxPeersPerAddress ? null : maxPeersPerAddress ?? capacity,
            PeerLimitIPv6PrefixLength = peerLimitIPv6PrefixLength,
            ReceiveBufferSize = receiveBufferSize,
            SendBufferSize = sendBufferSize,
            MissedHeartbeats = missedHeartbeats,
            SendIdleInterval = SendIdleInterval,
            Linger = Linger,
            Endpoints = endpoints ?? (bindLoopback ? [new IPEndPoint(IPAddress.Loopback, 0)] : []),
            Entropy = Entropy,
        });
    }

    /// <summary>The capacity this Reactor was built with — the number of Slots <see cref="FillToCapacity"/> occupies.</summary>
    public int Capacity { get; }

    /// <summary>
    /// The clock the Reactor was built with — advance it to exercise anything time-dependent. A fresh
    /// <see cref="FakeTimeProvider"/> unless the constructor's <c>timeProvider</c> parameter supplies
    /// one shared with another harness, which two loopback-connected Reactors need whenever a test's
    /// assertion depends on both ends acting at the same instant (issue #131) — a Reactor's own clock is
    /// otherwise private to it, exactly like a real one's.
    /// </summary>
    public FakeTimeProvider TimeProvider { get; }

    /// <summary>
    /// The <see cref="StreamSessionOptions.SendIdleInterval"/> the Reactor was built with — 1 second
    /// unless the constructor's <c>sendIdleInterval</c> parameter overrides it. Exposed so a test can
    /// advance <see cref="TimeProvider"/> by exactly this much without duplicating the constant.
    /// </summary>
    public TimeSpan SendIdleInterval { get; }

    /// <summary>
    /// The <see cref="StreamSessionOptions.Linger"/> the Reactor was built with — 1 second unless the
    /// constructor's <c>linger</c> parameter overrides it. A test that needs the re-dial schedule's
    /// ceiling to bite sets it below the cadence it pumps at, since that ceiling is the Linger.
    /// </summary>
    public TimeSpan Linger { get; }

    /// <summary>The recording Sink installed on the Reactor, unless the harness was told to override or omit it.</summary>
    public RecordingDiagnosticsSink Sink { get; } = new();

    /// <summary>The entropy the Reactor draws the resume secret from.</summary>
    public RandomNumberGenerator Entropy { get; }

    /// <summary>The Reactor under test.</summary>
    public StreamSessionReactor Reactor { get; }

    /// <summary>The first endpoint this Reactor is bound to — what a counterparty connects to.</summary>
    public IPEndPoint Endpoint => Reactor.BoundEndpoints[0];

    /// <summary>Drains the current Frame into a list of plain values that outlive the drain.</summary>
    public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> DrainToList()
    {
        List<(SessionEventKind, PeerId, DisconnectReason)> drained = [];
        foreach (SessionEvent evt in Reactor.Drain())
        {
            drained.Add((evt.Kind, evt.Peer, evt.Reason));
        }

        return drained;
    }

    /// <summary>
    /// Drains the current Frame into a list of plain values that outlive the drain, copying each
    /// event's payload out as a <see cref="byte"/> array — empty for anything but
    /// <see cref="SessionEventKind.MessageReceived"/> — since <see cref="SessionEvent.Payload"/>
    /// aliases the frame arena and is invalid past the next Pump (ADR 0008).
    /// </summary>
    public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> DrainToListWithPayload()
    {
        List<(SessionEventKind, PeerId, DisconnectReason, byte[])> drained = [];
        foreach (SessionEvent evt in Reactor.Drain())
        {
            drained.Add((evt.Kind, evt.Peer, evt.Reason, evt.Payload.ToArray()));
        }

        return drained;
    }

    /// <summary>
    /// Pumps exactly <paramref name="pumps"/> times, accumulating everything drained. For asserting
    /// that something does <em>not</em> happen: a fixed count of Pumps bounds syscalls, not time, so
    /// it is not a way to wait for something to arrive — see <see cref="PumpDeadline"/> and use
    /// <see cref="PumpUntilDrained"/> for that.
    /// </summary>
    public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> PumpAndDrainAll(int pumps)
    {
        List<(SessionEventKind, PeerId, DisconnectReason)> all = [];
        for (int i = 0; i < pumps; i++)
        {
            Reactor.Pump();
            all.AddRange(DrainToList());
        }

        return all;
    }

    /// <summary>
    /// Pumps until <paramref name="condition"/> holds over everything drained so far, or until
    /// <paramref name="budget"/> of real time is spent, and returns everything drained either way.
    /// This is how a test waits for an event to arrive; <see cref="PumpAndDrainAll"/> counts Pumps
    /// and so waits for essentially no time at all (<see cref="PumpDeadline"/>, issue #153).
    /// </summary>
    /// <remarks>
    /// The condition decides only when to stop waiting, never what passes: a test still asserts on
    /// the returned events exactly as it did against a fixed count, so a real regression still fails
    /// — after the budget rather than immediately.
    /// </remarks>
    public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> PumpUntilDrained(
        Func<IReadOnlyList<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)>, bool> condition,
        TimeSpan? budget = null)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> all = [];
        PumpDeadline deadline = PumpDeadline.Start(budget);
        while (true)
        {
            Reactor.Pump();
            all.AddRange(DrainToList());
            if (condition(all) || deadline.Expired)
            {
                return all;
            }

            PumpDeadline.Yield();
        }
    }

    /// <summary>
    /// Pumps both ends alternately until <paramref name="condition"/> holds or <paramref name="budget"/>
    /// of real time is spent, accumulating what each end drains. A TCP handshake needs several round
    /// trips of the connect and the record exchange to settle, and delivery is not synchronous with a
    /// send, so the wait is bounded by a clock rather than a round count (<see cref="PumpDeadline"/>).
    /// </summary>
    public static (
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Dialler,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Accepter)
        PumpBothUntil(
            StreamSessionReactorHarness dialler,
            StreamSessionReactorHarness accepter,
            Func<bool> condition,
            TimeSpan? budget = null)
    {
        List<(SessionEventKind, PeerId, DisconnectReason)> diallerEvents = [];
        List<(SessionEventKind, PeerId, DisconnectReason)> accepterEvents = [];

        // The condition is asked once per round, after both ends have pumped, never before: several
        // callers phrase it as a DrainToList call, which consumes what it counts, so asking twice in a
        // round would swallow the very events the round produced.
        PumpDeadline deadline = PumpDeadline.Start(budget);
        while (true)
        {
            dialler.Reactor.Pump();
            diallerEvents.AddRange(dialler.DrainToList());
            accepter.Reactor.Pump();
            accepterEvents.AddRange(accepter.DrainToList());
            if (condition() || deadline.Expired)
            {
                return (diallerEvents, accepterEvents);
            }

            PumpDeadline.Yield();
        }
    }

    /// <summary>
    /// Pumps both ends alternately until <paramref name="condition"/> holds over what each has drained
    /// so far, or until <paramref name="budget"/> of real time is spent. The two-Reactor counterpart to
    /// <see cref="PumpUntilDrained"/>, and what a test asserting on a completed handshake wants:
    /// <see cref="PumpBothRounds"/> counts rounds and so waits for essentially no time at all
    /// (<see cref="PumpDeadline"/>, issue #153).
    /// </summary>
    public static (
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Dialler,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Accepter)
        PumpBothUntilDrained(
            StreamSessionReactorHarness dialler,
            StreamSessionReactorHarness accepter,
            Func<
                IReadOnlyList<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)>,
                IReadOnlyList<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)>,
                bool> condition,
            TimeSpan? budget = null)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> accepterEvents = [];

        PumpDeadline deadline = PumpDeadline.Start(budget);
        while (true)
        {
            dialler.Reactor.Pump();
            diallerEvents.AddRange(dialler.DrainToList());
            accepter.Reactor.Pump();
            accepterEvents.AddRange(accepter.DrainToList());
            if (condition(diallerEvents, accepterEvents) || deadline.Expired)
            {
                return (diallerEvents, accepterEvents);
            }

            PumpDeadline.Yield();
        }
    }

    /// <summary>
    /// Pumps both ends alternately until each has raised <see cref="SessionEventKind.PeerConnected"/>,
    /// which is what almost every two-Reactor test needs before it can assert anything: the handshake
    /// has completed on both sides and both ends' events are in hand.
    /// </summary>
    public static (
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Dialler,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Accepter)
        PumpBothUntilConnected(
            StreamSessionReactorHarness dialler,
            StreamSessionReactorHarness accepter,
            TimeSpan? budget = null) =>
        PumpBothUntilDrained(
            dialler,
            accepter,
            (diallerEvents, accepterEvents) =>
                diallerEvents.Any(e => e.Kind == SessionEventKind.PeerConnected)
                && accepterEvents.Any(e => e.Kind == SessionEventKind.PeerConnected),
            budget);

    /// <summary>
    /// Pumps both ends alternately exactly <paramref name="rounds"/> times, accumulating what each
    /// drains. For asserting that something does <em>not</em> happen; to wait for something that
    /// should, use <see cref="PumpBothUntilDrained"/>.
    /// </summary>
    public static (
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Dialler,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Accepter)
        PumpBothRounds(
            StreamSessionReactorHarness dialler,
            StreamSessionReactorHarness accepter,
            int rounds)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> diallerEvents = [];
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> accepterEvents = [];

        for (int round = 0; round < rounds; round++)
        {
            dialler.Reactor.Pump();
            diallerEvents.AddRange(dialler.DrainToList());
            accepter.Reactor.Pump();
            accepterEvents.AddRange(accepter.DrainToList());
        }

        return (diallerEvents, accepterEvents);
    }

    /// <summary>
    /// Pumps until <paramref name="condition"/> holds or <paramref name="budget"/> of real time is
    /// spent. For a condition read off something other than the drained events — a Sink, a counter —
    /// where <see cref="PumpUntilDrained"/> has nothing to hand the caller (<see cref="PumpDeadline"/>).
    /// </summary>
    public void PumpUntil(Func<bool> condition, TimeSpan? budget = null)
    {
        PumpDeadline deadline = PumpDeadline.Start(budget);
        while (true)
        {
            Reactor.Pump();
            if (condition() || deadline.Expired)
            {
                return;
            }

            PumpDeadline.Yield();
        }
    }

    /// <summary>
    /// Fills every one of this Reactor's capacity Slots with an established Peer: dials
    /// <see cref="Capacity"/> fresh <see cref="RawStreamPeer"/> counterparties against
    /// <see cref="Endpoint"/>, one at a time, pumping each to a completed handshake before dialing the
    /// next. Returns each held Peer's id paired with the raw counterparty socket occupying its Slot, so
    /// a follow-on test can interrupt, Goodbye, or reset any one of them — the caller owns disposing
    /// every counterparty this returns — along with the resume secret this Reactor announced to each,
    /// read out of the Hello it replied with, which is what a later Resume has to present to be
    /// matched back onto that Peer's Slot (issue #123).
    /// </summary>
    public List<HeldPeer> FillToCapacity()
    {
        List<HeldPeer> held = [];
        for (int i = 0; i < Capacity; i++)
        {
            held.Add(EstablishOnePeer());
        }

        return held;
    }

    /// <summary>
    /// Establishes one Peer against <see cref="Endpoint"/> and returns it paired with the raw
    /// counterparty holding its Slot and the resume secret this Reactor announced to it — the single
    /// step <see cref="FillToCapacity"/> repeats per Slot, for tests that want a Slot left free so a
    /// refusal cannot be mistaken for a capacity refusal (issue #124). The caller owns disposing the
    /// counterparty, exactly as with <see cref="FillToCapacity"/>.
    /// </summary>
    public HeldPeer EstablishOnePeer()
    {
        RawStreamPeer counterparty = new(Endpoint);
        PeerId id = StreamSessionTestRecords.EstablishSession(this, counterparty.Send);
        byte[] hello = counterparty.Receive(StreamSessionTestRecords.HandshakeRecordSize);
        return new HeldPeer(id, counterparty, StreamSessionTestRecords.ReadResumeSecretFromHello(hello));
    }

    /// <summary>
    /// Interrupts one Peer <see cref="FillToCapacity"/> established by resetting its counterparty
    /// socket abruptly. A transport loss without a Goodbye moves an established Peer to Interrupted
    /// rather than closing it (ADR 0010), so the Peer goes on holding its capacity Slot until its
    /// Linger expires. Pumps until this Reactor raises <see cref="SessionEventKind.PeerInterrupted"/>
    /// for that Peer, and returns everything drained on the way so a caller can assert on it. Throws
    /// if the interruption is not observed within <paramref name="budget"/>.
    /// </summary>
    public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> InterruptHeldPeer(
        HeldPeer held,
        TimeSpan? budget = null)
    {
        held.Counterparty.ResetAbruptly();

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = PumpUntilDrained(
            events => events.Any(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == held.Peer),
            budget);

        if (!drained.Any(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == held.Peer))
        {
            throw new InvalidOperationException(
                "The held Peer never moved to Interrupted within the Pump budget.");
        }

        return drained;
    }

    /// <summary>
    /// Dials one fresh <see cref="RawStreamPeer"/> against <see cref="Endpoint"/>, sends a default
    /// Hello, and pumps until the Reactor's reply reveals whether the connection was admitted (a Hello
    /// record, and a drained <see cref="SessionEventKind.PeerConnected"/> if one arrives within budget)
    /// or refused (a Refusal record naming the reason and the capacity figure it carries — see
    /// <c>StreamSessionReactor.RefuseForCapacity</c>). The returned <see cref="InboundAttemptResult"/>
    /// owns the raw counterparty; dispose it once a test is done inspecting the connection's own fate.
    /// </summary>
    /// <param name="resumeSecret">
    /// The secret the dial announces in its Hello, for the tests that need a late reconnection to
    /// carry a once-valid secret rather than the zeroed default (issue #124's second refusal path).
    /// Left null, the Hello announces a zeroed secret, which matches no reservation.
    /// </param>
    /// <param name="localAddress">The address the dial connects from, loopback when null.</param>
    public InboundAttemptResult AttemptFreshConnection(
        int maxPumps = 20,
        int perPumpTimeoutMs = 100,
        byte[]? resumeSecret = null,
        IPAddress? localAddress = null)
    {
        RawStreamPeer counterparty = new(Endpoint, localAddress);
        counterparty.Send(StreamSessionTestRecords.BuildHello(resumeSecret: resumeSecret));

        // Unlike the loops above, this one already waits on a clock rather than only on a Pump count:
        // each round blocks up to perPumpTimeoutMs for the reply, so the budget is maxPumps × that
        // rather than the microseconds a bare Pump count buys (PumpDeadline, issue #153).
        byte[]? reply = null;
        PeerId? connectedPeer = null;
        for (int i = 0; i < maxPumps && (reply is null || (reply[0] == StreamSessionTestRecords.HelloRecordType && connectedPeer is null)); i++)
        {
            Reactor.Pump();
            foreach ((SessionEventKind Kind, PeerId Peer, DisconnectReason Reason) evt in DrainToList())
            {
                if (evt.Kind == SessionEventKind.PeerConnected)
                {
                    connectedPeer = evt.Peer;
                }
            }

            if (reply is null && counterparty.TryReceiveWithinTimeout(StreamSessionTestRecords.HandshakeRecordSize, out byte[] record, perPumpTimeoutMs))
            {
                reply = record;
            }
        }

        if (reply is null)
        {
            counterparty.Dispose();
            throw new InvalidOperationException("The Reactor never replied to the fresh connection within the Pump budget.");
        }

        if (reply[0] == StreamSessionTestRecords.HelloRecordType)
        {
            return new InboundAttemptResult(Admitted: true, connectedPeer, RefusalReason: null, RefusalCapacity: null, counterparty);
        }

        byte reason = reply[1];
        uint capacity = BinaryPrimitives.ReadUInt32BigEndian(reply.AsSpan(2, 4));
        return new InboundAttemptResult(Admitted: false, Peer: null, reason, capacity, counterparty);
    }

    public void Dispose()
    {
        Reactor.Dispose();
        Entropy.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// One Peer occupying a capacity Slot that <see cref="FillToCapacity"/> established, paired with
    /// the raw counterparty socket holding it open. Interrupt, Goodbye, or reset
    /// <see cref="Counterparty"/> to exercise what happens to a held Slot; the other entries
    /// <see cref="FillToCapacity"/> returns alongside it just keep occupying theirs.
    /// </summary>
    /// <param name="ResumeSecret">
    /// The 16-byte secret this Reactor announced in the Hello it replied to this Peer with — the one a
    /// Resume must present to be matched back onto this Peer's reserved Slot (issue #123). Read off the
    /// wire, never out of the Reactor: how the secret is stored is not something a test may assert on.
    /// </param>
    public sealed record HeldPeer(PeerId Peer, RawStreamPeer Counterparty, byte[] ResumeSecret);

    /// <summary>
    /// The fate of a fresh inbound connection <see cref="AttemptFreshConnection"/> made against this
    /// Reactor: admitted and now a Peer named by <see cref="Peer"/>, or refused in-protocol naming
    /// <see cref="RefusalReason"/> and the <see cref="RefusalCapacity"/> figure the Refusal named.
    /// Disposing this disposes the raw counterparty socket the attempt used — inspect
    /// <see cref="Counterparty"/> (for example, a graceful close) before disposing.
    /// </summary>
    public sealed record InboundAttemptResult(
        bool Admitted,
        PeerId? Peer,
        byte? RefusalReason,
        uint? RefusalCapacity,
        RawStreamPeer Counterparty) : IDisposable
    {
        public void Dispose() => Counterparty.Dispose();
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// the Reactor has never seen: send, blocking receive, a bounded-timeout receive for polling a
    /// Reactor's reply across Pump calls, an abrupt reset, and a graceful half-close. Several stream
    /// test files still carry their own private equivalent — <c>StreamSessionInterruptedTests</c>'
    /// <c>InterruptedPeer</c>, <c>StreamSessionLingerExpiryTests</c>' <c>LingerPeer</c>,
    /// <c>StreamSessionHandshakeGuardTests</c>' <c>RawTcpPeer</c>. This is the shared one a new test
    /// should reach for, not a replacement those have been migrated onto.
    /// </summary>
    public sealed class RawStreamPeer : IDisposable
    {
        private readonly Socket _socket;

        /// <param name="destination">The Reactor's bound endpoint to connect to.</param>
        /// <param name="localAddress">
        /// The address to connect from, on an ephemeral port. Null leaves the choice to the OS, which
        /// for a loopback destination is loopback.
        /// </param>
        public RawStreamPeer(IPEndPoint destination, IPAddress? localAddress = null)
        {
            _socket = new Socket(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                ReceiveTimeout = 5000,
            };
            if (localAddress is not null)
            {
                _socket.Bind(new IPEndPoint(localAddress, 0));
            }

            _socket.Connect(destination);
        }

        /// <summary>The address and port this counterparty connected from.</summary>
        public IPEndPoint LocalEndPoint => (IPEndPoint)_socket.LocalEndPoint!;

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
        /// Reads <paramref name="count"/> bytes within <paramref name="timeoutMs"/>, answering whether
        /// it succeeded rather than throwing or blocking indefinitely — used to poll a Reactor's reply
        /// across repeated Pump calls without knowing in advance which Pump call will produce it.
        /// </summary>
        public bool TryReceiveWithinTimeout(int count, out byte[] record, int timeoutMs = 100)
        {
            _socket.ReceiveTimeout = timeoutMs;
            byte[] buffer = new byte[count];
            int totalRead = 0;
            try
            {
                while (totalRead < count)
                {
                    int read = _socket.Receive(buffer.AsSpan(totalRead));
                    if (read == 0)
                    {
                        record = [];
                        return false;
                    }

                    totalRead += read;
                }
            }
            catch (SocketException)
            {
                record = [];
                return false;
            }
            finally
            {
                _socket.ReceiveTimeout = 5000;
            }

            record = buffer;
            return true;
        }

        /// <summary>
        /// Whether this socket has something to read — bytes, or the end-of-stream a graceful close
        /// leaves behind. A non-blocking poll, so a test can Pump until the Reactor has actually closed
        /// the connection rather than guessing at a Pump count that might be short of it (issue #153,
        /// <see cref="PumpDeadline"/>).
        /// </summary>
        public bool HasPendingReadOrClose => _socket.Poll(0, SelectMode.SelectRead);

        /// <summary>Expects the remote to have closed gracefully: a further read reports end-of-stream, never a reset.</summary>
        public void ExpectGracefulClose()
        {
            byte[] buffer = new byte[1];
            int read = _socket.Receive(buffer);
            read.Should().Be(0);
        }

        /// <summary>
        /// Ends the connection gracefully — a bare FIN with nothing further on the wire, and no Goodbye
        /// sentinel unless a test sent one first.
        /// </summary>
        public void CloseGracefully() => _socket.Shutdown(SocketShutdown.Send);

        /// <summary>
        /// Kills the connection abruptly: <c>SO_LINGER</c> set to zero, then closed, which forces the OS
        /// to send a reset instead of an ordinary FIN.
        /// </summary>
        public void ResetAbruptly()
        {
            _socket.LingerState = new LingerOption(true, 0);
            _socket.Close();
        }

        public void Dispose() => _socket.Dispose();
    }
}
