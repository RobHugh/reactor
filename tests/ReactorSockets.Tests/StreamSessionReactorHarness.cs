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
        RandomNumberGenerator? entropy = null)
    {
        Capacity = capacity;
        SendIdleInterval = sendIdleInterval ?? TimeSpan.FromSeconds(1);
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
            ReceiveBufferSize = receiveBufferSize,
            SendBufferSize = sendBufferSize,
            MissedHeartbeats = missedHeartbeats,
            SendIdleInterval = SendIdleInterval,
            Linger = TimeSpan.FromSeconds(1),
            Endpoints = endpoints ?? (bindLoopback ? [new IPEndPoint(IPAddress.Loopback, 0)] : []),
            Entropy = Entropy,
        });
    }

    /// <summary>The capacity this Reactor was built with — the number of Slots <see cref="FillToCapacity"/> occupies.</summary>
    public int Capacity { get; }

    /// <summary>The clock the Reactor was built with — advance it to exercise anything time-dependent.</summary>
    public FakeTimeProvider TimeProvider { get; } = new();

    /// <summary>
    /// The <see cref="StreamSessionOptions.SendIdleInterval"/> the Reactor was built with — 1 second
    /// unless the constructor's <c>sendIdleInterval</c> parameter overrides it. Exposed so a test can
    /// advance <see cref="TimeProvider"/> by exactly this much without duplicating the constant.
    /// </summary>
    public TimeSpan SendIdleInterval { get; }

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

    /// <summary>Pumps exactly <paramref name="pumps"/> times, accumulating everything drained.</summary>
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
    /// Pumps both ends alternately until <paramref name="condition"/> holds or
    /// <paramref name="maxRounds"/> is spent, accumulating what each end drains. A TCP handshake needs
    /// several round trips of the connect and the record exchange to settle, and delivery is not
    /// synchronous with a send, so the number of rounds is bounded rather than exact.
    /// </summary>
    public static (
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Dialler,
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> Accepter)
        PumpBothUntil(
            StreamSessionReactorHarness dialler,
            StreamSessionReactorHarness accepter,
            Func<bool> condition,
            int maxRounds = 20)
    {
        List<(SessionEventKind, PeerId, DisconnectReason)> diallerEvents = [];
        List<(SessionEventKind, PeerId, DisconnectReason)> accepterEvents = [];

        for (int round = 0; round < maxRounds && !condition(); round++)
        {
            dialler.Reactor.Pump();
            diallerEvents.AddRange(dialler.DrainToList());
            accepter.Reactor.Pump();
            accepterEvents.AddRange(accepter.DrainToList());
        }

        return (diallerEvents, accepterEvents);
    }

    /// <summary>Pumps up to <paramref name="maxPumps"/> times, stopping as soon as <paramref name="condition"/> holds.</summary>
    public void PumpUntil(Func<bool> condition, int maxPumps = 20)
    {
        for (int i = 0; i < maxPumps && !condition(); i++)
        {
            Reactor.Pump();
        }
    }

    /// <summary>
    /// Fills every one of this Reactor's capacity Slots with an established Peer: dials
    /// <see cref="Capacity"/> fresh <see cref="RawStreamPeer"/> counterparties against
    /// <see cref="Endpoint"/>, one at a time, pumping each to a completed handshake before dialing the
    /// next. Returns each held Peer's id paired with the raw counterparty socket occupying its Slot, so
    /// a follow-on test can interrupt, Goodbye, or reset any one of them — the caller owns disposing
    /// every counterparty this returns.
    /// </summary>
    public List<HeldPeer> FillToCapacity()
    {
        List<HeldPeer> held = [];
        for (int i = 0; i < Capacity; i++)
        {
            RawStreamPeer counterparty = new(Endpoint);
            PeerId id = StreamSessionTestRecords.EstablishSession(this, counterparty.Send);
            counterparty.Receive(StreamSessionTestRecords.HandshakeRecordSize);
            held.Add(new HeldPeer(id, counterparty));
        }

        return held;
    }

    /// <summary>
    /// Interrupts one Peer <see cref="FillToCapacity"/> established by resetting its counterparty
    /// socket abruptly. A transport loss without a Goodbye moves an established Peer to Interrupted
    /// rather than closing it (ADR 0010), so the Peer goes on holding its capacity Slot until its
    /// Linger expires. Pumps until this Reactor raises <see cref="SessionEventKind.PeerInterrupted"/>
    /// for that Peer, and returns everything drained on the way so a caller can assert on it. Throws
    /// if the interruption is not observed within <paramref name="maxPumps"/>.
    /// </summary>
    public List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> InterruptHeldPeer(
        HeldPeer held,
        int maxPumps = 20)
    {
        held.Counterparty.ResetAbruptly();

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        bool Interrupted() => drained.Any(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == held.Peer);

        for (int i = 0; i < maxPumps && !Interrupted(); i++)
        {
            Reactor.Pump();
            drained.AddRange(DrainToList());
        }

        if (!Interrupted())
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
    public InboundAttemptResult AttemptFreshConnection(int maxPumps = 20, int perPumpTimeoutMs = 100)
    {
        RawStreamPeer counterparty = new(Endpoint);
        counterparty.Send(StreamSessionTestRecords.BuildHello());

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
    public sealed record HeldPeer(PeerId Peer, RawStreamPeer Counterparty);

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
        public RawStreamPeer(IPEndPoint destination)
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
