using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A retention flush must not write to a socket that still owes the remainder of a
/// short-sent delivery-counter record, or its bytes land inside that record on the wire. The guard
/// that prevents it must never outlive the connection the record was begun on.
/// </summary>
/// <remarks>
/// <para>
/// <b>How the short send is produced.</b> The Reactor's send buffer is pinned small and the
/// counterparty never reads after the handshake, so a burst of Messages fills the transport. The
/// clock is then advanced one send-idle interval per Pump, with nothing queued, so each Pump owes a
/// heartbeat delivery counter (issue #114). A heartbeat the socket still has room for takes a write. One
/// the full socket refuses leaves its record pending, and every heartbeat after that returns without a
/// write, because <c>SendDeliveryCounterOrArm</c> never starts a record over a pending one. A heartbeat
/// Pump that takes no write is therefore proof the remainder is pending, on any platform and whether
/// or not the flush guard exists.
/// </para>
/// <para>
/// A fixed stimulus is not enough. On Linux, bytes keep moving from the Reactor's send buffer to the
/// counterparty after a Pump returns, freeing room for a 12-byte record that a Windows socket refuses.
/// The counterparty's receive buffer is left at the OS default for a related reason: pinned small on
/// Linux, it lets bytes trickle across while nothing reads, and slows the later drain to a few hundred
/// bytes per 250ms.
/// </para>
/// <para>
/// <b>How a write is observed.</b> A write the socket refuses puts nothing on the wire, so writes are
/// read from <see cref="PeerCounters.SocketWrites"/> (issue #325), which counts every send call however
/// it ended.
/// </para>
/// <para>
/// The handshake half of the guard (<c>PendingHandshakeBytesToSend</c>) has no test here. A Hello is
/// the first write on a fresh connection, so no buffer can be full enough to short-send it.
/// </para>
/// </remarks>
public class StreamSessionPendingRecordFlushTests
{
    private const int RecordSize = StreamSessionTestRecords.HandshakeRecordSize;

    private const int MaxMessageSize = 8192;

    private const int FillMessageCount = 100;

    private const int ReactorSendBufferSize = 1024;

    /// <summary>
    /// How many heartbeat Pumps <see cref="LeaveADeliveryCounterPending"/> tries before giving up. Each
    /// one advances the clock a whole send-idle interval.
    /// </summary>
    private const int MaxHeartbeatAttempts = 50;

    /// <summary>
    /// Keeps the receive-idle deadline beyond every heartbeat attempt, so the Peer is never interrupted
    /// while the counterparty sends nothing.
    /// </summary>
    private const int MissedHeartbeats = 1000;

    /// <summary>The clock step used while waiting for an interruption and its re-dial.</summary>
    private static readonly TimeSpan _cadence = TimeSpan.FromMilliseconds(50);

    private static byte[] BuildFrame(byte[] payload) => StreamSessionTestRecords.BuildFrame(payload);

    private static byte[] PayloadOf(int length, byte marker) => StreamSessionTestRecords.PayloadOf(length, marker);

    private static byte[] FillFrame => BuildFrame(PayloadOf(MaxMessageSize, 1));

    [Fact]
    public void Pump_RetentionDueWhileADeliveryCounterRemainderIsDraining_TakesNoSocketWrite()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = CreateReactor();
        using Counterparty peer = Counterparty.Connect(accepter.Endpoint);
        PeerId id = EstablishAcceptedSession(accepter, peer);
        FillTransport(accepter, id);
        LeaveADeliveryCounterPending(accepter, id);
        ulong writesBefore = accepter.Reactor.CountersOf(id).SocketWrites;

        // Act
        accepter.Reactor.Send(id, PayloadOf(16, 7)).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        accepter.DrainToList();

        // Assert
        (accepter.Reactor.CountersOf(id).SocketWrites - writesBefore).Should().Be(
            0,
            "retention written now would land inside the delivery-counter record still owed to the wire");
    }

    [Fact]
    public void Pump_AfterADrainingDeliveryCounterRemainderClears_FlushesTheRetentionItHeldBack()
    {
        // Arrange — the same stall as above, with a Message queued behind the pending record.
        using StreamSessionReactorHarness accepter = CreateReactor();
        using Counterparty peer = Counterparty.Connect(accepter.Endpoint);
        PeerId id = EstablishAcceptedSession(accepter, peer);
        FillTransport(accepter, id);
        LeaveADeliveryCounterPending(accepter, id);

        byte[] queuedBehindRecord = BuildFrame(PayloadOf(16, 7));
        accepter.Reactor.Send(id, PayloadOf(16, 7)).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        accepter.DrainToList();

        // Act — the counterparty starts reading, which reopens the window for the pending record and
        // then for the retention the guard held back.
        byte[] received = PumpAndReadUntilEndsWith(accepter, peer, queuedBehindRecord);

        // Assert — the Message queued behind the record is the last retention this end owes, so it
        // arriving last means the skipped flush resumed. How many heartbeat records reached the wire
        // before the socket filled is platform timing, so the stream's length is not asserted.
        received.TakeLast(queuedBehindRecord.Length).Should().Equal(
            queuedBehindRecord,
            "a flush the guard skipped is marked pending again once the record drains, so no retained byte is stranded");
    }

    [Fact]
    public void Pump_DiallingSideResumeAfterLosingAConnectionWithADeliveryCounterPending_FlushesRetentionToTheNewConnection()
    {
        // Arrange — a dialled Session whose delivery counter is left pending, then a reset that loses
        // that connection before the remainder can drain.
        using StreamSessionReactorHarness dialer = CreateReactor();
        using CounterpartyListener listener = new();
        PeerId id = dialer.Reactor.Connect(listener.Endpoint);
        using Counterparty far = listener.Accept();
        EstablishDialledSession(dialer, far, id);
        FillTransport(dialer, id);
        LeaveADeliveryCounterPending(dialer, id);

        far.ResetAbruptly();
        PumpUntilInterrupted(dialer, id);
        using Counterparty redialed = AcceptRedial(dialer, listener);
        PumpAndRead(dialer, redialed, RecordSize).Should().HaveCount(RecordSize, "the re-dial presents its Resume record first");

        // Act — the far end's first post-resume counter reports none of this end's Messages received,
        // so the whole ring is rewound and due to be resent on the new connection.
        redialed.Send(StreamSessionTestRecords.BuildDeliveryCounterRecord(0));
        byte[] received = PumpAndRead(dialer, redialed, FillFrame.Length);

        // Assert
        received.Should().Equal(
            FillFrame,
            "a remainder begun on the lost connection must not keep blocking retention on the new one");
    }

    private static StreamSessionReactorHarness CreateReactor() => new(
        maxMessageSize: MaxMessageSize,
        retentionBytes: 2 * FillMessageCount * (MaxMessageSize + 4),
        sendBufferSize: ReactorSendBufferSize,
        missedHeartbeats: MissedHeartbeats);

    /// <summary>
    /// Establishes the Session and reads the accepter's own Hello off <paramref name="peer"/>'s socket,
    /// so only post-handshake bytes remain for the counterparty to read.
    /// </summary>
    private static PeerId EstablishAcceptedSession(StreamSessionReactorHarness accepter, Counterparty peer)
    {
        PeerId id = StreamSessionTestRecords.EstablishSession(accepter, peer.Send, announcedMaxMessageSize: MaxMessageSize);
        peer.Receive(RecordSize);
        return id;
    }

    /// <summary>
    /// Reads the dialler's Hello off <paramref name="far"/>, replies with a Hello of its own, and pumps
    /// until <paramref name="id"/> is connected.
    /// </summary>
    private static void EstablishDialledSession(StreamSessionReactorHarness dialer, Counterparty far, PeerId id)
    {
        PumpAndRead(dialer, far, RecordSize).Should().HaveCount(RecordSize, "the dialler sends its Hello once connected");
        far.Send(StreamSessionTestRecords.BuildHello(maxMessageSize: MaxMessageSize));

        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            dialer.Reactor.Pump();
            if (dialer.DrainToList().Any(e => e.Kind == SessionEventKind.PeerConnected && e.Peer == id))
            {
                return;
            }

            if (deadline.Expired)
            {
                throw new InvalidOperationException("The Session never established within the Pump budget.");
            }

            PumpDeadline.Yield();
        }
    }

    /// <summary>
    /// Queues and flushes far more than the counterparty's receive buffer and the Reactor's pinned send
    /// buffer hold together, with nothing reading. Retention is sized to hold every Message twice over,
    /// so the ring never fills.
    /// </summary>
    private static void FillTransport(StreamSessionReactorHarness reactor, PeerId id)
    {
        byte[] message = PayloadOf(MaxMessageSize, 1);
        for (int i = 0; i < FillMessageCount; i++)
        {
            reactor.Reactor.Send(id, message).Should().Be(SendOutcome.Sent);
            reactor.Reactor.Pump();
            reactor.DrainToList();
        }
    }

    /// <summary>
    /// Advances the clock one send-idle interval per Pump, with nothing queued, until a Pump that owes
    /// a heartbeat takes no socket write — the proof, described in the class remarks, that a
    /// delivery-counter remainder is pending. Throws if the Peer is interrupted, since an Interrupted
    /// Peer also takes no write, or if no heartbeat is ever refused.
    /// </summary>
    private static void LeaveADeliveryCounterPending(StreamSessionReactorHarness reactor, PeerId id)
    {
        for (int attempt = 0; attempt < MaxHeartbeatAttempts; attempt++)
        {
            ulong writesBefore = reactor.Reactor.CountersOf(id).SocketWrites;
            reactor.TimeProvider.Advance(reactor.SendIdleInterval);
            reactor.Reactor.Pump();
            if (reactor.DrainToList().Any(e => e.Kind is SessionEventKind.PeerInterrupted or SessionEventKind.PeerClosed))
            {
                throw new InvalidOperationException("The Peer was lost while waiting for a heartbeat to be refused.");
            }

            if (reactor.Reactor.CountersOf(id).SocketWrites == writesBefore)
            {
                return;
            }
        }

        throw new InvalidOperationException(
            $"No heartbeat was refused within {MaxHeartbeatAttempts} attempts, so the transport never filled.");
    }

    private static void PumpUntilInterrupted(StreamSessionReactorHarness dialer, PeerId id)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            dialer.TimeProvider.Advance(_cadence);
            dialer.Reactor.Pump();
            if (dialer.DrainToList().Any(e => e.Kind == SessionEventKind.PeerInterrupted && e.Peer == id))
            {
                return;
            }

            if (deadline.Expired)
            {
                throw new InvalidOperationException("The reset was never observed as an interruption within the Pump budget.");
            }

            PumpDeadline.Yield();
        }
    }

    private static Counterparty AcceptRedial(StreamSessionReactorHarness dialer, CounterpartyListener listener)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            dialer.TimeProvider.Advance(_cadence);
            dialer.Reactor.Pump();
            dialer.DrainToList();
            if (listener.TryAccept(out Counterparty? redialed, timeoutMs: 50))
            {
                return redialed;
            }

            if (deadline.Expired)
            {
                throw new InvalidOperationException("The re-dial never reached the listener within the Pump budget.");
            }
        }
    }

    /// <summary>
    /// Alternates Pumps with non-blocking reads until <paramref name="count"/> bytes have arrived or the
    /// Pump budget runs out, returning at most <paramref name="count"/> of whatever arrived so a caller
    /// asserts on a short result rather than a timeout exception.
    /// </summary>
    private static byte[] PumpAndRead(StreamSessionReactorHarness reactor, Counterparty counterparty, int count)
    {
        List<byte> received = [];
        PumpDeadline deadline = PumpDeadline.Start();
        while (received.Count < count && !deadline.Expired)
        {
            reactor.Reactor.Pump();
            reactor.DrainToList();
            counterparty.ReadAvailable(received);
            PumpDeadline.Yield();
        }

        return [.. received.Take(count)];
    }

    /// <summary>
    /// Alternates Pumps with non-blocking reads until the bytes received end with
    /// <paramref name="tail"/> or the Pump budget runs out, returning everything received.
    /// </summary>
    private static byte[] PumpAndReadUntilEndsWith(StreamSessionReactorHarness reactor, Counterparty counterparty, byte[] tail)
    {
        List<byte> received = [];
        PumpDeadline deadline = PumpDeadline.Start();
        while (!deadline.Expired)
        {
            reactor.Reactor.Pump();
            reactor.DrainToList();
            counterparty.ReadAvailable(received);
            if (received.Count >= tail.Length
                && received.Skip(received.Count - tail.Length).SequenceEqual(tail))
            {
                break;
            }

            PumpDeadline.Yield();
        }

        return [.. received];
    }

    /// <summary>
    /// A plain BCL TCP socket standing in for the far end of a Session, either dialling the Reactor
    /// under test or accepted from its dial. Reads nothing unless a test asks it to.
    /// </summary>
    private sealed class Counterparty(Socket socket) : IDisposable
    {
        public static Counterparty Connect(IPEndPoint destination)
        {
            Socket socket = new(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                ReceiveTimeout = 5000,
            };
            socket.Connect(destination);
            return new Counterparty(socket);
        }

        public void Send(byte[] bytes) => socket.Send(bytes);

        /// <summary>Blocks until exactly <paramref name="count"/> bytes have been read off the socket.</summary>
        public byte[] Receive(int count)
        {
            byte[] buffer = new byte[count];
            int totalRead = 0;
            while (totalRead < count)
            {
                int read = socket.Receive(buffer.AsSpan(totalRead));
                if (read == 0)
                {
                    throw new InvalidOperationException("The Reactor closed the connection before sending everything expected.");
                }

                totalRead += read;
            }

            return buffer;
        }

        /// <summary>Appends every byte already waiting on the socket to <paramref name="into"/> without blocking.</summary>
        public void ReadAvailable(List<byte> into)
        {
            byte[] scratch = new byte[64 * 1024];
            while (socket.Available > 0)
            {
                int read = socket.Receive(scratch);
                into.AddRange(scratch.AsSpan(0, read));
            }
        }

        /// <summary>Closes with a zero linger, so the Reactor sees a reset rather than a graceful close.</summary>
        public void ResetAbruptly()
        {
            socket.LingerState = new LingerOption(true, 0);
            socket.Close();
        }

        public void Dispose() => socket.Dispose();
    }

    /// <summary>
    /// A loopback listener whose accepted connections are <see cref="Counterparty"/> instances, standing
    /// in for the far end of a Session the Reactor dials. The shared <c>RawStreamListener</c> has no
    /// non-blocking read, which the drain loops here need.
    /// </summary>
    private sealed class CounterpartyListener : IDisposable
    {
        private readonly Socket _listener;

        public CounterpartyListener()
        {
            _listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            _listener.Listen();
            Endpoint = (IPEndPoint?)_listener.LocalEndPoint
                ?? throw new InvalidOperationException("A bound listener has no local endpoint.");
        }

        public IPEndPoint Endpoint { get; }

        public Counterparty Accept()
        {
            if (!TryAccept(out Counterparty? accepted, timeoutMs: 5000))
            {
                throw new InvalidOperationException("No connection arrived within the timeout.");
            }

            return accepted;
        }

        public bool TryAccept([System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out Counterparty? accepted, int timeoutMs)
        {
            if (_listener.Poll(timeoutMs * 1000, SelectMode.SelectRead))
            {
                Socket socket = _listener.Accept();
                socket.ReceiveTimeout = 5000;
                accepted = new Counterparty(socket);
                return true;
            }

            accepted = null;
            return false;
        }

        public void Dispose() => _listener.Dispose();
    }
}
