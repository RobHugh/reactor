using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #326: a retention flush must not write to a socket that still owes the remainder of a
/// short-sent delivery-counter record, or its bytes land inside that record on the wire. The guard
/// that prevents it must never outlive the connection the record was begun on.
/// </summary>
/// <remarks>
/// <para>
/// <b>How the short send is produced.</b> Both ends of the transport are pinned small and the
/// counterparty never reads after the handshake, so a burst of Messages fills every buffer between
/// them, the same stimulus <c>StreamSessionRetentionFlushTests</c>' exhaustion test uses. A
/// counterparty Message then advances the Reactor's received count, and the delivery counter emitted
/// for it cannot be accepted, which leaves its whole record pending. Readiness only reports writable
/// for a socket a short send armed, and a socket nobody reads never becomes writable, so nothing
/// drains that remainder until the counterparty reads.
/// </para>
/// <para>
/// <b>How a write is observed.</b> A write the socket refuses puts nothing on the wire, so the flush
/// behind the pending record is read from <see cref="PeerCounters.SocketWrites"/> (issue #325), which
/// counts every send call however it ended.
/// </para>
/// <para>
/// The handshake half of the guard (<c>PendingHandshakeBytesToSend</c>) has no test here. A Hello is
/// the first write on a fresh connection, so no buffer can be full enough to short-send it.
/// </para>
/// </remarks>
public class StreamSessionPendingRecordFlushTests
{
    private const int RecordSize = StreamSessionTestRecords.HandshakeRecordSize;

    private const int DeliveryCounterRecordSize = 12;

    private const int MaxMessageSize = 8192;

    private const int FillMessageCount = 100;

    private const int PinnedBufferSize = 1024;

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
        using Counterparty peer = Counterparty.Connect(accepter.Endpoint, PinnedBufferSize);
        PeerId id = EstablishAcceptedSession(accepter, peer);
        FillTransport(accepter, id);
        LeaveADeliveryCounterPending(accepter, peer, id);
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
    public void Pump_AfterADrainingDeliveryCounterRemainderClears_FlushesEveryRetainedByte()
    {
        // Arrange — the same stall as above, with a Message queued behind the pending record.
        using StreamSessionReactorHarness accepter = CreateReactor();
        using Counterparty peer = Counterparty.Connect(accepter.Endpoint, PinnedBufferSize);
        PeerId id = EstablishAcceptedSession(accepter, peer);
        FillTransport(accepter, id);
        LeaveADeliveryCounterPending(accepter, peer, id);

        byte[] queuedBehindRecord = PayloadOf(16, 7);
        accepter.Reactor.Send(id, queuedBehindRecord).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        accepter.DrainToList();

        int expectedBytes = (FillMessageCount * FillFrame.Length)
            + DeliveryCounterRecordSize
            + BuildFrame(queuedBehindRecord).Length;

        // Act — the counterparty starts reading, which reopens the window for the pending record and
        // then for the retention the guard held back.
        byte[] received = PumpAndRead(accepter, peer, expectedBytes);

        // Assert
        received.Should().HaveCount(
            expectedBytes,
            "a flush the guard skipped is marked pending again once the record drains, so no retained byte is stranded");
    }

    [Fact]
    public void Pump_DiallingSideResumeAfterLosingAConnectionWithADeliveryCounterPending_FlushesRetentionToTheNewConnection()
    {
        // Arrange — a dialled Session whose delivery counter is left pending, then a reset that loses
        // that connection before the remainder can drain.
        using StreamSessionReactorHarness dialer = CreateReactor();
        using StalledListener listener = new(PinnedBufferSize);
        PeerId id = dialer.Reactor.Connect(listener.Endpoint);
        using Counterparty far = listener.Accept();
        EstablishDialledSession(dialer, far, id);
        FillTransport(dialer, id);
        LeaveADeliveryCounterPending(dialer, far, id);

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
        sendBufferSize: PinnedBufferSize);

    /// <summary>
    /// Establishes the Session and reads the accepter's own Hello off <paramref name="peer"/>'s socket,
    /// so only post-handshake bytes remain for the counterparty to read.
    /// </summary>
    private static PeerId EstablishAcceptedSession(StreamSessionReactorHarness accepter, Counterparty peer)
    {
        PeerId id = StreamSessionTestRecords.EstablishSession(accepter, peer.Send);
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
        far.Send(StreamSessionTestRecords.BuildHello());

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
    /// Queues and flushes far more than the pinned transport buffers hold, with nothing reading, so the
    /// Reactor's socket refuses further writes. Retention is sized to hold every Message twice over,
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
    /// Delivers one counterparty Message and pumps until the Reactor has read it. No Send is queued in
    /// those Pumps, so the counter has no retention write to ride and is sent alone into the full
    /// socket, which leaves its remainder pending.
    /// </summary>
    private static void LeaveADeliveryCounterPending(StreamSessionReactorHarness reactor, Counterparty counterparty, PeerId id)
    {
        counterparty.Send(BuildFrame(PayloadOf(16, 5)));

        PumpDeadline deadline = PumpDeadline.Start();
        while (reactor.Reactor.CountersOf(id).MessagesReceived == 0)
        {
            if (deadline.Expired)
            {
                throw new InvalidOperationException("The counterparty's Message was never delivered within the Pump budget.");
            }

            reactor.Reactor.Pump();
            reactor.DrainToList();
            PumpDeadline.Yield();
        }
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

    private static Counterparty AcceptRedial(StreamSessionReactorHarness dialer, StalledListener listener)
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
    /// A plain BCL TCP socket standing in for the far end of a Session, either dialling the Reactor
    /// under test or accepted from its re-dial. Reads nothing unless a test asks it to.
    /// </summary>
    private sealed class Counterparty(Socket socket) : IDisposable
    {
        /// <summary>
        /// Connects to <paramref name="destination"/> with <paramref name="receiveBufferSize"/> applied
        /// before <c>Connect</c> (commit 8426ff3).
        /// </summary>
        public static Counterparty Connect(IPEndPoint destination, int receiveBufferSize)
        {
            Socket socket = new(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                ReceiveTimeout = 5000,
                ReceiveBufferSize = receiveBufferSize,
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
    /// A listener whose accepted connections inherit a small receive buffer, set before <c>Listen</c>
    /// so the window is small from the handshake onward. The shared <c>RawStreamListener</c> has no
    /// such setting.
    /// </summary>
    private sealed class StalledListener : IDisposable
    {
        private readonly Socket _listener;

        public StalledListener(int receiveBufferSize)
        {
            _listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            {
                ReceiveBufferSize = receiveBufferSize,
            };
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
