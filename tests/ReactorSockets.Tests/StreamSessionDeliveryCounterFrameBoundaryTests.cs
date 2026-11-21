using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A delivery-counter record must never land inside a Message frame that a short retention
/// write left half-sent, on any path that emits one: the standalone send and the ride on a retention
/// write (issue #132) when the received count advances, and the send-idle heartbeat (issue #114).
/// </summary>
/// <remarks>
/// <para>
/// <b>How the half-sent frame is produced.</b> The Reactor's send buffer and the counterparty's receive
/// buffer are both pinned small, and the counterparty never reads after the handshake, so the first
/// maximum-size Message is larger than the transport holds and its retention write stops partway through
/// the frame. The counterparty's receive buffer is grown again before it drains, since a pinned one
/// drains a few hundred bytes per 250ms on Linux.
/// </para>
/// <para>
/// A Windows loopback socket accepts a non-blocking send whole or not at all, so the frame is never split
/// there and these tests pass on Windows with or without the fix. They fail without it on Linux.
/// </para>
/// <para>
/// <b>How a misplaced record is observed.</b> The counterparty then reads the whole stream and parses
/// it the way a far-end Reactor would. Every Message this end sends has a payload of one repeated
/// marker byte, so a record written inside a frame shows up as a payload that is not uniform, and
/// every frame boundary after it as a length no Message has.
/// </para>
/// </remarks>
public class StreamSessionDeliveryCounterFrameBoundaryTests
{
    private const int RecordSize = StreamSessionTestRecords.HandshakeRecordSize;

    private const int DeliveryCounterRecordSize = 12;

    private const int MaxMessageSize = 8192;

    private const int FillMessageCount = 4;

    private const int PinnedBufferSize = 1024;

    private const int DrainReceiveBufferSize = 1024 * 1024;

    private const byte FillMarker = 1;

    private const byte RideMarker = 8;

    private const byte TrailingMarker = 7;

    private static byte[] BuildFrame(byte[] payload) => StreamSessionTestRecords.BuildFrame(payload);

    private static byte[] PayloadOf(int length, byte marker) => StreamSessionTestRecords.PayloadOf(length, marker);

    [Fact]
    public void Pump_ReceivedCountAdvancesWhileAFrameIsHalfSent_KeepsTheStandaloneDeliveryCounterOutOfThatFrame()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = CreateReactor();
        using Counterparty peer = Counterparty.Connect(accepter.Endpoint);
        PeerId id = EstablishAcceptedSession(accepter, peer);
        FillTransport(accepter, id);

        // Act — nothing is queued while the received count advances, so the counter has no write to ride.
        peer.Send(BuildFrame(PayloadOf(16, 9)));
        PumpUntilReceived(accepter, id, queueEachPump: false);
        accepter.Reactor.Send(id, PayloadOf(16, TrailingMarker)).Should().Be(SendOutcome.Sent);
        WireParse wire = DrainAndParse(accepter, peer);

        // Assert
        wire.Malformed.Should().BeNull("a record written behind a half-sent frame corrupts every frame boundary after it");
    }

    [Fact]
    public void Pump_ReceivedCountAdvancesWithASendQueuedWhileAFrameIsHalfSent_KeepsTheRiddenDeliveryCounterOutOfThatFrame()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = CreateReactor();
        using Counterparty peer = Counterparty.Connect(accepter.Endpoint);
        PeerId id = EstablishAcceptedSession(accepter, peer);
        FillTransport(accepter, id);

        // Act — a Send before every Pump, so whichever Pump reads the Message already owes a retention write.
        peer.Send(BuildFrame(PayloadOf(16, 9)));
        PumpUntilReceived(accepter, id, queueEachPump: true);
        accepter.Reactor.Send(id, PayloadOf(16, TrailingMarker)).Should().Be(SendOutcome.Sent);
        WireParse wire = DrainAndParse(accepter, peer);

        // Assert
        wire.Malformed.Should().BeNull("a record leading the gather write lands ahead of the half-sent frame's unsent tail");
    }

    [Fact]
    public void Pump_DeliveryCounterHeldBackByAHalfSentFrame_ReachesTheWireOnceTheFrameDrains()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = CreateReactor();
        using Counterparty peer = Counterparty.Connect(accepter.Endpoint);
        PeerId id = EstablishAcceptedSession(accepter, peer);
        FillTransport(accepter, id);

        // Act
        peer.Send(BuildFrame(PayloadOf(16, 9)));
        PumpUntilReceived(accepter, id, queueEachPump: false);
        accepter.Reactor.Send(id, PayloadOf(16, TrailingMarker)).Should().Be(SendOutcome.Sent);
        WireParse wire = DrainAndParse(accepter, peer);

        // Assert
        wire.CounterCounts.Should().Contain(
            1UL,
            "a counter that waits for a frame boundary is delayed, never dropped");
    }

    [Fact]
    public void Pump_SendIdleIntervalElapsesWhileAFrameIsHalfSent_KeepsTheHeartbeatDeliveryCounterOutOfThatFrame()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = CreateReactor();
        using Counterparty peer = Counterparty.Connect(accepter.Endpoint);
        PeerId id = EstablishAcceptedSession(accepter, peer);
        FillTransport(accepter, id);

        // Act — nothing is written for a whole send-idle interval, so the next Pump owes a heartbeat.
        accepter.TimeProvider.Advance(accepter.SendIdleInterval);
        accepter.Reactor.Pump();
        accepter.DrainToList();
        accepter.Reactor.Send(id, PayloadOf(16, TrailingMarker)).Should().Be(SendOutcome.Sent);
        WireParse wire = DrainAndParse(accepter, peer, awaitCounter: false);

        // Assert
        wire.Malformed.Should().BeNull("a heartbeat written behind a half-sent frame corrupts every frame boundary after it");
    }

    private static StreamSessionReactorHarness CreateReactor() => new(
        maxMessageSize: MaxMessageSize,
        retentionBytes: 2 * FillMessageCount * (MaxMessageSize + 4),
        sendBufferSize: PinnedBufferSize,
        missedHeartbeats: 1000);

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
    /// Queues and flushes maximum-size Messages, each larger than both pinned buffers hold together, with
    /// nothing reading, so a retention write stops partway through a frame.
    /// </summary>
    private static void FillTransport(StreamSessionReactorHarness reactor, PeerId id)
    {
        byte[] message = PayloadOf(MaxMessageSize, FillMarker);
        for (int i = 0; i < FillMessageCount; i++)
        {
            reactor.Reactor.Send(id, message).Should().Be(SendOutcome.Sent);
            reactor.Reactor.Pump();
            reactor.DrainToList();
        }
    }

    /// <summary>
    /// Pumps until the counterparty's Message has been delivered, optionally queueing a small Send
    /// before every Pump so the delivering Pump owes a retention write.
    /// </summary>
    private static void PumpUntilReceived(StreamSessionReactorHarness reactor, PeerId id, bool queueEachPump)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        while (reactor.Reactor.CountersOf(id).MessagesReceived == 0)
        {
            if (deadline.Expired)
            {
                throw new InvalidOperationException("The counterparty's Message was never delivered within the Pump budget.");
            }

            if (queueEachPump)
            {
                reactor.Reactor.Send(id, PayloadOf(16, RideMarker)).Should().Be(SendOutcome.Sent);
            }

            reactor.Reactor.Pump();
            reactor.DrainToList();
            PumpDeadline.Yield();
        }
    }

    /// <summary>
    /// Alternates Pumps with non-blocking reads, parsing the stream as it grows, until it turns out
    /// malformed, or the trailing Message has arrived (and a delivery counter too, when
    /// <paramref name="awaitCounter"/> is set), or the Pump budget runs out.
    /// </summary>
    private static WireParse DrainAndParse(StreamSessionReactorHarness reactor, Counterparty counterparty, bool awaitCounter = true)
    {
        counterparty.GrowReceiveBuffer(DrainReceiveBufferSize);
        List<byte> received = [];
        WireParse parse = Parse(received);
        PumpDeadline deadline = PumpDeadline.Start();
        while (!deadline.Expired)
        {
            reactor.Reactor.Pump();
            reactor.DrainToList();
            counterparty.ReadAvailable(received);
            parse = Parse(received);
            if (parse.Malformed is not null || (parse.SawTrailingMessage && (!awaitCounter || parse.CounterCounts.Count > 0)))
            {
                break;
            }

            PumpDeadline.Yield();
        }

        return parse;
    }

    /// <summary>
    /// Parses every whole record in <paramref name="stream"/> as a far-end Reactor would: a delivery
    /// counter by its sentinel, otherwise a Message whose payload must be one repeated marker byte.
    /// A partial record at the end is not yet an error.
    /// </summary>
    private static WireParse Parse(List<byte> stream)
    {
        byte[] bytes = [.. stream];
        List<ulong> counterCounts = [];
        bool sawTrailingMessage = false;
        int offset = 0;
        while (bytes.Length - offset >= 4)
        {
            uint prefix = BinaryPrimitives.ReadUInt32BigEndian(bytes.AsSpan(offset, 4));
            if (prefix == StreamSessionTestRecords.DeliveryCounterSentinel)
            {
                if (bytes.Length - offset < DeliveryCounterRecordSize)
                {
                    break;
                }

                counterCounts.Add(BinaryPrimitives.ReadUInt64BigEndian(bytes.AsSpan(offset + 4, 8)));
                offset += DeliveryCounterRecordSize;
                continue;
            }

            if (prefix is 0 or > MaxMessageSize)
            {
                return new WireParse($"length prefix {prefix} at offset {offset} is no Message", counterCounts, sawTrailingMessage);
            }

            if (bytes.Length - offset - 4 < prefix)
            {
                break;
            }

            ReadOnlySpan<byte> payload = bytes.AsSpan(offset + 4, (int)prefix);
            if (payload.IndexOfAnyExcept(payload[0]) >= 0)
            {
                return new WireParse($"the Message at offset {offset} has a payload that is not one repeated marker", counterCounts, sawTrailingMessage);
            }

            sawTrailingMessage |= payload[0] == TrailingMarker;
            offset += 4 + (int)prefix;
        }

        return new WireParse(null, counterCounts, sawTrailingMessage);
    }

    /// <summary>What <see cref="Parse"/> found: the first defect, if any, and what arrived before it.</summary>
    private sealed record WireParse(string? Malformed, List<ulong> CounterCounts, bool SawTrailingMessage);

    /// <summary>
    /// A plain BCL TCP socket standing in for the far end of a Session, dialling the Reactor under test
    /// with its receive buffer pinned to <see cref="PinnedBufferSize"/> before it connects. Reads nothing
    /// unless a test asks it to.
    /// </summary>
    private sealed class Counterparty(Socket socket) : IDisposable
    {
        public static Counterparty Connect(IPEndPoint destination)
        {
            Socket socket = new(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                ReceiveTimeout = 5000,
                ReceiveBufferSize = PinnedBufferSize,
            };
            socket.Connect(destination);
            return new Counterparty(socket);
        }

        public void Send(byte[] bytes) => socket.Send(bytes);

        /// <summary>Grows the receive buffer, so draining is not throttled by the pinned window.</summary>
        public void GrowReceiveBuffer(int size) => socket.ReceiveBufferSize = size;

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

        public void Dispose() => socket.Dispose();
    }
}
