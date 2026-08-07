using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 3.4.4 (issue #82): the per-Peer whole-Message delivery counter, and its load-bearing
/// property — it advances only when a Message is actually handed to the frame arena for delivery,
/// never merely because its bytes have all arrived. The held-back case reuses #81's read-share
/// exhaustion mechanism (a capacity/arena small enough that a second whole Message in one Frame
/// cannot be reserved) to make that distinction observable through the public counter surface
/// across two <see cref="StreamSessionReactor.Pump"/> calls, exactly as the issue's negative
/// acceptance criterion requires.
/// </summary>
public class StreamSessionDeliveryCounterTests
{
    private const int RecordSize = 41; // 1 type byte + 40-byte Hello payload; a Refusal is padded to it.
    private const byte HelloType = 1;
    private const uint ProtocolVersion = 1;

    private static byte[] BuildHello(uint maxMessageSize = 4096, ulong sendIdleMs = 1000, ulong lingerMs = 1000)
    {
        byte[] record = new byte[RecordSize];
        record[0] = HelloType;
        int offset = 1;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(offset, 4), ProtocolVersion);
        offset += 4;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(offset, 4), maxMessageSize);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(record.AsSpan(offset, 8), sendIdleMs);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(record.AsSpan(offset, 8), lingerMs);
        return record;
    }

    private static byte[] BuildFrame(byte[] payload)
    {
        byte[] frame = new byte[4 + payload.Length];
        BinaryPrimitives.WriteUInt32BigEndian(frame, (uint)payload.Length);
        payload.CopyTo(frame.AsSpan(4));
        return frame;
    }

    private static byte[] PayloadOf(int length, byte marker)
    {
        byte[] payload = new byte[length];
        Array.Fill(payload, marker);
        return payload;
    }

    /// <summary>Pumps <paramref name="accepter"/> until it has drained a <see cref="SessionEventKind.PeerConnected"/> event, returning the established Peer's id.</summary>
    private static PeerId EstablishSession(StreamSessionReactorHarness accepter, CounterPeer peer, int maxPumps = 20)
    {
        peer.Send(BuildHello());
        for (int i = 0; i < maxPumps; i++)
        {
            accepter.Reactor.Pump();
            foreach (var evt in accepter.DrainToListWithPayload())
            {
                if (evt.Kind == SessionEventKind.PeerConnected)
                {
                    return evt.Peer;
                }
            }
        }

        throw new InvalidOperationException("The Session never established within the Pump budget.");
    }

    [Fact]
    public void CountersOf_AfterThreeDeliveredMessages_MessagesReceivedReadsThree()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(maxMessageSize: 64, retentionBytes: 68);
        using CounterPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        // Act — three whole Messages, each given its own Pump so each is delivered before the next
        // is sent.
        for (byte i = 0; i < 3; i++)
        {
            peer.Send(BuildFrame(PayloadOf(10, i)));
            accepter.PumpUntil(() => accepter.Reactor.CountersOf(id).MessagesReceived == (ulong)(i + 1));
        }

        // Assert
        accepter.Reactor.CountersOf(id).MessagesReceived.Should().Be(3);
    }

    [Fact]
    public void CountersOf_AMessageHeldBackByAnExhaustedReadShare_DoesNotAdvanceUntilThePumpThatDeliversIt()
    {
        // Arrange — capacity 4, arena 200 bytes -> a 50-byte read share per Peer (mirrors
        // StreamSessionReadShareTests). Three 20-byte Messages total 60 bytes, so the third cannot be
        // reserved alongside the first two in the same Frame and stays buffered, whole, with its
        // bytes fully arrived but not yet handed to the arena.
        using StreamSessionReactorHarness accepter = new(
            capacity: 4, maxMessageSize: 64, retentionBytes: 68, receiveBufferSize: 100, arenaSize: 200);
        using CounterPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        byte[] first = PayloadOf(20, 1);
        byte[] second = PayloadOf(20, 2);
        byte[] third = PayloadOf(20, 3);

        // Act — all three frames arrive before any Pump runs, so the third Message's bytes are fully
        // present at the front of the receive buffer from the very first read. Pump until this Peer's
        // buffer actually has something delivered — the counter itself is the condition, not a drain,
        // so nothing here can race with observing it.
        peer.Send([.. BuildFrame(first), .. BuildFrame(second), .. BuildFrame(third)]);
        accepter.PumpUntil(() => accepter.Reactor.CountersOf(id).MessagesReceived > 0);

        // Assert — the Pump that delivers the first two Messages (all the share allows) counts
        // exactly two, not three: the third's bytes are fully arrived but it was never handed to the
        // arena this Frame.
        accepter.Reactor.CountersOf(id).MessagesReceived.Should().Be(2,
            "the third Message's bytes have all arrived but it lost the read-share race and was not delivered");

        // Act — no further bytes are ever sent by the peer; the held-back Message surfaces on a later
        // Pump via the retry mechanism (#81).
        accepter.PumpUntil(() => accepter.Reactor.CountersOf(id).MessagesReceived == 3);

        // Assert — the counter advances to three only once the third Message is actually delivered.
        accepter.Reactor.CountersOf(id).MessagesReceived.Should().Be(3);
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// writing raw framed bytes on the wire — mirroring <see cref="StreamSessionReadShareTests"/>'s
    /// <c>SharePeer</c>.
    /// </summary>
    private sealed class CounterPeer : IDisposable
    {
        private readonly Socket _socket;

        public CounterPeer(IPEndPoint destination)
        {
            _socket = new Socket(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _socket.Connect(destination);
        }

        /// <summary>Sends every byte of <paramref name="bytes"/> in a single write.</summary>
        public void Send(byte[] bytes) => _socket.Send(bytes);

        public void Dispose() => _socket.Dispose();
    }
}
