using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 3.5.4 (issue #86): retained bytes appended by <see cref="StreamSessionReactor.Send"/>
/// actually reach the socket, and released space becomes available for reuse. This ticket's scope
/// boundary (ADR 0011, #21, #82) is a local flush only — retention is released once bytes are handed
/// to the OS socket buffer, not once the peer has acknowledged them via the delivery-counter
/// mechanism #25 builds — so every assertion here is about what a raw counterparty socket actually
/// receives, and about a Peer regaining room after its bytes are flushed, never about a peer-ack.
/// </summary>
public class StreamSessionRetentionFlushTests
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

    /// <summary>
    /// Pumps <paramref name="accepter"/> until it has drained a <see cref="SessionEventKind.PeerConnected"/>
    /// event, returning the established Peer's id. Also reads the accepter's own Hello record off
    /// <paramref name="peer"/>'s socket, so the bytes this ticket's flush later writes are the only
    /// thing left for <see cref="FlushPeer.Receive"/> to see.
    /// </summary>
    private static PeerId EstablishSession(StreamSessionReactorHarness accepter, FlushPeer peer, int maxPumps = 20)
    {
        peer.Send(BuildHello());
        for (int i = 0; i < maxPumps; i++)
        {
            accepter.Reactor.Pump();
            foreach ((SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload) evt in accepter.DrainToListWithPayload())
            {
                if (evt.Kind == SessionEventKind.PeerConnected)
                {
                    peer.Receive(RecordSize);
                    return evt.Peer;
                }
            }
        }

        throw new InvalidOperationException("The Session never established within the Pump budget.");
    }

    [Fact]
    public void Send_SequenceCrossingTheRingBoundary_DeliversAllMessagesCorrectlyToThePeer()
    {
        // Arrange — a 16-byte max Message frames to 20 bytes (4-byte prefix + payload). A 50-byte
        // retention bound holds two whole framed Messages with 10 bytes to spare, so the third Send —
        // landing at write position 40 with only 10 bytes left before the region's end — wraps into
        // two segments both inside RetentionArena.TryAppend and, on this ticket's flush, inside
        // RetentionArena.GetReadable. Each Send is followed by its own Pump so the previous Message is
        // fully flushed (and released) before the next Send lands, which is what walks the write
        // position all the way to the wrap boundary using only whole, uncontended Sends.
        const int maxMessageSize = 16;
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: 50);
        using FlushPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        byte[] first = PayloadOf(maxMessageSize, 1);
        byte[] second = PayloadOf(maxMessageSize, 2);
        byte[] third = PayloadOf(maxMessageSize, 3);

        // Act
        accepter.Reactor.Send(id, first).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        byte[] onWireFirst = peer.Receive(BuildFrame(first).Length);

        accepter.Reactor.Send(id, second).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        byte[] onWireSecond = peer.Receive(BuildFrame(second).Length);

        // This third Send is the one that wraps: the previous two Sends, each fully flushed and
        // released before the next was issued, have advanced the ring's write position to exactly 40
        // of the 50-byte bound.
        accepter.Reactor.Send(id, third).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        byte[] onWireThird = peer.Receive(BuildFrame(third).Length);

        // Assert — the bytes that actually crossed the wire are exactly the framed Messages, in order,
        // regardless of whether any individual flush happened to span the ring's wrap boundary.
        onWireFirst.Should().Equal(BuildFrame(first));
        onWireSecond.Should().Equal(BuildFrame(second));
        onWireThird.Should().Equal(BuildFrame(third));
    }

    [Fact]
    public void Send_TwoQueuedMessagesWhoseUnreadRangeWrapsInOneFlush_DeliversBothSegmentsInOrder()
    {
        // Arrange — same 20-byte framing and 50-byte bound as the boundary test above. The first Send
        // is flushed and released on its own Pump, leaving the ring's write/release position at 20.
        // The second and third Sends are then queued back-to-back with NO Pump between them: the
        // second lands at 20..40 (no wrap), and the third lands at 40..60, which does not fit before
        // the region's 50-byte end and wraps in RetentionArena.TryAppend. Because nothing has been
        // released since the first Message, the pending unread range (20 released .. 60 written) itself
        // spans the wrap boundary, so the single Pump that flushes it makes RetentionArena.GetReadable
        // return two segments here — a 30-byte first segment (all of the second Message plus the first
        // 10 bytes of the third) and a 10-byte second segment (the third Message's remaining bytes) —
        // that split falls mid-frame, not on a Message boundary, which is exactly the case
        // FlushRetention has to get right.
        const int maxMessageSize = 16;
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: 50);
        using FlushPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        byte[] first = PayloadOf(maxMessageSize, 11);
        byte[] second = PayloadOf(maxMessageSize, 12);
        byte[] third = PayloadOf(maxMessageSize, 13);

        accepter.Reactor.Send(id, first).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        peer.Receive(BuildFrame(first).Length); // drain and release, advancing the write/release position to 20

        // Act — queue both remaining Messages before the next Pump does any flushing at all.
        accepter.Reactor.Send(id, second).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, third).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        byte[] onWire = peer.Receive(BuildFrame(second).Length + BuildFrame(third).Length);

        // Assert — both Messages arrive whole and in order despite the internal segment split landing
        // in the middle of the third Message's frame.
        onWire.Should().Equal([.. BuildFrame(second), .. BuildFrame(third)]);
    }

    [Fact]
    public void Send_AfterThePeerDrainsAFlushedMessage_AcceptsAnotherMaximumSizeSendThatWouldOtherwiseExhaustRetention()
    {
        // Arrange — retention bounded to exactly one maximum Message plus its 4-byte prefix (mirrors
        // StreamSessionSendRetentionTests), so a second Send before anything is released returns
        // RetentionExhausted.
        const int maxMessageSize = 64;
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: maxMessageSize + 4);
        using FlushPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        byte[] firstMessage = PayloadOf(maxMessageSize, 1);

        // Act — fill the region exactly, then confirm it really is exhausted before anything flushes.
        accepter.Reactor.Send(id, firstMessage).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, [9]).Should().Be(SendOutcome.RetentionExhausted);

        // Act — Pump flushes the first Message to the socket and releases its retention (a local
        // flush, per this ticket's scope boundary — the peer has not read anything yet at this point).
        // The peer then drains the bytes off the wire, exactly as the acceptance criterion describes.
        accepter.Reactor.Pump();
        byte[] onWire = peer.Receive(BuildFrame(firstMessage).Length);

        // Act — with the region's only occupant released, a second maximum-size Send now fits.
        byte[] secondMessage = PayloadOf(maxMessageSize, 2);
        SendOutcome secondOutcome = accepter.Reactor.Send(id, secondMessage);
        accepter.Reactor.Pump();
        byte[] onWireSecond = peer.Receive(BuildFrame(secondMessage).Length);

        // Assert
        onWire.Should().Equal(BuildFrame(firstMessage));
        secondOutcome.Should().Be(SendOutcome.Sent);
        onWireSecond.Should().Equal(BuildFrame(secondMessage));
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// that both drives the handshake and reads raw bytes the Reactor sends back — mirroring
    /// <see cref="StreamSessionHandshakeGuardTests"/>'s <c>RawTcpPeer</c>, the house pattern for a
    /// test that needs to read bytes off the wire rather than only write them.
    /// </summary>
    private sealed class FlushPeer : IDisposable
    {
        private readonly Socket _socket;

        public FlushPeer(IPEndPoint destination)
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
