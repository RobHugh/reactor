using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 3.5.4 (issue #86): retained bytes appended by <see cref="StreamSessionReactor.Send"/>
/// actually reach the socket. Originally also covered released space becoming available for reuse
/// purely from a local flush; issue #113 replaced that release-on-write policy with release-on-peer-
/// confirmation (the delivery-counter mechanism #25 describes), so every test here that used to rely
/// on a flush alone freeing room now has its <see cref="FlushPeer"/> send back a hand-built
/// delivery-counter record confirming what it read, exactly as a real counterparty would. What every
/// assertion here still checks is what a raw counterparty socket actually receives, and — now,
/// genuinely — a Peer regaining room only once its peer has said so.
/// Also carries slice 3.5.5 (issue #87), which reuses this file's real-socket <see cref="FlushPeer"/>
/// for two acceptance criteria #21 left untested: that
/// <see cref="PeerCounters.RetentionHighWaterBytes"/> reads the true peak after a burst and never
/// decreases as the ring drains, and that <see cref="SendOutcome.RetentionExhausted"/> is reachable
/// over a real stalled connection, not only by direct ring-capacity calls.
/// </summary>
public class StreamSessionRetentionFlushTests
{
    private const int RecordSize = StreamSessionTestRecords.HandshakeRecordSize;

    private static byte[] BuildFrame(byte[] payload) => StreamSessionTestRecords.BuildFrame(payload);

    private static byte[] PayloadOf(int length, byte marker) => StreamSessionTestRecords.PayloadOf(length, marker);

    /// <summary>
    /// Pumps <paramref name="accepter"/> until it has drained a <see cref="SessionEventKind.PeerConnected"/>
    /// event, returning the established Peer's id. Also reads the accepter's own Hello record off
    /// <paramref name="peer"/>'s socket, so the bytes this ticket's flush later writes are the only
    /// thing left for <see cref="FlushPeer.Receive"/> to see.
    /// </summary>
    private static PeerId EstablishSession(StreamSessionReactorHarness accepter, FlushPeer peer, int maxPumps = 20)
    {
        PeerId id = StreamSessionTestRecords.EstablishSession(accepter, peer.Send, maxPumps);
        peer.Receive(RecordSize);
        return id;
    }

    [Fact]
    public void Send_SequenceCrossingTheRingBoundary_DeliversAllMessagesCorrectlyToThePeer()
    {
        // Arrange — a 16-byte max Message frames to 20 bytes (4-byte prefix + payload). A 50-byte
        // retention bound holds two whole framed Messages with 10 bytes to spare, so the third Send —
        // landing at write position 40 with only 10 bytes left before the region's end — wraps into
        // two segments both inside RetentionArena.TryAppend and, on this ticket's flush, inside
        // RetentionArena.GetUnsent. Each Send is followed by its own Pump and the peer confirming what
        // it just read before the next Send lands, which is what walks the write position all the way
        // to the wrap boundary using only whole, uncontended Sends — retention is released only by that
        // confirmation now (issue #113), not merely by the flush handing bytes to the socket.
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
        peer.SendDeliveryCounter(1);
        accepter.Reactor.Pump();

        accepter.Reactor.Send(id, second).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        byte[] onWireSecond = peer.Receive(BuildFrame(second).Length);
        peer.SendDeliveryCounter(2);
        accepter.Reactor.Pump();

        // This third Send is the one that wraps: the previous two Sends, each fully flushed and
        // confirmed before the next was issued, have advanced the ring's write position to exactly 40
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
        // is flushed on its own Pump and the peer confirms it before anything else is queued, leaving
        // the ring's write/released position at 20 (issue #113 — a flush alone no longer releases
        // anything). The second and third Sends are then queued back-to-back with NO Pump between them:
        // the second lands at 20..40 (no wrap), and the third lands at 40..60, which does not fit before
        // the region's 50-byte end and wraps in RetentionArena.TryAppend. Because nothing has been
        // released since the first Message, the pending unsent range (20 sent .. 60 written) itself
        // spans the wrap boundary, so the single Pump that flushes it makes RetentionArena.GetUnsent
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
        peer.Receive(BuildFrame(first).Length); // drain, then confirm below to actually release the room
        peer.SendDeliveryCounter(1);
        accepter.Reactor.Pump();

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
    public void Send_AfterThePeerConfirmsAFlushedMessage_AcceptsAnotherMaximumSizeSendThatWouldOtherwiseExhaustRetention()
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

        // Act — Pump flushes the first Message to the socket; the peer reads it off the wire and, per
        // issue #113, must actually confirm it via a delivery-counter record before this end's own
        // retention frees any room — merely draining the socket is no longer enough (the regression
        // this file's class remarks describe, and the behaviour Send_AgainstACounterpartyThatNeverReads
        // AfterTheHandshake_EventuallyReturnsRetentionExhausted below now demonstrates the other side
        // of).
        accepter.Reactor.Pump();
        byte[] onWire = peer.Receive(BuildFrame(firstMessage).Length);
        SendOutcome stillExhausted = accepter.Reactor.Send(id, [9]);
        peer.SendDeliveryCounter(1);
        accepter.Reactor.Pump();

        // Act — with the region's only occupant now actually released, a second maximum-size Send fits.
        byte[] secondMessage = PayloadOf(maxMessageSize, 2);
        SendOutcome secondOutcome = accepter.Reactor.Send(id, secondMessage);
        accepter.Reactor.Pump();
        byte[] onWireSecond = peer.Receive(BuildFrame(secondMessage).Length);

        // Assert
        onWire.Should().Equal(BuildFrame(firstMessage));
        stillExhausted.Should().Be(SendOutcome.RetentionExhausted,
            "the socket has taken the bytes but the peer has not yet confirmed them");
        secondOutcome.Should().Be(SendOutcome.Sent);
        onWireSecond.Should().Equal(BuildFrame(secondMessage));
    }

    [Fact]
    public void CountersOf_AfterABurstThatFillsRetentionThenDrains_RetentionHighWaterBytesReflectsThePeakAndDoesNotDecrease()
    {
        // Arrange — three 16-byte max Messages each frame to 20 bytes (4-byte prefix + payload); a
        // 60-byte bound holds exactly all three at once, so queuing them back-to-back before any Pump
        // pushes retention to that peak with nothing left over.
        const int maxMessageSize = 16;
        const int messageCount = 3;
        const int retentionBytes = messageCount * (maxMessageSize + 4);
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: retentionBytes);
        using FlushPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        byte[] first = PayloadOf(maxMessageSize, 21);
        byte[] second = PayloadOf(maxMessageSize, 22);
        byte[] third = PayloadOf(maxMessageSize, 23);

        // Act — queue all three before any flush, filling the ring exactly to its bound.
        accepter.Reactor.Send(id, first).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, second).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, third).Should().Be(SendOutcome.Sent);

        ulong peakHighWater = accepter.Reactor.CountersOf(id).RetentionHighWaterBytes;

        // Drain — flush to the socket and have the peer read every byte off the wire, releasing all
        // of the retention the burst above claimed.
        accepter.Reactor.Pump();
        peer.Receive(BuildFrame(first).Length + BuildFrame(second).Length + BuildFrame(third).Length);

        ulong afterDrainHighWater = accepter.Reactor.CountersOf(id).RetentionHighWaterBytes;

        // Assert
        peakHighWater.Should().Be((ulong)retentionBytes, "three whole framed Messages exactly fill the ring before anything flushes");
        afterDrainHighWater.Should().Be(peakHighWater, "high-water is the true peak ever observed and must not decrease as the ring drains");
    }

    [Fact]
    public void Send_AgainstACounterpartyThatNeverReadsAfterTheHandshake_EventuallyReturnsRetentionExhausted()
    {
        // Arrange — the real-socket counterpart to StreamSessionSendRetentionTests' pure ring-capacity
        // exhaustion test. The counterparty's receive buffer is shrunk before it connects — never on
        // an already-established socket, per CreateCounterpartyListener's doc and commit 8426ff3 —
        // and it never reads again once the handshake completes. Both ends of the transport are pinned
        // here, not just the counterparty's: shrinking its receive window bounds what it will accept,
        // while SendBufferSize bounds what this end's own kernel will absorb on its behalf, and issue
        // #135 found that without the second the first buys nothing on Linux, whose send autotuning
        // will swallow the whole ring into the accepter's send queue and report every byte of it sent.
        //
        // What the assertions below then separate is two different ways this configuration can reach
        // RetentionExhausted. Retention is released only by a peer's delivery-counter confirmation now
        // (issue #113), which this counterparty never sends, so the ring fills on appends alone after
        // exactly its hundred Messages whatever the transport does — an outcome that would be true even
        // against a peer reading everything as fast as it arrives, and not what this test is for. The
        // drained-byte count is what makes the transport's own refusal observable and this test's name
        // honest: far less crosses the wire than was appended, so the ring is full of bytes the
        // transport genuinely would not take.
        const int maxMessageSize = 8192;
        const int retentionBytes = 100 * (maxMessageSize + 4);
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: retentionBytes, sendBufferSize: 1024);
        using FlushPeer peer = new(accepter.Endpoint, receiveBufferSize: 1024);
        PeerId id = EstablishSession(accepter, peer);

        byte[] message = PayloadOf(maxMessageSize, 99);

        // Act — keep sending and pumping; nothing ever reads the wire again after the handshake, so
        // eventually a flush makes no progress and a Send finds the ring still full of what the last
        // one couldn't get out. No test may pin how many attempts that takes — it depends on the
        // platform's own socket buffer sizing — so this bounds attempts generously and keeps a single
        // assertion regardless of how many it took.
        SendOutcome outcome = SendOutcome.Sent;
        for (int attempt = 0; attempt < 500 && outcome == SendOutcome.Sent; attempt++)
        {
            outcome = accepter.Reactor.Send(id, message);
            accepter.Reactor.Pump();
        }

        // Act — only now, with exhaustion already reached, does the counterparty read: this counts what
        // the transport actually carried, including whatever drains out of the accepter's send queue on
        // its own once the window reopens, since nothing pumps this Reactor again.
        long carried = peer.DrainAvailableBytes();

        // Assert
        outcome.Should().Be(SendOutcome.RetentionExhausted);
        carried.Should().BeLessThan(retentionBytes,
            "the ring is full of bytes the transport refused, not merely of bytes no peer has confirmed");
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

        /// <param name="destination">The Reactor's bound endpoint to connect to.</param>
        /// <param name="receiveBufferSize">
        /// Applied before <c>Connect</c>, so the negotiated receive window is small from the handshake
        /// onward — never applied after connecting, which on Linux clamps the advertised window and
        /// disables receive autotuning for the rest of the connection's life (mirrors
        /// <c>RawReactorHarness.CreateCounterpartyListener</c>'s doc and commit 8426ff3). Omit for the
        /// OS default.
        /// </param>
        public FlushPeer(IPEndPoint destination, int? receiveBufferSize = null)
        {
            _socket = new Socket(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                ReceiveTimeout = 5000,
            };
            if (receiveBufferSize is int size)
            {
                _socket.ReceiveBufferSize = size;
            }

            _socket.Connect(destination);
        }

        /// <summary>Sends every byte of <paramref name="bytes"/> in a single write.</summary>
        public void Send(byte[] bytes) => _socket.Send(bytes);

        /// <summary>
        /// Sends a hand-built delivery-counter control record confirming <paramref name="count"/> —
        /// standing in for the real acknowledgement <see cref="StreamSessionReactor"/> itself would
        /// emit once #116 lands the send-idle timer and per-Pump trigger (issue #25), needed here so
        /// this file's own accepter can see its retention actually released (issue #113).
        /// </summary>
        public void SendDeliveryCounter(ulong count) => Send(StreamSessionTestRecords.BuildDeliveryCounterRecord(count));

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
        /// Reads and counts every byte the transport has already carried, stopping once a poll of
        /// <paramref name="quietMilliseconds"/> finds nothing further — a count, not a payload, since
        /// its only caller asks how much crossed rather than what. Deliberately keeps reading past the
        /// first quiet moment's worth of data: draining reopens the receive window, so bytes still
        /// queued in the far end's send buffer follow on their own, and those were handed to the
        /// transport too.
        /// </summary>
        public long DrainAvailableBytes(int quietMilliseconds = 200)
        {
            byte[] scratch = new byte[64 * 1024];
            long total = 0;
            while (_socket.Poll(quietMilliseconds * 1000, SelectMode.SelectRead))
            {
                int read = _socket.Receive(scratch);
                if (read == 0)
                {
                    break;
                }

                total += read;
            }

            return total;
        }

        public void Dispose() => _socket.Dispose();
    }
}
