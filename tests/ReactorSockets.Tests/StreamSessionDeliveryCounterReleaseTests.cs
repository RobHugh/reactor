using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 4.1.3 (issue #113, folding in #110): receiving a peer's delivery-counter record releases this
/// end's own outbound retention through the confirmed Message count
/// (<see cref="RetentionArena.ReleaseThroughMessageCount"/>), replacing the release-on-socket-write
/// policy <see cref="StreamSessionRetentionFlushTests"/> used to cover. <see cref="RetentionArena"/> is
/// L0 and untested-through (tests/CLAUDE.md, ADR 0015), and #110 originally proposed testing
/// <c>ReleaseThroughMessageCount</c> directly — that was in error, since nothing sanctioned could reach
/// it before this ticket's wiring existed, so its six acceptance criteria are proven here instead,
/// entirely through the public L2 surface: a real <see cref="StreamSessionReactor"/> Sending framed
/// Messages, a hand-built raw counterparty peer replying with delivery-counter records, and room
/// recovering (or not) exactly as byte-precise as each criterion demands. The counterparty is
/// <see cref="ReleasePeer"/>, mirroring <see cref="StreamSessionRetentionFlushTests"/>'s <c>FlushPeer</c>
/// plus the ability to send a delivery-counter record back.
/// </summary>
public class StreamSessionDeliveryCounterReleaseTests
{
    private const int RecordSize = StreamSessionTestRecords.HandshakeRecordSize;

    private static byte[] BuildFrame(byte[] payload) => StreamSessionTestRecords.BuildFrame(payload);

    private static byte[] PayloadOf(int length, byte marker) => StreamSessionTestRecords.PayloadOf(length, marker);

    /// <summary>
    /// Pumps <paramref name="accepter"/> until it has drained a <see cref="SessionEventKind.PeerConnected"/>
    /// event, returning the established Peer's id, and reads the accepter's own Hello record off
    /// <paramref name="peer"/>'s socket first.
    /// </summary>
    private static PeerId EstablishSession(StreamSessionReactorHarness accepter, ReleasePeer peer, int maxPumps = 20)
    {
        PeerId id = StreamSessionTestRecords.EstablishSession(accepter, peer.Send, maxPumps);
        peer.Receive(RecordSize);
        return id;
    }

    // =========================================================================================
    // #110 AC1 — releasing through Message 2 of 3 frees exactly Messages 1 and 2, and no more
    // =========================================================================================

    [Fact]
    public void ReleaseThroughMessageCount_ConfirmingMessage2Of3_FreesExactlyMessages1And2AndNoMore()
    {
        // Arrange — a 16-byte max Message frames to 20 bytes; a 60-byte bound holds exactly three.
        const int maxMessageSize = 16;
        const int retentionBytes = 3 * (maxMessageSize + 4);
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: retentionBytes);
        using ReleasePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        byte[] first = PayloadOf(maxMessageSize, 1);
        byte[] second = PayloadOf(maxMessageSize, 2);
        byte[] third = PayloadOf(maxMessageSize, 3);

        // Act — fill the ring exactly with three Messages, none yet confirmed.
        accepter.Reactor.Send(id, first).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, second).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, third).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        peer.Receive(BuildFrame(first).Length + BuildFrame(second).Length + BuildFrame(third).Length);

        accepter.Reactor.Send(id, [9]).Should().Be(SendOutcome.RetentionExhausted,
            "nothing has been confirmed yet, so the ring is still full");

        // Act — the peer confirms only through Message 2.
        peer.SendDeliveryCounter(2);
        accepter.Reactor.Pump();

        // Assert — exactly 40 bytes (Messages 1 and 2, prefixes included) are free: two more maximum
        // Messages fit, but a third does not, which is only true if Message 3's own 20 bytes are still
        // retained rather than also having been freed.
        byte[] fourth = PayloadOf(maxMessageSize, 4);
        byte[] fifth = PayloadOf(maxMessageSize, 5);
        accepter.Reactor.Send(id, fourth).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, fifth).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, [9]).Should().Be(SendOutcome.RetentionExhausted,
            "Message 3's own bytes were never confirmed and must still be retained");
    }

    // =========================================================================================
    // #110 AC2 — a repeated or lower count frees nothing further and never moves the released
    // total backwards
    // =========================================================================================

    [Fact]
    public void ReleaseThroughMessageCount_ARepeatedOrLowerCount_FreesNothingFurtherAndIsNotAnError()
    {
        // Arrange — same 60-byte/three-Message setup as the AC1 test above.
        const int maxMessageSize = 16;
        const int retentionBytes = 3 * (maxMessageSize + 4);
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: retentionBytes);
        using ReleasePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        byte[] first = PayloadOf(maxMessageSize, 11);
        byte[] second = PayloadOf(maxMessageSize, 12);
        byte[] third = PayloadOf(maxMessageSize, 13);

        accepter.Reactor.Send(id, first).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, second).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, third).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        peer.Receive(BuildFrame(first).Length + BuildFrame(second).Length + BuildFrame(third).Length);

        // Act — confirm through Message 2, then fill the freed 40 bytes exactly, leaving the ring full
        // again (Message 3 still unconfirmed, plus two brand-new Messages).
        peer.SendDeliveryCounter(2);
        accepter.Reactor.Pump();
        byte[] fourth = PayloadOf(maxMessageSize, 14);
        byte[] fifth = PayloadOf(maxMessageSize, 15);
        accepter.Reactor.Send(id, fourth).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, fifth).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        peer.Receive(BuildFrame(fourth).Length + BuildFrame(fifth).Length);

        // Act — the peer repeats its already-reported count of 2, exactly as a real peer does since
        // the same number rides both of #25's emission triggers.
        peer.SendDeliveryCounter(2);
        accepter.Reactor.Pump();

        // Assert — repeating frees nothing further: still no room for even one more byte.
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> afterRepeat = accepter.DrainToList();
        accepter.Reactor.Send(id, [9]).Should().Be(SendOutcome.RetentionExhausted,
            "a repeated count must not free anything further");
        afterRepeat.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed, "repeating a count is not an error");

        // Act — the peer then reports a count *lower* than what it already confirmed.
        peer.SendDeliveryCounter(1);
        accepter.Reactor.Pump();

        // Assert — the released total never moves backwards: still no room, and still no violation.
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> afterLower = accepter.DrainToList();
        accepter.Reactor.Send(id, [9]).Should().Be(SendOutcome.RetentionExhausted,
            "a lower count must not un-release bytes already released, freeing nothing further either");
        afterLower.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed, "a lower count is not an error");
    }

    // =========================================================================================
    // #110 AC3 — a count above what this end has appended is refused, distinguishably, and the
    // Session ends with ProtocolViolation (#113's own acceptance criterion)
    // =========================================================================================

    [Fact]
    public void ReleaseThroughMessageCount_ACountOneAboveWhatWasEverAppended_ClosesWithProtocolViolation()
    {
        // Arrange — exactly one Message is ever sent to this Peer.
        const int maxMessageSize = 16;
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: maxMessageSize + 4);
        using ReleasePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        byte[] message = PayloadOf(maxMessageSize, 1);

        accepter.Reactor.Send(id, message).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        peer.Receive(BuildFrame(message).Length);

        // Act — the peer claims to have received a second Message that was never sent.
        peer.SendDeliveryCounter(2);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.ProtocolViolation);
    }

    // =========================================================================================
    // #110 AC4 — boundary bookkeeping survives a write that wraps the region's end
    // =========================================================================================

    [Fact]
    public void ReleaseThroughMessageCount_ConfirmingAMessageThatWrappedTheRingsEnd_FreesItsFullExtent()
    {
        // Arrange — mirrors StreamSessionRetentionFlushTests' own ring-boundary test: a 50-byte bound
        // with 20-byte framed Messages puts the third Send's write position at 40, wrapping 10 bytes at
        // the region's end and 10 more back at its start, inside RetentionArena.TryAppend. Each Send is
        // confirmed before the next is queued, walking the write position to the wrap using only whole,
        // uncontended Sends.
        const int maxMessageSize = 16;
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: 50);
        using ReleasePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        byte[] first = PayloadOf(maxMessageSize, 1);
        byte[] second = PayloadOf(maxMessageSize, 2);
        byte[] third = PayloadOf(maxMessageSize, 3); // this Send wraps the ring's end

        accepter.Reactor.Send(id, first).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        peer.Receive(BuildFrame(first).Length);
        peer.SendDeliveryCounter(1);
        accepter.Reactor.Pump();

        accepter.Reactor.Send(id, second).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        peer.Receive(BuildFrame(second).Length);
        peer.SendDeliveryCounter(2);
        accepter.Reactor.Pump();

        // Act — the wrapping Send, then its own confirmation.
        accepter.Reactor.Send(id, third).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        byte[] onWireThird = peer.Receive(BuildFrame(third).Length);
        peer.SendDeliveryCounter(3);
        accepter.Reactor.Pump();

        // Assert — the wrapped Message's bytes reached the peer intact despite the internal split...
        onWireThird.Should().Equal(BuildFrame(third));

        // ... and its confirmation freed its full 20 bytes, not a partial or corrupted extent. Two more
        // maximum Messages (40 bytes) fit against the 50-byte bound, leaving exactly 10 — asserted on
        // both sides, since a partial release would leave fewer and an over-release more: an 11-byte
        // frame is refused, and a 10-byte one is accepted.
        byte[] fourth = PayloadOf(maxMessageSize, 4);
        byte[] fifth = PayloadOf(maxMessageSize, 5);
        accepter.Reactor.Send(id, fourth).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, fifth).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, PayloadOf(7, 6)).Should().Be(SendOutcome.RetentionExhausted,
            "an 11-byte frame does not fit the 10 bytes a full release of Message 3 leaves");
        accepter.Reactor.Send(id, PayloadOf(6, 7)).Should().Be(SendOutcome.Sent,
            "a 10-byte frame fits exactly, which is only true if Message 3's whole 20-byte extent was freed");
    }

    // =========================================================================================
    // #110 AC5 — releasing through every appended Message leaves retained bytes at zero and the
    // region wholly reusable
    // =========================================================================================

    [Fact]
    public void ReleaseThroughMessageCount_ConfirmingEveryAppendedMessage_LeavesTheRegionWhollyReusable()
    {
        // Arrange — a 60-byte bound holds exactly three 20-byte framed Messages.
        const int maxMessageSize = 16;
        const int retentionBytes = 3 * (maxMessageSize + 4);
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: retentionBytes);
        using ReleasePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        byte[] first = PayloadOf(maxMessageSize, 1);
        byte[] second = PayloadOf(maxMessageSize, 2);
        byte[] third = PayloadOf(maxMessageSize, 3);

        accepter.Reactor.Send(id, first).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, second).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, third).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        peer.Receive(BuildFrame(first).Length + BuildFrame(second).Length + BuildFrame(third).Length);

        // Act — the peer confirms every Message it received.
        peer.SendDeliveryCounter(3);
        accepter.Reactor.Pump();

        // Assert — the whole 60-byte bound is free again: three brand-new maximum Messages fit, but a
        // fourth does not.
        byte[] fourth = PayloadOf(maxMessageSize, 4);
        byte[] fifth = PayloadOf(maxMessageSize, 5);
        byte[] sixth = PayloadOf(maxMessageSize, 6);
        accepter.Reactor.Send(id, fourth).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, fifth).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, sixth).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, [9]).Should().Be(SendOutcome.RetentionExhausted);
    }

    // =========================================================================================
    // #110 AC6 — the high-water mark is unchanged by any release
    // =========================================================================================

    [Fact]
    public void ReleaseThroughMessageCount_ConfirmingEveryAppendedMessage_DoesNotChangeRetentionHighWaterBytes()
    {
        // Arrange — same three-Message burst as the AC5 test above, sent before any Pump so the ring
        // peaks at exactly its bound.
        const int maxMessageSize = 16;
        const int retentionBytes = 3 * (maxMessageSize + 4);
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: retentionBytes);
        using ReleasePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        byte[] first = PayloadOf(maxMessageSize, 1);
        byte[] second = PayloadOf(maxMessageSize, 2);
        byte[] third = PayloadOf(maxMessageSize, 3);

        accepter.Reactor.Send(id, first).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, second).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, third).Should().Be(SendOutcome.Sent);

        ulong peakHighWater = accepter.Reactor.CountersOf(id).RetentionHighWaterBytes;

        // Act — flush, drain, and fully confirm.
        accepter.Reactor.Pump();
        peer.Receive(BuildFrame(first).Length + BuildFrame(second).Length + BuildFrame(third).Length);
        peer.SendDeliveryCounter(3);
        accepter.Reactor.Pump();

        ulong afterReleaseHighWater = accepter.Reactor.CountersOf(id).RetentionHighWaterBytes;

        // Assert
        peakHighWater.Should().Be((ulong)retentionBytes, "three whole framed Messages exactly fill the ring before anything flushes");
        afterReleaseHighWater.Should().Be(peakHighWater, "a peer's confirmation frees room but must never lower the true observed peak");
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// that both drives the handshake, reads raw framed Messages off the wire, and replies with
    /// hand-built delivery-counter records — mirroring <see cref="StreamSessionRetentionFlushTests"/>'s
    /// <c>FlushPeer</c>, plus the confirmation half a real peer would eventually provide via #116's
    /// emission triggers.
    /// </summary>
    private sealed class ReleasePeer : IDisposable
    {
        private readonly Socket _socket;

        public ReleasePeer(IPEndPoint destination)
        {
            _socket = new Socket(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                ReceiveTimeout = 5000,
            };
            _socket.Connect(destination);
        }

        /// <summary>Sends every byte of <paramref name="bytes"/> in a single write.</summary>
        public void Send(byte[] bytes) => _socket.Send(bytes);

        /// <summary>
        /// Sends a hand-built delivery-counter control record confirming <paramref name="count"/> —
        /// standing in for the real acknowledgement <see cref="StreamSessionReactor"/> itself would
        /// emit once #116 lands the send-idle timer and per-Pump trigger (issue #25).
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

        public void Dispose() => _socket.Dispose();
    }
}
