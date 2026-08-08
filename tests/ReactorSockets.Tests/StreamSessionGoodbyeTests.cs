using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 3.7 (issue #23): Goodbye, Closing, and the <see cref="DisconnectReason"/> roster. Goodbye is
/// the announcement that a Session is ending on purpose (ADR 0010); Closing is reached only when a
/// Goodbye arrives while something is still outstanding — either this end's own outbound retention
/// for that Peer still has bytes to deliver, or the consumer has not yet drained Messages this end
/// already reassembled from it. The counterparty in every test here is a hand-built raw TCP peer — mirrors
/// <see cref="StreamSessionMalformedFramingTests"/>'s <c>MalformedPeer</c> and
/// <see cref="StreamSessionRetentionFlushTests"/>'s <c>FlushPeer</c>, the house pattern for a hostile
/// or otherwise raw counterparty — since only <see cref="GoodbyePeer"/> can put the bare
/// <c>uint.MaxValue</c> sentinel on the wire without going through <see cref="StreamSessionReactor.Close"/>.
/// </summary>
public class StreamSessionGoodbyeTests
{
    private const int RecordSize = 41; // 1 type byte + 40-byte Hello payload; a Refusal is padded to it.
    private const byte HelloType = 1;
    private const uint ProtocolVersion = 1;
    private const uint GoodbyeSentinel = uint.MaxValue;

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

    private static byte[] BuildGoodbye()
    {
        byte[] record = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(record, GoodbyeSentinel);
        return record;
    }

    private static byte[] PayloadOf(int length, byte marker)
    {
        byte[] payload = new byte[length];
        Array.Fill(payload, marker);
        return payload;
    }

    /// <summary>
    /// Pumps <paramref name="accepter"/> until it has drained a <see cref="SessionEventKind.PeerConnected"/>
    /// event, returning the established Peer's id, and reads the accepter's own Hello record off
    /// <paramref name="peer"/>'s socket first, so later reads see only what this ticket's flush writes.
    /// </summary>
    private static PeerId EstablishSession(StreamSessionReactorHarness accepter, GoodbyePeer peer, int maxPumps = 20)
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

    // =========================================================================================
    // Local Close: ordering, Send-after-Close, and the flush-then-close sequence (AC1, AC2)
    // =========================================================================================

    [Fact]
    public void Send_AfterClose_ReturnsClosedOutcome()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        accepter.Reactor.Close(id);

        // Act — a Send after Close fails outright: nothing further is queued behind the Goodbye.
        SendOutcome afterClose = accepter.Reactor.Send(id, PayloadOf(16, 3));

        // Assert
        afterClose.Should().Be(SendOutcome.Closed);
    }

    [Fact]
    public void Close_WithMessagesQueuedBeforeIt_DeliversThemToTheWireAheadOfTheGoodbyeInOrder()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        byte[] first = PayloadOf(16, 1);
        byte[] second = PayloadOf(16, 2);

        // Act — two Messages queued, then Close, all before any Pump: everything queued ahead of the
        // Goodbye in this end's own retention must reach the wire first and whole.
        SendOutcome firstOutcome = accepter.Reactor.Send(id, first);
        SendOutcome secondOutcome = accepter.Reactor.Send(id, second);
        accepter.Reactor.Close(id);
        accepter.Reactor.Pump();
        byte[] onWire = peer.Receive(BuildFrame(first).Length + BuildFrame(second).Length + BuildGoodbye().Length);

        // Assert — both Sends were accepted, and both Messages arrived whole and in order, with the
        // Goodbye sentinel immediately behind them and nothing else.
        firstOutcome.Should().Be(SendOutcome.Sent);
        secondOutcome.Should().Be(SendOutcome.Sent);
        onWire.Should().Equal([.. BuildFrame(first), .. BuildFrame(second), .. BuildGoodbye()]);
    }

    [Fact]
    public void Close_WithMessagesQueuedBeforeIt_ClosesLocallyWithLocalCloseAndNoPeerClosing()
    {
        // Arrange — the queued Messages themselves are not this test's subject; only that Close was
        // preceded by outstanding retention.
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        accepter.Reactor.Send(id, PayloadOf(16, 1));
        accepter.Reactor.Send(id, PayloadOf(16, 2));

        // Act — this end closes itself once its own retention (Messages plus Goodbye) has drained.
        accepter.Reactor.Close(id);
        accepter.Reactor.Pump();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();

        // Assert
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.LocalClose);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosing,
            "nothing was outstanding for the far end to still be delivering — this is a local close, not a Goodbye received");
    }

    [Fact]
    public void Close_CalledTwice_IsIdempotentAndSendsExactlyOneGoodbye()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        // Act
        accepter.Reactor.Close(id);
        accepter.Reactor.Close(id);
        accepter.Reactor.Pump();
        byte[] onWire = peer.Receive(BuildGoodbye().Length);

        // Assert — exactly one Goodbye's worth of bytes, not two.
        onWire.Should().Equal(BuildGoodbye());
        accepter.DrainToList().Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.LocalClose);
    }

    // =========================================================================================
    // Goodbye received with nothing outstanding: closes directly (AC3, AC5 — PeerGoodbye)
    // =========================================================================================

    [Fact]
    public void GoodbyeReceived_WithNothingOutstandingInThisEndsOwnRetention_ClosesDirectlyWithNoObservableClosingState()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        // Act — the peer says Goodbye with nothing ever queued for it to still be delivering.
        peer.Send(BuildGoodbye());
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert — closed directly with PeerGoodbye, never a PeerClosing event first.
        drained.Should().ContainSingle(e => e.Peer == id)
            .Which.Should().BeEquivalentTo((SessionEventKind.PeerClosed, id, DisconnectReason.PeerGoodbye));
    }

    // =========================================================================================
    // Goodbye received while this end's own retention still has bytes to deliver: Closing, then
    // Closed once that retention drains (AC1)
    // =========================================================================================

    [Fact]
    public void GoodbyeReceived_WhileThisEndsOwnRetentionForThatPeerIsStillNonEmpty_ObservesClosingThenClosesWithPeerGoodbyeOnceDrained()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        byte[] message = PayloadOf(16, 7);

        // Act — the peer's Goodbye is on the wire, and this end's own retention for the peer holds a
        // Message not yet flushed, before the single Pump that reads the one and sees the other:
        // Reactor.Send commits straight into retention synchronously, and a Pump's dispatcher read
        // (which classifies the Goodbye) always runs before that same Pump's own flush step
        // (FlushPendingRetentionWrites), so this ordering is exactly what a Goodbye arriving with
        // outstanding retention looks like — not a race against how fast the bytes cross loopback.
        peer.Send(BuildGoodbye());
        SendOutcome sendOutcome = accepter.Reactor.Send(id, message);
        accepter.Reactor.Pump();
        byte[] onWire = peer.Receive(BuildFrame(message).Length);

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert — both the Closing observation and the Closed event landed once the outstanding
        // Message had a chance to flush, in that order, within however many Frames it took.
        sendOutcome.Should().Be(SendOutcome.Sent);
        onWire.Should().Equal(BuildFrame(message));
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosing && e.Peer == id);
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.PeerGoodbye);
        drained.IndexOf(drained.First(e => e.Kind == SessionEventKind.PeerClosing))
            .Should().BeLessThan(drained.IndexOf(drained.First(e => e.Kind == SessionEventKind.PeerClosed)),
                "the Closing observation must precede the Closed event that follows it");
    }

    // =========================================================================================
    // Goodbye received with Messages still queued ahead of it for the consumer: Closing observes,
    // then closes once those Messages count as delivered (issue #23 review finding)
    // =========================================================================================

    [Fact]
    public void GoodbyeReceived_WithMessagesQueuedAheadOfItInTheSameRead_DeliversThemThenObservesClosingBeforeClosingOnALaterPump()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        byte[] first = PayloadOf(16, 1);
        byte[] second = PayloadOf(16, 2);

        // Act — two Messages and the peer's Goodbye all arrive in a single write, so one Pump's
        // DeliverCompleteMessages call reassembles all three in order: the Messages this end already
        // holds for the consumer must still count as outstanding even though each already reached
        // MessageReceived before the Goodbye immediately behind it was classified.
        peer.Send([.. BuildFrame(first), .. BuildFrame(second), .. BuildGoodbye()]);

        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosing); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert — both Messages were delivered, whole and in order, before Closing was ever
        // observed — the Goodbye never jumps ahead of Messages that arrived before it in the same read.
        drained.Count(e => e.Kind == SessionEventKind.MessageReceived).Should().Be(2);
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosing && e.Peer == id);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed,
            "something this end already reassembled from the Peer was still outstanding to the consumer");
        drained.IndexOf(drained.First(e => e.Kind == SessionEventKind.PeerClosing))
            .Should().BeGreaterThan(drained.FindLastIndex(e => e.Kind == SessionEventKind.MessageReceived),
                "Closing must be observed only after every Message queued ahead of the Goodbye");

        // Act — a later Pump, once the Frame that delivered those Messages has passed, finally closes it.
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert — closes with PeerGoodbye, and Closing is never raised a second time for the same Goodbye.
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.PeerGoodbye);
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosing);
    }

    // =========================================================================================
    // Close silently dropping its own Goodbye when retention has no room for it (issue #23 review
    // finding): the append must retry until it commits, and this end must never close locally as if
    // it had announced when it had not
    // =========================================================================================

    [Fact]
    public void Close_WhenRetentionHasNoRoomForTheGoodbye_RetriesTheAppendOnALaterPumpAndAnnouncesBeforeClosing()
    {
        // Arrange — retention sized to hold exactly one maximum-size Message frame and nothing more
        // (the smallest legal pair under issue #16's cross-check, retention >= MaxMessageSize + 4):
        // a single 16-byte Send occupies the whole 20-byte ring, leaving the 4-byte Goodbye sentinel
        // Close tries to append no room at all.
        const int maxMessageSize = 16;
        const int retentionBytes = maxMessageSize + 4; // 4-byte length prefix, matching BuildFrame.
        byte[] message = PayloadOf(maxMessageSize, 7);
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: retentionBytes);
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        // Act — fill retention exactly, then Close: the Goodbye append finds RetentionOutcome.Exhausted.
        SendOutcome sendOutcome = accepter.Reactor.Send(id, message);
        accepter.Reactor.Close(id);
        accepter.Reactor.Pump();
        byte[] onWire = peer.Receive(BuildFrame(message).Length);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drainedAfterFirstPump = accepter.DrainToList();
        bool goodbyeArrivedEarly = peer.TryReceiveWithinTimeout(1, out _, timeoutMs: 200);

        // Assert — the Message already queued reaches the wire and retention is now empty, but the
        // Goodbye Close promised has not been sent: the bug this closes would have this end close
        // itself locally right here, having never announced the ending, because retention happened to
        // drain to empty on this exact Pump (the retry that lands the Goodbye runs at the *start* of
        // the next Pump, before that Pump's own flush carries it to the wire).
        sendOutcome.Should().Be(SendOutcome.Sent);
        onWire.Should().Equal(BuildFrame(message));
        drainedAfterFirstPump.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        goodbyeArrivedEarly.Should().BeFalse(
            "the Goodbye must not reach the wire until its own retention append actually commits");

        // Act — a later Pump retries the deferred append now that retention has room again, and its
        // own flush step carries the now-committed Goodbye to the wire in that same Pump.
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        byte[] goodbyeOnWire = peer.Receive(BuildGoodbye().Length);

        // Assert — the Goodbye actually reached the wire, and this end closed itself only once it did:
        // FlushRetention hands the Goodbye's bytes to the socket before it ever calls
        // TryCloseAfterRetentionDrained, so a successful read here is a genuine wire observation, not
        // an artifact of the Peer having already closed.
        goodbyeOnWire.Should().Equal(BuildGoodbye());
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.LocalClose);
    }

    // =========================================================================================
    // Send refusing a Peer already Closing from an inbound Goodbye, not just one this end told to
    // Close (issue #23 review finding — Closing means no further Sends worth making, either direction)
    // =========================================================================================

    [Fact]
    public void Send_ToAPeerStillClosingFromAnInboundGoodbye_ReturnsClosedAndPutsNothingOnTheWire()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        byte[] message = PayloadOf(16, 9);

        // Act — a Message queued ahead of the peer's own Goodbye in the same read keeps this end's
        // Peer in Closing for the rest of this Frame (HasUndeliveredInboundWork stays true), so the
        // Peer is still resolvable — but already Closing — when Send is called against it below.
        peer.Send([.. BuildFrame(message), .. BuildGoodbye()]);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosing); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert — the Peer reached Closing from the inbound Goodbye alone, without this end ever
        // calling Close.
        drained.Should().Contain(e => e.Kind == SessionEventKind.PeerClosing);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);

        // Act — this end never called Close; only the far end's Goodbye arrived. Pumping on past the
        // refusal is what makes the wire check mean something: a Send that had wrongly been accepted
        // would have queued bytes into retention, and it is the following Pump's flush that would
        // carry them out. Reading after the Peer has closed is safe — the close is orderly, so
        // anything written ahead of the FIN is still delivered (the deferred-Goodbye test above
        // reads its Goodbye off the wire exactly that way).
        SendOutcome outcome = accepter.Reactor.Send(id, PayloadOf(16, 3));
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        bool anythingFollowedTheGoodbye = peer.TryReceiveWithinTimeout(1, out _, timeoutMs: 200);

        // Assert — refused all the same: a Goodbye has already passed for this Peer, so nothing
        // further belongs on the wire behind it.
        outcome.Should().Be(SendOutcome.Closed);
        anythingFollowedTheGoodbye.Should().BeFalse(
            "no Message may follow a Goodbye onto the wire, whichever end sent it");
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// that can put a raw Goodbye sentinel on the wire directly — something no consumer of the public
    /// <see cref="StreamSessionReactor"/> surface can do, since <see cref="StreamSessionReactor.Close"/>
    /// is the only way this library ever writes one. Mirrors <see cref="StreamSessionRetentionFlushTests"/>'s
    /// <c>FlushPeer</c>, minus the shrunk receive buffer that ticket's backpressure tests need and this
    /// one does not.
    /// </summary>
    private sealed class GoodbyePeer : IDisposable
    {
        private readonly Socket _socket;

        public GoodbyePeer(IPEndPoint destination)
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
        /// Reads <paramref name="count"/> bytes within a short timeout, answering whether it succeeded
        /// rather than throwing or blocking indefinitely — used to assert nothing has arrived yet.
        /// Mirrors <c>HandshakePeer.TryReceiveWithinTimeout</c> in
        /// <see cref="StreamSessionHandshakeGuardTests"/>, the house pattern for this assertion.
        /// </summary>
        public bool TryReceiveWithinTimeout(int count, out byte[] record, int timeoutMs = 300)
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

        public void Dispose() => _socket.Dispose();
    }
}
