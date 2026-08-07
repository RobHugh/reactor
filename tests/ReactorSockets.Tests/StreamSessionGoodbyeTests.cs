using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 3.7 (issue #23): Goodbye, Closing, and the <see cref="DisconnectReason"/> roster. Goodbye is
/// the announcement that a Session is ending on purpose (ADR 0010); Closing is reached only when a
/// Goodbye arrives while this end's own retention for that Peer still has bytes to deliver. The
/// counterparty in every test here is a hand-built raw TCP peer — mirrors
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
    public void Close_WithMessagesQueuedBeforeIt_DeliversThemAllBeforeTheGoodbyeThenClosesLocallyWithLocalClose()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using GoodbyePeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        byte[] first = PayloadOf(16, 1);
        byte[] second = PayloadOf(16, 2);

        // Act — two Messages queued, then Close, all before any Pump: everything queued ahead of the
        // Goodbye in this end's own retention must reach the wire first and whole.
        accepter.Reactor.Send(id, first).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Send(id, second).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Close(id);

        // A Send after Close fails outright — nothing further is queued behind the Goodbye.
        SendOutcome afterClose = accepter.Reactor.Send(id, PayloadOf(16, 3));

        accepter.Reactor.Pump();
        byte[] onWire = peer.Receive(BuildFrame(first).Length + BuildFrame(second).Length + BuildGoodbye().Length);

        // Assert — Send after Close fails.
        afterClose.Should().Be(SendOutcome.Closed);

        // Assert — both Messages arrived whole and in order, with the Goodbye sentinel immediately
        // behind them and nothing else.
        onWire.Should().Equal([.. BuildFrame(first), .. BuildFrame(second), .. BuildGoodbye()]);

        // Assert — this end closes itself once its own retention (Messages plus Goodbye) has drained.
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();
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
        accepter.Reactor.Send(id, message).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        byte[] onWire = peer.Receive(BuildFrame(message).Length);

        // Assert — both the Closing observation and the Closed event landed once the outstanding
        // Message had a chance to flush, in that order, within however many Frames it took.
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();
        for (int i = 0; i < 20 && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        onWire.Should().Equal(BuildFrame(message));
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosing && e.Peer == id);
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.PeerGoodbye);
        drained.IndexOf(drained.First(e => e.Kind == SessionEventKind.PeerClosing))
            .Should().BeLessThan(drained.IndexOf(drained.First(e => e.Kind == SessionEventKind.PeerClosed)),
                "the Closing observation must precede the Closed event that follows it");
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

        public void Dispose() => _socket.Dispose();
    }
}
