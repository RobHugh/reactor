using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 3.4.2 (issue #80): the malformed-framing table — an oversized prefix, a zero-length prefix,
/// a stream that ends mid-prefix or mid-Message, and the transport-failure branch that stays distinct
/// from all four. The counterparty in every test here is a hand-built raw TCP peer, exactly as
/// <see cref="StreamSessionMessageFramingTests"/>'s <c>MessagePeer</c> — a hostile peer is an ordinary
/// Raw consumer, which is what removes the need for an internal codec seam.
/// </summary>
public class StreamSessionMalformedFramingTests
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

    private static byte[] BuildPrefix(uint length)
    {
        byte[] prefix = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(prefix, length);
        return prefix;
    }

    /// <summary>Pumps <paramref name="accepter"/> until it has drained a <see cref="SessionEventKind.PeerConnected"/> event.</summary>
    private static void EstablishSession(StreamSessionReactorHarness accepter, MalformedPeer peer, int maxPumps = 20)
    {
        peer.Send(BuildHello());
        for (int i = 0; i < maxPumps; i++)
        {
            accepter.Reactor.Pump();
            if (accepter.DrainToListWithPayload().Any(e => e.Kind == SessionEventKind.PeerConnected))
            {
                return;
            }
        }

        throw new InvalidOperationException("The Session never established within the Pump budget.");
    }

    /// <summary>Pumps until a <see cref="SessionEventKind.PeerClosed"/> drains, or the budget runs out.</summary>
    private static List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> PumpUntilClosed(
        StreamSessionReactorHarness accepter, int maxPumps = 20)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> drained = [];
        for (int i = 0; i < maxPumps && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToListWithPayload());
        }

        return drained;
    }

    // =========================================================================================
    // Oversized prefix
    // =========================================================================================

    [Fact]
    public void DeliverCompleteMessages_PrefixOneByteLargerThanMaxMessageSize_ClosesWithProtocolViolation()
    {
        // Arrange
        const int maxMessageSize = 64;
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: maxMessageSize + 4);
        using MalformedPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);
        byte[] payload = new byte[maxMessageSize + 1];

        // Act
        peer.Send(BuildFrame(payload));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> drained = PumpUntilClosed(accepter);

        // Assert
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.ProtocolViolation);
    }

    [Fact]
    public void DeliverCompleteMessages_PrefixAnnouncing4GB_ClosesWithProtocolViolationAndAllocatesNothingSizedByTheClaim()
    {
        // Arrange — a well-behaved MaxMessageSize of 4096 makes a 4 GB claim absurdly oversized; only
        // the four prefix bytes ever need to reach the wire for the violation to be detected.
        using StreamSessionReactorHarness accepter = new();
        using MalformedPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);

        // Act
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        peer.Send(BuildPrefix(4_000_000_000));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> drained = PumpUntilClosed(accepter);
        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();

        // Assert — closed with a protocol violation.
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.ProtocolViolation);

        // Assert — nothing sized anywhere near the claimed 4 GB was ever allocated. A generous ceiling
        // well below the claim, rather than a fixed byte budget lifted from another test's assertion.
        (allocatedAfter - allocatedBefore).Should().BeLessThan(1_000_000,
            "a peer's claimed length must never drive an allocation, whatever else this Pump budget allocates");
    }

    [Fact]
    public void DeliverCompleteMessages_PrefixAnnouncing4GB_LogsMalformedFramingAtWarnWithThePeerSubject()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using MalformedPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);
        accepter.Sink.Received.Clear();

        // Act
        peer.Send(BuildPrefix(4_000_000_000));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.MalformedFraming));

        // Assert
        DiagnosticRecord record = accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.MalformedFraming).Which;
        record.Level.Should().Be(DiagnosticLevel.Warn);
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Peer);
        record.Value0.Should().Be(4_000_000_000);
    }

    // =========================================================================================
    // Zero-length prefix
    // =========================================================================================

    [Fact]
    public void DeliverCompleteMessages_ZeroLengthPrefix_ClosesWithProtocolViolationRatherThanAnEmptyMessage()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using MalformedPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);

        // Act
        peer.Send(BuildPrefix(0));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> drained = PumpUntilClosed(accepter);

        // Assert — never a MessageReceived event for an empty Message, and never anything else either.
        drained.Should().ContainSingle()
            .Which.Should().Match<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)>(
                e => e.Kind == SessionEventKind.PeerClosed && e.Reason == DisconnectReason.ProtocolViolation);
    }

    // =========================================================================================
    // Truncation
    // =========================================================================================

    [Fact]
    public void OnEstablishedPeerReadable_StreamEndsMidPrefix_ClosesWithProtocolViolation()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using MalformedPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);

        // Act — two of the four prefix bytes arrive, then the connection ends.
        peer.Send([0x00, 0x01]);
        peer.CloseGracefully();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> drained = PumpUntilClosed(accepter);

        // Assert
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.ProtocolViolation);
    }

    [Fact]
    public void OnEstablishedPeerReadable_StreamEndsMidMessage_ClosesWithProtocolViolation()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using MalformedPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);
        byte[] frame = BuildFrame([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // Act — the full prefix and part of the body arrive, then the connection ends.
        peer.Send(frame.AsSpan(0, 7).ToArray());
        peer.CloseGracefully();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> drained = PumpUntilClosed(accepter);

        // Assert
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.ProtocolViolation);
    }

    [Fact]
    public void OnEstablishedPeerReadable_StreamEndsWithNothingBuffered_ClosesWithTransportFailedNotProtocolViolation()
    {
        // Arrange — a clean end between Messages, with no partial frame in flight, is an ordinary
        // transport ending rather than a framing violation.
        using StreamSessionReactorHarness accepter = new();
        using MalformedPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);

        // Act
        peer.CloseGracefully();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> drained = PumpUntilClosed(accepter);

        // Assert
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.TransportFailed);
    }

    // =========================================================================================
    // Transport failure stays distinct
    // =========================================================================================

    [Fact]
    public void OnEstablishedPeerReadable_APendingSocketErrorOnRead_ClosesWithTransportFailedNotProtocolViolation()
    {
        // Arrange — a peer whose close is forced to send an immediate RST rather than a graceful FIN
        // surfaces a pending socket error on the next read, exactly the path PR #73's readiness
        // dispatcher promises: the error set is scanned every Pump and dispatched to the same read
        // callback this parser sits behind.
        using StreamSessionReactorHarness accepter = new();
        using MalformedPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);

        // Act
        peer.ResetAbruptly();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> drained = PumpUntilClosed(accepter);

        // Assert — TransportFailed, distinct from every ProtocolViolation case above.
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.TransportFailed);
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a hostile
    /// counterparty that can put deliberately malformed framing on the wire — the instrument #80 itself
    /// calls for ("a <c>RawReactor</c> hostile peer ... an ordinary Raw consumer"). Mirrors
    /// <see cref="StreamSessionMessageFramingTests"/>'s <c>MessagePeer</c>, the house pattern for this
    /// shape of test, with the two ways of ending the connection a malformed-framing test needs: a
    /// graceful close (a bare FIN, for the truncation cases) and an abrupt reset (for the
    /// transport-failure case).
    /// </summary>
    private sealed class MalformedPeer : IDisposable
    {
        private readonly Socket _socket;

        public MalformedPeer(IPEndPoint destination)
        {
            _socket = new Socket(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _socket.Connect(destination);
        }

        /// <summary>Sends every byte of <paramref name="bytes"/> in a single write.</summary>
        public void Send(byte[] bytes) => _socket.Send(bytes);

        /// <summary>
        /// Ends the connection gracefully — a bare FIN with nothing further on the wire — the shape a
        /// truncation mid-prefix or mid-Message takes on a genuine stream close.
        /// </summary>
        public void CloseGracefully() => _socket.Shutdown(SocketShutdown.Send);

        /// <summary>
        /// Forces this socket's close to send an immediate RST instead of a graceful FIN, the reliable
        /// way to provoke a pending socket error on the far end's next read (mirrors
        /// <c>RawReactorHarness.CloseCounterpartyAbruptly</c>, this codebase's house pattern for it).
        /// </summary>
        public void ResetAbruptly()
        {
            _socket.LingerState = new LingerOption(true, 0);
            _socket.Close();
        }

        public void Dispose() => _socket.Dispose();
    }
}
