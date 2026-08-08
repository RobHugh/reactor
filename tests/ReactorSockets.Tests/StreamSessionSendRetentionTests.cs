using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 3.5.3 (issue #85): <see cref="StreamSessionReactor.Send(PeerId, ReadOnlySpan{byte})"/> frames
/// a Message and appends it to a Peer's retention region, reporting <see cref="SendOutcome.Sent"/> or
/// <see cref="SendOutcome.RetentionExhausted"/>. No socket write happens here — flushing retained bytes
/// to the wire is #86 — so every assertion here is about the ring accepting or refusing, never about
/// what a counterparty observes.
/// </summary>
public class StreamSessionSendRetentionTests
{
    /// <summary>
    /// Connects a bare TCP peer, drives the handshake to completion, and returns the <see cref="PeerId"/>
    /// the accepter assigned it — the address every <c>Send</c> call in this file is made against.
    /// </summary>
    private static PeerId EstablishSession(StreamSessionReactorHarness accepter, out HandshakePeer peer, int maxPumps = 20)
    {
        HandshakePeer newPeer = new(accepter.Endpoint);
        peer = newPeer;
        return StreamSessionTestRecords.EstablishSession(accepter, newPeer.Send, maxPumps);
    }

    [Fact]
    public void Send_ExactlyMaxMessageSizeIntoRetentionBoundedToFitExactlyOne_ReturnsSent()
    {
        // Arrange — retention bounded to exactly one maximum Message plus its 4-byte prefix, so this
        // first Send fills the region to the byte with no room left over.
        const int maxMessageSize = 64;
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: maxMessageSize + 4);
        PeerId peerId = EstablishSession(accepter, out HandshakePeer peer);
        using (peer)
        {
            byte[] message = new byte[maxMessageSize];

            // Act
            SendOutcome outcome = accepter.Reactor.Send(peerId, message);

            // Assert
            outcome.Should().Be(SendOutcome.Sent);
        }
    }

    [Fact]
    public void Send_SecondSendBeforeAnyRelease_ReturnsRetentionExhausted()
    {
        // Arrange — the first Send fills the region exactly (see the test above); a second Send of any
        // size, before anything is released, has nowhere left to go.
        const int maxMessageSize = 64;
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: maxMessageSize + 4);
        PeerId peerId = EstablishSession(accepter, out HandshakePeer peer);
        using (peer)
        {
            accepter.Reactor.Send(peerId, new byte[maxMessageSize]).Should().Be(SendOutcome.Sent);

            // Act
            SendOutcome outcome = accepter.Reactor.Send(peerId, [1]);

            // Assert
            outcome.Should().Be(SendOutcome.RetentionExhausted);
        }
    }

    [Fact]
    public void Send_MessageLongerThanMaxMessageSize_Throws()
    {
        // Arrange
        const int maxMessageSize = 64;
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: maxMessageSize + 4);
        PeerId peerId = EstablishSession(accepter, out HandshakePeer peer);
        using (peer)
        {
            byte[] tooLong = new byte[maxMessageSize + 1];

            // Act
            Action act = () => accepter.Reactor.Send(peerId, tooLong);

            // Assert
            act.Should().Throw<ArgumentException>();
        }
    }

    [Fact]
    public void Send_UnknownPeerId_ThrowsMatchingCountersOfsConvention()
    {
        // Arrange — a PeerId that was never issued by this Reactor at all.
        using StreamSessionReactorHarness accepter = new();
        PeerId unknown = new(Origin: 1, Index: 0, Generation: 1);

        // Act
        Action act = () => accepter.Reactor.Send(unknown, [1, 2, 3]);

        // Assert — the same failure shape CountersOf(PeerId) uses for an unresolved Peer.
        act.Should().Throw<ArgumentException>();
    }

    /// <summary>
    /// The distinctness this ticket's second acceptance criterion asks for, proven directly rather than
    /// by construction alone: <see cref="SendOutcome"/> and <see cref="DisconnectReason"/> are separate
    /// enum types that share no member name, so no consumer switch or pattern match over one can be
    /// silently satisfied by a value meant for the other.
    /// </summary>
    [Fact]
    public void SendOutcome_RetentionExhausted_IsDistinctFromEveryDisconnectReasonValue()
    {
        // Arrange
        Type sendOutcomeType = typeof(SendOutcome);
        Type disconnectReasonType = typeof(DisconnectReason);
        string[] sendOutcomeNames = Enum.GetNames(sendOutcomeType);
        string[] disconnectReasonNames = Enum.GetNames(disconnectReasonType);

        // Assert — genuinely separate CLR types, not two names for one enum.
        sendOutcomeType.Should().NotBe(disconnectReasonType);

        // Assert — RetentionExhausted names no DisconnectReason member, so handling code cannot
        // pattern-match one enum's member as if it belonged to the other by name.
        disconnectReasonNames.Should().NotContain(nameof(SendOutcome.RetentionExhausted));

        // Assert — no member name at all is shared between the two enums, so a switch expression
        // ported from one to the other without changing the type would not silently keep compiling.
        sendOutcomeNames.Should().NotIntersectWith(disconnectReasonNames);

        // Assert — an object boxing a SendOutcome is never, at runtime, an instance of
        // DisconnectReason (or vice versa): a consumer cannot receive a Send outcome through a
        // DisconnectReason-typed hole, whatever the numeric value underneath happens to be.
        object boxedRetentionExhausted = SendOutcome.RetentionExhausted;
        (boxedRetentionExhausted is DisconnectReason).Should().BeFalse();
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint — enough to complete the Hello
    /// exchange and hold the connection open for the Peer's whole lifetime in a test. Mirrors
    /// <see cref="StreamSessionMessageFramingTests"/>'s <c>MessagePeer</c>, the house pattern for this
    /// shape of test.
    /// </summary>
    private sealed class HandshakePeer : IDisposable
    {
        private readonly Socket _socket;

        public HandshakePeer(IPEndPoint destination)
        {
            _socket = new Socket(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _socket.Connect(destination);
        }

        /// <summary>Sends every byte of <paramref name="bytes"/> in a single write.</summary>
        public void Send(byte[] bytes) => _socket.Send(bytes);

        public void Dispose() => _socket.Dispose();
    }
}
