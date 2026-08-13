using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 3.4.1 (issue #79): the per-Peer receive buffer and whole-Message delivery through the frame
/// arena. The counterparty in every test here is a hand-built raw TCP peer writing framed bytes
/// on the wire — never a second <see cref="StreamSessionReactor"/> — mirroring
/// <see cref="StreamSessionHandshakeGuardTests"/>'s <c>RawTcpPeer</c>, the house pattern for this shape
/// of test.
/// </summary>
public class StreamSessionMessageFramingTests
{
    private static byte[] BuildFrame(byte[] payload) => StreamSessionTestRecords.BuildFrame(payload);

    /// <summary>Pumps <paramref name="accepter"/> until it has drained a <see cref="SessionEventKind.PeerConnected"/> event.</summary>
    private static void EstablishSession(StreamSessionReactorHarness accepter, MessagePeer peer, int maxPumps = 20) =>
        StreamSessionTestRecords.EstablishSession(accepter, peer.Send, maxPumps);

    [Fact]
    public void DeliverCompleteMessages_WholeFrameWrittenInOneSend_DrainsExactlyOneMessageReceivedWithThoseBytes()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using MessagePeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);
        byte[] payload = [1, 2, 3, 4, 5, 6, 7, 8];

        // Act
        peer.Send(BuildFrame(payload));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> drained = [];
        for (int i = 0; i < 20 && drained.Count == 0; i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToListWithPayload());
        }

        // Assert
        drained.Should().ContainSingle()
            .Which.Should().Match<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)>(
                e => e.Kind == SessionEventKind.MessageReceived);
        drained[0].Payload.Should().Equal(payload);
    }

    [Fact]
    public void DeliverCompleteMessages_MessageDeliveredOneByteAtATime_IsDeliveredExactlyOnceWholeAndOnlyWhenComplete()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using MessagePeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);
        byte[] payload = [10, 20, 30, 40, 50];
        byte[] frame = BuildFrame(payload);

        // Act / Assert — nothing drains while the frame is still arriving. Sending strictly fewer than
        // the full frame's bytes makes early completion impossible regardless of read/Pump timing.
        for (int i = 0; i < frame.Length - 1; i++)
        {
            peer.SendByte(frame[i]);
            accepter.Reactor.Pump();
            accepter.DrainToListWithPayload().Should().BeEmpty("the Message is still incomplete");
        }

        // Act — the final byte completes the frame. A run of single-byte writes on the same connection
        // can trip Nagle's algorithm, which withholds a small segment until an earlier one is
        // acknowledged — on Linux that ack can take longer than a tight, sleepless Pump loop allows for,
        // so the retry gives real wall-clock time between attempts (mirrors RawReactorSendTests).
        peer.SendByte(frame[^1]);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> drained = [];
        for (int i = 0; i < 20 && drained.Count == 0; i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToListWithPayload());
            if (drained.Count == 0)
            {
                Thread.Sleep(20);
            }
        }

        // Assert
        drained.Should().ContainSingle()
            .Which.Should().Match<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)>(
                e => e.Kind == SessionEventKind.MessageReceived);
        drained[0].Payload.Should().Equal(payload);
    }

    [Fact]
    public void DeliverCompleteMessages_MessageOfExactlyMaxMessageSize_RoundTripsIntact()
    {
        // Arrange
        const int maxMessageSize = 64;
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: maxMessageSize + 4);
        using MessagePeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);
        byte[] payload = new byte[maxMessageSize];
        Random.Shared.NextBytes(payload);

        // Act
        peer.Send(BuildFrame(payload));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> drained = [];
        for (int i = 0; i < 20 && drained.Count == 0; i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToListWithPayload());
        }

        // Assert
        drained.Should().ContainSingle()
            .Which.Should().Match<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)>(
                e => e.Kind == SessionEventKind.MessageReceived);
        drained[0].Payload.Should().Equal(payload);
    }

    [Fact]
    public void DeliverCompleteMessages_PartialMessageSplitAcrossPumps_SurvivesAndCompletesOnceTheRestArrives()
    {
        // Arrange — the frame arena is wiped every Pump, so this proves the tail survives in the
        // persistent per-Peer receive buffer rather than in the arena.
        using StreamSessionReactorHarness accepter = new();
        using MessagePeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);
        byte[] payload = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        byte[] frame = BuildFrame(payload);
        byte[] firstHalf = frame.AsSpan(0, 6).ToArray();
        byte[] secondHalf = frame.AsSpan(6).ToArray();

        // Act — the first half arrives, is read on its own Pump, and produces nothing.
        peer.Send(firstHalf);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> firstDrain = [];
        for (int i = 0; i < 20 && firstDrain.Count == 0; i++)
        {
            accepter.Reactor.Pump();
            firstDrain.AddRange(accepter.DrainToListWithPayload());
            if (i > 2)
            {
                break; // give a genuine chance for a (wrong) early delivery to show up, then stop waiting.
            }
        }

        firstDrain.Should().BeEmpty("only part of the Message has arrived");

        // Act — several more Pumps with nothing sent must not lose the buffered tail.
        for (int i = 0; i < 3; i++)
        {
            accepter.Reactor.Pump();
            accepter.DrainToListWithPayload().Should().BeEmpty();
        }

        // The first half's write may still be unacknowledged when this one lands, which can trip
        // Nagle's algorithm on Linux; give the retry real wall-clock time (mirrors RawReactorSendTests).
        peer.Send(secondHalf);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> secondDrain = [];
        for (int i = 0; i < 20 && secondDrain.Count == 0; i++)
        {
            accepter.Reactor.Pump();
            secondDrain.AddRange(accepter.DrainToListWithPayload());
            if (secondDrain.Count == 0)
            {
                Thread.Sleep(20);
            }
        }

        // Assert
        secondDrain.Should().ContainSingle()
            .Which.Should().Match<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)>(
                e => e.Kind == SessionEventKind.MessageReceived);
        secondDrain[0].Payload.Should().Equal(payload);
    }

    [Fact]
    public void Drain_AfterTheFrameThatDeliveredAMessage_TheNextPumpsDrainIsEmpty()
    {
        // Arrange — proves the payload was copied into the arena, not merely referenced: the arena
        // wipes every Pump (ADR 0008), so a second drain of the same Frame's data must not be possible
        // at all — there is nothing left to drain once the Frame that produced it has passed.
        using StreamSessionReactorHarness accepter = new();
        using MessagePeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);
        byte[] payload = [42, 43, 44];

        // Act
        peer.Send(BuildFrame(payload));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> drained = [];
        for (int i = 0; i < 20 && drained.Count == 0; i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToListWithPayload());
        }

        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.MessageReceived);

        // Act
        accepter.Reactor.Pump();
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> nextFrame =
            accepter.DrainToListWithPayload();

        // Assert
        nextFrame.Should().BeEmpty();
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// writing raw framed bytes on the wire — the instrument issue #79 itself calls for ("a
    /// <c>RawReactor</c> writing framed bytes, not a second Session Reactor"). This project's existing
    /// stream-handshake tests use a hand-built raw <see cref="Socket"/> for exactly this role (see
    /// <c>StreamSessionHandshakeGuardTests.RawTcpPeer</c>); this type follows the same house pattern
    /// rather than driving a full <see cref="RawReactor"/> Pump loop purely to write bytes it never
    /// needs to read back.
    /// </summary>
    private sealed class MessagePeer : IDisposable
    {
        private readonly Socket _socket;

        public MessagePeer(IPEndPoint destination)
        {
            _socket = new Socket(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _socket.Connect(destination);
        }

        /// <summary>Sends every byte of <paramref name="bytes"/> in a single write.</summary>
        public void Send(byte[] bytes) => _socket.Send(bytes);

        /// <summary>Sends exactly one byte as its own write, so the far end can never see more than it yet.</summary>
        public void SendByte(byte value) => _socket.Send([value]);

        public void Dispose() => _socket.Dispose();
    }
}
