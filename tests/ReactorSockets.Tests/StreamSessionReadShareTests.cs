using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 3.4.3 (issue #81): the per-Peer read share and the one-whole-Message escape.
/// <see cref="Core.FrameArena"/> already derives Arena ÷ Capacity and implements the escape that lets
/// a share-holder always deliver one whole unit (see <c>FrameArena.Reserve</c>); this class exercises
/// <see cref="StreamSessionReactor"/> making Peers the share-holders, by slot index, and — the actual
/// gap #79's and #80's reports flagged — that a Peer whose buffer still holds a whole, undelivered
/// Message after losing the share race is revisited on a later Pump even when the far end sends
/// nothing further. The counterparty in every test here is a hand-built raw TCP peer writing framed
/// bytes on the wire, mirroring <see cref="StreamSessionMessageFramingTests"/>'s <c>MessagePeer</c>.
/// </summary>
public class StreamSessionReadShareTests
{
    private static byte[] BuildFrame(byte[] payload) => StreamSessionTestRecords.BuildFrame(payload);

    private static byte[] PayloadOf(int length, byte marker) => StreamSessionTestRecords.PayloadOf(length, marker);

    /// <summary>Pumps <paramref name="accepter"/> until it has drained a <see cref="SessionEventKind.PeerConnected"/> event, returning the established Peer's id.</summary>
    private static PeerId EstablishSession(StreamSessionReactorHarness accepter, SharePeer peer, TimeSpan? budget = null) =>
        StreamSessionTestRecords.EstablishSession(accepter, peer.Send, budget);

    /// <summary>Pumps until at least one event drains, returning what that first non-empty Pump produced.</summary>
    private static List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason, byte[] Payload)> PumpUntilNonEmpty(
        StreamSessionReactorHarness accepter, TimeSpan? budget = null)
    {
        PumpDeadline deadline = PumpDeadline.Start(budget);
        while (true)
        {
            accepter.Reactor.Pump();
            var drained = accepter.DrainToListWithPayload();
            if (drained.Count > 0)
            {
                return drained;
            }

            if (deadline.Expired)
            {
                return [];
            }

            PumpDeadline.Yield();
        }
    }

    [Fact]
    public void DeliverCompleteMessages_SeveralMessagesInOneReadWithinTheShare_DeliversAllInTheSamePump()
    {
        // Arrange — capacity 4, arena 200 bytes -> a 50-byte read share per Peer. Two 20-byte
        // Messages fit comfortably inside one Peer's share.
        using StreamSessionReactorHarness accepter = new(
            capacity: 4, maxMessageSize: 64, retentionBytes: 68, receiveBufferSize: 100, arenaSize: 200);
        using SharePeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);

        byte[] first = PayloadOf(20, 1);
        byte[] second = PayloadOf(20, 2);

        // Act — both frames written before the first Pump, so a single TCP read can carry both.
        byte[] combined = [.. BuildFrame(first), .. BuildFrame(second)];
        peer.Send(combined);
        var drained = PumpUntilNonEmpty(accepter);

        // Assert
        drained.Should().HaveCount(2);
        drained.Should().OnlyContain(e => e.Kind == SessionEventKind.MessageReceived);
        drained[0].Payload.Should().Equal(first);
        drained[1].Payload.Should().Equal(second);
    }

    [Fact]
    public void DeliverCompleteMessages_MessagesBeyondTheShare_DeliverOnALaterPumpInOrderWithoutFurtherDataFromThePeer()
    {
        // Arrange — same 50-byte share as above. Three 20-byte Messages total 60 bytes, so the third
        // cannot fit alongside the first two in the same Frame.
        using StreamSessionReactorHarness accepter = new(
            capacity: 4, maxMessageSize: 64, retentionBytes: 68, receiveBufferSize: 100, arenaSize: 200);
        using SharePeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);

        byte[] first = PayloadOf(20, 1);
        byte[] second = PayloadOf(20, 2);
        byte[] third = PayloadOf(20, 3);

        // Act — all three frames written up front; the peer never sends anything else.
        byte[] combined = [.. BuildFrame(first), .. BuildFrame(second), .. BuildFrame(third)];
        peer.Send(combined);

        var firstDrain = PumpUntilNonEmpty(accepter);

        // Assert — the first Pump to see data delivers only what fits the share.
        firstDrain.Should().HaveCount(2, "the third Message exceeds this Peer's read share for the Frame");
        firstDrain[0].Payload.Should().Equal(first);
        firstDrain[1].Payload.Should().Equal(second);

        // Act — no further bytes are ever sent by the peer. The leftover Message must still surface.
        var secondDrain = PumpUntilNonEmpty(accepter);

        // Assert — delivered on a later Pump, whole, without any new socket activity from the peer.
        secondDrain.Should().ContainSingle();
        secondDrain[0].Kind.Should().Be(SessionEventKind.MessageReceived);
        secondDrain[0].Payload.Should().Equal(third);
    }

    [Fact]
    public void DeliverCompleteMessages_MessageLargerThanTheShare_StillDeliversWholeInOnePump()
    {
        // Arrange — capacity 8, arena defaults to the profile floor of MaxMessageSize (64) -> an
        // 8-byte read share per Peer, far smaller than the 50-byte Message about to be sent.
        using StreamSessionReactorHarness accepter = new(capacity: 8, maxMessageSize: 64, retentionBytes: 68);
        using SharePeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);
        byte[] payload = PayloadOf(50, 7);

        // Act
        peer.Send(BuildFrame(payload));
        var drained = PumpUntilNonEmpty(accepter);

        // Assert — the escape: a share-holder's first request in a Frame always goes through, even
        // though it exceeds its fair share.
        drained.Should().ContainSingle();
        drained[0].Kind.Should().Be(SessionEventKind.MessageReceived);
        drained[0].Payload.Should().Equal(payload);
    }

    [Fact]
    public void DeliverCompleteMessages_TwoPeers_OneSaturatingItsShareDoesNotReduceTheOthersDeliveryInTheSameFrame()
    {
        // Arrange — same 50-byte share as the earlier tests, but two established Peers.
        using StreamSessionReactorHarness accepter = new(
            capacity: 4, maxMessageSize: 64, retentionBytes: 68, receiveBufferSize: 100, arenaSize: 200);
        using SharePeer peerA = new(accepter.Endpoint);
        using SharePeer peerB = new(accepter.Endpoint);
        PeerId idA = EstablishSession(accepter, peerA);
        PeerId idB = EstablishSession(accepter, peerB);

        byte[] a1 = PayloadOf(20, 11);
        byte[] a2 = PayloadOf(20, 12);
        byte[] a3 = PayloadOf(20, 13);
        byte[] b1 = PayloadOf(20, 21);
        byte[] b2 = PayloadOf(20, 22);

        // Act — Peer A sends three Messages (exceeding its own share); Peer B sends two (fitting its
        // own share entirely), both before the Pump that reads them.
        peerA.Send([.. BuildFrame(a1), .. BuildFrame(a2), .. BuildFrame(a3)]);
        peerB.Send([.. BuildFrame(b1), .. BuildFrame(b2)]);
        var drained = PumpUntilNonEmpty(accepter);

        // Assert — Peer B's two Messages both land in this same Frame, untouched by Peer A saturating
        // its own, separate share.
        var fromB = drained.Where(e => e.Peer.Equals(idB)).ToList();
        fromB.Should().HaveCount(2);
        fromB[0].Payload.Should().Equal(b1);
        fromB[1].Payload.Should().Equal(b2);

        // Assert — Peer A only got as far as its own share allowed.
        var fromA = drained.Where(e => e.Peer.Equals(idA)).ToList();
        fromA.Should().HaveCount(2);
        fromA[0].Payload.Should().Equal(a1);
        fromA[1].Payload.Should().Equal(a2);

        // Act — Peer A's third Message surfaces on a later Pump with no further data sent.
        var laterDrain = PumpUntilNonEmpty(accepter);

        // Assert
        laterDrain.Should().ContainSingle();
        laterDrain[0].Peer.Should().Be(idA);
        laterDrain[0].Payload.Should().Equal(a3);
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// writing raw framed bytes on the wire — mirroring <see cref="StreamSessionMessageFramingTests"/>'s
    /// <c>MessagePeer</c> and <see cref="StreamSessionHandshakeGuardTests"/>'s <c>RawTcpPeer</c>.
    /// </summary>
    private sealed class SharePeer : IDisposable
    {
        private readonly Socket _socket;

        public SharePeer(IPEndPoint destination)
        {
            _socket = new Socket(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _socket.Connect(destination);
        }

        /// <summary>Sends every byte of <paramref name="bytes"/> in a single write.</summary>
        public void Send(byte[] bytes) => _socket.Send(bytes);

        public void Dispose() => _socket.Dispose();
    }
}
