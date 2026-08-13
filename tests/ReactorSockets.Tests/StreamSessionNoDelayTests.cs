using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 3.6.1 (issue #89): every stream Session socket — accepted and dialled alike — runs with
/// Nagle off (ADR 0011). The socket option itself sits on the internal <c>TcpLink</c> and is not
/// reachable through the public L2 surface (tests/CLAUDE.md), so this proves the property the
/// acceptance criteria explicitly allow instead: a small Message sent while an earlier one is still
/// unacknowledged arrives without the Nagle-shaped stall — the classic interaction with a peer's own
/// delayed-ACK timer, tens of milliseconds — that Nagle would otherwise impose.
/// </summary>
public class StreamSessionNoDelayTests
{
    private const int RecordSize = StreamSessionTestRecords.HandshakeRecordSize;

    private static byte[] BuildFrame(byte[] payload) => StreamSessionTestRecords.BuildFrame(payload);

    private static byte[] PayloadOf(int length, byte marker) => StreamSessionTestRecords.PayloadOf(length, marker);

    /// <summary>
    /// Pumps <paramref name="accepter"/> until it has drained a <see cref="SessionEventKind.PeerConnected"/>
    /// event, returning the established Peer's id. Also reads the accepter's own Hello record off
    /// <paramref name="peer"/>'s socket, so nothing but this test's own Messages is left on the wire.
    /// </summary>
    private static PeerId EstablishSession(StreamSessionReactorHarness accepter, NoDelayPeer peer, int maxPumps = 20)
    {
        PeerId id = StreamSessionTestRecords.EstablishSession(accepter, peer.Send, maxPumps);
        peer.Receive(RecordSize);
        return id;
    }

    [Fact]
    public void Send_ASmallMessageWhileAnEarlierOneIsStillUnacknowledged_ArrivesWithoutANagleShapedStall()
    {
        // Arrange — two tiny Messages, sent from the accepted socket back-to-back with nothing read
        // from the peer between them, so the first is still unacknowledged when the second is flushed.
        // Nagle, if it were on, would hold that second segment back until an ACK for the first arrived
        // — and it is exactly loopback's own delayed-ACK timer (tens of milliseconds either platform)
        // that would surface as a stall here, which is why this asserts elapsed time rather than
        // reading the socket option directly (unreachable through the public surface, tests/CLAUDE.md).
        const int maxMessageSize = 16;
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: 100);
        using NoDelayPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        byte[] first = PayloadOf(4, 1);
        byte[] second = PayloadOf(4, 2);

        // Act
        Stopwatch stopwatch = Stopwatch.StartNew();
        accepter.Reactor.Send(id, first).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        accepter.Reactor.Send(id, second).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        byte[] onWire = peer.Receive(BuildFrame(first).Length + BuildFrame(second).Length);
        stopwatch.Stop();

        // Assert — both Messages arrive intact, in order, and well inside a Nagle-shaped stall. The
        // classic Nagle-plus-delayed-ACK combination runs into the tens of milliseconds; this bound is
        // generous enough to absorb CI jitter while still catching a reintroduced Nagle delay.
        onWire.Should().Equal([.. BuildFrame(first), .. BuildFrame(second)]);
        stopwatch.Elapsed.Should().BeLessThan(TimeSpan.FromMilliseconds(30));
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// that both drives the handshake and reads raw bytes the Reactor sends back — mirroring
    /// <see cref="StreamSessionHandshakeGuardTests"/>'s <c>RawTcpPeer</c> and
    /// <see cref="StreamSessionRetentionFlushTests"/>'s <c>FlushPeer</c>, the house pattern for a test
    /// that needs to read bytes off the wire rather than only write them.
    /// </summary>
    private sealed class NoDelayPeer : IDisposable
    {
        private readonly Socket _socket;

        public NoDelayPeer(IPEndPoint destination)
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
