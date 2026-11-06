using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The per-Peer whole-Message delivery counter, and its load-bearing
/// property — it advances only when a Message is actually handed to the frame arena for delivery,
/// never merely because its bytes have all arrived. The held-back case reuses #81's read-share
/// exhaustion mechanism (a capacity/arena small enough that a second whole Message in one Frame
/// cannot be reserved) to make that distinction observable through the public counter surface
/// across two <see cref="StreamSessionReactor.Pump"/> calls, exactly as the issue's negative
/// acceptance criterion requires.
/// </summary>
public class StreamSessionDeliveryCounterTests
{
    private static byte[] BuildFrame(byte[] payload) => StreamSessionTestRecords.BuildFrame(payload);

    private static byte[] PayloadOf(int length, byte marker) => StreamSessionTestRecords.PayloadOf(length, marker);

    private static PeerId EstablishSession(StreamSessionReactorHarness accepter, CounterPeer peer, TimeSpan? budget = null) =>
        StreamSessionTestRecords.EstablishSession(accepter, peer.Send, budget);

    [Fact]
    public void CountersOf_AfterThreeDeliveredMessages_MessagesReceivedReadsThree()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(maxMessageSize: 64, retentionBytes: 68);
        using CounterPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        // Act — three whole Messages, each given its own Pump so each is delivered before the next
        // is sent. A run of small sends on one connection can trip Nagle's algorithm, which withholds
        // a segment until an earlier one is acknowledged — on Linux that ack can take longer than
        // PumpUntil's sleepless retries allow for, so a failed attempt gets real wall-clock time before
        // retrying (mirrors RawReactorSendTests' sleep-on-no-progress idiom).
        for (byte i = 0; i < 3; i++)
        {
            peer.Send(BuildFrame(PayloadOf(10, i)));
            for (int attempt = 0; attempt < 5 && accepter.Reactor.CountersOf(id).MessagesReceived != (ulong)(i + 1); attempt++)
            {
                if (attempt > 0)
                {
                    Thread.Sleep(20);
                }

                accepter.PumpUntil(() => accepter.Reactor.CountersOf(id).MessagesReceived == (ulong)(i + 1));
            }
        }

        // Assert
        accepter.Reactor.CountersOf(id).MessagesReceived.Should().Be(3);
    }

    [Fact]
    public void CountersOf_AMessageHeldBackByAnExhaustedReadShare_DoesNotAdvanceUntilThePumpThatDeliversIt()
    {
        // Arrange — capacity 4, arena 200 bytes -> a 50-byte read share per Peer (mirrors
        // StreamSessionReadShareTests). Three 20-byte Messages total 60 bytes, so the third cannot be
        // reserved alongside the first two in the same Frame and stays buffered, whole, with its
        // bytes fully arrived but not yet handed to the arena.
        using StreamSessionReactorHarness accepter = new(
            capacity: 4, maxMessageSize: 64, retentionBytes: 68, receiveBufferSize: 100, arenaSize: 200);
        using CounterPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        byte[] first = PayloadOf(20, 1);
        byte[] second = PayloadOf(20, 2);
        byte[] third = PayloadOf(20, 3);

        // Act — all three frames arrive before any Pump runs, so the third Message's bytes are fully
        // present at the front of the receive buffer from the very first read. Pump until this Peer's
        // buffer actually has something delivered — the counter itself is the condition, not a drain,
        // so nothing here can race with observing it.
        peer.Send([.. BuildFrame(first), .. BuildFrame(second), .. BuildFrame(third)]);
        accepter.PumpUntil(() => accepter.Reactor.CountersOf(id).MessagesReceived > 0);

        // Assert — the Pump that delivers the first two Messages (all the share allows) counts
        // exactly two, not three: the third's bytes are fully arrived but it was never handed to the
        // arena this Frame.
        accepter.Reactor.CountersOf(id).MessagesReceived.Should().Be(2,
            "the third Message's bytes have all arrived but it lost the read-share race and was not delivered");

        // Act — no further bytes are ever sent by the peer; the held-back Message surfaces on a later
        // Pump via the retry mechanism (#81).
        accepter.PumpUntil(() => accepter.Reactor.CountersOf(id).MessagesReceived == 3);

        // Assert — the counter advances to three only once the third Message is actually delivered.
        accepter.Reactor.CountersOf(id).MessagesReceived.Should().Be(3);
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// writing raw framed bytes on the wire — mirroring <see cref="StreamSessionReadShareTests"/>'s
    /// <c>SharePeer</c>.
    /// </summary>
    private sealed class CounterPeer : IDisposable
    {
        private readonly Socket _socket;

        public CounterPeer(IPEndPoint destination)
        {
            _socket = new Socket(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _socket.Connect(destination);
        }

        /// <summary>Sends every byte of <paramref name="bytes"/> in a single write.</summary>
        public void Send(byte[] bytes) => _socket.Send(bytes);

        public void Dispose() => _socket.Dispose();
    }
}
