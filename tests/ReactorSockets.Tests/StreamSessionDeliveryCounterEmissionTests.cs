using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 4.1.4 (issue #116, #25's second emission trigger): a delivery-counter record is emitted once
/// per <see cref="StreamSessionReactor.Pump"/> in which a Peer's received count actually advanced,
/// riding the Pump boundary the consumer already chose rather than a new timer or a trigger tied to a
/// count of Messages. The mandatory acceptance criterion — a one-way sender at high rate into a
/// receiver that never Sends application data must not exhaust retention over many Pumps — is
/// <see cref="Send_OneWayTrafficAtHighRateIntoAReceiverThatNeverSendsApplicationData_NeverExhaustsRetentionOverManyPumps"/>,
/// the only test in this file that needs two real <see cref="StreamSessionReactor"/> instances rather
/// than one Reactor and a hand-built raw counterparty, because the emission under test happens on the
/// *receiving* end and nothing but a real Reactor performs it. Every other test here uses the same
/// hand-built <see cref="EmissionPeer"/> counterparty shape <see cref="StreamSessionDeliveryCounterReleaseTests"/>'s
/// <c>ReleasePeer</c> established, reading the emitted record straight off the wire rather than
/// through any internal surface (L0/L1 are untested-through, tests/CLAUDE.md, ADR 0015).
/// </summary>
public class StreamSessionDeliveryCounterEmissionTests
{
    private static byte[] BuildFrame(byte[] payload) => StreamSessionTestRecords.BuildFrame(payload);

    private static byte[] PayloadOf(int length, byte marker) => StreamSessionTestRecords.PayloadOf(length, marker);

    /// <summary>
    /// Establishes a Session against a hand-built <see cref="EmissionPeer"/>, also draining the
    /// accepter's own Hello reply off the wire first — mirrors
    /// <see cref="StreamSessionDeliveryCounterReleaseTests"/>'s local <c>EstablishSession</c>, the house
    /// pattern for a raw counterparty that goes on to read further records off the same socket.
    /// </summary>
    private static PeerId EstablishSession(StreamSessionReactorHarness accepter, EmissionPeer peer, int maxPumps = 20)
    {
        PeerId id = StreamSessionTestRecords.EstablishSession(accepter, peer.Send, maxPumps);
        peer.Receive(StreamSessionTestRecords.HandshakeRecordSize);
        return id;
    }

    // =========================================================================================
    // The mandatory acceptance criterion: one-way traffic must not exhaust retention (the bug
    // this ticket exists to fix — issue #25's motivating scenario)
    // =========================================================================================

    [Fact]
    public void Send_OneWayTrafficAtHighRateIntoAReceiverThatNeverSendsApplicationData_NeverExhaustsRetentionOverManyPumps()
    {
        // Arrange — a ring that holds only five in-flight Messages: small enough that a sender flooding
        // it at line rate would exhaust it within a handful of Sends if nothing ever released it. The
        // receiver never calls Send at all — this is one-way traffic — so the send-idle timer (#114,
        // not yet landed on this branch) plays no part; the only thing that can possibly keep this ring
        // from exhausting is the receiver's own per-Pump delivery-counter emission (issue #116).
        const int maxMessageSize = 32;
        const int retentionBytes = 5 * (maxMessageSize + 4);
        const int totalMessages = 200;

        using StreamSessionReactorHarness sender = new(
            bindLoopback: false, maxMessageSize: maxMessageSize, retentionBytes: retentionBytes);
        using StreamSessionReactorHarness receiver = new(maxMessageSize: maxMessageSize, retentionBytes: retentionBytes);
        sender.Reactor.Connect(receiver.Endpoint);

        (List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> senderEvents,
            List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> receiverEvents) =
            StreamSessionReactorHarness.PumpBothUntil(sender, receiver, () => false, maxRounds: 10);

        PeerId senderSidePeer = senderEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerConnected).Which.Peer;
        PeerId receiverSidePeer = receiverEvents.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerConnected).Which.Peer;

        // Act — the sender floods Messages one per round, pumping both ends each round so the
        // receiver's delivery counter (emitted the moment its received count advances) has a chance to
        // reach the sender and release room before the next Send is attempted. Removing #116's per-Pump
        // trigger leaves nothing on this branch to ever release this ring, and this Send starts failing
        // with RetentionExhausted almost immediately — this is the test that must fail without it.
        for (int i = 0; i < totalMessages; i++)
        {
            byte[] payload = PayloadOf(maxMessageSize, (byte)(i % 256));
            SendOutcome outcome = sender.Reactor.Send(senderSidePeer, payload);
            sender.Reactor.Pump();
            receiver.Reactor.Pump();
            sender.Reactor.Pump();

            outcome.Should().NotBe(SendOutcome.RetentionExhausted,
                $"the receiver's per-Pump delivery-counter emission should keep releasing room well ahead of a {retentionBytes}-byte ring at Message {i}, and the Session is otherwise entirely healthy");
        }

        // Assert — the Session stayed open and every Message the sender ever queued actually reached
        // the receiver, confirming this was never a silent drop or a Session that quietly closed under
        // the loop above.
        receiver.PumpUntil(() => receiver.Reactor.CountersOf(receiverSidePeer).MessagesReceived == totalMessages);
        receiver.Reactor.CountersOf(receiverSidePeer).MessagesReceived.Should().Be(totalMessages);
    }

    // =========================================================================================
    // A Pump in which the received count did not advance emits nothing on this account
    // =========================================================================================

    [Fact]
    public void Pump_ReceivedCountDidNotAdvance_EmitsNoDeliveryCounterRecordThatPump()
    {
        // Arrange — one Message delivered and its emitted counter drained off the wire, establishing a
        // clean baseline to pump past.
        using StreamSessionReactorHarness accepter = new();
        using EmissionPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        peer.Send(BuildFrame(PayloadOf(16, 1)));
        accepter.PumpUntil(() => accepter.Reactor.CountersOf(id).MessagesReceived == 1);
        peer.Receive(12);

        // Act — a further Pump with nothing new having arrived: the received count cannot have advanced.
        accepter.Reactor.Pump();

        // Assert — nothing reaches the wire on this account.
        peer.TryReceiveWithinTimeout(1, out _, timeoutMs: 200).Should().BeFalse(
            "a Pump in which the received count did not advance must emit no counter record");
    }

    // =========================================================================================
    // A Pump delivering several Messages emits the counter once, not once per Message
    // =========================================================================================

    [Fact]
    public void Pump_DeliveringSeveralMessagesInOnePump_EmitsExactlyOneDeliveryCounterRecordCarryingTheirTotal()
    {
        // Arrange — arenaSize and receiveBufferSize both explicit and generous: their defaults each
        // resolve to roughly one Message's worth (MaxMessageSize), which would force the three Messages
        // below to arrive and deliver across more than one Pump regardless of the emission trigger under
        // test, for reasons unrelated to it — the per-Peer receive buffer in particular must hold all
        // three whole frames at once for a single socket read to ever hand DeliverCompleteMessages all
        // of them in the same Pump.
        using StreamSessionReactorHarness accepter = new(
            maxMessageSize: 32, retentionBytes: 200, arenaSize: 200, receiveBufferSize: 200);
        using EmissionPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        byte[] first = PayloadOf(16, 1);
        byte[] second = PayloadOf(16, 2);
        byte[] third = PayloadOf(16, 3);

        // Act — all three frames go out in a single write. A short sleep before the first Pump call —
        // the same idiom StreamSessionDeliveryCounterTests uses around Nagle-sensitive timing — is what
        // makes "one Pump delivers all three" true of the read side deterministically: without it, the
        // OS is free to hand the reactor's very first read only part of what was written, which would
        // split this Frame's delivery (and, correctly per this ticket's own rule, its counter emission)
        // across more than one Pump.
        peer.Send([.. BuildFrame(first), .. BuildFrame(second), .. BuildFrame(third)]);
        Thread.Sleep(20);
        accepter.Reactor.Pump();
        accepter.Reactor.CountersOf(id).MessagesReceived.Should().Be(3, "all three frames were on the wire together before this Pump ran");

        // Assert — exactly one 12-byte record is on the wire, carrying the total of three, not three
        // separate records.
        byte[] record = peer.Receive(12);
        StreamSessionTestRecords.TryReadDeliveryCounterRecord(record, out ulong count).Should().BeTrue();
        count.Should().Be(3);

        peer.TryReceiveWithinTimeout(1, out _, timeoutMs: 200).Should().BeFalse(
            "only one record should have been emitted for the whole Pump, not one per Message");
    }

    // =========================================================================================
    // The emitted record re-asserts issue #82's property: it counts only what was handed to the
    // arena, never merely what has fully arrived
    // =========================================================================================

    [Fact]
    public void Pump_AMessageHeldBackByAnExhaustedReadShare_TheEmittedRecordCountsOnlyWhatWasHandedToTheArena()
    {
        // Arrange — capacity 4, arena 200 bytes -> a 50-byte read share per Peer (mirrors
        // StreamSessionDeliveryCounterTests). Three 20-byte Messages total 60 bytes, so the third
        // cannot be reserved alongside the first two in the same Frame and stays buffered, whole, with
        // its bytes fully arrived but not yet handed to the arena.
        using StreamSessionReactorHarness accepter = new(
            capacity: 4, maxMessageSize: 64, retentionBytes: 68, receiveBufferSize: 100, arenaSize: 200);
        using EmissionPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        byte[] first = PayloadOf(20, 1);
        byte[] second = PayloadOf(20, 2);
        byte[] third = PayloadOf(20, 3);

        // Act — all three frames' bytes arrive before any Pump runs.
        peer.Send([.. BuildFrame(first), .. BuildFrame(second), .. BuildFrame(third)]);
        accepter.PumpUntil(() => accepter.Reactor.CountersOf(id).MessagesReceived > 0);

        // Assert — the first emitted record reads 2, not 3: the third Message's bytes have all arrived
        // but were not yet handed to the arena this Frame.
        byte[] firstRecord = peer.Receive(12);
        StreamSessionTestRecords.TryReadDeliveryCounterRecord(firstRecord, out ulong firstCount).Should().BeTrue();
        firstCount.Should().Be(2, "the third Message's bytes have all arrived but it lost the read-share race and was not delivered");

        // Act — no further bytes are ever sent; the held-back Message surfaces on a later Pump via the
        // retry mechanism (#81).
        accepter.PumpUntil(() => accepter.Reactor.CountersOf(id).MessagesReceived == 3);

        // Assert — the next emitted record reads 3, once the third Message is actually delivered.
        byte[] secondRecord = peer.Receive(12);
        StreamSessionTestRecords.TryReadDeliveryCounterRecord(secondRecord, out ulong secondCount).Should().BeTrue();
        secondCount.Should().Be(3);
    }

    // =========================================================================================
    // Two-way traffic rides the write already occurring — no additional record beyond the one
    // this Pump's own flush already carries
    // =========================================================================================

    [Fact]
    public void Pump_TwoWayTrafficInTheSamePump_EmitsExactlyOneDeliveryCounterRecordAlongsideTheOutboundMessage()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(maxMessageSize: 32, retentionBytes: 200);
        using EmissionPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        byte[] inbound = PayloadOf(16, 9);
        byte[] outboundPayload = PayloadOf(16, 8);

        // Act — the peer's Message is already waiting to be read, and this end also queues its own
        // outbound Send before the same Pump runs: the delivery-counter emission and the retention
        // flush both fire once, inside that one Pump, rather than the two-way traffic doubling anything.
        peer.Send(BuildFrame(inbound));
        accepter.Reactor.Send(id, outboundPayload).Should().Be(SendOutcome.Sent);
        accepter.PumpUntil(() => accepter.Reactor.CountersOf(id).MessagesReceived == 1);

        // Assert — exactly the counter record (emitted ahead of the retention flush within the same
        // Pump) followed by the outbound Message frame, in that order, and nothing further.
        byte[] counterRecord = peer.Receive(12);
        StreamSessionTestRecords.TryReadDeliveryCounterRecord(counterRecord, out ulong count).Should().BeTrue();
        count.Should().Be(1);

        byte[] outboundFrame = peer.Receive(BuildFrame(outboundPayload).Length);
        outboundFrame.Should().Equal(BuildFrame(outboundPayload));

        peer.TryReceiveWithinTimeout(1, out _, timeoutMs: 200).Should().BeFalse(
            "a two-way Peer must not emit any additional record beyond the one riding this Pump's already-occurring write");
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// that drives the handshake, sends raw framed Messages, and reads whatever
    /// <see cref="StreamSessionReactor"/> emits back — including the delivery-counter records this
    /// file's tests are about. Mirrors <see cref="StreamSessionDeliveryCounterReleaseTests"/>'s
    /// <c>ReleasePeer</c>.
    /// </summary>
    private sealed class EmissionPeer : IDisposable
    {
        private readonly Socket _socket;

        public EmissionPeer(IPEndPoint destination)
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
        /// Attempts to read exactly <paramref name="count"/> bytes within <paramref name="timeoutMs"/>,
        /// returning <see langword="false"/> on timeout, graceful close, or any socket error rather than
        /// throwing — mirrors <c>StreamSessionGoodbyeTests</c>' <c>GoodbyePeer.TryReceiveWithinTimeout</c>,
        /// the house pattern for asserting nothing further arrives.
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
