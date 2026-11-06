using System.Net;
using System.Net.Sockets;
using System.Reflection;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A Peer that has written nothing to the wire for
/// <see cref="StreamSessionOptions.SendIdleInterval"/> sends its delivery-counter record as a heartbeat
/// — unconditionally, never consulting whether anything was received (issue #118 tests that rule
/// directly against a Peer that only ever sends into a receiver that never confirms). Every test here
/// drives a real <see cref="StreamSessionReactor"/> against a hand-built raw counterparty
/// (<see cref="HeartbeatPeer"/>, mirroring <see cref="StreamSessionDeliveryCounterEmissionTests"/>'s
/// <c>EmissionPeer</c>) through the public L2 surface only (tests/CLAUDE.md, ADR 0015), driven entirely
/// by <see cref="StreamSessionReactorHarness.TimeProvider"/> — no test here sleeps to observe timer
/// behaviour.
/// </summary>
public class StreamSessionSendIdleHeartbeatTests
{
    private static byte[] BuildFrame(byte[] payload) => StreamSessionTestRecords.BuildFrame(payload);

    private static byte[] PayloadOf(int length, byte marker) => StreamSessionTestRecords.PayloadOf(length, marker);

    /// <summary>
    /// Establishes a Session against a hand-built <see cref="HeartbeatPeer"/>, also draining the
    /// accepter's own Hello reply off the wire first — mirrors every other file in this suite that goes
    /// on to read further records off the same raw socket.
    /// </summary>
    private static PeerId EstablishSession(StreamSessionReactorHarness accepter, HeartbeatPeer peer, TimeSpan? budget = null)
    {
        PeerId id = StreamSessionTestRecords.EstablishSession(accepter, peer.Send, budget);
        peer.Receive(StreamSessionTestRecords.HandshakeRecordSize);
        return id;
    }

    // =========================================================================================
    // AC1 — a Session sending nothing emits the record once SendIdleInterval has elapsed
    // =========================================================================================

    [Fact]
    public void Pump_SessionSendingNothing_EmitsADeliveryCounterRecordOnceSendIdleIntervalElapses()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using HeartbeatPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);

        // Act — one Pump short of the interval: nothing must be on the wire yet.
        accepter.TimeProvider.Advance(accepter.SendIdleInterval - TimeSpan.FromMilliseconds(1));
        accepter.Reactor.Pump();

        // Assert
        peer.TryReceiveWithinTimeout(12, out _, timeoutMs: 200).Should().BeFalse(
            "the send-idle interval has not fully elapsed yet");

        // Act — the last millisecond closes the interval.
        accepter.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
        accepter.Reactor.Pump();

        // Assert — a heartbeat carrying a received count of zero, since nothing was ever received.
        byte[] record = peer.Receive(12);
        StreamSessionTestRecords.TryReadDeliveryCounterRecord(record, out ulong count).Should().BeTrue();
        count.Should().Be(0, "nothing was ever received on this Session");
    }

    // =========================================================================================
    // AC2 — a Session with continuous outbound traffic never emits a heartbeat
    // =========================================================================================

    [Fact]
    public void Pump_ContinuousOutboundTrafficWithinEveryInterval_NeverEmitsASendIdleHeartbeat()
    {
        // Arrange — missedHeartbeats is raised well past the default so this test's own receive-idle
        // deadline (issue #117: RemoteSendIdleInterval x ResolvedMissedHeartbeats) cannot fire during
        // the roughly five seconds of silence from HeartbeatPeer below, which never sends anything back;
        // that silence is irrelevant to what this test is about — whether continuous outbound traffic
        // suppresses the send-idle heartbeat — so the receive-idle timer is pushed out of the way rather
        // than exercised here.
        using StreamSessionReactorHarness accepter = new(maxMessageSize: 32, retentionBytes: 400, missedHeartbeats: 60);
        using HeartbeatPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        // Act — five rounds of a Message sent just under the interval apart, every one of them
        // resetting the deadline before it can ever elapse.
        TimeSpan margin = accepter.SendIdleInterval - TimeSpan.FromMilliseconds(1);
        for (int i = 0; i < 5; i++)
        {
            accepter.TimeProvider.Advance(margin);
            byte[] payload = PayloadOf(16, (byte)i);
            accepter.Reactor.Send(id, payload).Should().Be(SendOutcome.Sent);
            accepter.Reactor.Pump();

            // Drain exactly this round's Message frame — a heartbeat interleaved here would leave
            // extra bytes behind that the next round's Receive would misread as part of the frame.
            peer.Receive(BuildFrame(payload).Length);
        }

        // Assert — nothing beyond the five Message frames ever reached the wire.
        peer.TryReceiveWithinTimeout(1, out _, timeoutMs: 200).Should().BeFalse(
            "continuous outbound traffic within every interval must never trigger the send-idle heartbeat");
    }

    // =========================================================================================
    // AC3 — sending a Message resets the timer to one interval after that Message, not after
    // connection (or any earlier heartbeat)
    // =========================================================================================

    [Fact]
    public void Pump_SendingAMessage_ResetsTheSendIdleTimerToOneIntervalAfterThatMessage()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new(maxMessageSize: 32, retentionBytes: 200);
        using HeartbeatPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        // Act — halfway through the interval, this end sends a Message. If the timer only ever counted
        // from connection, a full interval would already have elapsed by the second Pump below; the
        // Message must instead push the deadline out to one interval after itself.
        TimeSpan halfInterval = TimeSpan.FromTicks(accepter.SendIdleInterval.Ticks / 2);
        accepter.TimeProvider.Advance(halfInterval);

        byte[] payload = PayloadOf(16, 1);
        accepter.Reactor.Send(id, payload).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        peer.Receive(BuildFrame(payload).Length);

        // Act — advance to one millisecond short of a full interval past that Message. Measured from
        // connection instead, this point is already well past the interval, so a Reactor that failed to
        // reset the deadline on the Message would already have fired here.
        accepter.TimeProvider.Advance(accepter.SendIdleInterval - TimeSpan.FromMilliseconds(1));
        accepter.Reactor.Pump();

        // Assert — still nothing: the deadline is measured from the Message, not from connection.
        peer.TryReceiveWithinTimeout(12, out _, timeoutMs: 200).Should().BeFalse(
            "the Message reset the timer, so a full interval has not yet passed since it was sent");

        // Act — the last millisecond closes the interval measured from the Message.
        accepter.TimeProvider.Advance(TimeSpan.FromMilliseconds(1));
        accepter.Reactor.Pump();

        // Assert
        byte[] record = peer.Receive(12);
        StreamSessionTestRecords.TryReadDeliveryCounterRecord(record, out ulong count).Should().BeTrue();
        count.Should().Be(0);
    }

    // =========================================================================================
    // AC4 — the emitted record carries this end's current received-Message count, and the peer's
    // retention releases against it exactly as any delivery-counter record does
    // =========================================================================================

    [Fact]
    public void Pump_SendIdleHeartbeat_CarriesTheCurrentReceivedCountAndReleasesThePeersRetentionWhenApplied()
    {
        // Arrange — Part 1: isolate the record the send-idle timer itself produces. Two Messages are
        // received and immediately confirmed by the ordinary received-count trigger (#116) first, which
        // is drained and set aside — what is under test is the *next* record, which can only come from
        // the send-idle timer, since the received count does not advance again after this point.
        // receiveBufferSize and arenaSize are both explicit and generous: their defaults each resolve to
        // roughly one Message's worth (MaxMessageSize), which would force the two Messages below to
        // arrive and deliver across more than one Pump regardless of Thread.Sleep below — mirrors
        // StreamSessionDeliveryCounterEmissionTests' own note on this exact pitfall.
        using StreamSessionReactorHarness accepter = new(
            maxMessageSize: 32, retentionBytes: 200, receiveBufferSize: 200, arenaSize: 200);
        using HeartbeatPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);

        // Both frames go out in a single write, with a short sleep before the first Pump — the same
        // idiom StreamSessionDeliveryCounterEmissionTests uses — so that both arrive and deliver in one
        // Pump. Without it, the OS is free to hand the Reactor's read only the first frame, which would
        // split delivery (and #116's own confirmation) across two Pumps, leaving a second #116
        // confirmation behind for the assertions below to mistake for the send-idle timer's record.
        peer.Send([.. BuildFrame(PayloadOf(16, 1)), .. BuildFrame(PayloadOf(16, 2))]);
        Thread.Sleep(20);
        accepter.Reactor.Pump();
        accepter.Reactor.CountersOf(id).MessagesReceived.Should().Be(2, "both frames were on the wire together before this Pump ran");
        peer.Receive(12); // the received-count trigger's own confirmation (#116) — not what is under test

        // Act — the peer sends nothing further; once the interval elapses, only the send-idle timer can
        // possibly be the source of a further record, since the received count cannot advance again.
        accepter.TimeProvider.Advance(accepter.SendIdleInterval);
        accepter.Reactor.Pump();

        // Assert — the record is well-formed and carries this end's current received count.
        byte[] heartbeatRecord = peer.Receive(12);
        StreamSessionTestRecords.TryReadDeliveryCounterRecord(heartbeatRecord, out ulong count).Should().BeTrue();
        count.Should().Be(accepter.Reactor.CountersOf(id).MessagesReceived,
            "the send-idle heartbeat must carry this end's current received-Message count");
        count.Should().Be(2);

        // Arrange — Part 2: apply the *exact bytes* captured above — the real record the send-idle timer
        // just emitted, not a hand-rolled equivalent — against a fresh Session with retention it cannot
        // yet release, to prove the peer's retention releases against it exactly as any delivery-counter
        // record does (#113/#110).
        const int maxMessageSize = 16;
        const int retentionBytes = 2 * (maxMessageSize + 4);
        using StreamSessionReactorHarness sender = new(maxMessageSize: maxMessageSize, retentionBytes: retentionBytes);
        using HeartbeatPeer far = new(sender.Endpoint);
        PeerId senderPeer = EstablishSession(sender, far);

        byte[] first = PayloadOf(maxMessageSize, 9);
        byte[] second = PayloadOf(maxMessageSize, 10);
        sender.Reactor.Send(senderPeer, first).Should().Be(SendOutcome.Sent);
        sender.Reactor.Send(senderPeer, second).Should().Be(SendOutcome.Sent);
        sender.Reactor.Pump();
        far.Receive(BuildFrame(first).Length + BuildFrame(second).Length);

        sender.Reactor.Send(senderPeer, [9]).Should().Be(SendOutcome.RetentionExhausted,
            "nothing has been confirmed yet, so the ring is still full");

        // Act — the far end replays the real send-idle heartbeat record captured in Part 1, confirming
        // through Message 2.
        far.Send(heartbeatRecord);
        sender.Reactor.Pump();

        // Assert — the ring is released and a further Send succeeds.
        sender.Reactor.Send(senderPeer, [9]).Should().Be(SendOutcome.Sent,
            "the send-idle heartbeat's own record must release retention exactly as any delivery-counter record does");
    }

    // =========================================================================================
    // AC5 — no option to disable the heartbeat exists on StreamSessionOptions
    // =========================================================================================

    [Fact]
    public void StreamSessionOptions_HasNoOptionToDisableTheHeartbeat()
    {
        // Arrange
        PropertyInfo[] properties = typeof(StreamSessionOptions).GetProperties(BindingFlags.Public | BindingFlags.Instance);

        // Assert — no property anywhere on the surface names disabling, enabling or toggling either the
        // heartbeat or the send-idle timer that produces it.
        properties.Should().NotContain(
            p => p.Name.Contains("Heartbeat", StringComparison.OrdinalIgnoreCase)
                && (p.Name.Contains("Disable", StringComparison.OrdinalIgnoreCase)
                    || p.Name.Contains("Enable", StringComparison.OrdinalIgnoreCase)),
            "the heartbeat is never a wasted packet (issue #109) and cannot be turned off");

        properties.Should().NotContain(
            p => p.Name.Contains("SendIdle", StringComparison.OrdinalIgnoreCase)
                && (p.Name.Contains("Disable", StringComparison.OrdinalIgnoreCase)
                    || p.Name.Contains("Enable", StringComparison.OrdinalIgnoreCase)),
            "the send-idle timer that drives the heartbeat cannot be turned off");

        // Assert — SendIdleInterval itself remains the one required, non-nullable knob: a nullable
        // TimeSpan would itself be a way to smuggle in a disable switch.
        PropertyInfo sendIdleInterval = properties.Should()
            .ContainSingle(p => p.Name == nameof(StreamSessionOptions.SendIdleInterval)).Which;
        sendIdleInterval.PropertyType.Should().Be(typeof(TimeSpan),
            "a nullable SendIdleInterval would itself be a way to disable the heartbeat");
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// that drives the handshake, sends raw framed Messages, and reads whatever
    /// <see cref="StreamSessionReactor"/> emits back — including the send-idle heartbeat this file's
    /// tests are about. Mirrors <see cref="StreamSessionDeliveryCounterEmissionTests"/>'s
    /// <c>EmissionPeer</c>.
    /// </summary>
    private sealed class HeartbeatPeer : IDisposable
    {
        private readonly Socket _socket;

        public HeartbeatPeer(IPEndPoint destination)
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
        /// throwing — the house pattern for asserting nothing further arrives.
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
