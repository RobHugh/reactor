using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 4.1.1 (issue #109): the delivery-counter control record's wire format and its classification
/// in the framing table — <see cref="StreamSessionTestRecords.DeliveryCounterSentinel"/> followed by an
/// 8-byte <c>u64</c> body, a second reserved sentinel alongside Goodbye's (issue #23/#95). This ticket
/// is the wire format and classification only: nothing here wires a received counter into an emitted
/// Event, a retention release or a timer (that is #113/#116's job), so a whole record still fails
/// closed once classified — the same discard-arm-refuses rule <see cref="StreamSessionMalformedFramingTests"/>
/// already covers for an oversized or zero-length prefix. The counterparty in every test here is a
/// hand-built raw TCP peer, mirroring <see cref="StreamSessionMalformedFramingTests"/>'s
/// <c>MalformedPeer</c> — the house pattern for this shape of test.
/// </summary>
public class StreamSessionDeliveryCounterFramingTests
{
    private static byte[] BuildFrame(byte[] payload) => StreamSessionTestRecords.BuildFrame(payload);

    private static byte[] BuildPrefix(uint length) => StreamSessionTestRecords.BuildPrefix(length);

    private static byte[] BuildDeliveryCounterRecord(ulong count) => StreamSessionTestRecords.BuildDeliveryCounterRecord(count);

    private static void EstablishSession(StreamSessionReactorHarness accepter, CounterRecordPeer peer, int maxPumps = 20) =>
        StreamSessionTestRecords.EstablishSession(accepter, peer.Send, maxPumps);

    /// <summary>Pumps until a <see cref="SessionEventKind.PeerClosed"/> drains, or the budget runs out.</summary>
    private static List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> PumpUntilClosed(
        StreamSessionReactorHarness accepter, int maxPumps = 20)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < maxPumps && !drained.Any(e => e.Kind == SessionEventKind.PeerClosed); i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        return drained;
    }

    // =========================================================================================
    // Incomplete: a sentinel with fewer than eight body bytes is an ordinary wait, not a violation
    // =========================================================================================

    [Fact]
    public void DeliverCompleteMessages_BareSentinelWithNoBodyYet_DoesNotCloseThePeer()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using CounterRecordPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);

        // Act — only the 4-byte sentinel arrives, none of its 8-byte body.
        peer.Send(BuildPrefix(StreamSessionTestRecords.DeliveryCounterSentinel));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20; i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert — never closed: an ordinary, expected wait while the body is still arriving.
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    [Fact]
    public void DeliverCompleteMessages_SentinelWithPartialBody_DoesNotCloseThePeer()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using CounterRecordPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);
        byte[] wholeRecord = BuildDeliveryCounterRecord(42);

        // Act — the sentinel and three of its eight body bytes arrive; five are still missing.
        peer.Send(wholeRecord.AsSpan(0, 7).ToArray());
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20; i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    [Fact]
    public void DeliverCompleteMessages_BodySplitAcrossTwoWrites_StaysOpenThenFailsClosedOnceTheRestArrives()
    {
        // Arrange — proves the classification genuinely completes once the remaining bytes arrive
        // (rather than being stuck forever), while this ticket deliberately leaves consumption of a
        // complete record to #113/#116: the fail-closed discard arm is what a not-yet-wired but
        // fully-classified record hits for now.
        using StreamSessionReactorHarness accepter = new();
        using CounterRecordPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);
        byte[] wholeRecord = BuildDeliveryCounterRecord(7);
        byte[] firstPart = wholeRecord.AsSpan(0, 7).ToArray();
        byte[] secondPart = wholeRecord.AsSpan(7).ToArray();

        // Act — the first part arrives and several Pumps pass with nothing further: still waiting.
        peer.Send(firstPart);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> earlyDrain = [];
        for (int i = 0; i < 3; i++)
        {
            accepter.Reactor.Pump();
            earlyDrain.AddRange(accepter.DrainToList());
        }

        earlyDrain.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed, "the body has not fully arrived yet");

        // Act — the rest arrives, completing the record.
        peer.Send(secondPart);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = PumpUntilClosed(accepter);

        // Assert — the record is now whole and classified, but not yet consumed by anything, so it
        // fails closed rather than being silently accepted or waited on forever.
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.ProtocolViolation);
    }

    // =========================================================================================
    // Complete: the discard arm refuses rather than permits (mirrors the malformed-framing table)
    // =========================================================================================

    [Fact]
    public void DeliverCompleteMessages_WholeRecordArrivesInOneWrite_FailsClosedRatherThanBeingSilentlyAcceptedOrTreatedAsGoodbye()
    {
        // Arrange
        using StreamSessionReactorHarness accepter = new();
        using CounterRecordPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);

        // Act
        peer.Send(BuildDeliveryCounterRecord(100));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = PumpUntilClosed(accepter);

        // Assert — closed, but distinctly not as a clean Goodbye: the two sentinels are different
        // values, and only Goodbye's carries the PeerGoodbye close reason.
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.ProtocolViolation);
    }

    // =========================================================================================
    // Sentinel precedence over the oversized-prefix rule (mirrors #95's pinning of GoodbyeSentinel)
    // =========================================================================================

    [Fact]
    public void DeliverCompleteMessages_BareSentinelUnderASmallMaxMessageSize_IsNotTreatedAsOversized()
    {
        // Arrange — the sentinel's own numeric value (uint.MaxValue - 2) vastly exceeds this small
        // MaxMessageSize, so this test fails if the oversized-prefix check were ever reordered ahead of
        // the sentinel check: a reordered classifier would read the sentinel as an outrageous claimed
        // length and close immediately with ProtocolViolation instead of waiting for its body.
        const int maxMessageSize = 64;
        using StreamSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize, retentionBytes: maxMessageSize + 4);
        using CounterRecordPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);

        // Act — only the bare sentinel arrives, no body at all.
        peer.Send(BuildPrefix(StreamSessionTestRecords.DeliveryCounterSentinel));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = [];
        for (int i = 0; i < 20; i++)
        {
            accepter.Reactor.Pump();
            drained.AddRange(accepter.DrainToList());
        }

        // Assert — correctly recognised as the start of a delivery-counter record and left waiting for
        // its body, not misread as an oversized Message claim.
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
    }

    [Fact]
    public void DeliverCompleteMessages_PrefixOneLessThanTheDeliveryCounterSentinel_ClosesWithProtocolViolation()
    {
        // Arrange — pins the boundary from the violation side: the value directly below the sentinel is
        // an ordinary oversized claim, not a near-miss on the sentinel itself.
        using StreamSessionReactorHarness accepter = new();
        using CounterRecordPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);

        // Act
        peer.Send(BuildPrefix(StreamSessionTestRecords.DeliveryCounterSentinel - 1));
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = PumpUntilClosed(accepter);

        // Assert
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.ProtocolViolation);
    }

    // =========================================================================================
    // The post-Goodbye rule (#93) still outranks a delivery-counter record
    // =========================================================================================

    [Fact]
    public void DataAfterGoodbye_DeliveryCounterRecordArrivesBehindGoodbyeInTheSameWrite_ClosesWithProtocolViolationAndNeverDeliversTheRecord()
    {
        // Arrange — a preceding legitimate Message keeps HasUndeliveredInboundWork true for the Frame
        // the Goodbye is classified in, so GoodbyeReceived defers the close instead of taking it
        // immediately, and the parse loop keeps going to the bytes right behind the Goodbye — mirrors
        // StreamSessionGoodbyeTests' DataAfterGoodbye_MessageArrivesBehindGoodbyeInTheSameWrite_...
        // for a delivery-counter record rather than an ordinary Message.
        using StreamSessionReactorHarness accepter = new();
        using CounterRecordPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);
        byte[] precedingMessage = StreamSessionTestRecords.PayloadOf(16, 1);
        byte[] goodbye = BuildPrefix(uint.MaxValue);

        // Act — one write: a Message, the Goodbye, then a delivery-counter record immediately behind it.
        peer.Send([.. BuildFrame(precedingMessage), .. goodbye, .. BuildDeliveryCounterRecord(1)]);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = PumpUntilClosed(accepter);

        // Assert — the post-Goodbye rule outranks the delivery-counter classification: this closes as
        // an ordinary DataAfterGoodbye violation, not as an awaited or accepted counter record.
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.ProtocolViolation);
    }

    /// <summary>
    /// A plain BCL TCP socket connected to a Reactor's bound endpoint, standing in for a counterparty
    /// writing raw framed bytes — including a hand-built delivery-counter record — on the wire. Mirrors
    /// <see cref="StreamSessionMalformedFramingTests"/>'s <c>MalformedPeer</c>, the house pattern for
    /// this shape of test.
    /// </summary>
    private sealed class CounterRecordPeer : IDisposable
    {
        private readonly Socket _socket;

        public CounterRecordPeer(IPEndPoint destination)
        {
            _socket = new Socket(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _socket.Connect(destination);
        }

        /// <summary>Sends every byte of <paramref name="bytes"/> in a single write.</summary>
        public void Send(byte[] bytes) => _socket.Send(bytes);

        public void Dispose() => _socket.Dispose();
    }
}
