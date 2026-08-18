using System.Net;
using System.Net.Sockets;
using System.Reflection;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Slice 4.1.1 (issue #109): the delivery-counter control record's wire format and its classification
/// in the framing table — <see cref="StreamSessionTestRecords.DeliveryCounterSentinel"/> followed by an
/// 8-byte <c>u64</c> body, a second reserved sentinel alongside Goodbye's (issue #23/#95). Issue #113
/// then wired a complete record into <see cref="RetentionArena.ReleaseThroughMessageCount"/>: a
/// legitimate count — at or below what this end has actually sent this Peer — is consumed silently and
/// releases retention, while a count above that is refused and closes the Peer with
/// <see cref="DisconnectReason.ProtocolViolation"/>, the same discard-arm-refuses rule
/// <see cref="StreamSessionMalformedFramingTests"/> already covers for an oversized or zero-length
/// prefix. Every test in this file happens to describe the second shape — no Send is ever made to this
/// Peer, so any nonzero claimed count is already above what was sent — because that is what this file's
/// job (classification, not release accounting) needs; <see cref="StreamSessionDeliveryCounterReleaseTests"/>
/// is where a legitimate count and the retention it frees are actually exercised. The counterparty in
/// every test here is a hand-built raw TCP peer, mirroring <see cref="StreamSessionMalformedFramingTests"/>'s
/// <c>MalformedPeer</c> — the house pattern for this shape of test.
/// </summary>
public class StreamSessionDeliveryCounterFramingTests
{
    private static byte[] BuildFrame(byte[] payload) => StreamSessionTestRecords.BuildFrame(payload);

    private static byte[] BuildPrefix(uint length) => StreamSessionTestRecords.BuildPrefix(length);

    private static byte[] BuildDeliveryCounterRecord(ulong count) => StreamSessionTestRecords.BuildDeliveryCounterRecord(count);

    private static PeerId EstablishSession(StreamSessionReactorHarness accepter, CounterRecordPeer peer, int maxPumps = 20) =>
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
    public void DeliverCompleteMessages_BodySplitAcrossTwoWrites_StaysOpenThenClosesOnceTheRestArrives()
    {
        // Arrange — proves the classification genuinely completes once the remaining bytes arrive
        // (rather than being stuck forever): once whole, the record is consumed by
        // RetentionArena.ReleaseThroughMessageCount (issue #113), and a claimed count of 7 against a
        // Peer this end never Sent anything to is refused — the peer is confirming Messages that were
        // never sent.
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

        // Assert — the record is now whole, classified, and consumed, but a count of 7 exceeds the zero
        // Messages this end ever sent this Peer, so it is refused rather than silently accepted.
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Reason.Should().Be(DisconnectReason.ProtocolViolation);
    }

    // =========================================================================================
    // Complete: a legitimate count is consumed silently; a count above what was sent is refused
    // (mirrors the malformed-framing table's discard-arm-refuses rule)
    // =========================================================================================

    [Fact]
    public void DeliverCompleteMessages_ACountAboveWhatThisEndEverSent_ClosesWithProtocolViolationRatherThanBeingSilentlyAcceptedOrTreatedAsGoodbye()
    {
        // Arrange — no Send is ever made to this Peer, so any nonzero claimed count already exceeds
        // what this end sent it.
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

    [Fact]
    public void DeliverCompleteMessages_ACountOfZeroAgainstAPeerNeverSentAnything_IsConsumedSilentlyAndNeverClosesThePeer()
    {
        // Arrange — a legitimate confirmation: zero Messages sent, zero confirmed. Distinguishes
        // "consumed" from "fails closed": before issue #113 wired this record into
        // RetentionArena.ReleaseThroughMessageCount, every whole record — legitimate or not — fell into
        // the fail-closed discard arm and closed the Peer.
        using StreamSessionReactorHarness accepter = new();
        using CounterRecordPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);

        // Act
        peer.Send(BuildDeliveryCounterRecord(0));
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
    public void DeliverCompleteMessages_ACountAboveWhatThisEndEverSent_LogsDeliveryCounterRefusedRatherThanMalformedFraming()
    {
        // Arrange — the distinct-diagnostic-shape rule #106 established for DataAfterGoodbye applies
        // here too: a refused delivery counter is its own violation, not a reuse of MalformedFraming's
        // claimed-length-vs-MaxMessageSize shape, which this claim has nothing to do with.
        using StreamSessionReactorHarness accepter = new();
        using CounterRecordPeer peer = new(accepter.Endpoint);
        PeerId id = EstablishSession(accepter, peer);
        accepter.Sink.Received.Clear();

        // Act
        peer.Send(BuildDeliveryCounterRecord(100));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.DeliveryCounterRefused));

        // Assert
        DiagnosticRecord record = accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.DeliveryCounterRefused).Which;
        record.Level.Should().Be(DiagnosticLevel.Warn);
        record.SubjectKind.Should().Be(DiagnosticSubjectKind.Peer);
        record.Peer.Should().Be(id);
        record.Value0.Should().Be(100, "the claimed cumulative count");
        record.Value1.Should().Be(0, "this end never sent this Peer anything");
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.MalformedFraming,
            "DeliveryCounterRefused is a distinct diagnostic shape, not a reuse of MalformedFraming's claimed-length one");
    }

    [Fact]
    public void DeliverCompleteMessages_ACountUsingEveryByteOfTheBody_IsDecodedInFullRatherThanTruncated()
    {
        // Arrange — every one of the eight body bytes is distinct and nonzero, and the value sits far
        // above uint.MaxValue. Every other count in this suite is small enough to live in the body's
        // last byte alone, so nothing else here would notice a decoder that read four bytes instead of
        // eight, or read them little-endian: both report a different number for this one.
        const ulong count = 0x0102030405060708UL;
        using StreamSessionReactorHarness accepter = new();
        using CounterRecordPeer peer = new(accepter.Endpoint);
        EstablishSession(accepter, peer);
        accepter.Sink.Received.Clear();

        // Act
        peer.Send(BuildDeliveryCounterRecord(count));
        accepter.PumpUntil(() => accepter.Sink.Received.Any(r => r.Id == DiagnosticId.DeliveryCounterRefused));

        // Assert — refused, since this end never sent this Peer anything, and the refusal reports the
        // whole u64 the record carried.
        accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.DeliveryCounterRefused)
            .Which.Value0.Should().Be((long)count, "the claimed count survives decoding whole");
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

    // =========================================================================================
    // The sentinel's own value: distinct from Goodbye's, and unreachable by any legal Message
    // length (issue #109)
    // =========================================================================================

    [Fact]
    public void DeliveryCounterSentinel_AgainstMaxMessageSizesOwnBound_IsAboveEveryLengthALegalMessageCanAnnounce()
    {
        // Arrange — the bound is MaxMessageSize's own declared type rather than a number restated
        // here: it is a signed int, so the largest length any configuration can announce is
        // int.MaxValue. Read through reflection over the public options surface so this follows the
        // type if it ever widens, instead of quietly keeping an assumption the options no longer hold.
        PropertyInfo maxMessageSize = typeof(StreamSessionOptions)
            .GetProperty(nameof(StreamSessionOptions.MaxMessageSize))!;

        // Act
        Type bound = maxMessageSize.PropertyType;

        // Assert — no legal Message length reaches the sentinel, so no Message is ever misread as a
        // delivery-counter record. That this file's mirrored copy of the value is the one production
        // treats as the sentinel is pinned behaviourally by
        // DeliverCompleteMessages_BareSentinelWithNoBodyYet_DoesNotCloseThePeer, which fails if the two
        // drift apart.
        bound.Should().Be<int>("MaxMessageSize is what bounds every announced length");
        ((uint)int.MaxValue).Should().BeLessThan(StreamSessionTestRecords.DeliveryCounterSentinel,
            "the largest length a legal Message can announce still falls short of the sentinel");
    }

    [Fact]
    public void DeliveryCounterSentinel_ComparedWithTheGoodbyeSentinel_IsADistinctReservedValue()
    {
        // Arrange — Goodbye's sentinel, mirrored here exactly as StreamSessionGoodbyeTests mirrors it.
        const uint goodbyeSentinel = uint.MaxValue;

        // Assert — two reserved prefixes, two different values. Production draws the same distinction:
        // a Goodbye closes cleanly with PeerGoodbye (StreamSessionGoodbyeTests) where a
        // delivery-counter record never does.
        StreamSessionTestRecords.DeliveryCounterSentinel.Should().NotBe(goodbyeSentinel);
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

        /// <param name="destination">The Reactor's bound endpoint to connect to.</param>
        /// <remarks>
        /// <c>NoDelay</c> is set for the same reason <c>TcpLink</c> sets it on the Reactor's own accepted
        /// socket, but for the opposite direction: without it, a small write issued while an earlier one
        /// is still unacknowledged is held by Nagle until the far end's delayed ACK — up to roughly 40ms
        /// on Linux — which is far longer than a sleepless Pump loop ever waits.
        /// <see cref="DeliverCompleteMessages_BodySplitAcrossTwoWrites_StaysOpenThenClosesOnceTheRestArrives"/>
        /// splits a record across exactly that pattern, and failed on Linux CI while passing on Windows
        /// for this reason alone.
        /// </remarks>
        public CounterRecordPeer(IPEndPoint destination)
        {
            _socket = new Socket(destination.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true,
            };
            _socket.Connect(destination);
        }

        /// <summary>Sends every byte of <paramref name="bytes"/> in a single write.</summary>
        public void Send(byte[] bytes) => _socket.Send(bytes);

        public void Dispose() => _socket.Dispose();
    }
}
