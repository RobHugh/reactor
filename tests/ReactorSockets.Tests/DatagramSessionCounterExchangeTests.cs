using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.7.12 (issue #239): one scripted exchange in which every counter slice 6.7 completes moves
/// by an amount this test states in advance — the seven per-Peer figures (<see cref="PeerCounters.BytesSent"/>,
/// <see cref="PeerCounters.BytesReceived"/>, <see cref="PeerCounters.PacketsRetransmitted"/>,
/// <see cref="PeerCounters.DuplicatePackets"/>, <see cref="PeerCounters.OutOfWindowPackets"/>,
/// <see cref="PeerCounters.HorizonDeferrals"/>, <see cref="PeerCounters.RetentionHighWaterBytes"/>) and the
/// three Reactor-scope figures (<see cref="ReactorCounters.DatagramsSent"/>,
/// <see cref="ReactorCounters.DatagramsReceived"/>, <see cref="ReactorCounters.UnknownSourceDiscards"/>).
/// </summary>
/// <remarks>
/// <para>
/// <b>One Peer, driven from both directions.</b> The accepter under test (<c>node</c>) is a live
/// <see cref="DatagramSessionReactor"/>; its one Peer is a hand-built client over a bare
/// <see cref="RawReactor"/>, exactly the instrument <see cref="DatagramSessionSpaceRoutingTests"/> and
/// <see cref="DatagramSessionUnreliableDuplicateTests"/> already use. A live second Session cannot
/// express this exchange: out-of-window and duplicate classification need sequences the public
/// <see cref="DatagramSessionReactor.Send"/> allocator cannot be made to skip or repeat, and the horizon
/// deferral needs an ack that never arrives — an ordinary counterparty would keep acknowledging. Every
/// crafted datagram's ack blocks are left zeroed throughout (<see cref="BuildHeader"/>'s default), so
/// nothing this test sends is ever acknowledged and nothing outbound is ever released from retention.
/// </para>
/// <para>
/// <b>Independence.</b> Every figure below is driven by its own distinct literal amount, and each
/// expected value is a literal this test states before acting, never one counter's reading used to
/// compute another's expectation. <see cref="PeerCounters.BytesSent"/> and
/// <see cref="PeerCounters.RetentionHighWaterBytes"/> use the same delta-of-two-flushes technique
/// <see cref="DatagramSessionByteCounterTests"/> established (#231) — the two flushes' fixed overhead is
/// identical, so only the payload bytes this test chose can account for the difference —
/// rather than assuming any internal header size (ADR 0015).
/// </para>
/// <para>
/// <b>Traps avoided.</b> Retention is sized at 1200 × 48 (<c>DatagramSessionOutboundSequenceSpaceTests</c>'s
/// unit) so the 38 ReliableOrdered records this test never acknowledges, plus the two ReliableUnordered
/// records the retransmission phase needs, all fit at once. No Frame ever delivers more than one reliable
/// Message — this test's one duplicate-admission is the only delivery that occurs — so the default
/// <c>FrameArena</c>'s per-share-holder budget is never in play.
/// </para>
/// </remarks>
public class DatagramSessionCounterExchangeTests
{
    private const int ControlDatagramSize = 256;
    private const uint ControlSession = 0;

    private const byte HelloType = 1;
    private const byte HelloWithCookieType = 3;
    private const int ControlBodyOffset = 5;
    private const int CookieLength = 32;
    private const int ResumeSecretLength = 16;

    private const uint ProtocolVersion = 1;
    private const uint DialerSessionId = 0xCAFE_BABE;
    private const uint DialerMaxMessageSize = 4096;

    private const int DatagramHeaderSize = 20;

    private const byte ModeUnreliable = 0b0000_0000;
    private const byte ModeReliableUnordered = 0b0000_0010;
    private const byte ModeReliableOrdered = 0b0000_0011;

    /// <summary>Comfortably past the RTO, matching <see cref="DatagramSessionRetransmitResendTests"/>'s own constant.</summary>
    private static readonly TimeSpan _pastTheRetransmitDeadline = TimeSpan.FromSeconds(3);

    [Fact]
    public void Pump_OneScriptedExchange_MovesEveryCompletedCounterByItsOwnDistinctLiteralAmount()
    {
        // Arrange — a 4-fragment receive window (the smallest this profile's floor allows for the
        // harness's default 4096-byte MaxMessageSize and 1200-byte fragment size), and retention sized
        // for 48 ReliableOrdered/ReliableUnordered records so the 38-send horizon phase and the two
        // retransmission-phase records all fit without RetentionExhausted.
        using DatagramSessionReactorHarness node = new(receiveWindowFragments: 4, retentionBytes: 1200 * 48);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);

        // Assert — every figure this exchange drives starts at zero, proving each genuinely moves off
        // zero below rather than having already moved during the handshake.
        PeerCounters baseline = node.Reactor.CountersOf(nodePeer);
        baseline.BytesSent.Should().Be(0);
        baseline.BytesReceived.Should().Be(0);
        baseline.PacketsRetransmitted.Should().Be(0);
        baseline.DuplicatePackets.Should().Be(0);
        baseline.OutOfWindowPackets.Should().Be(0);
        baseline.HorizonDeferrals.Should().Be(0);
        baseline.RetentionHighWaterBytes.Should().Be(0);
        node.Reactor.Counters.UnknownSourceDiscards.Should().Be(0);

        ulong reactorDatagramsSentBaseline = node.Reactor.Counters.DatagramsSent;
        ulong reactorDatagramsReceivedBaseline = node.Reactor.Counters.DatagramsReceived;

        // ------------------------------------------------------------------------------------------
        // Phase 1 — BytesSent, literal delta 9. Unreliable: no retention, no ack needed, isolates this
        // figure from every reliable-Space phase below. BytesSent is cumulative across every flush this
        // Peer has ever sent (like #231's own byte-counter tests), so each flush's own cost is read as a
        // "before this flush" / "after this flush" pair, and it is those two flushes' own costs — not
        // the running total — that are compared.
        // ------------------------------------------------------------------------------------------
        ulong bytesSentBeforeEmptyFlush = node.Reactor.CountersOf(nodePeer).BytesSent;
        node.Reactor.Send(nodePeer, DeliveryMode.Unreliable, []).Should().Be(SendOutcome.Sent);
        node.Reactor.Pump();
        ulong emptyFlushBytes = node.Reactor.CountersOf(nodePeer).BytesSent - bytesSentBeforeEmptyFlush;

        ulong bytesSentBeforeNineFlush = node.Reactor.CountersOf(nodePeer).BytesSent;
        node.Reactor.Send(nodePeer, DeliveryMode.Unreliable, new byte[9]).Should().Be(SendOutcome.Sent);
        node.Reactor.Pump();
        ulong nineFlushBytes = node.Reactor.CountersOf(nodePeer).BytesSent - bytesSentBeforeNineFlush;
        ulong bytesSentAfterPhase1 = bytesSentBeforeNineFlush + nineFlushBytes;

        (nineFlushBytes - emptyFlushBytes).Should().Be(
            9UL, "the two flushes' fixed overhead is identical, so only the nine payload bytes differ");

        // ------------------------------------------------------------------------------------------
        // Phase 2 — BytesReceived, literal 40: a hand-built Unreliable datagram whose total length this
        // test knows exactly (20-byte header + 7-byte record framing + 13-byte payload), so the expected
        // value is a literal this test states up front rather than a value read off any other counter.
        // ------------------------------------------------------------------------------------------
        ulong bytesReceivedBefore = node.Reactor.CountersOf(nodePeer).BytesReceived;
        byte[] thirteenBytePayload = new byte[13];
        byte[] receivedDatagram = [.. BuildHeader(sessionId), .. BuildRecord(ModeUnreliable, sequence: 1, thirteenBytePayload)];
        receivedDatagram.Length.Should().Be(40, "20-byte header + 7-byte record framing + 13-byte payload");

        ulong datagramsReceivedBeforePhase2 = node.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(node.Endpoint), receivedDatagram);
        node.PumpUntil(() => node.Reactor.Counters.DatagramsReceived > datagramsReceivedBeforePhase2);

        (node.Reactor.CountersOf(nodePeer).BytesReceived - bytesReceivedBefore).Should().Be(
            40UL, "the whole Datagram this test built, header included, is what BytesReceived attributes to the Peer");

        // ------------------------------------------------------------------------------------------
        // Phase 3 — RetentionHighWaterBytes, literal delta 7. ReliableUnordered, sequences 0 and 1: both
        // stay outstanding (never acked) through the rest of this test, so Phase 4 below retransmits
        // exactly these two.
        // ------------------------------------------------------------------------------------------
        node.Reactor.Send(nodePeer, DeliveryMode.ReliableUnordered, []).Should().Be(SendOutcome.Sent);
        node.Reactor.Pump();
        ulong retentionHighWaterAfterEmpty = node.Reactor.CountersOf(nodePeer).RetentionHighWaterBytes;

        node.Reactor.Send(nodePeer, DeliveryMode.ReliableUnordered, new byte[7]).Should().Be(SendOutcome.Sent);
        node.Reactor.Pump();
        ulong retentionHighWaterAfterSeven = node.Reactor.CountersOf(nodePeer).RetentionHighWaterBytes;

        (retentionHighWaterAfterSeven - retentionHighWaterAfterEmpty).Should().Be(
            7UL, "the two flushes' fixed overhead is identical, so only the seven payload bytes differ");

        // ------------------------------------------------------------------------------------------
        // Phase 4 — PacketsRetransmitted, literal 2. At this point the ReliableOrdered Space has never
        // sent anything, so the sweep below can only find Phase 3's two ReliableUnordered records —
        // nothing else is outstanding yet to contaminate the count.
        // ------------------------------------------------------------------------------------------
        ulong retransmittedBefore = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted;
        node.TimeProvider.Advance(_pastTheRetransmitDeadline);
        node.Reactor.Pump();

        (node.Reactor.CountersOf(nodePeer).PacketsRetransmitted - retransmittedBefore).Should().Be(
            2UL, "exactly Phase 3's two never-acknowledged ReliableUnordered records had expired deadlines");

        // ------------------------------------------------------------------------------------------
        // Phase 5 — HorizonDeferrals, literal 5. The ReliableOrdered horizon is [0, 32] (33 sequences);
        // sending 38 (sequences 0 through 37) leaves exactly 5 (33 through 37) beyond it, deferred rather
        // than armed. Nothing ever acknowledges this Space, so the horizon never slides.
        // ------------------------------------------------------------------------------------------
        ulong horizonDeferralsBefore = node.Reactor.CountersOf(nodePeer).HorizonDeferrals;
        for (int sequence = 0; sequence < 38; sequence++)
        {
            node.Reactor.Send(nodePeer, DeliveryMode.ReliableOrdered, [0]).Should().Be(SendOutcome.Sent);
        }

        node.Reactor.Pump();

        // The deferral count above is folded into the Peer's public Counters only when a Datagram from
        // this Peer is next parsed (DatagramSessionReactor.ReconcileHorizonDeferrals), so one more
        // crafted arrival — carrying no ack of its own — is needed before the figure is observable.
        byte[] reconcilePing = [.. BuildHeader(sessionId), .. BuildRecord(ModeUnreliable, sequence: 2, payload: [])];
        ulong datagramsReceivedBeforeReconcile = node.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(node.Endpoint), reconcilePing);
        node.PumpUntil(() => node.Reactor.Counters.DatagramsReceived > datagramsReceivedBeforeReconcile);

        (node.Reactor.CountersOf(nodePeer).HorizonDeferrals - horizonDeferralsBefore).Should().Be(
            5UL, "sequences 33 through 37 of 0-through-37 sit beyond the [0, 32] horizon, which never slides here");

        // ------------------------------------------------------------------------------------------
        // Phase 6 — OutOfWindowPackets, literal 4. ReliableOrdered inbound, four sequences each
        // unreachably far ahead of the untouched base at 0 — none ever admits, so the base never moves
        // and each of the four discards independently (DatagramSessionSpaceRoutingTests's own pattern).
        // ------------------------------------------------------------------------------------------
        ulong outOfWindowBefore = node.Reactor.CountersOf(nodePeer).OutOfWindowPackets;
        foreach (uint farAheadSequence in new uint[] { 100, 101, 102, 103 })
        {
            byte[] farAhead = [.. BuildHeader(sessionId), .. BuildRecord(ModeReliableOrdered, farAheadSequence, payload: [])];
            ulong before = node.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, Source.FromIPEndPoint(node.Endpoint), farAhead);
            node.PumpUntil(() => node.Reactor.Counters.DatagramsReceived > before);
        }

        (node.Reactor.CountersOf(nodePeer).OutOfWindowPackets - outOfWindowBefore).Should().Be(
            4UL, "each of the four far-ahead sequences discarded independently; none ever admitted");

        // ------------------------------------------------------------------------------------------
        // Phase 7 — DuplicatePackets, literal 3. ReliableUnordered inbound, sequence 1 — leaving a hole
        // at 0 open so the first arrival admits but is not immediately retired below the base — sent
        // once more, then twice more again, so the first arrival is not itself a duplicate.
        // ------------------------------------------------------------------------------------------
        ulong duplicateBefore = node.Reactor.CountersOf(nodePeer).DuplicatePackets;
        byte[] repeatable = [.. BuildHeader(sessionId), .. BuildRecord(ModeReliableUnordered, sequence: 1, payload: [0xAB])];

        for (int attempt = 0; attempt < 4; attempt++)
        {
            ulong before = node.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, Source.FromIPEndPoint(node.Endpoint), repeatable);
            node.PumpUntil(() => node.Reactor.Counters.DatagramsReceived > before);
        }

        (node.Reactor.CountersOf(nodePeer).DuplicatePackets - duplicateBefore).Should().Be(
            3UL, "the first of the four arrivals admits; the next three are each counted a duplicate");

        // ------------------------------------------------------------------------------------------
        // Phase 8 — UnknownSourceDiscards, literal 1, charged to the Reactor and to no Peer. A stray
        // datagram naming a session id no Session holds (0 is reserved for control; every assigned id
        // here starts from 1).
        // ------------------------------------------------------------------------------------------
        using HostilePeer stray = new();
        ulong unknownSourceDiscardsBefore = node.Reactor.Counters.UnknownSourceDiscards;
        ulong bytesReceivedBeforeStray = node.Reactor.CountersOf(nodePeer).BytesReceived;

        byte[] strayDatagram = new byte[24];
        BinaryPrimitives.WriteUInt32BigEndian(strayDatagram, 0xFFFF_FFFEU);
        stray.SendDatagram(node.Endpoint, strayDatagram);
        node.PumpUntil(() => node.Reactor.Counters.UnknownSourceDiscards > unknownSourceDiscardsBefore);

        (node.Reactor.Counters.UnknownSourceDiscards - unknownSourceDiscardsBefore).Should().Be(
            1UL, "the one stray datagram named no Session this Reactor holds");
        node.Reactor.CountersOf(nodePeer).BytesReceived.Should().Be(
            bytesReceivedBeforeStray, "a datagram naming no Peer charges no Peer's bytes received");

        // ------------------------------------------------------------------------------------------
        // Reactor-scope DatagramsSent and DatagramsReceived, literals 6 and 11 — the exact count of
        // flushes Node performed (Phases 1, 3, 4 and 5: 2 + 2 + 1 + 1) and datagrams the client sent
        // Node (Phases 2, 5's reconcile ping, 6, 7 and 8: 1 + 1 + 4 + 4 + 1), each stated up front by
        // this test's own script rather than read from any other counter.
        // ------------------------------------------------------------------------------------------
        (node.Reactor.Counters.DatagramsSent - reactorDatagramsSentBaseline).Should().Be(
            6UL, "six flushes left Node: two in Phase 1, two in Phase 3, one in Phase 4, one in Phase 5");
        (node.Reactor.Counters.DatagramsReceived - reactorDatagramsReceivedBaseline).Should().Be(
            11UL, "eleven datagrams reached Node: Phase 2 (1), Phase 5's reconcile ping (1), Phase 6 (4), Phase 7 (4), Phase 8 (1)");

        // ------------------------------------------------------------------------------------------
        // Monotonicity — every figure's final reading is at least its own baseline, for each of the ten
        // figures this exchange moved, confirming no assertion above read a lower value than an earlier
        // one for the same figure.
        // ------------------------------------------------------------------------------------------
        PeerCounters final = node.Reactor.CountersOf(nodePeer);
        final.BytesSent.Should().BeGreaterThanOrEqualTo(bytesSentAfterPhase1);
        final.BytesReceived.Should().BeGreaterThanOrEqualTo(bytesReceivedBefore);
        final.RetentionHighWaterBytes.Should().BeGreaterThanOrEqualTo(retentionHighWaterAfterSeven);
        final.PacketsRetransmitted.Should().BeGreaterThanOrEqualTo(retransmittedBefore + 2);
        final.HorizonDeferrals.Should().BeGreaterThanOrEqualTo(horizonDeferralsBefore + 5);
        final.OutOfWindowPackets.Should().BeGreaterThanOrEqualTo(outOfWindowBefore + 4);
        final.DuplicatePackets.Should().BeGreaterThanOrEqualTo(duplicateBefore + 3);
        node.Reactor.Counters.DatagramsSent.Should().BeGreaterThanOrEqualTo(reactorDatagramsSentBaseline + 6);
        node.Reactor.Counters.DatagramsReceived.Should().BeGreaterThanOrEqualTo(reactorDatagramsReceivedBaseline + 11);
        node.Reactor.Counters.UnknownSourceDiscards.Should().BeGreaterThanOrEqualTo(unknownSourceDiscardsBefore + 1);
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> — a bare
    /// <see cref="RawReactor"/> — and returns the wire session id <paramref name="node"/> assigned this
    /// Peer, mirroring <see cref="DatagramSessionSpaceRoutingTests"/>'s helper of the same name.
    /// </summary>
    private static uint EstablishLiveSessionId(DatagramSessionReactorHarness node, RawReactor client, LinkId clientLink)
    {
        const ulong clientNonce = 0x0102_0304_0506_0708UL;
        Source destination = Source.FromIPEndPoint(node.Endpoint);

        byte[] hello = BuildHelloDatagram(clientNonce);
        client.SendTo(clientLink, destination, hello);
        byte[] cookieDatagram = ExchangeUntilReceived(client, node);

        ulong echoedNonce = BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset, 8));
        long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset + 8, 8)));
        byte[] cookie = cookieDatagram.AsSpan(ControlBodyOffset + 16, CookieLength).ToArray();
        echoedNonce.Should().Be(clientNonce);

        byte[] helloWithCookie = BuildHelloWithCookieDatagram(clientNonce, epoch, cookie);
        client.SendTo(clientLink, destination, helloWithCookie);
        byte[] acceptDatagram = ExchangeUntilReceived(client, node);

        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
    }

    /// <summary>The <see cref="PeerId"/> Node assigned the one Peer this test's handshake produced.</summary>
    private static PeerId FindNodePeer(DatagramSessionReactorHarness node)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = node.DrainToList();
        return drained.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;
    }

    private static byte[] ExchangeUntilReceived(RawReactor client, DatagramSessionReactorHarness node)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            node.Reactor.Pump();
            client.Pump();
            foreach (RawEvent evt in client.Drain())
            {
                if (evt.Kind == RawEventKind.DatagramReceived)
                {
                    return evt.Payload.ToArray();
                }
            }

            if (deadline.Expired)
            {
                throw new InvalidOperationException("The handshake reply did not arrive within the Pump budget.");
            }

            PumpDeadline.Yield();
        }
    }

    private static byte[] BuildControlDatagram(byte controlType, ReadOnlySpan<byte> body)
    {
        byte[] datagram = new byte[ControlDatagramSize];
        BinaryPrimitives.WriteUInt32BigEndian(datagram, ControlSession);
        datagram[4] = controlType;
        body.CopyTo(datagram.AsSpan(ControlBodyOffset));
        return datagram;
    }

    private static byte[] BuildHelloDatagram(ulong clientNonce)
    {
        byte[] body = new byte[4 + 8];
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(0, 4), ProtocolVersion);
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(4, 8), clientNonce);
        return BuildControlDatagram(HelloType, body);
    }

    private static byte[] BuildHelloWithCookieDatagram(ulong clientNonce, long epoch, byte[] cookie)
    {
        byte[] body = new byte[4 + 8 + 8 + CookieLength + 4 + 4 + ResumeSecretLength];
        int offset = 0;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), ProtocolVersion);
        offset += 4;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), clientNonce);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), unchecked((ulong)epoch));
        offset += 8;
        cookie.CopyTo(body.AsSpan(offset, CookieLength));
        offset += CookieLength;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), DialerSessionId);
        offset += 4;
        BinaryPrimitives.WriteUInt32BigEndian(body.AsSpan(offset, 4), DialerMaxMessageSize);
        offset += 4;
        return BuildControlDatagram(HelloWithCookieType, body);
    }

    /// <summary>Builds a 20-byte live-Session datagram header naming <paramref name="session"/> with both ack blocks zeroed.</summary>
    private static byte[] BuildHeader(uint session)
    {
        byte[] header = new byte[DatagramHeaderSize];
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0, 4), session);
        return header;
    }

    /// <summary>Builds one unfragmented packet record: flags, a four-byte sequence, a two-byte length, then the payload.</summary>
    private static byte[] BuildRecord(byte modeBits, uint sequence, ReadOnlySpan<byte> payload)
    {
        const int unfragmentedRecordSize = 7;
        byte[] record = new byte[unfragmentedRecordSize + payload.Length];
        record[0] = modeBits;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        payload.CopyTo(record.AsSpan(unfragmentedRecordSize));
        return record;
    }
}
