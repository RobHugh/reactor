using System.Buffers.Binary;
using System.Net;
using System.Runtime.CompilerServices;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The two reliable ack blocks an arriving live-Session datagram carries, read into the receiving
/// Peer's <c>OutstandingReliableSequences</c> tracker. The accepter in every test here Sends nothing, so
/// it has no outstanding sequence for an ack to release or for the horizon rule to defer. What these
/// tests prove through the public seam is that arriving ack blocks are read and applied without
/// disturbing anything else the parse loop does, that <see cref="PeerCounters.HorizonDeferrals"/> stays
/// at zero, and that reading two ack blocks off every arrival costs no allocation. Whether
/// <c>ApplyAck</c> itself ran is not observable through the public surface, because
/// <c>OutstandingReliableSequences</c> has no public reader, so it is not asserted here.
/// </summary>
/// <remarks>
/// <para>
/// <b>Untrusted ack blocks.</b> Several tests drive a raw client's ack blocks with values no legitimate
/// sender produces — every bit set, a base at the <c>u32</c> wrap, a base far ahead of anything sent,
/// and a long fixed-seed run of wildly flipping bases and bits — and assert only what the seam shows:
/// no <see cref="DiagnosticId.DatagramMalformed"/>, no thrown exception, no Session event,
/// <c>HorizonDeferrals</c> staying at zero, and the Session still processing an ordinary arrival
/// afterwards.
/// </para>
/// <para>
/// <b>Effective RTO per arrival.</b>
/// <see cref="ParseDataDatagram_ArrivalsSeparatedByVaryingPumpCadences_StillDeriveTheEffectiveRtoWithoutMalformedOrWarnDiagnostic"/>
/// proves that the per-arrival read of <c>ObservedPumpInterval</c> and <c>Linger</c> feeding every
/// <c>ApplyAck</c> call works under no fixed cadence, a very fast one and a very slow one, without a
/// malformed report or a Warn diagnostic.
/// </para>
/// <para>
/// <b>Acks against sequences actually sent.</b> The horizon withholding deadlines, an arriving ack
/// block releasing outstanding sequences, and a slide arming what it newly covers all need a sequence
/// recorded as sent, which <see cref="DatagramSessionReactor.Send"/> produces.
/// <see cref="DatagramSessionHorizonTests"/> asserts those through
/// <see cref="PeerCounters.PacketsRetransmitted"/> and <see cref="PeerCounters.HorizonDeferrals"/>.
/// This file is the untrusted-input and parse-robustness half.
/// </para>
/// </remarks>
public class DatagramSessionAckBlockTests
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
    private const ulong DialerSendIdleIntervalMilliseconds = 60_000;
    private const ulong DialerLingerMilliseconds = 30_000;

    private const int DatagramHeaderSize = 20;

    [Fact]
    public void Pump_ArrivalCarryingNonZeroAckBlocksForBothReliableSpaces_ParsesCleanlyAndLeavesHorizonDeferralsAtZero()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] datagram = BuildHeader(
            sessionId,
            reliableOrderedAckBase: 5,
            reliableOrderedAckBits: 0b1010_1010,
            reliableUnorderedAckBase: 9,
            reliableUnorderedAckBits: 0b0000_0001);

        // Act
        ulong receivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > receivedBefore);

        // Assert — the ack-bearing header parsed without being reported malformed, nothing was
        // delivered or discarded as a record, and the deferral count still reads zero: the accepter
        // has sent nothing, so it has no outstanding sequence to defer.
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        accepter.DrainToList().Should().BeEmpty();
        accepter.Reactor.CountersOf(accepterPeer).HorizonDeferrals.Should().Be(0);
    }

    [Fact]
    public void Pump_HeaderTruncatedInsideAnAckBlock_IsDiscardedWithDatagramMalformedExactlyAsBefore()
    {
        // Arrange — 16 of the header's 20 bytes: past the session id and the whole ReliableOrdered
        // block, but short of the ReliableUnordered block. This proves the ack-block read does not
        // change what makes a short header fail to parse.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] datagram = BuildHeader(sessionId, reliableOrderedAckBase: 5, reliableOrderedAckBits: 0xFF, 0, 0)[..16];

        // Act
        ulong before = accepter.Reactor.Counters.UnknownSourceDatagrams;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDatagrams > before);

        // Assert — discarded exactly as an unrecognisable-length datagram always was, and nothing was
        // applied: the horizon-deferral figure stays at zero, matching a Peer nothing was ever applied
        // to.
        accepter.Sink.Received.Should().Contain(record => record.Id == DiagnosticId.DatagramMalformed);
        accepter.Reactor.CountersOf(accepterPeer).HorizonDeferrals.Should().Be(0);
    }

    [Fact]
    public void Pump_ASustainedRunOfArrivalsCarryingChangingAckBlocks_DoesNotGrowAllocationAsTheRunContinues()
    {
        // Arrange — the receive path reads the two ack blocks off every
        // arrival, whether or not they carry anything new; this proves that reading them and applying
        // them to the Peer's tracker costs no allocation, matching the receive path's existing
        // allocation-free discipline (issue #171 and its neighbours).
        const int warmupCycles = 20;
        const int measuredCyclesPerPhase = 200;

        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        RunCycles(accepter, client, clientLink, sessionId, warmupCycles);

        // Act — two equal-length phases, each advancing the ack base it sends so every arrival is a
        // fresh, newer block the tracker actually moves its horizon for, not a repeat of the one before.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long beforePhaseOne = GC.GetAllocatedBytesForCurrentThread();
        RunCycles(accepter, client, clientLink, sessionId, measuredCyclesPerPhase);
        long phaseOneDelta = GC.GetAllocatedBytesForCurrentThread() - beforePhaseOne;

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long beforePhaseTwo = GC.GetAllocatedBytesForCurrentThread();
        RunCycles(accepter, client, clientLink, sessionId, measuredCyclesPerPhase);
        long phaseTwoDelta = GC.GetAllocatedBytesForCurrentThread() - beforePhaseTwo;

        // Assert — a per-datagram L1 cost is expected and unavoidable; what must not happen is that
        // cost climbing as the run goes on, which is what a tracker that allocated on every ack it
        // applied would show up as.
        phaseTwoDelta.Should().BeLessThanOrEqualTo(phaseOneDelta * 2 + 1);
    }

    [Fact]
    public void Pump_AckBlocksWithEveryBitSetInBothReliableSpaces_SurvivesWithoutMalformedOrThrowingAndLeavesHorizonDeferralsAtZero()
    {
        // Arrange — an ack block describing sequences the sender never sent is
        // untrusted input. All 32 bits set claims every one of the 32 sequences above the base is
        // outstanding and now acknowledged, none of which this raw client ever sent.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] datagram = BuildHeader(
            sessionId,
            reliableOrderedAckBase: 5,
            reliableOrderedAckBits: 0xFFFF_FFFF,
            reliableUnorderedAckBase: 5,
            reliableUnorderedAckBits: 0xFFFF_FFFF);

        // Act
        ulong receivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > receivedBefore);

        // Assert — nothing was ever recorded as sent, so nothing exists to release; the point is that
        // an all-bits-set block against an empty tracker parses cleanly, is not reported malformed, and
        // never arms a deadline for a sequence that was never sent.
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        accepter.DrainToList().Should().BeEmpty();
        accepter.Reactor.CountersOf(accepterPeer).HorizonDeferrals.Should().Be(0);
    }

    [Fact]
    public void Pump_AckBaseAtUInt32MaxValueWithEveryBitSet_SurvivesTheWrappingAdditionWithoutThrowing()
    {
        // Arrange — a base of uint.MaxValue makes every bit's described sequence
        // (ackBase + n + 1) wrap past zero. This is the arithmetic edge an untrusted ack must survive:
        // a hostile base placed exactly where unchecked addition wraps.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] datagram = BuildHeader(
            sessionId,
            reliableOrderedAckBase: uint.MaxValue,
            reliableOrderedAckBits: 0xFFFF_FFFF,
            reliableUnorderedAckBase: uint.MaxValue,
            reliableUnorderedAckBits: 0xFFFF_FFFF);

        // Act
        ulong receivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > receivedBefore);

        // Assert — parsed and applied cleanly across the wrap, nothing reported malformed, nothing
        // deferred (nothing was ever sent to defer).
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        accepter.DrainToList().Should().BeEmpty();
        accepter.Reactor.CountersOf(accepterPeer).HorizonDeferrals.Should().Be(0);
    }

    [Fact]
    public void Pump_AckBaseFarAheadOfAnythingEverSent_SurvivesAndAppliesWithoutThrowing()
    {
        // Arrange — a base far ahead of anything this Session's Peer could ever
        // legitimately have sent (nothing has been sent at all — no outbound data send exists yet on
        // this profile). The receiving Peer has no way to know that, so it must accept the claim without
        // misbehaving.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        const uint farAheadBase = 0x7000_0000;
        byte[] datagram = BuildHeader(
            sessionId,
            reliableOrderedAckBase: farAheadBase,
            reliableOrderedAckBits: 0xAAAA_AAAA,
            reliableUnorderedAckBase: farAheadBase,
            reliableUnorderedAckBits: 0x5555_5555);

        // Act
        ulong receivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > receivedBefore);

        // Assert
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        accepter.DrainToList().Should().BeEmpty();
        accepter.Reactor.CountersOf(accepterPeer).HorizonDeferrals.Should().Be(0);
    }

    [Fact]
    public void Pump_ASustainedRunOfWildlyFlippingHostileAckBlocks_NeverThrowsKeepsHorizonDeferralsAtZeroAndLeavesThePeerConnected()
    {
        // Arrange — a long run of arrivals whose ack bases and bits flip wildly
        // between one datagram and the next — forward jumps, backward jumps landing on now-stale bases,
        // the two wrap edges, all-zero and all-one bitfields, and ordinary small values — driven from a
        // fixed seed so a failure is reproducible. Nothing here was ever legitimately sent, so every ack
        // here is untrusted: the Peer must survive every shape without throwing,
        // without gaining a deferral that has nothing behind it, and without the Session itself being
        // torn down.
        const int seed = 20260821;
        const int arrivals = 500;

        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        Random random = new(seed);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> allDrained = [];

        // Act
        for (int i = 0; i < arrivals; i++)
        {
            uint orderedBase = NextHostileValue(random);
            uint orderedBits = NextHostileValue(random);
            uint unorderedBase = NextHostileValue(random);
            uint unorderedBits = NextHostileValue(random);

            byte[] datagram = BuildHeader(sessionId, orderedBase, orderedBits, unorderedBase, unorderedBits);

            ulong before = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);
            allDrained.AddRange(accepter.DrainToList());
        }

        // Assert — no record ever hit the Sink as malformed, nothing was ever delivered or disconnected,
        // and the deferral figure stayed at zero throughout: no legitimate send exists for the horizon
        // rule to have anything real to defer.
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        allDrained.Should().BeEmpty();
        accepter.Reactor.CountersOf(accepterPeer).HorizonDeferrals.Should().Be(0);

        // Assert — the Peer is still alive and the Reactor still usable: an ordinary further exchange
        // still works after the hostile run, proving the run left the Session itself intact rather than
        // merely failing to crash mid-run.
        byte[] ordinaryFollowUp = BuildHeader(sessionId, 0, 0, 0, 0);
        ulong followUpBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), ordinaryFollowUp);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > followUpBefore);
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        accepter.DrainToList().Should().BeEmpty();
    }

    [Fact]
    public void ParseDataDatagram_ArrivalsSeparatedByVaryingPumpCadences_StillDeriveTheEffectiveRtoWithoutMalformedOrWarnDiagnostic()
    {
        // Arrange — every arrival's two ApplyAck calls take an effective RTO read fresh from
        // ObservedPumpInterval and Linger. This
        // varies the gap between Pumps under the harness's FakeTimeProvider — no advance at all (the
        // unseeded TimeSpan.MaxValue case), a very fast cadence, then a very slow one — so the value fed
        // into ApplyAck actually changes across the run, exercising the new computation rather than
        // leaving it always evaluated once at a single cadence.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);
        Source destination = Source.FromIPEndPoint(accepter.Endpoint);

        TimeSpan[] gapsBetweenArrivals =
        [
            TimeSpan.Zero,
            TimeSpan.FromMilliseconds(1),
            TimeSpan.FromMilliseconds(1),
            TimeSpan.FromSeconds(5),
            TimeSpan.FromSeconds(5),
        ];

        // Act
        for (int i = 0; i < gapsBetweenArrivals.Length; i++)
        {
            accepter.TimeProvider.Advance(gapsBetweenArrivals[i]);

            byte[] datagram = BuildHeader(
                sessionId,
                reliableOrderedAckBase: (uint)i,
                reliableOrderedAckBits: 0xFFFF_FFFF,
                reliableUnorderedAckBase: (uint)i,
                reliableUnorderedAckBits: 0xFFFF_FFFF);

            ulong before = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, destination, datagram);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);
        }

        // Assert — the wiring stayed healthy across every cadence the run exercised: nothing was
        // reported malformed, nothing was delivered or disconnected, no Warn-or-above diagnostic was
        // raised, and the deferral figure stayed at zero, matching a Peer nothing was ever legitimately
        // sent to.
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        accepter.Sink.Received.Should().NotContain(record => record.Level >= DiagnosticLevel.Warn);
        accepter.DrainToList().Should().BeEmpty();
        accepter.Reactor.CountersOf(accepterPeer).HorizonDeferrals.Should().Be(0);
    }

    /// <summary>
    /// Draws one hostile <c>u32</c>: mostly a uniformly random value, but biased toward the edges a
    /// wrap-aware tracker must handle correctly — zero, <see cref="uint.MaxValue"/>, and values either
    /// side of the wrap — so the fixed-seed run in
    /// <see cref="Pump_ASustainedRunOfWildlyFlippingHostileAckBlocks_NeverThrowsKeepsHorizonDeferralsAtZeroAndLeavesThePeerConnected"/>
    /// reliably lands on those edges rather than only on the interior of the range.
    /// </summary>
    private static uint NextHostileValue(Random random)
    {
        int roll = random.Next(10);
        return roll switch
        {
            0 => 0u,
            1 => uint.MaxValue,
            2 => unchecked(uint.MaxValue - (uint)random.Next(1, 32)),
            3 => unchecked((uint)(0 - random.Next(1, 32))),
            4 => (uint)random.Next(0, 32),
            _ => unchecked((uint)random.NextInt64(0, uint.MaxValue)),
        };
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void RunCycles(
        DatagramSessionReactorHarness accepter,
        RawReactor client,
        LinkId clientLink,
        uint sessionId,
        int cycles)
    {
        Source destination = Source.FromIPEndPoint(accepter.Endpoint);
        for (int cycle = 0; cycle < cycles; cycle++)
        {
            uint ackBase = (uint)cycle;
            byte[] datagram = BuildHeader(
                sessionId,
                reliableOrderedAckBase: ackBase,
                reliableOrderedAckBits: 0xFFFF_FFFF,
                reliableUnorderedAckBase: ackBase,
                reliableUnorderedAckBits: 0xFFFF_FFFF);

            ulong before = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, destination, datagram);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);
            accepter.DrainToList();
        }
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> — a bare
    /// <see cref="RawReactor"/> — and returns the wire session id <paramref name="accepter"/> assigned
    /// this Peer, mirroring <see cref="DatagramSessionSpaceRoutingTests"/>'s helper of the same name.
    /// </summary>
    private static uint EstablishLiveSessionId(DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink)
    {
        const ulong clientNonce = 0x0102_0304_0506_0708UL;
        Source destination = Source.FromIPEndPoint(accepter.Endpoint);

        byte[] hello = BuildHelloDatagram(clientNonce);
        client.SendTo(clientLink, destination, hello);
        byte[] cookieDatagram = ExchangeUntilReceived(client, accepter);

        ulong echoedNonce = BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset, 8));
        long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset + 8, 8)));
        byte[] cookie = cookieDatagram.AsSpan(ControlBodyOffset + 16, CookieLength).ToArray();
        echoedNonce.Should().Be(clientNonce);

        byte[] helloWithCookie = BuildHelloWithCookieDatagram(clientNonce, epoch, cookie);
        client.SendTo(clientLink, destination, helloWithCookie);
        byte[] acceptDatagram = ExchangeUntilReceived(client, accepter);

        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
    }

    /// <summary>
    /// The <see cref="PeerId"/> the accepter assigned the one Peer this test's handshake produced,
    /// mirroring <see cref="DatagramSessionSpaceRoutingTests"/>'s helper of the same name.
    /// </summary>
    private static PeerId FindAccepterPeer(DatagramSessionReactorHarness accepter)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();
        return drained.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;
    }

    private static byte[] ExchangeUntilReceived(RawReactor client, DatagramSessionReactorHarness accepter)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            accepter.Reactor.Pump();
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
        byte[] body = new byte[4 + 8 + 8 + CookieLength + 4 + 4 + 8 + 8 + ResumeSecretLength];
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
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), DialerSendIdleIntervalMilliseconds);
        offset += 8;
        BinaryPrimitives.WriteUInt64BigEndian(body.AsSpan(offset, 8), DialerLingerMilliseconds);
        offset += 8;
        return BuildControlDatagram(HelloWithCookieType, body);
    }

    /// <summary>Builds a 20-byte live-Session datagram header naming <paramref name="session"/> with both ack blocks set explicitly.</summary>
    private static byte[] BuildHeader(
        uint session,
        uint reliableOrderedAckBase,
        uint reliableOrderedAckBits,
        uint reliableUnorderedAckBase,
        uint reliableUnorderedAckBits)
    {
        byte[] header = new byte[DatagramHeaderSize];
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0, 4), session);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(4, 4), reliableOrderedAckBase);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(8, 4), reliableOrderedAckBits);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(12, 4), reliableUnorderedAckBase);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(16, 4), reliableUnorderedAckBits);
        return header;
    }
}
