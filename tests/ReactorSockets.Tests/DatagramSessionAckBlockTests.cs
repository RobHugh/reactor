using System.Buffers.Binary;
using System.Net;
using System.Runtime.CompilerServices;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.3.6 (issue #188): wiring the two reliable ack blocks an arriving live-Session datagram
/// carries into the receiving Peer's <c>OutstandingReliableSequences</c> tracker. Nothing on this
/// ticket's live paths ever calls <c>RecordSent</c> — the datagram profile has no outbound data send
/// yet (6.4/6.5's) — so a deferral can never actually occur here; what these tests can prove through
/// the public seam is that arriving nonzero ack blocks are read and applied without disturbing anything
/// else the parse loop already does, that the horizon-deferral reconciliation this wiring adds still
/// reads zero exactly as it did before, and that reading two extra ack blocks off every arrival costs no
/// allocation. Whether <c>ApplyAck</c> itself was actually invoked is not observable through the public
/// surface at all — <c>OutstandingReliableSequences</c> has no public reader — so that half of the
/// ticket is not asserted here (see the ticket report).
/// </summary>
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
        // delivered or discarded as a record, and the reconciliation this ticket adds still reads
        // zero: nothing on the live receive path ever arms a deadline, so no deferral can occur.
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.DatagramMalformed);
        accepter.DrainToList().Should().BeEmpty();
        accepter.Reactor.CountersOf(accepterPeer).HorizonDeferrals.Should().Be(0);
    }

    [Fact]
    public void Pump_HeaderTruncatedInsideAnAckBlock_IsDiscardedWithDatagramMalformedExactlyAsBefore()
    {
        // Arrange — 16 of the header's 20 bytes: past the session id and the whole ReliableOrdered
        // block, but short of the ReliableUnordered block. This proves the ack-block read this ticket
        // added does not change what already made a short header fail to parse.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] datagram = BuildHeader(sessionId, reliableOrderedAckBase: 5, reliableOrderedAckBits: 0xFF, 0, 0)[..16];

        // Act
        ulong before = accepter.Reactor.Counters.UnknownSourceDiscards;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.UnknownSourceDiscards > before);

        // Assert — discarded exactly as an unrecognisable-length datagram always was, and nothing was
        // applied: the horizon-deferral figure stays at zero, matching a Peer nothing was ever applied
        // to.
        accepter.Sink.Received.Should().Contain(record => record.Id == DiagnosticId.DatagramMalformed);
        accepter.Reactor.CountersOf(accepterPeer).HorizonDeferrals.Should().Be(0);
    }

    [Fact]
    public void Pump_ASustainedRunOfArrivalsCarryingChangingAckBlocks_DoesNotGrowAllocationAsTheRunContinues()
    {
        // Arrange — the two ack blocks this ticket wires into the receive path are read off every
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
