using System.Buffers.Binary;
using System.Net;
using System.Runtime.CompilerServices;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #171: the receive arena — the datagram Session profile's receive side of the memory bill,
/// <c>Capacity × 2 reliable Spaces × ReceiveWindowFragments × FragmentSize</c> — is preallocated once
/// at <see cref="DatagramSessionReactor"/> construction, regardless of whether any Peer has connected,
/// and released when the Reactor is disposed. Mirrors the allocation-measurement pattern in
/// <see cref="StreamSessionRetentionAllocationTests"/>, the send-side equivalent (issue #84).
/// </summary>
public class DatagramSessionReceiveArenaAllocationTests
{
    [Fact]
    public void Construct_WithNoPeersConnected_AllocatesAtLeastCapacityTimesTwoTimesReceiveWindowTimesFragmentSize()
    {
        // Arrange
        const int capacity = 16;
        const int fragmentSize = 1200;
        const int receiveWindowFragments = 25;

        // Act
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        using DatagramSessionReactorHarness harness = new(
            capacity: capacity,
            maxMessageSize: fragmentSize,
            fragmentSize: fragmentSize,
            receiveWindowFragments: receiveWindowFragments);
        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();

        // Assert — the receive arena's backing buffer alone accounts for at least this many bytes,
        // preallocated up front rather than grown lazily as data arrives. Only the two reliable Spaces
        // get window storage (the unreliable Spaces classify sequences but retain no payload), which
        // is why this is Capacity × 2 rather than Capacity × 4.
        long expectedReceiveBudgetBytes = (long)capacity * 2 * receiveWindowFragments * fragmentSize;
        (allocatedAfter - allocatedBefore).Should().BeGreaterThanOrEqualTo(expectedReceiveBudgetBytes);
    }

    [Fact]
    public void Construct_DoublingReceiveWindowFragments_AtLeastDoublesTheAllocationDelta()
    {
        // Arrange
        const int capacity = 16;
        const int fragmentSize = 1200;
        const int narrowWindow = 25;
        const int wideWindow = 50;

        // Act
        long beforeNarrow = GC.GetAllocatedBytesForCurrentThread();
        using (DatagramSessionReactorHarness narrow = new(
            capacity: capacity,
            maxMessageSize: fragmentSize,
            fragmentSize: fragmentSize,
            receiveWindowFragments: narrowWindow))
        {
            long allocatedForNarrow = GC.GetAllocatedBytesForCurrentThread() - beforeNarrow;

            long beforeWide = GC.GetAllocatedBytesForCurrentThread();
            using DatagramSessionReactorHarness wide = new(
                capacity: capacity,
                maxMessageSize: fragmentSize,
                fragmentSize: fragmentSize,
                receiveWindowFragments: wideWindow);
            long allocatedForWide = GC.GetAllocatedBytesForCurrentThread() - beforeWide;

            // Assert — doubling the configured window at least doubles the arena's own contribution:
            // the wide Reactor's construction allocates at least one more narrow-window's worth of
            // receive-arena bytes than the narrow Reactor's construction did.
            long oneWindowWorthOfBytes = (long)capacity * 2 * narrowWindow * fragmentSize;
            (allocatedForWide - allocatedForNarrow).Should().BeGreaterThanOrEqualTo(oneWindowWorthOfBytes);
        }
    }

    [Fact]
    public void Dispose_ReleasesTheReactorAndItsPreallocatedArenas()
    {
        // Arrange
        WeakReference reactorReference = ConstructAndDispose();

        // Act — nothing in this method still holds a strong reference to the Reactor once
        // ConstructAndDispose has returned, so a full collection reclaims it and everything it
        // preallocated, the receive arena included.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // Assert
        reactorReference.IsAlive.Should().BeFalse();
    }

    /// <summary>
    /// Isolated in its own non-inlined method so the local <see cref="DatagramSessionReactorHarness"/>
    /// and its Reactor are out of scope — and so not kept artificially alive by the JIT — by the time
    /// the caller forces a collection.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference ConstructAndDispose()
    {
        DatagramSessionReactorHarness harness = new(capacity: 16, receiveWindowFragments: 25);
        WeakReference reference = new(harness.Reactor);
        harness.Dispose();
        return reference;
    }

    /// <summary>
    /// Closes issue #171's outstanding acceptance criterion — "memory does not grow across a sustained
    /// run of inbound datagrams" — left unticked when the arena landed because nothing wrote into the
    /// receive path yet; #172 and #173 wired that up. Each cycle sends every odd offset in the window
    /// in one datagram, then every even offset in another — the same out-of-order, hole-filling pattern
    /// the direct <c>ReceiveWindow</c> unit test used before the type went internal (this ticket) — over
    /// the <c>Unreliable</c> Space, which classifies without retaining any payload, so this measures the
    /// window's own bookkeeping rather than reliable retention.
    /// </summary>
    [Fact]
    public void Pump_ASustainedRunOfOutOfOrderInboundDatagrams_DoesNotGrowAllocationAsTheRunContinues()
    {
        // Arrange
        const int capacity = 16;
        const int warmupCycles = 50;
        const int measuredCyclesPerPhase = 300;

        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: capacity, maxMessageSize: 1300);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        uint cycleBase = RunCycles(accepter, client, clientLink, sessionId, capacity, 0, warmupCycles);

        // Act — two equal-length phases run back to back, deep into the same sustained run. If the
        // window's bookkeeping grew with how many sequences it had already seen, the second phase —
        // identical in shape to the first, just further along — would allocate visibly more.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long beforePhaseOne = GC.GetAllocatedBytesForCurrentThread();
        cycleBase = RunCycles(accepter, client, clientLink, sessionId, capacity, cycleBase, measuredCyclesPerPhase);
        long phaseOneDelta = GC.GetAllocatedBytesForCurrentThread() - beforePhaseOne;

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long beforePhaseTwo = GC.GetAllocatedBytesForCurrentThread();
        RunCycles(accepter, client, clientLink, sessionId, capacity, cycleBase, measuredCyclesPerPhase);
        long phaseTwoDelta = GC.GetAllocatedBytesForCurrentThread() - beforePhaseTwo;

        // Assert — a per-datagram cost from the socket layer (L1, not this ticket's concern) is
        // expected and unavoidable; what must not happen is that cost climbing as the run goes on,
        // which is what an accumulating receive window would show up as. The multiple is generous
        // headroom against GC noise, not a tight bound.
        phaseTwoDelta.Should().BeLessThanOrEqualTo(phaseOneDelta * 2 + 1);
    }

    /// <summary>
    /// Runs <paramref name="cycles"/> cycles of the odd-offsets-then-even-offsets pattern starting at
    /// <paramref name="startingCycleBase"/>, each cycle covering exactly <paramref name="capacity"/>
    /// sequences, and returns the next cycle's base.
    /// </summary>
    private static uint RunCycles(
        DatagramSessionReactorHarness accepter,
        RawReactor client,
        LinkId clientLink,
        uint sessionId,
        int capacity,
        uint startingCycleBase,
        int cycles)
    {
        uint cycleBase = startingCycleBase;
        Source destination = Source.FromIPEndPoint(accepter.Endpoint);

        for (int cycle = 0; cycle < cycles; cycle++)
        {
            List<byte> oddDatagram = [.. BuildHeader(sessionId)];
            for (int i = 1; i < capacity; i += 2)
            {
                oddDatagram.AddRange(BuildRecord(cycleBase + (uint)i));
            }

            ulong before = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, destination, [.. oddDatagram]);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);

            List<byte> evenDatagram = [.. BuildHeader(sessionId)];
            for (int i = 0; i < capacity; i += 2)
            {
                evenDatagram.AddRange(BuildRecord(cycleBase + (uint)i));
            }

            before = accepter.Reactor.Counters.DatagramsReceived;
            client.SendTo(clientLink, destination, [.. evenDatagram]);
            accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);

            accepter.DrainToList();
            cycleBase += (uint)capacity;
        }

        return cycleBase;
    }

    private const byte ModeUnreliable = 0b0000_0000;
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

    /// <summary>Builds a 20-byte live-Session datagram header naming <paramref name="session"/> with both ack blocks zeroed.</summary>
    private static byte[] BuildHeader(uint session)
    {
        byte[] header = new byte[DatagramHeaderSize];
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0, 4), session);
        return header;
    }

    /// <summary>Builds one unfragmented, empty-payload Unreliable-Space packet record for <paramref name="sequence"/>.</summary>
    private static byte[] BuildRecord(uint sequence)
    {
        const int unfragmentedRecordSize = 7;
        byte[] record = new byte[unfragmentedRecordSize];
        record[0] = ModeUnreliable;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), 0);
        return record;
    }

    [Fact]
    public void Construct_ReceiveWindowBelowItsFloor_ThrowsThroughTheReactorWithTheExistingMessage()
    {
        // Arrange — a window of one fragment cannot hold a Message that occupies four, so the
        // options' floor refuses it. Asserted through the Reactor rather than the options object
        // alone (issue #171): the arena is taken at construction, so a combination that cannot be
        // afforded must fail before any socket is bound, not later when the first datagram arrives.
        const int fragmentSize = 1200;
        const int maxMessageSize = fragmentSize * 4;

        // Act
        Action construct = () => _ = new DatagramSessionReactorHarness(
            maxMessageSize: maxMessageSize,
            fragmentSize: fragmentSize,
            receiveWindowFragments: 1);

        // Assert
        construct.Should().Throw<ArgumentException>()
            .WithMessage($"*a receive window of 1 fragments cannot hold one {maxMessageSize}-byte Message*");
    }
}
