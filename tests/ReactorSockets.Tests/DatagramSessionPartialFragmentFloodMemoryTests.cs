using System.Buffers.Binary;
using System.Net;
using System.Runtime.CompilerServices;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The central claim behind reassembly inside the receive window (#257-#260) is a memory
/// property, not a code-shape one — flooding partial Fragment sets fills only the flooding Peer's own
/// receive window, because there is no separate reassembly buffer for a hostile sender to grow. Asserted
/// here by measurement, the same way <see cref="DatagramSessionReceiveArenaAllocationTests"/> measures
/// the arena's own preallocation: a bare <see cref="RawReactor"/> peer — the hand-built-datagram
/// instrument <see cref="HostilePeer"/> wraps, and the one
/// <see cref="DatagramSessionHostileFragmentDiscardTests"/> and
/// <see cref="DatagramSessionFragmentInterleavingTests"/> already drive a live Session with — fills a
/// Reactor's window with Fragment groups that never complete, and
/// <see cref="GC.GetAllocatedBytesForCurrentThread"/> shows it costs no more than a Reactor holding
/// nothing (#279). The flooded Peer's own window is what limits
/// the damage, on the existing <see cref="PeerCounters.OutOfWindowPackets"/> and
/// <see cref="PeerCounters.DuplicatePackets"/> figures rather than a new counter; other Peers on the same
/// Reactor are unaffected; and nothing ever expires an incomplete set on a timer — see the doc comments
/// this issue adds to <see cref="ReceiveWindow"/> for why no such timer or constant exists.
/// </summary>
public class DatagramSessionPartialFragmentFloodMemoryTests
{
    private const int ControlDatagramSize = 256;
    private const uint ControlSession = 0;

    private const byte HelloType = 1;
    private const int ControlBodyOffset = 5;
    private const int CookieLength = 32;

    private const uint ProtocolVersion = 1;

    private const int DatagramHeaderSize = 20;
    private const int FragmentRecordSize = 11;
    private const byte FragmentBit = 0b0000_0100;

    private const byte ModeReliableOrdered = 0b0000_0011;

    private const int FloodWindowCapacity = 4096;

    /// <summary>The smallest fragment size the options accept, which keeps the 4096-slot arena small.</summary>
    private const int FloodFragmentSize = 576;
    private const int FloodRecordsPerDatagram = 128;

    /// <summary>
    /// The lone index-0 pieces the flood holds above the window's pinned base of 0: odd sequences 1
    /// through 4093. The flood holds only groups whose index-1 sequence would also fit the window, so
    /// it stops short of 4095, whose sibling 4096 would not.
    /// </summary>
    private const int HeldPartialSets = (FloodWindowCapacity / 2) - 1;

    /// <summary>The first odd sequence past the window's far edge.</summary>
    private const uint FirstBeyondWindowSequence = FloodWindowCapacity + 1;

    /// <summary>
    /// The bound sits above the 7.4 KB of jitter
    /// <see cref="DatagramSessionReceiveArenaAllocationTests"/>'s exact-equality measurements have shown
    /// between otherwise identical runs. A 24-byte object allocated per held Fragment, injected into the
    /// hold path, measured about 24 KB over the control, well above the bound.
    /// </summary>
    private const long AllocationToleranceBytes = 8 * 1024;

    private static readonly byte[] _floodPayload = [0xFF];

    /// <summary>
    /// Holding a window full of partial sets costs no more than holding nothing. The flood sends
    /// single-piece "index 0 of 2" Fragment records at odd sequences and never sends sequence 0. That hole pins the
    /// base (<c>ReliableOrdered</c> never slides), so every in-window piece is held as its own open group
    /// at once, and the window ends up full of partial sets. The flood then sends as many again beyond
    /// the window's far edge, all refused. The control Reactor receives the same records, same count
    /// and size, but all beyond the window, so it refuses everything and holds nothing, exactly as at
    /// construction. Both runs are measured from construction to the end of the flood. Construction
    /// and the per-datagram socket cost are identical, so any byte the flood takes beyond the control
    /// is the cost of holding the partial sets.
    /// </summary>
    [Fact]
    public void Pump_AFloodHoldingAWindowFullOfPartialSets_AllocatesNoMoreFromConstructionThanAReactorHoldingNothing()
    {
        // Arrange — prebuilt so building them is outside every measurement.
        byte[][] flood = BuildFloodDatagrams(holdInWindow: true);
        byte[][] control = BuildFloodDatagrams(holdInWindow: false);

        // A throwaway run of each pays the first-measurement JIT and static-initialisation cost at the
        // measured window size (#171/#174's measurement discipline).
        MeasureFromConstructionThrough(flood);
        MeasureFromConstructionThrough(control);

        // Act
        (long floodAllocated, PeerCounters floodCounters) = MeasureFromConstructionThrough(flood);
        (long controlAllocated, PeerCounters controlCounters) = MeasureFromConstructionThrough(control);

        // Assert — the flood held a window full of partial sets: every in-window piece was admitted,
        // every beyond-window piece was refused, and no set ever completed.
        floodCounters.OutOfWindowPackets.Should().Be((ulong)HeldPartialSets);
        floodCounters.DuplicatePackets.Should().Be(0);
        floodCounters.MessagesReceived.Should().Be(0, "no set in this flood ever completed");

        // Assert — the control held nothing: it refused every record.
        controlCounters.OutOfWindowPackets.Should().Be((ulong)HeldPartialSets * 2);

        // Assert — holding the window full of partial sets cost no more than holding nothing.
        (floodAllocated - controlAllocated).Should().BeLessThanOrEqualTo(AllocationToleranceBytes);
    }

    /// <summary>
    /// The flooded Peer's own receive window bounds the
    /// damage, and an arrival beyond it is refused and counted by the existing
    /// <see cref="PeerCounters.OutOfWindowPackets"/> and <see cref="PeerCounters.DuplicatePackets"/>
    /// figures rather than a new one invented for this case. As in the memory test above, every group
    /// here leaves a permanent hole — its index-0 piece is sent, its index-1 piece never is — so the
    /// base can never advance past the very first hole (<c>ReliableOrdered</c> never slides), which
    /// pins the window's reachable span for the rest of the flood.
    /// </summary>
    [Fact]
    public void Pump_AFloodOfPartialGroupsFarExceedingTheWindow_RefusesArrivalsAndCountsThemOnTheExistingWindowCounters()
    {
        // Arrange — a window of 8 sequences can admit at most 8 offsets from its (permanently pinned)
        // base; sending far more index-0 pieces than that proves the excess is refused rather than
        // silently accepted into some other, unbounded store.
        const int windowCapacity = 8;
        const int attemptedGroups = 20;

        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: windowCapacity, maxMessageSize: 4096);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        PeerCounters before = accepter.Reactor.CountersOf(accepterPeer);

        // Act — far more attempted groups than the window can ever admit, each individually a legal
        // Fragment (#253) but hostile in aggregate: a flood that never lets a single group complete.
        for (int i = 0; i < attemptedGroups; i++)
        {
            uint sequence = (uint)(i * 2);
            byte[] datagram =
            [
                .. BuildHeader(sessionId),
                .. BuildFragmentRecord(ModeReliableOrdered, sequence, fragmentIndex: 0, fragmentCount: 2, payload: [0x01]),
            ];
            SendAndPumpUntilReceived(accepter, client, clientLink, datagram);
        }

        accepter.DrainAndDiscard();
        PeerCounters afterFlood = accepter.Reactor.CountersOf(accepterPeer);

        // Assert — the flood's own window refused most of it, counted on the same existing figure any
        // other far-ahead arrival raises (ADR 0013); no group in this flood ever completed, because none
        // of them ever could.
        (afterFlood.OutOfWindowPackets - before.OutOfWindowPackets).Should().BeGreaterThan(0);
        afterFlood.MessagesReceived.Should().Be(before.MessagesReceived, "no group in this flood ever completed");

        // Act — sequence 4 (the third group's index-0 piece) was admitted above and, being above the
        // pinned base, is still sitting in its slot: resending it is a duplicate of a sequence still
        // inside the window, not an out-of-window one.
        byte[] resend =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 4, fragmentIndex: 0, fragmentCount: 2, payload: [0x01]),
        ];
        SendAndPumpUntilReceived(accepter, client, clientLink, resend);
        PeerCounters afterResend = accepter.Reactor.CountersOf(accepterPeer);

        // Assert — the resend is classified as a duplicate, on the same existing figure, and nothing
        // about the out-of-window figure moves for it.
        (afterResend.DuplicatePackets - afterFlood.DuplicatePackets).Should().BeGreaterThan(0);
        afterResend.OutOfWindowPackets.Should().Be(afterFlood.OutOfWindowPackets);
    }

    /// <summary>
    /// A hostile Peer flooding never-completing Fragment
    /// groups does not disturb another, ordinary Peer sharing the same Reactor. The ordinary Peer here
    /// is a full <see cref="DatagramSessionReactorHarness"/> dialer, sending and receiving one Message
    /// every round through the normal <c>Send</c>/Drain surface — interleaved with one flood datagram
    /// per round on the hostile Peer's own Space.
    /// </summary>
    [Fact]
    public void Pump_OtherPeersOnTheSameReactor_ContinueSendingAndReceivingMessagesDuringTheFlood()
    {
        // Arrange
        const int rounds = 50;

        using DatagramSessionReactorHarness accepter = new();
        using DatagramSessionReactorHarness normalDialer = new(bindLoopback: false, retentionBytes: 65536);
        using RawReactor hostileClient = new(new ReactorOptions { Capacity = 4 });
        LinkId hostileLink = hostileClient.Bind(new IPEndPoint(IPAddress.Any, 0));

        PeerId diallerPeer = normalDialer.Reactor.Connect(accepter.Endpoint);
        (_, List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> accepterConnectEvents) =
            DatagramSessionReactorHarness.PumpBothUntilConnected(normalDialer, accepter);
        PeerId accepterPeerForDialer = accepterConnectEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

        uint hostileSessionId = EstablishLiveSessionId(accepter, hostileClient, hostileLink);

        // Act — each round floods one more never-completing group on the hostile Peer's Space, and
        // sends and confirms delivery of one ordinary Message on the normal Peer's, on the same Reactor.
        uint floodSequence = 0;
        for (int round = 0; round < rounds; round++)
        {
            byte[] floodDatagram =
            [
                .. BuildHeader(hostileSessionId),
                .. BuildFragmentRecord(ModeReliableOrdered, floodSequence, fragmentIndex: 0, fragmentCount: 2, payload: _floodPayload),
            ];
            floodSequence = unchecked(floodSequence + 1);
            hostileClient.SendTo(hostileLink, Source.FromIPEndPoint(accepter.Endpoint), floodDatagram);

            byte[] payload = [(byte)round];
            normalDialer.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, payload).Should().Be(SendOutcome.Sent);

            List<byte[]> delivered = [];
            PumpDeadline deadline = PumpDeadline.Start();
            while (delivered.Count == 0)
            {
                normalDialer.Reactor.Pump();
                accepter.Reactor.Pump();
                hostileClient.Pump();
                delivered.AddRange(accepter.DrainPayloads());

                if (delivered.Count > 0)
                {
                    break;
                }

                if (deadline.Expired)
                {
                    throw new InvalidOperationException(
                        $"Round {round}'s normal Message never arrived within the Pump budget while a flood ran alongside it.");
                }

                PumpDeadline.Yield();
            }

            // Assert — the round's own Message, undisturbed by the flood arriving on the same Reactor
            // for an entirely different Peer.
            delivered.Should().ContainSingle().Which.Should().Equal(payload);

            // A few more Pumps let the accepter's acknowledgement flow back to the dialler so this
            // round's Message is released from the dialler's send retention before the next round sends
            // another — otherwise a long run of rounds would exhaust retention on unacked backlog that
            // has nothing to do with the flood this test is about.
            for (int settle = 0; settle < 5; settle++)
            {
                accepter.Reactor.Pump();
                normalDialer.Reactor.Pump();
            }
        }

        // Assert — the normal Peer received every Message the flood ran alongside, and its own counters
        // carry none of the flooded Peer's refusals: OutOfWindow and Duplicate are scoped per Peer.
        PeerCounters normalPeerCounters = accepter.Reactor.CountersOf(accepterPeerForDialer);
        normalPeerCounters.MessagesReceived.Should().Be((ulong)rounds);
        normalPeerCounters.OutOfWindowPackets.Should().Be(0);
        normalPeerCounters.DuplicatePackets.Should().Be(0);
    }

    /// <summary>
    /// No timer ever expires an incomplete Fragment set. Proved
    /// as a consequence rather than an absence — a held group's slot is still exactly where the flood
    /// left it after the clock advances far past any RTO, ack-horizon or retransmit interval this design
    /// actually has, the flooded Peer keeps classifying arrivals by its window's ordinary rules
    /// throughout, and the Reactor stays serviceable for a brand-new Peer afterwards.
    /// </summary>
    [Fact]
    public void Pump_ASustainedFloodFarBeyondAnyPlausibleTimeout_KeepsClassifyingByTheWindowsOwnRulesAndTheReactorStaysServiceable()
    {
        // Arrange — a lone index-0 piece above the base, permanently held: sequence 2 leaves a hole at
        // sequence 0 that pins the base at 0 forever, so slot 2 is never reclaimed by the base advancing
        // past it.
        const int windowCapacity = 8;
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: windowCapacity, maxMessageSize: 4096);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] openingDatagram =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 2, fragmentIndex: 0, fragmentCount: 2, payload: [0x01]),
        ];
        SendAndPumpUntilReceived(accepter, client, clientLink, openingDatagram);
        accepter.DrainAndDiscard();

        // Act — advance the clock by a full year and keep Pumping across that span. No timer fires for
        // the incomplete set: nothing is delivered and nothing about the Peer's state changes because
        // time passed.
        accepter.TimeProvider.Advance(TimeSpan.FromDays(365));
        for (int i = 0; i < 50; i++)
        {
            accepter.Reactor.Pump();
        }

        List<byte[]> deliveredAfterLongIdle = accepter.DrainPayloads();

        // Assert — still incomplete a year later.
        deliveredAfterLongIdle.Should().BeEmpty("no timer ever completes or discards an incomplete Fragment set");

        // Act — the year-old held piece is resent.
        PeerCounters beforeResend = accepter.Reactor.CountersOf(accepterPeer);
        byte[] resend =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 2, fragmentIndex: 0, fragmentCount: 2, payload: [0x01]),
        ];
        SendAndPumpUntilReceived(accepter, client, clientLink, resend);
        PeerCounters afterResend = accepter.Reactor.CountersOf(accepterPeer);

        // Assert — still classified as a duplicate of a slot the window itself still holds, exactly as
        // it would be seconds after the original arrival: the window's rules, not a clock, decide this.
        (afterResend.DuplicatePackets - beforeResend.DuplicatePackets).Should().Be(
            1, "the year-old incomplete group's slot is still exactly where the flood left it");

        // Assert — the Reactor stayed serviceable throughout: a brand-new Peer can still connect after
        // the long idle flood, and the flooded Peer's own counters remain readable.
        using RawReactor freshClient = new(new ReactorOptions { Capacity = 4 });
        LinkId freshLink = freshClient.Bind(new IPEndPoint(IPAddress.Any, 0));
        Action establishFreshPeer = () => EstablishLiveSessionId(accepter, freshClient, freshLink);
        establishFreshPeer.Should().NotThrow();

        Action readCounters = () => accepter.Reactor.CountersOf(accepterPeer);
        readCounters.Should().NotThrow();
    }

    /// <summary>
    /// Builds <see cref="HeldPartialSets"/> × 2 lone "index 0 of 2" Fragment records at odd sequences,
    /// so sequence 0 stays a permanent hole. With <paramref name="holdInWindow"/>, the first half run
    /// from sequence 1 inside the window and the second half from <see cref="FirstBeyondWindowSequence"/>.
    /// Without it, all of them run from <see cref="FirstBeyondWindowSequence"/>, so every record has the
    /// same size and only where it lands differs. The session id is stamped in once the handshake
    /// assigns it.
    /// </summary>
    private static byte[][] BuildFloodDatagrams(bool holdInWindow)
    {
        List<uint> sequences = [];
        int beyondWindowRecords = HeldPartialSets * 2;
        if (holdInWindow)
        {
            for (int i = 0; i < HeldPartialSets; i++)
            {
                sequences.Add((uint)((i * 2) + 1));
            }

            beyondWindowRecords = HeldPartialSets;
        }

        for (int i = 0; i < beyondWindowRecords; i++)
        {
            sequences.Add(FirstBeyondWindowSequence + (uint)(i * 2));
        }

        List<byte[]> datagrams = [];
        List<byte> body = [];

        for (int i = 0; i < sequences.Count; i++)
        {
            if (body.Count == 0)
            {
                body.AddRange(BuildHeader(0));
            }

            body.AddRange(BuildFragmentRecord(ModeReliableOrdered, sequences[i], fragmentIndex: 0, fragmentCount: 2, payload: _floodPayload));

            if ((i + 1) % FloodRecordsPerDatagram == 0 || i == sequences.Count - 1)
            {
                datagrams.Add([.. body]);
                body.Clear();
            }
        }

        return [.. datagrams];
    }

    /// <summary>
    /// Constructs a Reactor and a hostile <see cref="RawReactor"/>, completes the handshake, and sends
    /// <paramref name="datagrams"/> after writing the live session id into each one in place. Returns the bytes this thread
    /// allocated from construction to the last datagram's arrival, and the flooded Peer's counters.
    /// Non-inlined so nothing from one run is reachable during the next. Diagnostics are off so the
    /// test's own sink, which records into a list, is not part of what is measured.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static (long Allocated, PeerCounters Counters) MeasureFromConstructionThrough(byte[][] datagrams)
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        using DatagramSessionReactorHarness accepter = new(
            maxMessageSize: 4096, fragmentSize: FloodFragmentSize, receiveWindowFragments: FloodWindowCapacity, withoutDiagnostics: true);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        foreach (byte[] datagram in datagrams)
        {
            BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(0, 4), sessionId);
            SendAndPumpUntilReceived(accepter, client, clientLink, datagram);
            accepter.DrainAndDiscard();
        }

        long allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        return (allocated, accepter.Reactor.CountersOf(accepterPeer));
    }

    private static void SendAndPumpUntilReceived(
        DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink, byte[] datagram)
    {
        ulong before = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> — a bare
    /// <see cref="RawReactor"/>, exactly what <see cref="HostilePeer"/> is built from — and returns the
    /// wire session id <paramref name="accepter"/> assigned this Peer, mirroring
    /// <see cref="DatagramSessionFragmentInterleavingTests"/>'s helper of the same name.
    /// </summary>
    private static uint EstablishLiveSessionId(DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink)
    {
        ulong clientNonce = unchecked((ulong)Random.Shared.NextInt64());
        Source destination = Source.FromIPEndPoint(accepter.Endpoint);

        byte[] hello = BuildHelloDatagram(clientNonce);
        client.SendTo(clientLink, destination, hello);
        byte[] cookieDatagram = ExchangeUntilReceived(client, accepter);

        ulong echoedNonce = BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset, 8));
        long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset + 8, 8)));
        byte[] cookie = cookieDatagram.AsSpan(ControlBodyOffset + 16, CookieLength).ToArray();
        echoedNonce.Should().Be(clientNonce);

        byte[] helloWithCookie = DatagramSessionWire.BuildHelloWithCookie(clientNonce, epoch, cookie);
        client.SendTo(clientLink, destination, helloWithCookie);
        byte[] acceptDatagram = ExchangeUntilReceived(client, accepter);

        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
    }

    /// <summary>The <see cref="PeerId"/> the accepter assigned the one Peer this test's handshake produced.</summary>
    private static PeerId FindAccepterPeer(DatagramSessionReactorHarness accepter)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();
        return drained.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;
    }

    /// <summary>
    /// Alternately Pumps <paramref name="accepter"/> and <paramref name="client"/> until
    /// <paramref name="client"/> receives one datagram, or the deadline expires.
    /// </summary>
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

    /// <summary>Builds a 20-byte live-Session datagram header naming <paramref name="session"/> with both ack blocks zeroed.</summary>
    private static byte[] BuildHeader(uint session)
    {
        byte[] header = new byte[DatagramHeaderSize];
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(0, 4), session);
        return header;
    }

    /// <summary>
    /// Builds one Fragment record: the unfragmented shape's bytes with the fragment bit set, followed by
    /// a two-byte fragment index and a two-byte fragment count, then the payload.
    /// </summary>
    private static byte[] BuildFragmentRecord(
        byte modeBits, uint sequence, ushort fragmentIndex, ushort fragmentCount, ReadOnlySpan<byte> payload)
    {
        byte[] record = new byte[FragmentRecordSize + payload.Length];
        record[0] = (byte)(modeBits | FragmentBit);
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(7, 2), fragmentIndex);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(9, 2), fragmentCount);
        payload.CopyTo(record.AsSpan(FragmentRecordSize));
        return record;
    }
}
