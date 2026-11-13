using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A <c>ReliableOrdered</c> Space holds at most twice its receive window of ordinary records
/// behind its send-order head, each no wider than a fragment slot. A record past either limit is left
/// unadmitted and counted as out of window. Every stall here is built by hand and sent from a bare
/// <see cref="RawReactor"/>, the same way <see cref="DatagramSessionFragmentGroupCapacityTests"/> addresses
/// a live Session.
/// </summary>
public class DatagramSessionOrderedHoldBudgetTests
{
    private const int ControlDatagramSize = 256;
    private const uint ControlSession = 0;

    private const byte HelloType = 1;
    private const int ControlBodyOffset = 5;
    private const int CookieLength = 32;

    private const uint ProtocolVersion = DatagramSessionWire.ProtocolVersion;

    private const int DatagramHeaderSize = 20;
    private const int UnfragmentedRecordSize = 7;
    private const int FragmentRecordSize = 11;

    private const byte ModeReliableOrdered = 0b0000_0011;
    private const byte FragmentBit = 0b0000_0100;
    private const byte ControlBit = 0b0000_1000;

    private const int MaxMessageSize = 1024;
    private const int FragmentSize = 576;
    private const int ReceiveWindowFragments = 8;

    /// <summary>Twice <see cref="ReceiveWindowFragments"/>: the sequences a stalled head can have held behind it.</summary>
    private const int HoldLimitSlots = 2 * ReceiveWindowFragments;

    private const int FloodCapacity = 2;
    private const int FloodFragmentSize = 1200;
    private const int FloodPayloadLength = 1000;
    private const int FloodWindowFragments = 64;

    /// <summary>Five hold limits of ordinary records, so most of the flood arrives after the limit is reached.</summary>
    private const int FloodRecords = 5 * 2 * FloodWindowFragments;

    private const int NarrowWindowFragments = 2;

    /// <summary>Enough gaps to walk a held record across every position of any ring this window could use.</summary>
    private const int NarrowWindowGaps = 32;

    private const int SamplesPerMeasurement = 3;

    [Fact]
    public void Pump_RecordsPastTheHoldLimitBehindAStalledHead_CountsThemOutOfWindow()
    {
        // Arrange — a head that can never be delivered, with the base already past it.
        using DatagramSessionReactorHarness accepter = NewAccepter();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId peer = FindAccepterPeer(accepter);
        StallTheHeadAtSequenceZero(accepter, client, clientLink, sessionId);

        // Act — sequences 2 to 15 fit behind the head. The ten after them do not.
        const int recordsPastTheLimit = 10;
        for (uint sequence = 2; sequence < HoldLimitSlots + recordsPastTheLimit; sequence++)
        {
            SendAndPump(accepter, client, clientLink, sessionId, Record(ModeReliableOrdered, sequence, [0x01]));
        }

        // Assert
        accepter.Reactor.CountersOf(peer).OutOfWindowPackets.Should().Be(recordsPastTheLimit);
    }

    [Fact]
    public void Pump_RecordsPastTheSlotsHeldRecordBudgetBytesCounts_CountsThemOutOfWindow()
    {
        // Arrange — issue #540: the slot count is read back from the stated budget, not restated, so the
        // Reactor's limit and the budget must come from one hold rule. Each slot is a whole fragment plus
        // 44 bytes of bookkeeping.
        using DatagramSessionReactorHarness accepter = NewAccepter();
        long budgetSlots = accepter.Options.HeldRecordBudgetBytes
            / ((long)accepter.Options.Reactor.Capacity * (FragmentSize + 44));
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId peer = FindAccepterPeer(accepter);
        StallTheHeadAtSequenceZero(accepter, client, clientLink, sessionId);

        // Act — sequences from 2 up to the budget's slot count are held, and the ten after them are not.
        const int recordsPastTheLimit = 10;
        for (uint sequence = 2; sequence < budgetSlots + recordsPastTheLimit; sequence++)
        {
            SendAndPump(accepter, client, clientLink, sessionId, Record(ModeReliableOrdered, sequence, [0x01]));
        }

        // Assert
        accepter.Reactor.CountersOf(peer).OutOfWindowPackets.Should().Be(recordsPastTheLimit);
    }

    [Fact]
    public void Pump_ARecordWiderThanAFragmentSlotBehindAHole_CountsItOutOfWindow()
    {
        // Arrange — the receive path refuses only a Fragment wider than this end's FragmentSize
        // (ADR 0024), so an unfragmented record that wide is admitted.
        using DatagramSessionReactorHarness accepter = NewAccepter();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId peer = FindAccepterPeer(accepter);

        // Act — sequence 0 has not arrived, so sequence 1 would have to be held.
        SendAndPump(accepter, client, clientLink, sessionId, Record(ModeReliableOrdered, 1, new byte[FragmentSize + 1]));

        // Assert
        accepter.Reactor.CountersOf(peer).OutOfWindowPackets.Should().Be(1);
    }

    [Fact]
    public void Pump_ARecordWiderThanAFragmentSlotRetransmittedOnceItsTurnComes_IsDelivered()
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = NewAccepter();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        accepter.DrainAndDiscard();
        byte[] wide = new byte[FragmentSize + 1];
        byte[] wideRecord = Record(ModeReliableOrdered, 1, wide);
        SendAndPump(accepter, client, clientLink, sessionId, wideRecord);

        // Act — the hole fills, then the far end's retransmission arrives at the head.
        SendAndPump(accepter, client, clientLink, sessionId, Record(ModeReliableOrdered, 0, [0xAA]));
        List<byte[]> delivered = accepter.DrainPayloads();
        SendAndPump(accepter, client, clientLink, sessionId, wideRecord);
        delivered.AddRange(accepter.DrainPayloads());

        // Assert
        delivered.ConvertAll(payload => payload.Length).Should().Equal(1, wide.Length);
    }

    [Fact]
    public void Pump_RecordsRefusedWhileAHeadWaitsForArenaRoom_AreDeliveredInOrderOnceRetransmitted()
    {
        // Arrange — honest traffic. Sequence 0 takes the Peer's whole read share, so the record held at
        // sequence 1 waits for the next Frame while the base runs on behind it. Sequences 2 to 16 fit
        // within the hold limit from that head, and 17 to 30 do not.
        using DatagramSessionReactorHarness accepter = new(
            capacity: 2,
            maxMessageSize: MaxMessageSize,
            fragmentSize: FragmentSize,
            receiveWindowFragments: ReceiveWindowFragments,
            arenaSize: 2 * MaxMessageSize);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        accepter.DrainAndDiscard();

        List<byte> behindTheHole = [];
        for (byte sequence = 2; sequence < 8; sequence++)
        {
            behindTheHole.AddRange(Record(ModeReliableOrdered, sequence, [sequence]));
        }

        List<byte> burst =
        [
            .. Record(ModeReliableOrdered, 1, [1]),
            .. Record(ModeReliableOrdered, 0, new byte[MaxMessageSize]),
        ];
        for (byte sequence = 8; sequence <= 16; sequence++)
        {
            burst.AddRange(Record(ModeReliableOrdered, sequence, [sequence]));
        }

        List<byte> refused = [];
        for (byte sequence = 17; sequence <= 30; sequence++)
        {
            refused.AddRange(Record(ModeReliableOrdered, sequence, [sequence]));
        }

        // Act — the burst and the refused records share a Frame, and the refused ones come again after
        // the head has moved, standing in for the far end's retransmission.
        SendAndPump(accepter, client, clientLink, sessionId, [.. behindTheHole]);
        List<byte[]> delivered = accepter.DrainPayloads();
        SendAndPump(accepter, client, clientLink, sessionId, [.. burst, .. refused]);
        delivered.AddRange(accepter.DrainPayloads());
        accepter.Reactor.Pump();
        delivered.AddRange(accepter.DrainPayloads());
        SendAndPump(accepter, client, clientLink, sessionId, [.. refused]);
        delivered.AddRange(accepter.DrainPayloads());

        // Assert
        delivered.ConvertAll(payload => (int)payload[0]).Should().Equal(Enumerable.Range(0, 31));
    }

    [Fact]
    public void Pump_RecordsAfterADeliveredGoodbye_StayAdmittedPastTheHoldLimit()
    {
        // Arrange — this end is closing too, so the Peer outlives the remote's Goodbye. The Goodbye at
        // sequence 0 leaves the head at 1, and nothing after it is delivered or held.
        using DatagramSessionReactorHarness accepter = NewAccepter();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId peer = FindAccepterPeer(accepter);
        accepter.Reactor.Close(peer);
        SendAndPump(accepter, client, clientLink, sessionId, Record(ModeReliableOrdered | ControlBit, 0, []));

        // Act
        for (uint sequence = 1; sequence <= HoldLimitSlots + 4; sequence++)
        {
            SendAndPump(accepter, client, clientLink, sessionId, Record(ModeReliableOrdered, sequence, [0x01]));
        }

        // Assert
        accepter.Reactor.CountersOf(peer).OutOfWindowPackets.Should().Be(0);
    }

    [Fact]
    public void Pump_AFloodBehindAStalledHead_AllocatesNoMoreThanOnePeersHeldRecordBudget()
    {
        // Arrange — the same records in the same datagrams, with the control's sequences all beyond its
        // window so it refuses every one and holds nothing (ADR 0019's comparison, taken after the
        // handshake rather than from construction). Their difference is what holding cost. Payloads stop short of a whole slot so the ring's own doubling, which
        // the budget does not count because it is garbage rather than held, stays inside the margin.
        MeasureFlood(stalled: true);
        MeasureFlood(stalled: false);

        // Act
        (long floodAllocated, PeerCounters floodCounters) = MeasureFlood(stalled: true);
        (long controlAllocated, PeerCounters controlCounters) = MeasureFlood(stalled: false);

        // Assert — the flood reached the limit: sequences 2 up to the limit were held and the rest refused.
        floodCounters.OutOfWindowPackets.Should().Be((ulong)(FloodRecords - ((2 * FloodWindowFragments) - 2)));

        // Assert — the control held nothing, its two Fragments included.
        controlCounters.OutOfWindowPackets.Should().Be((ulong)FloodRecords + 2);

        // Assert — holding stayed inside what the options state for one Peer.
        using DatagramSessionReactorHarness budgetSource = NewFloodAccepter();
        long onePeersBudget = budgetSource.Options.HeldRecordBudgetBytes / FloodCapacity;
        (floodAllocated - controlAllocated).Should().BeLessThanOrEqualTo(onePeersBudget);
    }

    [Fact]
    public void Pump_GapsOpenedAndFilledUnderANarrowWindow_AllocatesNoMoreThanOnePeersHeldRecordBudget()
    {
        // Arrange — a slot keeps its buffer after release, so each gap can leave a whole-slot buffer on a
        // new ring position. Nothing stalls here: every record is delivered, and the control sends the
        // same records in order so it holds none of them.
        MeasureGaps(openGaps: true);
        MeasureGaps(openGaps: false);

        // Act
        (long gapsAllocated, ulong gapsDelivered) = MeasureGaps(openGaps: true);
        (long controlAllocated, ulong controlDelivered) = MeasureGaps(openGaps: false);

        // Assert — every record arrived in both runs, so the two differ only in what was held.
        gapsDelivered.Should().Be(3 * NarrowWindowGaps);
        controlDelivered.Should().Be(3 * NarrowWindowGaps);

        // Assert — the kept buffers stayed inside what the options state for one Peer.
        using DatagramSessionReactorHarness budgetSource = NewNarrowWindowAccepter();
        long onePeersBudget = budgetSource.Options.HeldRecordBudgetBytes / budgetSource.Options.Reactor.Capacity;
        (gapsAllocated - controlAllocated).Should().BeLessThanOrEqualTo(onePeersBudget);
    }

    /// <summary>
    /// Runs the gap traffic from a live Session and returns what it alone allocated, the smallest of
    /// <see cref="SamplesPerMeasurement"/> runs because noise only ever adds bytes (issue #415), and how
    /// many Messages the last run delivered.
    /// </summary>
    private static (long Allocated, ulong Delivered) MeasureGaps(bool openGaps)
    {
        long smallest = long.MaxValue;
        ulong delivered = 0;
        for (int sample = 0; sample < SamplesPerMeasurement; sample++)
        {
            using DatagramSessionReactorHarness accepter = NewNarrowWindowAccepter();
            using RawReactor client = new(new ReactorOptions { Capacity = 4 });
            LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
            uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
            PeerId peer = FindAccepterPeer(accepter);
            byte[][] datagrams = BuildGapDatagrams(sessionId, openGaps);
            Source destination = Source.FromIPEndPoint(accepter.Endpoint);

            long before = GC.GetAllocatedBytesForCurrentThread();
            foreach (byte[] datagram in datagrams)
            {
                ulong received = accepter.Reactor.Counters.DatagramsReceived;
                client.SendTo(clientLink, destination, datagram);
                accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > received);
                accepter.DrainAndDiscard();
            }

            smallest = Math.Min(smallest, GC.GetAllocatedBytesForCurrentThread() - before);
            delivered = accepter.Reactor.CountersOf(peer).MessagesReceived;
        }

        return (smallest, delivered);
    }

    /// <summary>
    /// Three whole-slot records per gap. With gaps open, the second arrives first and is held, then the
    /// first releases it, then the third arrives in order. That moves the head three positions per gap,
    /// which shares no factor with a power-of-two ring, so the held record lands on every position.
    /// </summary>
    private static byte[][] BuildGapDatagrams(uint sessionId, bool openGaps)
    {
        byte[] payload = new byte[FloodFragmentSize];
        byte[][] datagrams = new byte[3 * NarrowWindowGaps][];
        for (int gap = 0; gap < NarrowWindowGaps; gap++)
        {
            uint first = 3 * (uint)gap;
            uint[] order = openGaps ? [first + 1, first, first + 2] : [first, first + 1, first + 2];
            for (int i = 0; i < order.Length; i++)
            {
                datagrams[(3 * gap) + i] = [.. BuildHeader(sessionId), .. Record(ModeReliableOrdered, order[i], payload)];
            }
        }

        return datagrams;
    }

    private static DatagramSessionReactorHarness NewNarrowWindowAccepter() => new(
        capacity: 2,
        maxMessageSize: FloodFragmentSize,
        fragmentSize: FloodFragmentSize,
        receiveWindowFragments: NarrowWindowFragments,
        arenaSize: 8 * FloodFragmentSize,
        withoutDiagnostics: true);

    /// <summary>
    /// Runs one flood from a live Session and returns what the flood alone allocated, the smallest of
    /// <see cref="SamplesPerMeasurement"/> runs because noise only ever adds bytes (issue #415).
    /// </summary>
    private static (long Allocated, PeerCounters Counters) MeasureFlood(bool stalled)
    {
        long smallest = long.MaxValue;
        PeerCounters counters = default;
        for (int sample = 0; sample < SamplesPerMeasurement; sample++)
        {
            using DatagramSessionReactorHarness accepter = NewFloodAccepter();
            using RawReactor client = new(new ReactorOptions { Capacity = 4 });
            LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
            uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
            PeerId peer = FindAccepterPeer(accepter);
            byte[][] datagrams = BuildFloodDatagrams(sessionId, stalled);
            Source destination = Source.FromIPEndPoint(accepter.Endpoint);

            long before = GC.GetAllocatedBytesForCurrentThread();
            foreach (byte[] datagram in datagrams)
            {
                ulong received = accepter.Reactor.Counters.DatagramsReceived;
                client.SendTo(clientLink, destination, datagram);
                accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > received);
            }

            smallest = Math.Min(smallest, GC.GetAllocatedBytesForCurrentThread() - before);
            counters = accepter.Reactor.CountersOf(peer);
        }

        return (smallest, counters);
    }

    /// <summary>
    /// The stalled flood's two Fragments that stop the head, then ordinary records in sequence behind
    /// them. The control sends the same bytes with every sequence moved past its window.
    /// </summary>
    private static byte[][] BuildFloodDatagrams(uint sessionId, bool stalled)
    {
        uint first = stalled ? 0u : FloodWindowFragments;
        byte[] payload = new byte[FloodPayloadLength];
        byte[][] datagrams = new byte[FloodRecords + 2][];
        datagrams[0] = [.. BuildHeader(sessionId), .. Fragment(ModeReliableOrdered, first, 0, 2, payload)];
        datagrams[1] = [.. BuildHeader(sessionId), .. Fragment(ModeReliableOrdered, first + 1, 0, 2, payload)];
        for (int i = 0; i < FloodRecords; i++)
        {
            datagrams[i + 2] = [.. BuildHeader(sessionId), .. Record(ModeReliableOrdered, first + 2 + (uint)i, payload)];
        }

        return datagrams;
    }

    private static DatagramSessionReactorHarness NewFloodAccepter() => new(
        capacity: FloodCapacity,
        maxMessageSize: FloodFragmentSize,
        fragmentSize: FloodFragmentSize,
        receiveWindowFragments: FloodWindowFragments,
        withoutDiagnostics: true);

    private static DatagramSessionReactorHarness NewAccepter() => new(
        capacity: 2,
        maxMessageSize: MaxMessageSize,
        fragmentSize: FragmentSize,
        receiveWindowFragments: ReceiveWindowFragments);

    /// <summary>
    /// Sequence 0 opens a group that sequence 1 never joins, because sequence 1 names a group of its own.
    /// Both are admitted, so the base moves on to 2, and the group at sequence 0 can never complete, so
    /// the send-order head stays at 0 for good.
    /// </summary>
    private static void StallTheHeadAtSequenceZero(
        DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink, uint sessionId)
    {
        SendAndPump(accepter, client, clientLink, sessionId, Fragment(ModeReliableOrdered, 0, 0, 2, [0x11]));
        SendAndPump(accepter, client, clientLink, sessionId, Fragment(ModeReliableOrdered, 1, 0, 2, [0x12]));
    }

    private static void SendAndPump(
        DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink, uint sessionId, byte[] records)
    {
        ulong received = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), [.. BuildHeader(sessionId), .. records]);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > received);
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> and returns the wire
    /// session id <paramref name="accepter"/> assigned this Peer.
    /// </summary>
    private static uint EstablishLiveSessionId(DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink)
    {
        const ulong clientNonce = 0x0102_0304_0506_0708UL;
        Source destination = Source.FromIPEndPoint(accepter.Endpoint);

        client.SendTo(clientLink, destination, BuildHelloDatagram(clientNonce));
        byte[] cookieDatagram = ExchangeUntilReceived(client, accepter);

        long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset + 8, 8)));
        byte[] cookie = cookieDatagram.AsSpan(ControlBodyOffset + 16, CookieLength).ToArray();

        client.SendTo(clientLink, destination, DatagramSessionWire.BuildHelloWithCookie(clientNonce, epoch, cookie));
        byte[] acceptDatagram = ExchangeUntilReceived(client, accepter);

        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
    }

    /// <summary>The <see cref="PeerId"/> the accepter assigned the one Peer this test's handshake produced.</summary>
    private static PeerId FindAccepterPeer(DatagramSessionReactorHarness accepter) =>
        accepter.DrainToList().First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

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

    /// <summary>Builds one unfragmented packet record: flags, a four-byte sequence, a two-byte length, then the payload.</summary>
    private static byte[] Record(byte flags, uint sequence, ReadOnlySpan<byte> payload)
    {
        byte[] record = new byte[UnfragmentedRecordSize + payload.Length];
        record[0] = flags;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        payload.CopyTo(record.AsSpan(UnfragmentedRecordSize));
        return record;
    }

    /// <summary>
    /// Builds one Fragment record: the unfragmented shape's bytes with the fragment bit set, followed by
    /// a two-byte fragment index and a two-byte fragment count, then the payload.
    /// </summary>
    private static byte[] Fragment(byte modeBits, uint sequence, ushort fragmentIndex, ushort fragmentCount, ReadOnlySpan<byte> payload)
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
