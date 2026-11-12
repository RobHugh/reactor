using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The held-against-expected figures are the whole value of
/// <see cref="DiagnosticId.FragmentSetAbandoned"/>, so this file proves them at both ends of the range
/// a consumer acts on differently — one fragment held and one fragment short — rather than once in the
/// middle, where a bug that swapped or off-by-oned held and expected would be invisible. It also proves
/// each abandonment fires exactly once, subjected to the Peer by both kind and id, and that a set which
/// completes and is delivered normally emits nothing at all: a Diagnostic here marks a loss, and one
/// firing on a success would teach a consumer to ignore it. Issue #297's own emit-fires-at-all proof
/// lives on as <see cref="Pump_RingLapReachesAnUncompletableFragmentSetsSlot_EmitsFragmentSetAbandonedOnceWithHeldAndDeclaredCounts"/>.
/// </summary>
/// <remarks>
/// A 4-slot <c>ReliableOrdered</c> receive window and a 3-Fragment set at sequences 0, 1 and 2.
/// Since issue #446 a lap never overwrites a held Fragment whose set can still complete, so each case here
/// makes the set uncompletable: ordinary records take the sequences the set's missing Fragments needed, and
/// the base passes the whole set. Held-one-of-three sends only sequence 0 as a Fragment; held-two-of-three
/// also sends sequence 2. Either way, sequence 4 then lands on the exact physical ring slot sequence 0's
/// Fragment still occupies — the window is only 4 slots wide — and abandons the set.
/// </remarks>
public class DatagramSessionFragmentSetAbandonedTests
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

    [Fact]
    public void Pump_RingLapReachesAnUncompletableFragmentSetsSlot_EmitsFragmentSetAbandonedOnceWithHeldAndDeclaredCounts()
    {
        // Arrange — a window narrow enough (4 fragments) that a single far-ahead arrival laps the ring
        // back onto a still-held Fragment's own slot. Only the set's first Fragment (of three) ever arrives.
        using DatagramSessionReactorHarness accepter = NewNarrowWindowAccepter();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        // Act — the set's first Fragment, then ordinary records at the set's other two sequences, so the
        // base passes the whole set and it can never complete.
        SendFragment(accepter, client, clientLink, sessionId, sequence: 0, fragmentIndex: 0, fragmentCount: 3, payload: [0xAA]);
        SendRecord(accepter, client, clientLink, sessionId, sequence: 1, payload: [0x11]);
        SendRecord(accepter, client, clientLink, sessionId, sequence: 2, payload: [0x22]);

        // Act — sequence 4's ring slot is the same physical slot sequence 0's held Fragment still occupies.
        SendRecord(accepter, client, clientLink, sessionId, sequence: 4, payload: [0xBB]);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();

        // Assert — exactly one FragmentSetAbandoned record, subjecting the Peer by kind and id, and
        // carrying 1 fragment held against the 3 the set declared.
        AssertSingleAbandonment(accepter, accepterPeer, expectedHeld: 1, expectedDeclared: 3);

        // Assert — accounting only: the Peer is untouched, nothing closed or disconnected because of it.
        drained.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        Action stillUsable = () => accepter.Reactor.CountersOf(accepterPeer);
        stillUsable.Should().NotThrow("emitting FragmentSetAbandoned is accounting only and must not change Session state");
    }

    [Fact]
    public void Pump_RingLapReachesAnUncompletableSetHeldOneFragmentShortOfComplete_ReportsHeldOneBelowDeclared()
    {
        // Arrange — the same 3-piece set and 4-slot window, but this time two of its three declared
        // Fragments arrive before the lap: sequences 0 and 2. Sequence 1 is an ordinary record, so the base
        // passes the whole set and it can never complete.
        using DatagramSessionReactorHarness accepter = NewNarrowWindowAccepter();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        // Act — two of the set's three declared Fragments arrive, with an ordinary record between them.
        SendFragment(accepter, client, clientLink, sessionId, sequence: 0, fragmentIndex: 0, fragmentCount: 3, payload: [0xAA]);
        SendRecord(accepter, client, clientLink, sessionId, sequence: 1, payload: [0x11]);
        SendFragment(accepter, client, clientLink, sessionId, sequence: 2, fragmentIndex: 2, fragmentCount: 3, payload: [0xCC]);

        // Act — sequence 4 laps the ring onto sequence 0's still-held slot.
        SendRecord(accepter, client, clientLink, sessionId, sequence: 4, payload: [0xBB]);

        // Assert — exactly one FragmentSetAbandoned record, reporting 2 of the 3 declared pieces held —
        // one short of complete, not the one-of-three the sibling test proves.
        AssertSingleAbandonment(accepter, accepterPeer, expectedHeld: 2, expectedDeclared: 3);
    }

    [Fact]
    public void Pump_AFragmentSetThatCompletesAndIsDelivered_EmitsNoFragmentSetAbandoned()
    {
        // Arrange — the same 3-piece set and narrow window, but every declared piece arrives this time,
        // completing and delivering the set before anything ever reuses its ring slots.
        using DatagramSessionReactorHarness accepter = NewNarrowWindowAccepter();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        _ = FindAccepterPeer(accepter);

        // Act — every declared piece of the set arrives, in order.
        SendFragment(accepter, client, clientLink, sessionId, sequence: 0, fragmentIndex: 0, fragmentCount: 3, payload: [0xAA]);
        SendFragment(accepter, client, clientLink, sessionId, sequence: 1, fragmentIndex: 1, fragmentCount: 3, payload: [0xBB]);
        SendFragment(accepter, client, clientLink, sessionId, sequence: 2, fragmentIndex: 2, fragmentCount: 3, payload: [0xCC]);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert — the reassembled Message was actually delivered, so this is genuinely the completed
        // case rather than one that silently dropped the set.
        delivered.Should().ContainSingle().Which.Should().Equal([0xAA, 0xBB, 0xCC]);

        // Act — a later arrival laps the ring back onto the same physical slots the completed set used.
        SendRecord(accepter, client, clientLink, sessionId, sequence: 4, payload: [0xDD]);

        // Assert — completion clears the set's residue, so reusing its slots afterward never reports it
        // abandoned: a Diagnostic here marks a loss, and firing one on a success would teach a consumer
        // to ignore it.
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.FragmentSetAbandoned);
    }

    /// <summary>A 4-slot <c>ReliableOrdered</c> receive window, sized as small as <see cref="DatagramSessionOptions.Validate"/> allows.</summary>
    private static DatagramSessionReactorHarness NewNarrowWindowAccepter() =>
        new(maxMessageSize: 1200, fragmentSize: 1200, receiveWindowFragments: 4);

    /// <summary>Sends one Fragment record over <paramref name="sessionId"/> and pumps until it is received.</summary>
    private static void SendFragment(
        DatagramSessionReactorHarness accepter,
        RawReactor client,
        LinkId clientLink,
        uint sessionId,
        uint sequence,
        ushort fragmentIndex,
        ushort fragmentCount,
        ReadOnlySpan<byte> payload)
    {
        byte[] datagram =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(sequence, fragmentIndex, fragmentCount, payload),
        ];
        SendAndPumpUntilReceived(accepter, client, clientLink, datagram);
    }

    /// <summary>Sends one ordinary, unfragmented record over <paramref name="sessionId"/> and pumps until it is received.</summary>
    private static void SendRecord(
        DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink, uint sessionId, uint sequence, ReadOnlySpan<byte> payload)
    {
        byte[] datagram = [.. BuildHeader(sessionId), .. BuildRecord(sequence, payload)];
        SendAndPumpUntilReceived(accepter, client, clientLink, datagram);
    }

    private static void SendAndPumpUntilReceived(
        DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink, byte[] datagram)
    {
        ulong before = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > before);
    }

    /// <summary>
    /// Asserts exactly one <see cref="DiagnosticId.FragmentSetAbandoned"/> record was received,
    /// subjecting <paramref name="expectedPeer"/> by both <see cref="DiagnosticSubjectKind"/> and
    /// <see cref="PeerId"/> — not the id alone — and carrying <paramref name="expectedHeld"/> against
    /// <paramref name="expectedDeclared"/>.
    /// </summary>
    private static void AssertSingleAbandonment(
        DatagramSessionReactorHarness accepter, PeerId expectedPeer, long expectedHeld, long expectedDeclared)
    {
        accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.FragmentSetAbandoned)
            .Which.Should().Match<DiagnosticRecord>(r =>
                r.SubjectKind == DiagnosticSubjectKind.Peer
                && r.Peer.Equals(expectedPeer)
                && r.Value0 == expectedHeld
                && r.Value1 == expectedDeclared);
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> — a bare
    /// <see cref="RawReactor"/> — and returns the wire session id <paramref name="accepter"/> assigned
    /// this Peer.
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

        byte[] helloWithCookie = DatagramSessionWire.BuildHelloWithCookie(clientNonce, epoch, cookie);
        client.SendTo(clientLink, destination, helloWithCookie);
        byte[] acceptDatagram = ExchangeUntilReceived(client, accepter);

        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
    }

    /// <summary>The <see cref="PeerId"/> the accepter assigned the most recently connected Peer.</summary>
    private static PeerId FindAccepterPeer(DatagramSessionReactorHarness accepter)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = accepter.DrainToList();
        return drained.Last(e => e.Kind == SessionEventKind.PeerConnected).Peer;
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

    /// <summary>Builds one unfragmented <c>ReliableOrdered</c> packet record for <paramref name="sequence"/>.</summary>
    private static byte[] BuildRecord(uint sequence, ReadOnlySpan<byte> payload)
    {
        byte[] record = new byte[UnfragmentedRecordSize + payload.Length];
        record[0] = ModeReliableOrdered;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        payload.CopyTo(record.AsSpan(UnfragmentedRecordSize));
        return record;
    }

    /// <summary>
    /// Builds one <c>ReliableOrdered</c> Fragment record: the unfragmented shape's bytes with the
    /// fragment bit set, followed by a two-byte fragment index and a two-byte fragment count, then the
    /// payload.
    /// </summary>
    private static byte[] BuildFragmentRecord(uint sequence, ushort fragmentIndex, ushort fragmentCount, ReadOnlySpan<byte> payload)
    {
        byte[] record = new byte[FragmentRecordSize + payload.Length];
        record[0] = ModeReliableOrdered | FragmentBit;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(7, 2), fragmentIndex);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(9, 2), fragmentCount);
        payload.CopyTo(record.AsSpan(FragmentRecordSize));
        return record;
    }
}
