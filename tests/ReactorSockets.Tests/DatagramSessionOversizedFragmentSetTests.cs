using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// A Fragment declaring more pieces than this Reactor's own <c>MaxMessageSize</c> could ever
/// back is a claim that end could never honour, not merely a malformed record (#282). Rather than
/// discarding just the offending datagram, the Peer that made the claim is closed with
/// <see cref="DisconnectReason.ProtocolViolation"/> — the same treatment #253's individually-illegal
/// Fragment metadata and #256's forged sets ultimately fall back to when they cannot be trusted, but
/// stronger, because an oversized claim survives both of those checks (it is individually legal and its
/// own set is internally consistent) and is refused for a reason neither of them tests: the Message it
/// claims to assemble into could never fit the limit this end announced at the handshake.
/// </summary>
/// <remarks>
/// Modeled on <see cref="DatagramSessionHostileFragmentDiscardTests"/>: a live Session established by
/// hand over a bare <see cref="RawReactor"/> — the same instrument <see cref="HostilePeer"/> is built
/// from — then driven with hand-built datagrams so the exact wire shape under test is never left to
/// <c>Send</c>'s own chunking. Where that file's technique needs adapting is the outcome itself: an
/// oversized claim does not merely discard a datagram and leave the Peer alive to prove the window
/// untouched by resending at the same sequence — it closes the Peer outright, so there is no Peer left
/// to resend to. Each test below says how it adapts to that.
/// </remarks>
public class DatagramSessionOversizedFragmentSetTests
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
    private const int UnfragmentedRecordSize = 7;
    private const int FragmentRecordSize = 11;

    private const byte ModeUnreliable = 0b0000_0000;
    private const byte ModeReliableOrdered = 0b0000_0011;
    private const byte FragmentBit = 0b0000_0100;

    [Fact]
    public void Pump_FragmentDeclaringOneMoreThanReceiveWindowFloor_ClosesThePeerWithProtocolViolation()
    {
        // Arrange — the default harness resolves MaxMessageSize 4096 and FragmentSize 1200 to a
        // ReceiveWindowFloor of 4 (ceil(4096 / 1200)). A single Fragment declaring 5 is individually
        // legal (#253: index 0 < count 5) and trivially its own well-formed one-member set (#256), so it
        // reaches only the new #282 check, which is the one thing this test proves.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink, clientNonce: 0x1000_0000_0000_0001UL);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] oversizedDatagram =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 0, fragmentIndex: 0, fragmentCount: 5, payload: [0xAA]),
        ];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), oversizedDatagram);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained =
            accepter.PumpUntilDrained(all => all.Any(e => e.Kind == SessionEventKind.PeerClosed));

        // Assert
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Should().BeEquivalentTo((SessionEventKind.PeerClosed, accepterPeer, DisconnectReason.ProtocolViolation));
        accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.OversizedFragmentSet);
    }

    [Fact]
    public void Pump_OversizedFragmentSetTrailingAWellFormedRecord_DeliversNoMessageFromEitherRecord()
    {
        // Arrange — a well-formed ReliableOrdered record at sequence 0 ahead of the oversized Fragment
        // tail, exactly the shape DatagramSessionHostileFragmentDiscardTests uses to prove a discarded
        // datagram takes its whole content down with it. Here the tail does not merely fail parsing —
        // it closes the Peer — so the assertion is the same "nothing was delivered" one, read off the
        // drain rather than off a resend, since there is no Peer left to resend to afterward.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink, clientNonce: 0x1000_0000_0000_0002UL);

        byte[] wellFormedRecord = BuildRecord(ModeReliableOrdered, sequence: 0, payload: [0xBB]);
        byte[] oversizedTail = BuildFragmentRecord(ModeReliableOrdered, sequence: 1, fragmentIndex: 0, fragmentCount: 5, payload: [0xCC]);
        byte[] datagram = [.. BuildHeader(sessionId), .. wellFormedRecord, .. oversizedTail];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained =
            accepter.PumpUntilDrained(all => all.Any(e => e.Kind == SessionEventKind.PeerClosed));

        // Assert — the Peer closes, and neither record — not the leading well-formed one, not the
        // oversized Fragment — ever reached the consumer as a Message.
        drained.Should().Contain(e => e.Kind == SessionEventKind.PeerClosed && e.Reason == DisconnectReason.ProtocolViolation);
        drained.Should().NotContain(e => e.Kind == SessionEventKind.MessageReceived);
    }

    [Fact]
    public void Pump_OversizedFragmentSetClosesThePeer_TheFreedSlotAdmitsANewPeerCleanly()
    {
        // Arrange — the oversized set must occupy no receive-window
        // slot, but DatagramSessionHostileFragmentDiscardTests' own technique for proving that —
        // resending the same sequence to the SAME Peer and checking it is admitted rather than refused
        // as a duplicate — has no Peer left to run against here: the oversized claim ends the Session,
        // not merely the datagram. What the public surface still lets a test observe is the Slot the
        // closed Peer occupied: this harness is built with capacity 1, so closing the only Peer frees
        // the only Slot, and establishing a second Session necessarily reuses it. That new Peer's
        // ReceiveWindowSpaces is reset on admission (DatagramSessionReactor's own class doc: a Slot's
        // windows "outlive whichever Peer occupies the Slot" and are reset on every admission), so a
        // well-formed record at sequence 0 admitting cleanly for the new Peer is the strongest proof the
        // seam can give that nothing about the oversized claim's declared range was left behind for the
        // next occupant to trip over.
        const int capacity = 1;
        using DatagramSessionReactorHarness accepter = new(capacity: capacity);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));

        uint firstSessionId = EstablishLiveSessionId(accepter, client, clientLink, clientNonce: 0x2000_0000_0000_0001UL);
        byte[] oversizedDatagram =
        [
            .. BuildHeader(firstSessionId),
            .. BuildRecord(ModeReliableOrdered, sequence: 0, payload: [0xAA]),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 1, fragmentIndex: 0, fragmentCount: 5, payload: [0xBB]),
        ];

        // Act — the attack closes the only Peer this capacity-1 Reactor holds.
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), oversizedDatagram);
        accepter.PumpUntilDrained(all => all.Any(e => e.Kind == SessionEventKind.PeerClosed));

        // Act — a second Session, necessarily admitted into the Slot the first Peer just vacated.
        uint secondSessionId = EstablishLiveSessionId(accepter, client, clientLink, clientNonce: 0x2000_0000_0000_0002UL);
        byte[] followUpDatagram =
        [
            .. BuildHeader(secondSessionId),
            .. BuildRecord(ModeReliableOrdered, sequence: 0, payload: [0xCC]),
        ];

        ulong receivedBefore = accepter.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), followUpDatagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > receivedBefore);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert — sequence 0 admits for the new Peer exactly as it would for any freshly connected one:
        // the window it landed in carried nothing over from the closed Peer's aborted, oversized claim.
        delivered.Should().ContainSingle().Which.Should().Equal(new byte[] { 0xCC });
    }

    [Fact]
    public void Pump_FragmentDeclaringExactlyReceiveWindowFloor_IsAcceptedAndDeliveredWhole()
    {
        // Arrange — the inclusive boundary: MaxMessageSize 3600 over the default 1200-byte FragmentSize
        // resolves ReceiveWindowFloor to exactly 3, so a set declaring 3 is the largest a Peer may
        // legally claim. Three Fragments, the last shorter than the other two, so the assembled payload
        // also proves concatenation order survives right at the boundary.
        const int maxMessageSize = 3600;
        using DatagramSessionReactorHarness accepter = new(maxMessageSize: maxMessageSize);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink, clientNonce: 0x3000_0000_0000_0001UL);

        byte[] datagram =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 0, fragmentIndex: 0, fragmentCount: 3, payload: [0x01, 0x02]),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 1, fragmentIndex: 1, fragmentCount: 3, payload: [0x03, 0x04]),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 2, fragmentIndex: 2, fragmentCount: 3, payload: [0x05]),
        ];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > 0);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert — the set at exactly the floor is legal, completes, and delivers as one Message; no
        // Peer is closed.
        delivered.Should().ContainSingle().Which.Should().Equal(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 });
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.OversizedFragmentSet);
    }

    [Fact]
    public void Pump_UnreliableRecordWithTheFragmentBitAndALargeDeclaredCount_DoesNotCloseThePeerAndStillDelivers()
    {
        // Arrange — the #282 check gates on the two reliable modes only (matching ClassifyRecord's own
        // gate, per this Reactor's own remarks): a stray fragment bit riding an Unreliable-mode record
        // is never evaluated by it at all, however large its declared count. Unreliable delivery ignores
        // the fragment bit entirely (DeliverUnreliableRecord has no isFragment branch), so this record
        // is expected to be delivered as ordinary payload, not merely tolerated.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink, clientNonce: 0x4000_0000_0000_0001UL);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] datagram =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeUnreliable, sequence: 0, fragmentIndex: 0, fragmentCount: 1000, payload: [0xDD]),
        ];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > 0);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> events = accepter.DrainToList();

        // Assert — no close, and the record's payload reaches the consumer as an ordinary Unreliable
        // delivery.
        events.Should().NotContain(e => e.Kind == SessionEventKind.PeerClosed);
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.OversizedFragmentSet);

        Action stillUsable = () => accepter.Reactor.CountersOf(accepterPeer);
        stillUsable.Should().NotThrow("a stray fragment bit on an Unreliable record is not a protocol violation");
    }

    [Fact]
    public void Pump_FragmentDeclaringCountAboveFloorButWithinAWidenedReceiveWindow_IsAdmittedAndDeliveredWhole()
    {
        // Arrange — issue #264's rework: a receive window widened past ReceiveWindowFloor stands in
        // for a conforming peer whose own FragmentSize is smaller than this end's, so it legally cuts
        // a within-MaxMessageSize Message into more pieces than the floor alone would allow. Capacity 2,
        // FragmentSize 576 (the RFC 791 minimum) and MaxMessageSize 4096 resolve ReceiveWindowFloor to 8
        // (ceil(4096/576)); ReceiveWindowFragments is set to 20, well above that floor. A declared count
        // of 10 sits strictly between the two: above the floor, at or below the window. The memory bill
        // (DatagramSessionOptions.ReceiveBudgetBytes) at these numbers is Capacity(2) x 20 x (2 reliable
        // Spaces x (576 + 2 length bytes) + 4 admission bytes) + Capacity(2) x 2 x 10 pending-group
        // entries x 6 bytes = 46,640 bytes — trivial, so the whole 10-piece set is sent and delivered
        // rather than merely proving the first piece is accepted.
        const int capacity = 2;
        const int fragmentSize = 576;
        const int maxMessageSize = 4096;
        const int receiveWindowFragments = 20;
        const int fragmentCount = 10;

        using DatagramSessionReactorHarness accepter = new(
            capacity: capacity, maxMessageSize: maxMessageSize, fragmentSize: fragmentSize,
            receiveWindowFragments: receiveWindowFragments);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink, clientNonce: 0x5000_0000_0000_0001UL);

        byte[] datagram = [.. BuildHeader(sessionId), .. BuildWholeFragmentSet(ModeReliableOrdered, startSequence: 0, fragmentCount)];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > 0);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert — a count above the floor but within the widened window is admitted, assembles and
        // delivers whole; no Peer is closed and no oversized-set diagnostic is raised.
        delivered.Should().ContainSingle().Which.Should().Equal(ExpectedWholeFragmentSetPayload(fragmentCount));
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.OversizedFragmentSet);
    }

    [Fact]
    public void Pump_FragmentDeclaringOneMoreThanAWidenedReceiveWindow_ClosesThePeerWithProtocolViolation()
    {
        // Arrange — the bound moved with the window, it did not disappear: at the same widened options
        // as the admitted case above (ReceiveWindowFragments 20), a declared count of 21 — one past the
        // window — is still refused, exactly as a count one past ReceiveWindowFloor is refused at
        // default options in the first test in this file.
        const int capacity = 2;
        const int fragmentSize = 576;
        const int maxMessageSize = 4096;
        const int receiveWindowFragments = 20;

        using DatagramSessionReactorHarness accepter = new(
            capacity: capacity, maxMessageSize: maxMessageSize, fragmentSize: fragmentSize,
            receiveWindowFragments: receiveWindowFragments);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink, clientNonce: 0x5000_0000_0000_0002UL);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] oversizedDatagram =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 0, fragmentIndex: 0, fragmentCount: receiveWindowFragments + 1, payload: [0xAA]),
        ];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), oversizedDatagram);
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained =
            accepter.PumpUntilDrained(all => all.Any(e => e.Kind == SessionEventKind.PeerClosed));

        // Assert
        drained.Should().ContainSingle(e => e.Kind == SessionEventKind.PeerClosed)
            .Which.Should().BeEquivalentTo((SessionEventKind.PeerClosed, accepterPeer, DisconnectReason.ProtocolViolation));
        accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.OversizedFragmentSet);
    }

    [Fact]
    public void Pump_FragmentDeclaringExactlyAWidenedReceiveWindow_IsAdmittedAndDeliveredWhole()
    {
        // Arrange — the inclusive boundary at the widened bound: a declared count of exactly
        // ReceiveWindowFragments (20) is the largest a Peer may legally claim once the window is
        // widened past the floor, mirroring the default-options boundary test above but at the wider
        // window this rework exists to admit. Same options, and so the same trivial memory bill, as the
        // two tests above.
        const int capacity = 2;
        const int fragmentSize = 576;
        const int maxMessageSize = 4096;
        const int receiveWindowFragments = 20;

        using DatagramSessionReactorHarness accepter = new(
            capacity: capacity, maxMessageSize: maxMessageSize, fragmentSize: fragmentSize,
            receiveWindowFragments: receiveWindowFragments);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink, clientNonce: 0x5000_0000_0000_0003UL);

        byte[] datagram = [.. BuildHeader(sessionId), .. BuildWholeFragmentSet(ModeReliableOrdered, startSequence: 0, receiveWindowFragments)];

        // Act
        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), datagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > 0);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert — the set at exactly the widened window is legal, completes, and delivers as one
        // Message; no Peer is closed.
        delivered.Should().ContainSingle().Which.Should().Equal(ExpectedWholeFragmentSetPayload(receiveWindowFragments));
        accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.OversizedFragmentSet);
    }

    /// <summary>
    /// Builds a complete, whole Fragment set of <paramref name="fragmentCount"/> pieces starting at
    /// <paramref name="startSequence"/>, each carrying one payload byte distinct to its index (see
    /// <see cref="ExpectedWholeFragmentSetPayload"/>), for a test that sends every piece of a set rather
    /// than only enough to prove admission.
    /// </summary>
    private static byte[] BuildWholeFragmentSet(byte modeBits, uint startSequence, int fragmentCount)
    {
        List<byte> records = [];
        for (int index = 0; index < fragmentCount; index++)
        {
            records.AddRange(BuildFragmentRecord(
                modeBits,
                sequence: startSequence + (uint)index,
                fragmentIndex: (ushort)index,
                fragmentCount: (ushort)fragmentCount,
                payload: [(byte)(index & 0xFF)]));
        }

        return [.. records];
    }

    /// <summary>The assembled payload <see cref="BuildWholeFragmentSet"/> is expected to deliver: one byte per index, in order.</summary>
    private static byte[] ExpectedWholeFragmentSetPayload(int fragmentCount)
    {
        byte[] expected = new byte[fragmentCount];
        for (int index = 0; index < fragmentCount; index++)
        {
            expected[index] = (byte)(index & 0xFF);
        }

        return expected;
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> — a bare
    /// <see cref="RawReactor"/> — and returns the wire session id <paramref name="accepter"/> assigned
    /// this Peer, mirroring <see cref="DatagramSessionHostileFragmentDiscardTests"/>'s helper of the
    /// same name. Takes an explicit <paramref name="clientNonce"/> so a test establishing more than one
    /// Session over the same <paramref name="client"/> link — the Slot-reuse test above — never risks
    /// the accepter mistaking a second Hello for a retransmission of the first.
    /// </summary>
    private static uint EstablishLiveSessionId(
        DatagramSessionReactorHarness accepter, RawReactor client, LinkId clientLink, ulong clientNonce)
    {
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
        // The remaining ResumeSecretLength bytes are the dialler's resume secret; an all-zero one is
        // fine, since only a Resume reads it and these tests send none.
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
        byte[] record = new byte[UnfragmentedRecordSize + payload.Length];
        record[0] = modeBits;
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        payload.CopyTo(record.AsSpan(UnfragmentedRecordSize));
        return record;
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
