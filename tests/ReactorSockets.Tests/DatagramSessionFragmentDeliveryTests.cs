using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #259: a completed Fragment set (issue #258's <c>IsGroupComplete</c>) is delivered as one
/// contiguous Message and its receive-window slots are released. Built the same way
/// <see cref="DatagramSessionFragmentHoldingTests"/> is — a live Session driven by hand from a bare
/// <see cref="RawReactor"/> — since these tests need to construct exact Fragment sequences and byte
/// contents no ordinary <c>Send</c> call would produce on its own.
/// </summary>
public class DatagramSessionFragmentDeliveryTests
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
    private const int UnfragmentedRecordSize = 7;
    private const int FragmentRecordSize = 11;
    private const byte ModeReliableUnordered = 0b0000_0010;

    [Fact]
    public void Pump_ACompletedReliableUnorderedFragmentSet_DeliversOneAssembledMessageAndRaisesMessagesReceivedOnce()
    {
        // Arrange — three Fragments, the last one shorter than the other two, so the assembled payload
        // proves both concatenation order and that a short final Fragment is handled correctly.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);
        PeerCounters before = accepter.Reactor.CountersOf(accepterPeer);

        byte[] datagram =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 0, fragmentIndex: 0, fragmentCount: 3, payload: [0x01, 0x02]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 1, fragmentIndex: 1, fragmentCount: 3, payload: [0x03, 0x04]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 2, fragmentIndex: 2, fragmentCount: 3, payload: [0x05]),
        ];

        // Act
        SendAndPumpUntilReceived(accepter, client, clientLink, datagram);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert — one Message, byte-exact concatenation in fragment-index order, one MessagesReceived rise.
        delivered.Should().ContainSingle().Which.Should().Equal(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 });
        PeerCounters after = accepter.Reactor.CountersOf(accepterPeer);
        after.MessagesReceived.Should().Be(before.MessagesReceived + 1, "the completed set is one Message, not three");
    }

    [Fact]
    public void Pump_ADatagramCarryingANonContiguousSubsetOfOneGroup_IsAdmittedAndTheSetStillCompletes()
    {
        // Arrange — issue #274: the shape selective retransmission produces. A three-Fragment set whose
        // index 1 was acked while 0 and 2 both expired packs those two into one datagram, with no index
        // 1 in it. That is honest traffic, so the set check admits it: what makes a set well-formed is
        // the claimed range each Fragment declares, not whether the pieces sharing a datagram form a
        // gap-free run.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);
        PeerCounters before = accepter.Reactor.CountersOf(accepterPeer);

        byte[] subsetDatagram =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 0, fragmentIndex: 0, fragmentCount: 3, payload: [0x01, 0x02]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 2, fragmentIndex: 2, fragmentCount: 3, payload: [0x05]),
        ];

        // Act — the gapped subset first, then the missing middle piece in a datagram of its own.
        SendAndPumpUntilReceived(accepter, client, clientLink, subsetDatagram);
        List<byte[]> deliveredFromSubset = accepter.DrainPayloads();

        byte[] remainderDatagram =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 1, fragmentIndex: 1, fragmentCount: 3, payload: [0x03, 0x04]),
        ];
        SendAndPumpUntilReceived(accepter, client, clientLink, remainderDatagram);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert — the gapped datagram was not discarded: its two Fragments were held, and the set
        // completes and assembles in index order once the missing one arrives.
        deliveredFromSubset.Should().BeEmpty("an incomplete set is held, not delivered");
        accepter.Reactor.Counters.UnknownSourceDiscards.Should().Be(0, "a gapped subset of one group is honest traffic, not a forgery");
        delivered.Should().ContainSingle().Which.Should().Equal(new byte[] { 0x01, 0x02, 0x03, 0x04, 0x05 });
        PeerCounters after = accepter.Reactor.CountersOf(accepterPeer);
        after.MessagesReceived.Should().Be(before.MessagesReceived + 1);
    }

    [Fact]
    public void Pump_AFragmentArrivingAfterItsGroupAlreadyDelivered_DeliversNothingFurther()
    {
        // Arrange — a two-piece ReliableUnordered set completes and delivers.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        byte[] datagram =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 0, fragmentIndex: 0, fragmentCount: 2, payload: [0xAA]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 1, fragmentIndex: 1, fragmentCount: 2, payload: [0xBB]),
        ];
        SendAndPumpUntilReceived(accepter, client, clientLink, datagram);
        accepter.DrainAndDiscard();
        PeerCounters before = accepter.Reactor.CountersOf(accepterPeer);

        // Act — the first piece of the same, already-delivered group arrives again (a retransmission
        // the sender should never have needed to make, but nothing on the wire prevents it).
        byte[] resendDatagram =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 0, fragmentIndex: 0, fragmentCount: 2, payload: [0xAA]),
        ];
        SendAndPumpUntilReceived(accepter, client, clientLink, resendDatagram);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert — nothing further is delivered, and MessagesReceived does not rise again.
        delivered.Should().BeEmpty("the group already delivered once and its slot was released, not left ready to fire again");
        PeerCounters after = accepter.Reactor.CountersOf(accepterPeer);
        after.MessagesReceived.Should().Be(before.MessagesReceived);
    }

    [Fact]
    public void Pump_ACompletedFragmentSetFillingTheWholeWindow_ReleasesItsSlotsSoFurtherMessagesStillFitTheWindowBound()
    {
        // Arrange — a window sized to exactly the group's own Fragment count, so completing the group
        // is the only way any further sequence could ever be admitted: if its slots were not released,
        // nothing past it could ever be received again. MaxMessageSize is 3600 rather than the original
        // 1300 so that ReceiveWindowFloor (issue #282's own cap on a declared fragmentCount) is exactly
        // windowCapacity too — ceil(3600 / 1200) = 3 — since a declared count of 3 over a floor of 2
        // would now close the Peer before the group could ever complete.
        const int windowCapacity = 3;
        using DatagramSessionReactorHarness accepter = new(receiveWindowFragments: windowCapacity, maxMessageSize: 3600);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] completingDatagram =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 0, fragmentIndex: 0, fragmentCount: windowCapacity, payload: [0x01]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 1, fragmentIndex: 1, fragmentCount: windowCapacity, payload: [0x02]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 2, fragmentIndex: 2, fragmentCount: windowCapacity, payload: [0x03]),
        ];
        SendAndPumpUntilReceived(accepter, client, clientLink, completingDatagram);
        accepter.DrainPayloads().Should().ContainSingle().Which.Should().Equal(new byte[] { 0x01, 0x02, 0x03 });

        // Act — three more ordinary records, at the sequences right behind the group the window could
        // only reach if the group's own slots (and the base they were blocking) were actually released.
        byte[] furtherDatagram =
        [
            .. BuildHeader(sessionId),
            .. BuildRecord(ModeReliableUnordered, sequence: 3, payload: [0x11]),
            .. BuildRecord(ModeReliableUnordered, sequence: 4, payload: [0x12]),
            .. BuildRecord(ModeReliableUnordered, sequence: 5, payload: [0x13]),
        ];
        SendAndPumpUntilReceived(accepter, client, clientLink, furtherDatagram);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert
        delivered.Should().BeEquivalentTo(
            new[] { new byte[] { 0x11 }, new byte[] { 0x12 }, new byte[] { 0x13 } },
            "the completed group's slots released, so the window kept admitting up to its bound afterwards");
    }

    [Fact]
    public void Pump_AFragmentSetThatCompletesWhenTheFrameArenaHasNoRoom_IsRetainedAndDeliveredOnALaterPump()
    {
        // Arrange — a frame arena whose per-Peer fair share (ArenaSize / Capacity) fits the ordinary
        // record ahead of the Fragment set but not both together: the group's own reservation, as this
        // Peer's second reservation this Frame, is refused past its share. The receive window itself is
        // sized generously (well above the two-piece group), so only the frame arena is under pressure.
        // MaxMessageSize is 2400 rather than the original 64 so that ReceiveWindowFloor (issue #282) is
        // at least 2 over the default 1200-byte FragmentSize — the group below declares a fragmentCount
        // of 2, which a floor of 1 would now refuse with a Peer close before arena pressure ever mattered.
        // ArenaSize must rise alongside it (an arena cannot be smaller than one maximum Message), but the
        // arena-pressure mechanism itself — a share-holder's first reservation exactly consumes its own
        // fair share (ArenaSize / Capacity), so any second reservation that Frame is refused past it — is
        // scale-invariant: it depends only on the crowding payload equalling the share, computed fresh
        // below, never on the share's absolute size.
        const int maxMessageSize = 2400;
        const int arenaSize = 2400;
        const int capacity = 8;
        using DatagramSessionReactorHarness accepter = new(
            capacity: capacity, maxMessageSize: maxMessageSize, arenaSize: arenaSize, receiveWindowFragments: 8);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);
        PeerCounters before = accepter.Reactor.CountersOf(accepterPeer);

        // ArenaSize / Capacity: this Peer's fair share. The crowding record uses all of it as this
        // Peer's first (escaping) reservation, leaving no room for the group's own, second reservation.
        byte[] crowdingPayload = new byte[arenaSize / capacity];
        byte[] datagram =
        [
            .. BuildHeader(sessionId),
            .. BuildRecord(ModeReliableUnordered, sequence: 0, payload: crowdingPayload),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 1, fragmentIndex: 0, fragmentCount: 2, payload: [0xAA]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 2, fragmentIndex: 1, fragmentCount: 2, payload: [0xBB]),
        ];

        // Act — the Frame that completes the set: the ordinary record delivers, but the group's own
        // reservation is refused for want of arena room, so nothing is delivered for it and nothing is
        // counted as received.
        SendAndPumpUntilReceived(accepter, client, clientLink, datagram);
        List<byte[]> firstDrain = accepter.DrainPayloads();
        firstDrain.Should().ContainSingle().Which.Should().Equal(crowdingPayload);

        PeerCounters afterFirstPump = accepter.Reactor.CountersOf(accepterPeer);
        afterFirstPump.MessagesReceived.Should().Be(
            before.MessagesReceived + 1, "only the ordinary record counted as received; the group's reservation failed");

        // Act — a later Pump, with nothing further sent: the next Frame's wiped arena has room again,
        // so the retry sweep delivers the group that was held rather than lost.
        accepter.Reactor.Pump();
        List<byte[]> secondDrain = accepter.DrainPayloads();

        // Assert
        secondDrain.Should().ContainSingle("the retained group is now delivered on the retry sweep").Which.Should().Equal(new byte[] { 0xAA, 0xBB });
        PeerCounters afterRetry = accepter.Reactor.CountersOf(accepterPeer);
        afterRetry.MessagesReceived.Should().Be(afterFirstPump.MessagesReceived + 1, "the retained group is now delivered and counted exactly once");
    }

    [Fact]
    public void Pump_AMaxMessageSizeFragmentSet_DeliversWholeToAPeerWhoseReadShareIsSmaller()
    {
        // Arrange — four Peers share the frame arena (ADR 0012/0014), so this Peer's fair share is a
        // quarter of the arena — smaller than the MaxMessageSize Message about to complete. Only the
        // one-whole-Message escape (a share-holder's first reservation in a Frame is bounded by the
        // arena's total free space, not its own share) makes this Message reachable at all.
        const int maxMessageSize = 4096;
        const int fragmentSize = 1024;
        const int fragmentCount = maxMessageSize / fragmentSize;
        using DatagramSessionReactorHarness accepter = new(
            capacity: 4, maxMessageSize: maxMessageSize, fragmentSize: fragmentSize, arenaSize: maxMessageSize,
            receiveWindowFragments: fragmentCount);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] expected = Enumerable.Range(0, maxMessageSize).Select(i => (byte)i).ToArray();
        List<byte> datagram = [.. BuildHeader(sessionId)];
        for (int index = 0; index < fragmentCount; index++)
        {
            byte[] piece = expected.AsSpan(index * fragmentSize, fragmentSize).ToArray();
            datagram.AddRange(BuildFragmentRecord(
                ModeReliableUnordered, sequence: (uint)index, fragmentIndex: (ushort)index, fragmentCount: (ushort)fragmentCount, payload: piece));
        }

        // Act
        SendAndPumpUntilReceived(accepter, client, clientLink, [.. datagram]);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert — the whole MaxMessageSize Message arrives in one piece despite exceeding this Peer's
        // quarter-share of the frame arena.
        delivered.Should().ContainSingle().Which.Should().Equal(expected);
    }

    private const byte ModeReliableOrderedProbe = 0b0000_0011;

    [Fact]
    public void Pump_ASecondOrderedGroupCompletingWhileAnotherIsStillDeferred_DeliversBothMessages()
    {
        // Arrange — two ReliableOrdered groups in the SAME Space. The later group (sequences 2 and 3)
        // completes first, so it must wait its turn behind the hole at sequence 0. The earlier group
        // (sequences 0 and 1) then completes and releases that hole.
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);

        byte[] laterGroup =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeReliableOrderedProbe, sequence: 2, fragmentIndex: 0, fragmentCount: 2, payload: [0xC1]),
            .. BuildFragmentRecord(ModeReliableOrderedProbe, sequence: 3, fragmentIndex: 1, fragmentCount: 2, payload: [0xC2]),
        ];
        byte[] earlierGroup =
        [
            .. BuildHeader(sessionId),
            .. BuildFragmentRecord(ModeReliableOrderedProbe, sequence: 0, fragmentIndex: 0, fragmentCount: 2, payload: [0xA1]),
            .. BuildFragmentRecord(ModeReliableOrderedProbe, sequence: 1, fragmentIndex: 1, fragmentCount: 2, payload: [0xA2]),
        ];

        // Act
        SendAndPumpUntilReceived(accepter, client, clientLink, laterGroup);
        SendAndPumpUntilReceived(accepter, client, clientLink, earlierGroup);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert — both Messages reach the consumer, in send order.
        delivered.Should().HaveCount(2);
        delivered[0].Should().Equal(new byte[] { 0xA1, 0xA2 });
        delivered[1].Should().Equal(new byte[] { 0xC1, 0xC2 });
    }
    [Fact]
    public void Pump_TwoUnorderedGroupsCompletingWhileTheArenaIsFull_AreBothRetainedAndBothDelivered()
    {
        // Arrange — the same arena pressure as the single-group retry test, but with two groups
        // completing in the one Frame. Both reservations are refused, so both must be held: a single
        // waiting slot per Space would let the second overwrite the first and lose it outright.
        // MaxMessageSize is 2400 rather than the original 64 for the same reason as the single-group
        // retry test above: issue #282's ReceiveWindowFloor must reach the fragmentCount of 2 each group
        // below declares, or the Peer closes before either group can complete. ArenaSize rises alongside
        // it for the same reason too — see that test's comment for why the arena-pressure math is
        // unaffected by the scale-up.
        const int maxMessageSize = 2400;
        const int arenaSize = 2400;
        const int capacity = 8;
        using DatagramSessionReactorHarness accepter = new(
            capacity: capacity, maxMessageSize: maxMessageSize, arenaSize: arenaSize, receiveWindowFragments: 8);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);
        PeerCounters before = accepter.Reactor.CountersOf(accepterPeer);

        byte[] crowdingPayload = new byte[arenaSize / capacity];
        byte[] datagram =
        [
            .. BuildHeader(sessionId),
            .. BuildRecord(ModeReliableUnordered, sequence: 0, payload: crowdingPayload),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 1, fragmentIndex: 0, fragmentCount: 2, payload: [0xAA]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 2, fragmentIndex: 1, fragmentCount: 2, payload: [0xBB]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 3, fragmentIndex: 0, fragmentCount: 2, payload: [0xCC]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 4, fragmentIndex: 1, fragmentCount: 2, payload: [0xDD]),
        ];

        // Act — only the ordinary record fits this Frame; both groups are held.
        SendAndPumpUntilReceived(accepter, client, clientLink, datagram);
        accepter.DrainPayloads().Should().ContainSingle().Which.Should().Equal(crowdingPayload);

        // Act — later Pumps, with nothing further sent, retry what was held.
        List<byte[]> retried = [];
        for (int pump = 0; pump < 4; pump++)
        {
            accepter.Reactor.Pump();
            retried.AddRange(accepter.DrainPayloads());
        }

        // Assert — neither group was lost, and each was delivered exactly once.
        retried.Should().HaveCount(2);
        retried.Should().ContainSingle(payload => payload.SequenceEqual(new byte[] { 0xAA, 0xBB }));
        retried.Should().ContainSingle(payload => payload.SequenceEqual(new byte[] { 0xCC, 0xDD }));
        PeerCounters after = accepter.Reactor.CountersOf(accepterPeer);
        after.MessagesReceived.Should().Be(before.MessagesReceived + 3, "the crowding record plus both retained groups");
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
    /// <see cref="RawReactor"/> — and returns the wire session id <paramref name="accepter"/> assigned
    /// this Peer, mirroring <see cref="DatagramSessionFragmentHoldingTests"/>'s helper of the same name.
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
        // The remaining ResumeSecretLength bytes are the dialler's resume secret; an all-zero one is
        // fine, since nothing in this ticket's path ever reads it.
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
        const byte fragmentBit = 0b0000_0100;
        byte[] record = new byte[FragmentRecordSize + payload.Length];
        record[0] = (byte)(modeBits | fragmentBit);
        BinaryPrimitives.WriteUInt32BigEndian(record.AsSpan(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(5, 2), (ushort)payload.Length);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(7, 2), fragmentIndex);
        BinaryPrimitives.WriteUInt16BigEndian(record.AsSpan(9, 2), fragmentCount);
        payload.CopyTo(record.AsSpan(FragmentRecordSize));
        return record;
    }
}
