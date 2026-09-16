using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #255: the codec's refusal of impossible Fragment metadata (#253); issue #256 extends this
/// file with the claimed-set forgeries a Fragment can pass #253 individually and still commit â€” a
/// group claiming one index twice (a gap in the indices present is honest traffic since #274) and two
/// groups whose ranges overlap. Both are proved against a live Session
/// rather than in isolation â€” a real attacker sits at a hostile peer driving an established Session,
/// not at a unit test calling the codec directly. Every datagram here is built and sent by hand over a
/// bare <see cref="RawReactor"/> â€” the same instrument <see cref="HostilePeer"/> is built from, and the
/// one <see cref="DatagramSessionDataDatagramParseTests"/> and
/// <see cref="DatagramSessionUnreliableDuplicateTests"/> already use for a live Session's data path,
/// since establishing a Session and then addressing it needs the bidirectional handshake exchange a
/// bare Reactor gives a test, not the fire-and-forget <see cref="HostilePeer.SendDatagram"/>.
/// </summary>
/// <remarks>
/// Each malformed datagram carries a well-formed record at sequence 0 <em>ahead of</em> the malformed
/// Fragment tail, in <see cref="ReactorSockets.Session.PacketRecordMode.ReliableOrdered"/> â€” a Space
/// whose window never slides, so a record it never classifies leaves <c>Base</c> at exactly zero. The
/// whole datagram discarding is proved two ways: directly, by the drain staying empty (no
/// <see cref="SessionEventKind.MessageReceived"/> for either record, so the leading well-formed one was
/// never delivered either); and by the window itself, sending a second, genuinely well-formed record at
/// the same sequence 0 afterwards and confirming it is admitted rather than refused as a duplicate â€”
/// which only happens if the first record's sequence was never recorded.
/// </remarks>
public class DatagramSessionHostileFragmentDiscardTests
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

    private const int DatagramHeaderSize = 20;
    private const int UnfragmentedRecordSize = 7;
    private const int FragmentRecordSize = 11;

    private const byte ModeUnreliable = 0b0000_0000;
    private const byte ModeUnreliableSequenced = 0b0000_0001;
    private const byte ModeReliableUnordered = 0b0000_0010;
    private const byte ModeReliableOrdered = 0b0000_0011;
    private const byte FragmentBit = 0b0000_0100;

    // 0b0000_1000 is now DatagramSessionProtocol.ControlBit (issue #339), not a spare bit â€” this test
    // needs a bit that is still genuinely spare to prove the general "unknown bit discards" rule, so it
    // uses the next one up instead.
    private const byte SpareBit = 0b0001_0000;

    [Fact]
    public void Pump_HostileFragmentWithIndexAtItsCount_DiscardsWholeDatagramAndLeavesTheWindowUntouched()
    {
        AssertMalformedFragmentDiscardsWholeDatagramAndLeavesWindowUntouched(
            fragmentIndex: 2, fragmentCount: 2);
    }

    [Fact]
    public void Pump_HostileFragmentWithIndexBeyondItsCount_DiscardsWholeDatagramAndLeavesTheWindowUntouched()
    {
        AssertMalformedFragmentDiscardsWholeDatagramAndLeavesWindowUntouched(
            fragmentIndex: 5, fragmentCount: 2);
    }

    [Fact]
    public void Pump_HostileFragmentWithCountOfZero_DiscardsWholeDatagramAndLeavesTheWindowUntouched()
    {
        AssertMalformedFragmentDiscardsWholeDatagramAndLeavesWindowUntouched(
            fragmentIndex: 0, fragmentCount: 0);
    }

    [Fact]
    public void Pump_HostileFragmentWithCountOfOne_DiscardsWholeDatagramAndLeavesTheWindowUntouched()
    {
        AssertMalformedFragmentDiscardsWholeDatagramAndLeavesWindowUntouched(
            fragmentIndex: 0, fragmentCount: 1);
    }

    [Fact]
    public void Pump_HostileFragmentGroupWithARepeatedIndex_DiscardsWholeDatagramAndLeavesTheWindowUntouched()
    {
        // Arrange â€” issue #256, narrowed by #274 to what a claimed set actually rules out. A gap in the
        // indices present is honest traffic once retransmission coalesces (see
        // DatagramSessionFragmentDeliveryTests), but one index claimed twice is not: both Fragments here
        // are individually legal (#253) and both resolve to the same group (firstSeq = 1), and both call
        // themselves index 0 of it, so the piece that index names has two different contents. A sender
        // that cuts each index once can never produce it.
        byte[] forgedTail =
        [
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 1, fragmentIndex: 0, fragmentCount: 3, payload: [0xB0]),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 1, fragmentIndex: 0, fragmentCount: 3, payload: [0xB2]),
        ];

        AssertForgedFragmentTailDiscardsWholeDatagramAndLeavesTheWindowUntouched(forgedTail);
    }

    [Fact]
    public void Pump_HostileFragmentGroupsClaimingOverlappingRanges_DiscardsWholeDatagramAndLeavesTheWindowUntouched()
    {
        // Arrange â€” issue #256: two Fragments both name sequence 11, one as index 1 of a group starting
        // at 10 and the other as index 0 of a group starting at 11 â€” the same wire sequence claimed by
        // two different groups, exactly the ambiguity contiguity exists to rule out.
        byte[] forgedTail =
        [
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 11, fragmentIndex: 1, fragmentCount: 3, payload: [0xC0]),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 11, fragmentIndex: 0, fragmentCount: 2, payload: [0xC1]),
        ];

        AssertForgedFragmentTailDiscardsWholeDatagramAndLeavesTheWindowUntouched(forgedTail);
    }

    [Fact]
    public void Pump_HostileFragmentGroupsClaimingOverlappingDeclaredRangesWithNoObservedCollision_DiscardsWholeDatagramAndLeavesTheWindowUntouched()
    {
        // Arrange â€” issue #256: the overlap check must compare DECLARED ranges, not merely the
        // sequences actually observed. Group firstSeq=100 declares 5 Fragments (100..104) but only
        // two of them (100, 101) are present in this datagram; group firstSeq=103 declares 2
        // Fragments (103..104), both present. No single wire sequence is claimed twice by an
        // Fragment actually seen â€” 101 and 103 never collide â€” but sequence 104 is claimed by
        // BOTH groups' declared ranges: it would be index 4 of the first group or index 1 of the
        // second, depending on which Fragment happens to arrive there later. An overlap check built
        // only on observed spans (100-101 vs 103-103) would wrongly call these disjoint and admit
        // the forgery.
        byte[] forgedTail =
        [
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 100, fragmentIndex: 0, fragmentCount: 5, payload: [0xE0]),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 101, fragmentIndex: 1, fragmentCount: 5, payload: [0xE1]),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 103, fragmentIndex: 0, fragmentCount: 2, payload: [0xE3]),
        ];

        AssertForgedFragmentTailDiscardsWholeDatagramAndLeavesTheWindowUntouched(forgedTail);
    }

    [Fact]
    public void Pump_HostileFragmentGroupDisagreeingOnItsCountAcrossTheSequenceWrap_DiscardsWholeDatagramAndLeavesTheWindowUntouched()
    {
        // Arrange â€” proves FragmentGroup.FirstSequenceOf's group identity is correct at the u32 wrap
        // (carried over from #254), through the only seam a test may use: a set that reads as a forgery
        // ONLY if the wrap is handled correctly is refused. Sequence uint.MaxValue - 1 at index 0 and
        // sequence 0 at index 2 both name firstSeq = uint.MaxValue - 1 under wrap-aware unsigned
        // subtraction, so they are one group â€” and that group's members disagree on the count they
        // declare, 2 against 3, which leaves the set's own shape ambiguous. A naive, non-wrap-aware
        // subtraction would compute two unrelated first-sequences instead, and each lone Fragment would
        // trivially "pass" as its own one-member group, its declared range (uint.MaxValue - 1 ..
        // uint.MaxValue) nowhere near the other's â€” so this datagram is only ever discarded if the wrap
        // is handled correctly. The gap between indices 0 and 2 is not what refuses it: since #274 a
        // gapped subset is honest traffic.
        byte[] forgedTail =
        [
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: uint.MaxValue - 1, fragmentIndex: 0, fragmentCount: 2, payload: [0xD0]),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 0, fragmentIndex: 2, fragmentCount: 3, payload: [0xD2]),
        ];

        AssertForgedFragmentTailDiscardsWholeDatagramAndLeavesTheWindowUntouched(forgedTail);
    }

    [Fact]
    public void Pump_HostileFragmentGroupsClaimingOverlappingDeclaredRangesInTheSameModeAlongsideALegitimateOtherModeGroup_DiscardsWholeDatagramAndLeavesTheWindowUntouched()
    {
        // Arrange â€” issue #47: fixing the false-positive (two different-mode groups must never be
        // compared) must not weaken the genuine check â€” two groups in the SAME mode whose declared
        // ranges overlap (firstSeq 10 declaring count 5, range 10..14; firstSeq 13 declaring count 2,
        // range 13..14) are still a real forgery, whether or not a legitimate, unrelated group in a
        // different mode also rides in the same datagram.
        byte[] forgedTail =
        [
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 10, fragmentIndex: 0, fragmentCount: 5, payload: [0xF0]),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 11, fragmentIndex: 1, fragmentCount: 5, payload: [0xF1]),
            .. BuildFragmentRecord(ModeReliableOrdered, sequence: 13, fragmentIndex: 0, fragmentCount: 2, payload: [0xF3]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 10, fragmentIndex: 0, fragmentCount: 2, payload: [0xF4]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 11, fragmentIndex: 1, fragmentCount: 2, payload: [0xF5]),
        ];

        AssertForgedFragmentTailDiscardsWholeDatagramAndLeavesTheWindowUntouched(forgedTail);
    }

    [Fact]
    public void Pump_HostileFragmentGroupsWhoseOverlapIsHiddenByADecoyGroupsAnchor_DiscardsWholeDatagramAndLeavesTheWindowUntouched()
    {
        // Arrange â€” issue #47: the overlap sweep orders a mode's groups relative to an anchor, and an
        // anchor taken from the numerically smallest sequence is one a peer can move. Groups 0x7FFFFFFF
        // and 0x80000000 genuinely overlap: the first declares 0x7FFFFFFF..0x80000000, the second
        // declares 0x80000000..0x80000001, so sequence 0x80000000 is claimed twice. The group at 0 is a
        // decoy that claims nothing either of them claims â€” its only job is to drag the anchor to 0,
        // which pushes the two overlapping groups to opposite ends of the sorted slice so a single
        // forward sweep never compares them. The anchor must be derived from the sequences themselves
        // rather than taken from whichever happens to be numerically smallest.
        byte[] forgedTail =
        [
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 0x0000_0000, fragmentIndex: 0, fragmentCount: 2, payload: [0xC0]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 0x7FFF_FFFF, fragmentIndex: 0, fragmentCount: 2, payload: [0xC1]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 0x8000_0000, fragmentIndex: 0, fragmentCount: 2, payload: [0xC2]),
        ];

        AssertForgedFragmentTailDiscardsWholeDatagramAndLeavesTheWindowUntouched(forgedTail);
    }

    [Fact]
    public void Pump_HostileFragmentGroupsSpanningMoreThanHalfTheSequenceSpace_DiscardsWholeDatagramAndLeavesTheWindowUntouched()
    {
        // Arrange â€” issue #47: "forward around the circle" is only well defined for sequences spanning
        // less than half the u32 space, the same boundary SequenceNumber names. These three groups span
        // more than that however the circle is cut, so no ordering of them is meaningful and none can be
        // trusted to place them. An honest sender cannot produce this â€” a Peer's receive window is
        // smaller than 2^31 by many orders of magnitude â€” so the datagram is refused rather than swept
        // in whatever order the peer's choice of sequences happens to produce.
        byte[] forgedTail =
        [
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 0x0000_0000, fragmentIndex: 0, fragmentCount: 2, payload: [0xA0]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 0x6000_0000, fragmentIndex: 0, fragmentCount: 2, payload: [0xA1]),
            .. BuildFragmentRecord(ModeReliableUnordered, sequence: 0xC000_0000, fragmentIndex: 0, fragmentCount: 2, payload: [0xA2]),
        ];

        AssertForgedFragmentTailDiscardsWholeDatagramAndLeavesTheWindowUntouched(forgedTail);
    }

    [Theory]
    [InlineData(ModeUnreliable)]
    [InlineData(ModeUnreliableSequenced)]
    public void Pump_HostileFragmentInAnUnreliableMode_DiscardsWholeDatagramAndReportsTheRefusal(byte modeBits)
    {
        // Arrange â€” issue #273: fragmentation is reliable-only (ADR 0004), and the send path already
        // refuses to produce this. Nothing but a hostile or broken peer can put a Fragment on the wire
        // in an unreliable mode, so the read fails outright rather than admitting a record this profile
        // has no reassembly path for. The index and count here are entirely well-formed (#253) and the
        // set they claim is contiguous (#256): the mode alone is what refuses it.
        byte[] forgedTail = BuildFragmentRecord(
            modeBits, sequence: 1, fragmentIndex: 0, fragmentCount: 2, payload: [0x9A]);

        AssertForgedFragmentTailDiscardsWholeDatagramAndLeavesTheWindowUntouched(
            forgedTail, alsoReported: DiagnosticId.UnreliableFragment);
    }

    [Fact]
    public void Pump_HostileFragmentInAnUnreliableModeCarryingASpareBit_ReportsOnlyTheMalformedDiscard()
    {
        // Arrange â€” issue #273: a spare bit set is refused before the flags byte is read for anything
        // else, so a record breaking both rules reports the one that actually fired rather than also
        // claiming the unreliable-Fragment refusal.
        byte[] forgedTail = BuildFragmentRecord(
            ModeUnreliable, sequence: 1, fragmentIndex: 0, fragmentCount: 2, payload: [0x9B]);
        forgedTail[0] |= SpareBit;

        AssertForgedFragmentTailDiscardsWholeDatagramAndLeavesTheWindowUntouched(forgedTail);
    }

    /// <summary>
    /// The index/count-refusal cases above build their own single-record forged tail (#253) and drive
    /// it through the shared body every case in this file uses.
    /// </summary>
    private static void AssertMalformedFragmentDiscardsWholeDatagramAndLeavesWindowUntouched(
        ushort fragmentIndex, ushort fragmentCount)
    {
        byte[] malformedFragmentRecord = BuildFragmentRecord(
            ModeReliableOrdered, sequence: 1, fragmentIndex, fragmentCount, payload: [0xBB]);

        AssertForgedFragmentTailDiscardsWholeDatagramAndLeavesTheWindowUntouched(malformedFragmentRecord);
    }

    /// <summary>
    /// The shared body every case in this file drives: a hostile datagram carrying one well-formed
    /// record at sequence 0 ahead of <paramref name="forgedFragmentTail"/> â€” one or more Fragment
    /// records that are each individually legal (#253) but forge the set the whole tail claims to be
    /// (#256), or a single record #253 refuses outright â€” then a genuinely well-formed datagram that
    /// resends sequence 0 alone, proving the window never saw the first one.
    /// </summary>
    /// <param name="forgedFragmentTail">The forged record or records that follow the well-formed one.</param>
    /// <param name="alsoReported">
    /// A second Diagnostic this forgery raises alongside the malformed discard, or
    /// <see langword="null"/> when the discard is all that is reported. Every case here raises
    /// <see cref="DiagnosticId.DatagramMalformed"/>; only the unreliable-Fragment refusal (issue #273)
    /// names which rule the record broke as well.
    /// </param>
    private static void AssertForgedFragmentTailDiscardsWholeDatagramAndLeavesTheWindowUntouched(
        byte[] forgedFragmentTail,
        DiagnosticId? alsoReported = null)
    {
        // Arrange
        using DatagramSessionReactorHarness accepter = new();
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(accepter, client, clientLink);
        PeerId accepterPeer = FindAccepterPeer(accepter);

        PeerCounters before = accepter.Reactor.CountersOf(accepterPeer);

        byte[] wellFormedRecord = BuildRecord(ModeReliableOrdered, sequence: 0, payload: [0xAA]);
        byte[] hostileDatagram =
            [.. BuildHeader(sessionId), .. wellFormedRecord, .. forgedFragmentTail];

        // Act â€” the hostile datagram, sent from a bare RawReactor exactly as HostilePeer is built to
        // send from.
        Action sendHostileDatagram = () =>
        {
            client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), hostileDatagram);
            accepter.PumpUntil(() => accepter.Reactor.Counters.MalformedSessionDiscards > 0);
        };

        // Assert â€” nothing throws, the whole datagram is discarded (both the malformed tail and the
        // well-formed record ahead of it), the malformed-discard counter rises by one, and the Session
        // produces no event.
        sendHostileDatagram.Should().NotThrow();

        List<byte[]> deliveredFromHostileDatagram = accepter.DrainPayloads();
        deliveredFromHostileDatagram.Should().BeEmpty(
            "a datagram is a container, not a Message: a forged tail must discard the well-formed " +
            "record that parsed ahead of it too");
        accepter.Reactor.Counters.MalformedSessionDiscards.Should().Be(1);
        accepter.Sink.Received.Should().ContainSingle(r => r.Id == DiagnosticId.DatagramMalformed);
        if (alsoReported is DiagnosticId expectedId)
        {
            accepter.Sink.Received.Should().ContainSingle(r => r.Id == expectedId)
                .Which.Level.Should().Be(DiagnosticLevel.Warn);
        }
        else
        {
            accepter.Sink.Received.Should().NotContain(r => r.Id == DiagnosticId.UnreliableFragment);
        }

        PeerCounters afterDiscard = accepter.Reactor.CountersOf(accepterPeer);
        afterDiscard.Should().Be(before, "a discarded datagram never reaches record classification at all");

        // Act â€” a following, genuinely well-formed record at the very sequence the hostile datagram
        // tried to smuggle in.
        byte[] wellFormedFollowUp = BuildRecord(ModeReliableOrdered, sequence: 0, payload: [0xCC]);
        byte[] followUpDatagram = [.. BuildHeader(sessionId), .. wellFormedFollowUp];
        ulong receivedBefore = accepter.Reactor.Counters.DatagramsReceived;

        client.SendTo(clientLink, Source.FromIPEndPoint(accepter.Endpoint), followUpDatagram);
        accepter.PumpUntil(() => accepter.Reactor.Counters.DatagramsReceived > receivedBefore);
        List<byte[]> delivered = accepter.DrainPayloads();

        // Assert â€” sequence 0 admits now, exactly as it would have had the hostile datagram never
        // arrived. Had the hostile datagram's leading record secretly reached the window, this would be
        // a duplicate and refused instead.
        delivered.Should().ContainSingle(
                "sequence 0 admits at the sequence it would have, undisturbed by the discarded datagram")
            .Which.Should().Equal(new byte[] { 0xCC });

        PeerCounters afterFollowUp = accepter.Reactor.CountersOf(accepterPeer);
        afterFollowUp.MessagesReceived.Should().Be(before.MessagesReceived + 1);
        afterFollowUp.OutOfWindowPackets.Should().Be(before.OutOfWindowPackets, "the window was never touched by the discarded datagram");
        afterFollowUp.DuplicatePackets.Should().Be(before.DuplicatePackets, "the window was never touched by the discarded datagram");

        // The Session stays usable afterwards: CountersOf still resolves this Peer without throwing.
        Action stillUsable = () => accepter.Reactor.CountersOf(accepterPeer);
        stillUsable.Should().NotThrow();
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> â€” a bare
    /// <see cref="RawReactor"/>, exactly what <see cref="HostilePeer"/> is built from â€” and returns the
    /// wire session id <paramref name="accepter"/> assigned this Peer, mirroring
    /// <see cref="DatagramSessionDataDatagramParseTests"/>'s helper of the same name.
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
        byte[] body = new byte[4 + 8 + 8 + CookieLength + 4 + 4 + 8 + ResumeSecretLength];
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
