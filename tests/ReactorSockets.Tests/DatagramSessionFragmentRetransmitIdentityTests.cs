using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The fragment-shaped sibling of <see cref="DatagramSessionRetransmitIdentityTests"/>: a Fragment is cut and framed once, at <c>Send</c>, and lives in retention as finished
/// bytes (ADR 0004), so a retransmit copies those bytes out rather than rebuilding them. This is proved
/// on the wire — a <see cref="WireTap"/> captures a Fragment record's original transmission and its
/// later retransmission, and every field of the two captures is asserted equal, both individually and as
/// one whole byte range.
/// </summary>
/// <remarks>
/// <para>
/// <b>Tap placement.</b> The tap sits <em>ahead of</em> the <see cref="LossyProxy"/> — dialler -&gt; tap
/// -&gt; proxy -&gt; accepter, the same chain <see cref="DatagramSessionRetransmitIdentityTests"/> uses
/// — so it captures every Datagram the dialler actually sends, including the one about to be dropped
/// downstream. Issue #249's repair-cost test chains the other way (proxy ahead of the tap) because it
/// only needs to prove what genuinely arrived; this test needs the original's bytes even though the
/// proxy never lets them reach the accepter, so the tap has to see the wire before the proxy gets a
/// chance to drop it. The same tap then keeps capturing every later flush, so the retransmission is
/// caught too, with no repositioning required between the two.
/// </para>
/// <para>
/// <b>Isolating one Fragment to one Datagram.</b> Unlike <see cref="DatagramSessionFragmentRepairCostTests"/>,
/// whose fragment size only keeps two <em>full</em> Fragment records from sharing a Datagram, this test
/// also needs a full record and the short last-Fragment remainder to never coalesce — otherwise dropping
/// the Datagram that carries the last Fragment would drop its predecessor too, and the retransmit count
/// would no longer isolate to one Fragment. <see cref="FragmentSize"/> is set high enough that one full
/// Fragment record already leaves less headroom in a Datagram than any second record — even a 1-byte one
/// — could ever need, so every Fragment this test sends rides its own Datagram, full or short alike, and
/// <see cref="LossyProxy.DropNext"/> can target any one Fragment's transmission precisely.
/// </para>
/// </remarks>
public class DatagramSessionFragmentRetransmitIdentityTests
{
    private const int DatagramHeaderSize = 20;
    private const int UnfragmentedRecordSize = 7;
    private const int FragmentRecordSize = 11;
    private const byte FragmentBit = 0b0000_0100;
    private const byte ModeMask = 0b0000_0011;

    /// <summary>The largest Datagram this library ever writes or reads (matches <c>DatagramSessionReactor.MaxUdpDatagramSize</c>).</summary>
    private const int MaxUdpDatagramSize = 65_507;

    /// <summary>
    /// Bytes a full Fragment carries in this test: close enough to the 65 507-byte UDP payload ceiling
    /// that one full Fragment record's 11-byte header and payload, plus the 20-byte Datagram header,
    /// leave under 12 spare bytes — less than even the smallest possible second Fragment record (an
    /// 11-byte header plus a 1-byte payload) could fit into. No two Fragments this test sends, full or
    /// short, ever share a Datagram.
    /// </summary>
    private const int FragmentSize = MaxUdpDatagramSize - DatagramHeaderSize - FragmentRecordSize - 11;

    /// <summary>
    /// Fragment count small enough to keep the test lightweight while still giving a genuine middle
    /// position distinct from the first and last: index 0 (first, unused here), index 1 (middle), index 2
    /// (last, the short remainder).
    /// </summary>
    private const int FragmentCount = 3;

    /// <summary>
    /// What the last Fragment carries: far short of <see cref="FragmentSize"/>, so the last Fragment is
    /// a genuine partial remainder rather than a coincidental full one, which is what the last-Fragment
    /// case exists to prove.
    /// </summary>
    private const int LastFragmentRemainder = 1_000;

    /// <summary>An exact multiple of <see cref="FragmentSize"/> plus one short remainder, cutting into exactly <see cref="FragmentCount"/> Fragments.</summary>
    private const int MessageSize = (FragmentSize * (FragmentCount - 1)) + LastFragmentRemainder;

    /// <summary>
    /// Retention sized to hold every Fragment this one Send retains at once. The pool holds uniform
    /// <see cref="FragmentSize"/> slots regardless of a Fragment's actual payload length, so this must be
    /// <see cref="FragmentCount"/> full slots rather than <see cref="MessageSize"/>, whose short last
    /// Fragment would otherwise round a slot count down by one.
    /// </summary>
    private const int RetentionBytes = FragmentSize * FragmentCount;

    private const int MiddleFragmentIndex = FragmentCount / 2;
    private const int LastFragmentIndex = FragmentCount - 1;

    /// <summary>
    /// Comfortably past the RTO, whose ceiling is the harness's one-second Linger, so the deadline has
    /// certainly passed however the estimator has moved (matches <see cref="DatagramSessionRetransmitResendTests"/>
    /// and <see cref="DatagramSessionFragmentRepairCostTests"/>).
    /// </summary>
    private static readonly TimeSpan _pastTheRetransmitDeadline = TimeSpan.FromSeconds(3);

    /// <summary>The two positions tested: a middle Fragment, and the last, whose payload is short.</summary>
    public static TheoryData<int, string> FragmentDropPositions => new()
    {
        { MiddleFragmentIndex, "middle" },
        { LastFragmentIndex, "last" },
    };

    [Theory]
    [MemberData(nameof(FragmentDropPositions))]
    public void Pump_ARetransmittedFragment_IsByteIdenticalToItsDroppedOriginal(int dropIndex, string position)
    {
        // Arrange — tap ahead of proxy (dialler -> tap -> proxy -> accepter) so the tap captures every
        // Fragment the dialler sends, including the one the proxy is about to drop.
        using DatagramSessionReactorHarness dialler = new(
            capacity: 2, bindLoopback: false, maxMessageSize: MessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            capacity: 2, maxMessageSize: MessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        using WireTap tap = new(proxy.PublicEndPoint, dialler.TimeProvider);

        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        PeerId accepterPeer = ConnectThroughTapAndProxy(dialler, tap, proxy, accepter);

        byte[] message = new byte[MessageSize];
        for (int i = 0; i < message.Length; i++)
        {
            message[i] = (byte)i;
        }

        ulong retransmittedBefore = dialler.Reactor.CountersOf(diallerPeer).PacketsRetransmitted;

        // Act — Send the whole Message, then Pump one Fragment out per round, dropping the target
        // Fragment's own transmission between the proxy and the accepter. The tap, sitting ahead of the
        // proxy, still captures it before the drop.
        SendOutcome outcome = dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, message);
        outcome.Should().Be(SendOutcome.Sent, "retention was sized to hold every Fragment this Message cuts into");

        for (int round = 0; round < FragmentCount; round++)
        {
            if (round == dropIndex)
            {
                proxy.DropNext(RelayDirection.ClientToUpstream);
            }

            PumpRelayRound(dialler, tap, proxy, accepter);
        }

        // The accepter answers so its ack block — reflecting everything it admitted while the dropped
        // Fragment was still missing — travels back before the deadline passes, matching
        // DatagramSessionFragmentRepairCostTests. Without this reply, every Fragment the sender is still
        // tracking, not only the genuinely lost one, would eventually expire.
        accepter.Reactor.Send(accepterPeer, DeliveryMode.Unreliable, [0]).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        proxy.Pump();
        tap.Pump();
        dialler.Reactor.Pump();

        dialler.TimeProvider.Advance(_pastTheRetransmitDeadline);

        for (int round = 0; round < 8; round++)
        {
            PumpRelayRound(dialler, tap, proxy, accepter);
        }

        // Assert — exactly one resend happened, and the tap's captures show the target Fragment's
        // transmission exactly twice: its original send and its later retransmission.
        (dialler.Reactor.CountersOf(diallerPeer).PacketsRetransmitted - retransmittedBefore).Should().Be(
            1UL, $"repair costs exactly one Datagram — only the {position} Fragment (index {dropIndex}) was ever missing");

        List<DecodedFragmentRecord> occurrences = FindFragmentOccurrences(tap, (ushort)dropIndex, (ushort)FragmentCount);
        occurrences.Should().HaveCount(
            2, $"the {position} Fragment's record must appear exactly twice on the wire: its dropped original and its one retransmission");

        DecodedFragmentRecord original = occurrences[0];
        DecodedFragmentRecord resent = occurrences[1];

        int expectedLength = dropIndex == LastFragmentIndex ? LastFragmentRemainder : FragmentSize;
        original.Length.Should().Be(
            (ushort)expectedLength, $"the {position} Fragment carries {(dropIndex == LastFragmentIndex ? "the short remainder" : "a full fragment size")} of payload");

        // Field-by-field: every wire field a retransmit could plausibly rebuild differently must instead match exactly.
        resent.Flags.Should().Be(original.Flags, "the resent record's flags must match the original's");
        resent.Sequence.Should().Be(original.Sequence, "the resent record carries the same sequence number as the original — it is a copy, not a new Fragment");
        resent.Length.Should().Be(original.Length, "the resent record's declared length must match the original's");
        resent.FragmentIndex.Should().Be(original.FragmentIndex, "the resent record's fragment index must match the original's");
        resent.FragmentCount.Should().Be(original.FragmentCount, "the resent record's fragment count must match the original's");
        resent.Payload.Should().Equal(original.Payload, "the resent record's payload bytes must match the original's");

        // As a whole: the entire record — header and payload together — is byte-for-byte identical,
        // proving the retransmit copied finished bytes out of retention rather than reframing them.
        resent.RawBytes.Should().Equal(original.RawBytes, "the retransmitted record's bytes, taken as a whole, must be identical to the original's");
    }

    /// <summary>
    /// Pumps a dialler through a <see cref="WireTap"/> chained ahead of a <see cref="LossyProxy"/> (tap
    /// captures, then the proxy may fault) and an accepter until both ends have raised
    /// <see cref="SessionEventKind.PeerConnected"/> — the same composition
    /// <see cref="DatagramSessionRetransmitIdentityTests"/> drives, reproduced here since it is private
    /// to that class.
    /// </summary>
    private static PeerId ConnectThroughTapAndProxy(
        DatagramSessionReactorHarness dialler, WireTap tap, LossyProxy proxy, DatagramSessionReactorHarness accepter)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        PeerId? accepterPeer = null;
        bool diallerConnected = false;

        while (true)
        {
            PumpRelayRound(dialler, tap, proxy, accepter);

            foreach ((SessionEventKind kind, PeerId eventPeer, DisconnectReason _) in accepter.DrainToList())
            {
                if (kind == SessionEventKind.PeerConnected)
                {
                    accepterPeer = eventPeer;
                }
            }

            diallerConnected |= dialler.DrainToList().Any(e => e.Kind == SessionEventKind.PeerConnected);

            if (diallerConnected && accepterPeer is PeerId established)
            {
                return established;
            }

            if (deadline.Expired)
            {
                throw new InvalidOperationException("The tapped-and-proxied handshake did not complete within the Pump budget.");
            }

            PumpDeadline.Yield();
        }
    }

    /// <summary>
    /// One round through the chained instruments: the dialler, then the tap (captures and relays toward
    /// the proxy), then the proxy (applies any scheduled fault and relays toward the accepter), then the
    /// accepter, then the proxy and tap again so the accepter's reply travels back the same round.
    /// </summary>
    private static void PumpRelayRound(
        DatagramSessionReactorHarness dialler, WireTap tap, LossyProxy proxy, DatagramSessionReactorHarness accepter)
    {
        dialler.Reactor.Pump();
        tap.Pump();
        proxy.Pump();
        accepter.Reactor.Pump();
        proxy.Pump();
        tap.Pump();
    }

    /// <summary>
    /// Walks every Datagram the tap captured travelling <see cref="TapDirection.DiallerToAccepter"/>,
    /// skipping handshake Datagrams (the reserved control session id), and decodes each packet record by
    /// hand — the unfragmented 7-byte header or the 11-byte Fragment header, chosen by the flags byte's
    /// fragment bit, the same choice <c>PacketRecordCodec.TryRead</c> makes, reimplemented independently
    /// here (matching <see cref="DatagramSessionSendFragmentationTests"/>'s and
    /// <see cref="DatagramSessionFragmentRepairCostTests"/>'s own hand decoders) so a bug in the
    /// production reader could not hide itself from this test — returning only the records whose
    /// fragment index and count match <paramref name="fragmentIndex"/> and <paramref name="fragmentCount"/>,
    /// in the order they were captured, which is the original followed by any retransmission.
    /// </summary>
    private static List<DecodedFragmentRecord> FindFragmentOccurrences(WireTap tap, ushort fragmentIndex, ushort fragmentCount)
    {
        List<DecodedFragmentRecord> occurrences = [];
        foreach (byte[] datagram in tap.PayloadsInDirection(TapDirection.DiallerToAccepter))
        {
            if (datagram.Length < DatagramHeaderSize || BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(0, 4)) == 0)
            {
                continue;
            }

            int offset = DatagramHeaderSize;
            while (offset < datagram.Length)
            {
                byte flags = datagram[offset];
                uint sequence = BinaryPrimitives.ReadUInt32BigEndian(datagram.AsSpan(offset + 1, 4));
                ushort length = BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + 5, 2));
                bool isFragment = (flags & FragmentBit) != 0;
                int headerSize = isFragment ? FragmentRecordSize : UnfragmentedRecordSize;

                if (isFragment)
                {
                    ushort recordFragmentIndex = BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + 7, 2));
                    ushort recordFragmentCount = BinaryPrimitives.ReadUInt16BigEndian(datagram.AsSpan(offset + 9, 2));

                    if (recordFragmentIndex == fragmentIndex && recordFragmentCount == fragmentCount)
                    {
                        int payloadOffset = offset + headerSize;
                        byte[] payload = datagram.AsSpan(payloadOffset, length).ToArray();
                        byte[] rawBytes = datagram.AsSpan(offset, headerSize + length).ToArray();

                        occurrences.Add(new DecodedFragmentRecord(
                            flags, (DeliveryMode)(flags & ModeMask), sequence, length, recordFragmentIndex, recordFragmentCount, payload, rawBytes));
                    }
                }

                offset += headerSize + length;
            }
        }

        return occurrences;
    }

    /// <summary>One Fragment record decoded by hand off a captured live-Session Datagram, keeping both its individual fields and its whole raw byte range.</summary>
    private sealed record DecodedFragmentRecord(
        byte Flags,
        DeliveryMode Mode,
        uint Sequence,
        ushort Length,
        ushort FragmentIndex,
        ushort FragmentCount,
        byte[] Payload,
        byte[] RawBytes);
}
