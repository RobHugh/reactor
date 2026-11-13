using System.Buffers.Binary;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The test that justifies the whole fragmentation design (ADR 0004): each Fragment is an
/// ordinary reliable packet carrying its own sequence number, so the ack and retransmit machinery
/// repairs a lost Fragment by exactly the path that repairs a lost small Message — one Datagram, not
/// the whole Message. A 57-Fragment <see cref="DeliveryMode.ReliableOrdered"/> Message is Sent, exactly
/// one Fragment is dropped on the wire, and <see cref="PeerCounters.PacketsRetransmitted"/> is asserted
/// to read exactly one once the retransmit machinery has run. If that figure were ever the fragment
/// count instead, fragmentation would have silently become message-level retransmission.
/// </summary>
/// <remarks>
/// <para>
/// <b>Isolating one Fragment to one Datagram.</b> <see cref="LossyProxy"/> drops whole Datagrams, and
/// <see cref="DatagramSessionReactor"/> coalesces every Fragment that fits into one Pump's single
/// outbound Datagram (up to the 65 507-byte UDP payload ceiling), so an ordinary fragment size would
/// let several Fragments share one Datagram and a drop would take out more than one at a time. This
/// test instead configures a fragment size just over half that ceiling
/// (<see cref="FragmentSize"/>), so exactly one Fragment record fits per Datagram and each of the
/// dialler's 57 Pumps flushes exactly one Fragment as its own Datagram — <see cref="LossyProxy.DropNext"/>
/// can then target one Fragment precisely.
/// </para>
/// <para>
/// <b>What "completes" means here.</b> This test asserts completion at the wire, not as a
/// <see cref="SessionEventKind.MessageReceived"/> Event: delivery of a completed Fragment set is
/// <see cref="DatagramSessionFragmentDeliveryTests"/>' subject. A
/// <see cref="WireTap"/> chained behind the <see cref="LossyProxy"/> (dialler -&gt; proxy -&gt; tap -&gt;
/// accepter) captures only what the proxy actually let through, so it proves every one of the 57
/// Fragment sequences — including the one originally dropped, carried instead by its retransmission —
/// genuinely reached the accepter exactly once. The accepter is also made to Send a reply once the
/// transfer is done, and every reply Datagram carries both reliable Spaces' ack blocks unconditionally
/// (<c>DatagramAssembler.WriteHeader</c>), so that reply's mere existence is the far end acknowledging
/// what it received.
/// </para>
/// </remarks>
public class DatagramSessionFragmentRepairCostTests
{
    private const int DatagramHeaderSize = 20;
    private const int UnfragmentedRecordSize = 7;
    private const int FragmentRecordSize = 11;
    private const byte FragmentBit = 0b0000_0100;

    /// <summary>
    /// Bytes a Fragment carries in this test: just over half the 65 507-byte UDP payload ceiling, so
    /// two Fragment records (11-byte header apiece) plus the 20-byte Datagram header never both fit in
    /// one Datagram — only one ever does. Comfortably inside the [576, 65 476] range
    /// <see cref="DatagramSessionOptions.FragmentSize"/> accepts.
    /// </summary>
    private const int FragmentSize = 32_750;

    /// <summary>The Fragment count the repair-cost test protects — issue #249 names this figure explicitly.</summary>
    private const int FragmentCount = 57;

    /// <summary>An exact multiple of <see cref="FragmentSize"/>, so all 57 Fragments are full-sized and no remainder complicates sequencing.</summary>
    private const int MessageSize = FragmentSize * FragmentCount;

    /// <summary>Retention sized to hold every one of the 57 Fragments this one Send retains at once.</summary>
    private const int RetentionBytes = MessageSize;

    private const int MiddleFragmentIndex = FragmentCount / 2;

    /// <summary>
    /// Comfortably past the RTO, whose ceiling is the harness's one-second Linger, so the deadline has
    /// certainly passed however the estimator has moved (matches <see cref="DatagramSessionRetransmitResendTests"/>).
    /// </summary>
    private static readonly TimeSpan _pastTheRetransmitDeadline = TimeSpan.FromSeconds(3);

    /// <summary>
    /// How far the dialler's clock advances per relay round in the #542 test. At 30 ms the first
    /// Fragment's deadline, armed at the initial RTO before any round-trip sample, passes while Fragments
    /// are still queued ahead of its resend. At 10 ms it passes only after the queue has drained.
    /// </summary>
    private static readonly TimeSpan _roundInterval = TimeSpan.FromMilliseconds(30);

    /// <summary>The three positions issue #249 names explicitly: a theory over where in the Message the lost Fragment sits.</summary>
    public static TheoryData<int, string> FragmentDropPositions => new()
    {
        { 0, "first" },
        { MiddleFragmentIndex, "middle" },
        { FragmentCount - 1, "last" },
    };

    [Theory]
    [MemberData(nameof(FragmentDropPositions))]
    public void Send_A57FragmentMessageWithOneFragmentDropped_RepairsWithExactlyOneRetransmittedDatagram(int dropIndex, string position)
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(
            capacity: 2, bindLoopback: false, maxMessageSize: MessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            capacity: 2, maxMessageSize: MessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        using LossyProxy proxy = new(tap.PublicEndPoint, dialler.TimeProvider);

        PeerId diallerPeer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        PeerId accepterPeer = ConnectThroughProxyAndTap(dialler, proxy, tap, accepter);

        byte[] message = new byte[MessageSize];
        for (int i = 0; i < message.Length; i++)
        {
            message[i] = (byte)i;
        }

        ulong retransmittedBefore = dialler.Reactor.CountersOf(diallerPeer).PacketsRetransmitted;

        // Act — Send the whole Message, then Pump one Fragment out per round, dropping exactly one of
        // the 57 on the wire between the proxy and the tap.
        SendOutcome outcome = dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, message);
        outcome.Should().Be(SendOutcome.Sent, "retention was sized to hold every one of the 57 Fragments this Message cuts into");

        // The accepter acknowledges on its own one Pump after each Fragment arrives (ADR 0026). Those
        // acks are held back until the transfer is done, so the clock that never moves between rounds
        // cannot feed the estimator zero round-trip samples and collapse the RTO under the transfer.
        proxy.SetPayloadDropRule(RelayDirection.UpstreamToClient, _ => true);
        for (int round = 0; round < FragmentCount; round++)
        {
            if (round == dropIndex)
            {
                proxy.DropNext(RelayDirection.ClientToUpstream);
            }

            PumpRelayRound(dialler, proxy, tap, accepter);
        }

        // The accepter answers once the transfer is done, so its ack blocks — reflecting everything it
        // admitted while the dropped Fragment was still missing — travel back before the deadline
        // passes. Without this reply nothing the accepter received would ever be acknowledged, and every
        // Fragment the sender is still tracking would eventually expire alongside the one truly lost.
        proxy.SetPayloadDropRule(RelayDirection.UpstreamToClient, null);
        accepter.Reactor.Send(accepterPeer, DeliveryMode.Unreliable, [0]).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        tap.Pump();
        proxy.Pump();
        dialler.Reactor.Pump();

        dialler.TimeProvider.Advance(_pastTheRetransmitDeadline);

        for (int round = 0; round < 8; round++)
        {
            PumpRelayRound(dialler, proxy, tap, accepter);
        }

        // Assert — the invariant this whole design exists to prove (ADR 0004): a lost Fragment is
        // repaired by resending the one Datagram that carried it, never by re-sending the Message. Had
        // fragmentation silently become message-level retransmission, this figure would read 57 — the
        // whole Fragment count — instead of 1.
        (dialler.Reactor.CountersOf(diallerPeer).PacketsRetransmitted - retransmittedBefore).Should().Be(
            1UL, $"repair costs exactly one Datagram, not one Message — only the {position} Fragment (index {dropIndex}) was ever missing");

        // Wire-level completion evidence: every one of the 57 Fragment sequences reached the accepter,
        // each exactly once, whether on its original transmission or (for the dropped one) its
        // retransmission.
        List<uint> arrivedSequences = DecodeRecordSequences(tap, TapDirection.DiallerToAccepter);
        arrivedSequences.Should().HaveCount(FragmentCount, "every Fragment reached the accepter exactly once, across its original send and any retransmission");
        arrivedSequences.Distinct().Should().HaveCount(FragmentCount, "no Fragment sequence arrived twice — the retransmission is what filled the one gap the drop left");

        // The far end acknowledges what it received: every reply Datagram carries both reliable Spaces'
        // ack blocks unconditionally, so the accepter's reply having travelled back at all is itself the
        // acknowledgement — which is what let the sender narrow retransmission down to the one Fragment
        // still missing instead of every Fragment it had sent.
        tap.CountInDirection(TapDirection.AccepterToDialler).Should().BeGreaterThan(0, "the accepter's ack-carrying reply reached the dialler");
    }

    /// <summary>
    /// Issue #542: the clock advances every round and the accepter's acks flow back during the transfer,
    /// so the dropped Fragment's deadline passes while its retransmission still waits behind the
    /// Fragments queued ahead of it at one per Pump. The retransmission is counted once and reaches the
    /// wire once, however many RTOs it waits in the queue.
    /// </summary>
    [Theory]
    [MemberData(nameof(FragmentDropPositions))]
    public void Send_A57FragmentMessageWithOneFragmentDroppedWhileTheClockAdvances_CountsAndSendsOneRetransmission(int dropIndex, string position)
    {
        // Arrange
        using DatagramSessionReactorHarness dialler = new(
            capacity: 2, bindLoopback: false, maxMessageSize: MessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        using DatagramSessionReactorHarness accepter = new(
            capacity: 2, maxMessageSize: MessageSize, retentionBytes: RetentionBytes, fragmentSize: FragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        using LossyProxy proxy = new(tap.PublicEndPoint, dialler.TimeProvider);

        PeerId diallerPeer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        ConnectThroughProxyAndTap(dialler, proxy, tap, accepter);

        byte[] message = new byte[MessageSize];
        ulong retransmittedBefore = dialler.Reactor.CountersOf(diallerPeer).PacketsRetransmitted;

        // Act — one Fragment leaves per round, the clock advances each round, and acks flow back.
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, message).Should().Be(SendOutcome.Sent);
        for (int round = 0; round < FragmentCount * 3; round++)
        {
            if (round == dropIndex)
            {
                proxy.DropNext(RelayDirection.ClientToUpstream);
            }

            PumpRelayRound(dialler, proxy, tap, accepter);
            dialler.TimeProvider.Advance(_roundInterval);
        }

        // Assert
        (dialler.Reactor.CountersOf(diallerPeer).PacketsRetransmitted - retransmittedBefore).Should().Be(
            1UL, $"only the {position} Fragment (index {dropIndex}) was lost, and it waited in the send queue rather than being lost again");

        List<uint> arrivedSequences = DecodeRecordSequences(tap, TapDirection.DiallerToAccepter, fragmentsOnly: true);
        arrivedSequences.Should().HaveCount(FragmentCount, "the dropped Fragment reached the wire exactly once more and every other Fragment once")
            .And.OnlyHaveUniqueItems();
    }

    /// <summary>
    /// Pumps a dialler through a <see cref="LossyProxy"/> chained ahead of a <see cref="WireTap"/>
    /// (dialler -&gt; proxy, so a fault applies before the tap ever sees the datagram -&gt; tap -&gt;
    /// accepter) until both ends have raised <see cref="SessionEventKind.PeerConnected"/>. The reverse
    /// order from <see cref="DatagramSessionRetransmitIdentityTests"/>'s own chain: that test needs the
    /// tap to see a datagram before it can be dropped, where this test needs the tap to see only what
    /// genuinely got through, since that is what stands in for "the receiving end observed it" once
    /// reassembly (#49) makes a delivered Message impossible to assert here.
    /// </summary>
    private static PeerId ConnectThroughProxyAndTap(
        DatagramSessionReactorHarness dialler, LossyProxy proxy, WireTap tap, DatagramSessionReactorHarness accepter)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        PeerId? accepterPeer = null;
        bool diallerConnected = false;

        while (true)
        {
            PumpRelayRound(dialler, proxy, tap, accepter);

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
                throw new InvalidOperationException("The proxied-and-tapped handshake did not complete within the Pump budget.");
            }

            PumpDeadline.Yield();
        }
    }

    /// <summary>
    /// One round through the chained instruments: the dialler, then the proxy (applies any scheduled
    /// fault and relays toward the tap), then the tap (captures whatever the proxy actually forwarded and
    /// relays it on), then the accepter, then the tap and proxy again so the accepter's reply travels
    /// back the same round.
    /// </summary>
    private static void PumpRelayRound(
        DatagramSessionReactorHarness dialler, LossyProxy proxy, WireTap tap, DatagramSessionReactorHarness accepter)
    {
        dialler.Reactor.Pump();
        proxy.Pump();
        tap.Pump();
        accepter.Reactor.Pump();
        tap.Pump();
        proxy.Pump();
    }

    /// <summary>
    /// Reads every packet record's sequence number out of the <em>data</em> Datagrams captured
    /// travelling in <paramref name="direction"/>, walking either wire shape by hand (ADR 0009) — the
    /// unfragmented 7-byte header or the 11-byte Fragment header — chosen by the flags byte's fragment
    /// bit, the same choice <see cref="PacketRecordCodec.TryRead"/> makes, reimplemented independently
    /// here (matching <see cref="DatagramSessionSendFragmentationTests"/>'s own hand decoder) so a bug in
    /// the production reader could not hide itself from this test. <see cref="DatagramDecoder"/> is not
    /// used because it only understands the unfragmented record shape (issue #229) and every record this
    /// test's dialler sends is a Fragment.
    /// </summary>
    /// <remarks>
    /// A capture whose leading 4 bytes name the reserved control session id (zero) is a handshake
    /// Datagram, not a data one (ADR 0009's control-versus-data split): its bytes are
    /// <see cref="ControlDatagramCodec"/>'s shape, not <see cref="DatagramAssembler"/>'s, and are skipped
    /// entirely rather than walked as packet records — <see cref="DatagramSessionSendFragmentationTests.FirstDataDatagram"/>
    /// applies the same filter for the same reason.
    /// <para>
    /// With <paramref name="fragmentsOnly"/> set, unfragmented records such as Pings are left out.
    /// </para>
    /// </remarks>
    private static List<uint> DecodeRecordSequences(WireTap tap, TapDirection direction, bool fragmentsOnly = false)
    {
        List<uint> sequences = [];
        foreach (byte[] datagram in tap.PayloadsInDirection(direction))
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

                if (isFragment || !fragmentsOnly)
                {
                    sequences.Add(sequence);
                }

                offset += headerSize + length;
            }
        }

        return sequences;
    }
}
