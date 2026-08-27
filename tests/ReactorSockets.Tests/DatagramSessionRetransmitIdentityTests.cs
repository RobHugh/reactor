using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.7.11 (issue #238): retransmission is byte-identical at packet granularity, never at
/// datagram granularity. A <see cref="LossyProxy"/> drops a reliable record's only transmission so its
/// retransmit deadline genuinely expires, while a <see cref="WireTap"/> chained ahead of the proxy — so
/// it captures every Datagram the dialler sends before the proxy gets a chance to drop one — keeps a
/// byte-for-byte copy of both the dropped original and the later resend. Every assertion here is against
/// individual <see cref="DecodedPacketRecord"/>s pulled out of those two captures; the two Datagrams
/// themselves are only ever asserted to differ, never to be equal, because the protocol never promises
/// datagram-level reproducibility — only that a resent record's own bytes are unchanged.
/// </summary>
public class DatagramSessionRetransmitIdentityTests
{
    /// <summary>
    /// Comfortably past the RTO, whose ceiling is the harness's one-second Linger, so the deadline has
    /// certainly passed however the estimator has moved (matches <see cref="DatagramSessionRetransmitResendTests"/>).
    /// </summary>
    private static readonly TimeSpan _pastTheRetransmitDeadline = TimeSpan.FromSeconds(3);

    [Fact]
    public void Pump_AResentReliableRecord_IsByteIdenticalToItsDroppedFirstTransmissionAndRidesTheNextDatagram()
    {
        // Arrange — the tap sits ahead of the proxy (dialler -> tap -> proxy -> accepter) so it captures
        // every Datagram the dialler sends, including the one about to be dropped downstream. Retention
        // sized for the two in-flight ReliableOrdered/ReliableUnordered records this test holds at once
        // (see DatagramSessionCoalescingTests.cs: 1200 bytes is the default fragment size).
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false, retentionBytes: 1200 * 2);
        using DatagramSessionReactorHarness accepter = new();
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        using WireTap tap = new(proxy.PublicEndPoint, dialler.TimeProvider);
        PeerId peer = dialler.Reactor.Connect(tap.PublicEndPoint);
        PeerId accepterPeer = PumpThroughTapAndProxyUntilConnected(dialler, tap, proxy, accepter);

        byte[] original = [9, 9, 9];
        byte[] fresh = [4, 4];
        ulong retransmittedBefore = dialler.Reactor.CountersOf(peer).PacketsRetransmitted;

        // Act — the one transmission of `original` is dropped between the tap and the accepter, so the
        // tap still captured its bytes even though the accepter never received it.
        proxy.DropNext(RelayDirection.ClientToUpstream);
        dialler.Reactor.Send(peer, DeliveryMode.ReliableOrdered, original).Should().Be(SendOutcome.Sent);
        PumpRoundThroughTapAndProxy(dialler, tap, proxy, accepter);
        accepter.DrainPayloads().Should().BeEmpty("the only transmission was dropped before it reached the accepter");

        byte[] firstDatagram = tap.PayloadsInDirection(TapDirection.DiallerToAccepter).Last();
        DatagramDecodeResult firstDecode = DatagramDecoder.TryDecode(firstDatagram);
        firstDecode.IsSuccess.Should().BeTrue(firstDecode.Error);
        DecodedPacketRecord originalRecord = firstDecode.Datagram!.Records.Should().ContainSingle(
            "the first Datagram carries only the one record this test Sent").Which;
        originalRecord.Payload.Should().Equal(original, "the captured record must carry the bytes that were Sent");

        // The accepter answers with its own ReliableOrdered Message on a Space `original` never touches
        // (the dialler's own send sequence), so its own outgoing ack block moves for a reason unrelated
        // to `original` ever being acknowledged — proving the two dialler Datagrams differ for a genuine
        // protocol reason, not merely because a record was appended.
        byte[] accepterReply = [6, 6];
        accepter.Reactor.Send(accepterPeer, DeliveryMode.ReliableOrdered, accepterReply).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        proxy.Pump();
        tap.Pump();
        dialler.Reactor.Pump();
        dialler.DrainPayloads().Should().Equal(
            [accepterReply], (actual, expected) => actual.SequenceEqual(expected),
            "the accepter's reply travels back through the tap and proxy undropped");

        // Act — the deadline passes and a fresh Message is Sent in the same Frame, so the resend and the
        // new record share one Datagram.
        dialler.TimeProvider.Advance(_pastTheRetransmitDeadline);
        dialler.Reactor.Send(peer, DeliveryMode.ReliableUnordered, fresh).Should().Be(SendOutcome.Sent);
        ulong datagramsSentBefore = dialler.Reactor.Counters.DatagramsSent;
        dialler.Reactor.Pump();
        tap.Pump();
        proxy.Pump();

        // Assert — exactly one further Datagram left the dialler, carrying the resend alongside the
        // fresh record: the resend does not get a Datagram of its own.
        (dialler.Reactor.Counters.DatagramsSent - datagramsSentBefore).Should().Be(
            1UL, "the resend and the fresh Send both flush in this one Pump, in one Datagram");

        byte[] secondDatagram = tap.PayloadsInDirection(TapDirection.DiallerToAccepter).Last();
        secondDatagram.Should().NotEqual(
            firstDatagram, "a resend is repacked into whatever container goes out next, never a replay of the first Datagram");

        DatagramDecodeResult secondDecode = DatagramDecoder.TryDecode(secondDatagram);
        secondDecode.IsSuccess.Should().BeTrue(secondDecode.Error);
        secondDecode.Datagram!.ReliableOrderedAck.Should().NotBe(
            firstDecode.Datagram!.ReliableOrderedAck,
            "the intervening accepter reply moved the dialler's own ack block, so the two Datagrams differ in more than record content");

        IReadOnlyList<DecodedPacketRecord> secondRecords = secondDecode.Datagram!.Records;
        DecodedPacketRecord resent = secondRecords.Should().ContainSingle(
            r => r.Sequence == originalRecord.Sequence && r.Mode == originalRecord.Mode,
            "the expired record must reappear, once, in the later Datagram").Which;
        resent.Flags.Should().Be(originalRecord.Flags, "the resent record's flags must match the original's");
        resent.Length.Should().Be(originalRecord.Length, "the resent record's declared length must match the original's");
        resent.Payload.Should().Equal(originalRecord.Payload, "the resent record's payload bytes must match the original's");

        secondRecords.Should().Contain(
            r => r.Payload.SequenceEqual(fresh),
            "the resend must travel alongside a record Sent since, in the same Datagram");

        (dialler.Reactor.CountersOf(peer).PacketsRetransmitted - retransmittedBefore).Should().Be(
            1UL, "exactly one resend happened, so the Peer's retransmitted figure rises by exactly one");
    }

    /// <summary>
    /// Pumps a dialler through a <see cref="WireTap"/> chained ahead of a <see cref="LossyProxy"/> (tap
    /// captures, then the proxy may fault) and an accepter until both ends have raised
    /// <see cref="SessionEventKind.PeerConnected"/> — the composition
    /// <see cref="DatagramSessionReactorHarness"/> has no single helper for, since its own tap and proxy
    /// helpers are each typed to their instrument alone.
    /// </summary>
    private static PeerId PumpThroughTapAndProxyUntilConnected(
        DatagramSessionReactorHarness dialler, WireTap tap, LossyProxy proxy, DatagramSessionReactorHarness accepter)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        PeerId? accepterPeer = null;
        bool diallerConnected = false;

        while (true)
        {
            PumpRoundThroughTapAndProxy(dialler, tap, proxy, accepter);

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
    private static void PumpRoundThroughTapAndProxy(
        DatagramSessionReactorHarness dialler, WireTap tap, LossyProxy proxy, DatagramSessionReactorHarness accepter)
    {
        dialler.Reactor.Pump();
        tap.Pump();
        proxy.Pump();
        accepter.Reactor.Pump();
        proxy.Pump();
        tap.Pump();
    }
}
