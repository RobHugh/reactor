using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Issue #344 (parent #53): the Ping's round trip between two real <see cref="DatagramSessionReactor"/>s.
/// A Ping is admitted by the ordinary <c>ReliableUnordered</c> ack path (#339) exactly as any other
/// reliable record is — no special-case ack exists or is needed — so once the far end's ack for it
/// reaches the sender, the sender's own retransmission bookkeeping treats the Ping's sequence as
/// released and stops arming it for resend. Every test here drives two real Reactors through a
/// <see cref="WireTap"/> (tests/CLAUDE.md, ADR 0015): nothing here reaches past the public L2 surface.
/// </summary>
/// <remarks>
/// This file does not assert that the sender's <c>ReliableUnordered</c> retention slot is freed by the
/// ack (the ticket's first acceptance criterion). <c>RetentionPool.Release</c> — the only thing that
/// would free it — has no production caller anywhere in <see cref="DatagramSessionReactor"/> today: an
/// arriving ack updates <c>OutstandingReliableSequences</c>' own bookkeeping only (what
/// <see cref="DatagramSessionSendIdlePingTests"/>'s AC4 remark already documents as a pre-existing gap).
/// Wiring that up is a scope decision left to the ticket's owner, not made here.
/// </remarks>
public class DatagramSessionPingAckRoundTripTests
{
    /// <summary>
    /// Comfortably past the RTO, whose ceiling is the harness's one-second default Linger — the same
    /// value <see cref="DatagramSessionRetransmitResendTests"/> uses for the same reason.
    /// </summary>
    private static readonly TimeSpan _pastTheRetransmitDeadline = TimeSpan.FromSeconds(3);

    /// <summary>
    /// Long enough that three trips past <see cref="_pastTheRetransmitDeadline"/> (nine seconds) never
    /// crosses it and triggers a second, unrelated Ping — the send-idle interval restarts on every
    /// datagram the dialler sends, the first Ping included (issue #340).
    /// </summary>
    private static readonly TimeSpan _sendIdleInterval = TimeSpan.FromSeconds(30);

    [Fact]
    public void Pump_APingIsAcknowledged_NoResendOfItReachesTheWireAfterSeveralRTOsPastTheAck()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(
            bindLoopback: false, linger: TimeSpan.FromSeconds(1), sendIdleInterval: _sendIdleInterval);
        using DatagramSessionReactorHarness receiver = new();
        using WireTap tap = new(receiver.Endpoint, sender.TimeProvider);
        sender.Reactor.Connect(tap.PublicEndPoint);
        PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(sender, tap, receiver);

        // Act — the send-idle interval elapses, so the sender emits its one Ping.
        sender.TimeProvider.Advance(_sendIdleInterval);
        DatagramSessionReactorHarness.PumpRoundThroughTap(sender, tap, receiver);

        int wireCountAfterPing = tap.CountInDirection(TapDirection.DiallerToAccepter);

        // The Ping is admitted into the receiver's ReliableUnordered receive window on arrival, but an
        // ack only rides an outbound datagram, and a wholly silent receiver sends none of its own — the
        // same reason DatagramSessionPingReceiveTests and DatagramSessionRetransmitResendTests both make
        // the far end Send something to carry the ack back.
        receiver.Reactor.Send(receiverPeer, DeliveryMode.Unreliable, [0x01]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(sender, tap, receiver);

        // Act — several RTOs pass after the ack, in separate rounds so a re-arm between them would have
        // its own chance to fire.
        for (int round = 0; round < 3; round++)
        {
            sender.TimeProvider.Advance(_pastTheRetransmitDeadline);
            DatagramSessionReactorHarness.PumpRoundThroughTap(sender, tap, receiver);
        }

        // Assert — nothing further left the sender for this Peer: the acked Ping's sequence was never
        // re-armed, so no resend was ever queued, packed or sent.
        tap.CountInDirection(TapDirection.DiallerToAccepter).Should().Be(
            wireCountAfterPing, "the Ping was acknowledged before any RTO passed, so retransmission has nothing left to resend");
    }

    [Fact]
    public void Pump_APingIsAcknowledged_TheSenderSPacketsRetransmittedCounterNeverMoves()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(
            bindLoopback: false, linger: TimeSpan.FromSeconds(1), sendIdleInterval: _sendIdleInterval);
        using DatagramSessionReactorHarness receiver = new();
        using WireTap tap = new(receiver.Endpoint, sender.TimeProvider);
        PeerId senderPeer = sender.Reactor.Connect(tap.PublicEndPoint);
        PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(sender, tap, receiver);

        ulong retransmittedBefore = sender.Reactor.CountersOf(senderPeer).PacketsRetransmitted;

        // Act — the Ping is sent and acknowledged.
        sender.TimeProvider.Advance(_sendIdleInterval);
        DatagramSessionReactorHarness.PumpRoundThroughTap(sender, tap, receiver);
        receiver.Reactor.Send(receiverPeer, DeliveryMode.Unreliable, [0x01]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(sender, tap, receiver);

        // Act — several RTOs pass after the ack.
        for (int round = 0; round < 3; round++)
        {
            sender.TimeProvider.Advance(_pastTheRetransmitDeadline);
            DatagramSessionReactorHarness.PumpRoundThroughTap(sender, tap, receiver);
        }

        // Assert
        sender.Reactor.CountersOf(senderPeer).PacketsRetransmitted.Should().Be(
            retransmittedBefore, "an acknowledged Ping has nothing left to retransmit");
    }

    [Fact]
    public void Pump_APingIsSentAndAcknowledged_NeitherEndEverDrainsAMessageReceivedForIt()
    {
        // Arrange
        using DatagramSessionReactorHarness sender = new(
            bindLoopback: false, linger: TimeSpan.FromSeconds(1), sendIdleInterval: _sendIdleInterval);
        using DatagramSessionReactorHarness receiver = new();
        using WireTap tap = new(receiver.Endpoint, sender.TimeProvider);
        sender.Reactor.Connect(tap.PublicEndPoint);
        PeerId receiverPeer = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(sender, tap, receiver);

        List<byte[]> senderPayloads = [];
        List<byte[]> receiverPayloads = [];

        // Act — the Ping crosses the wire and is admitted.
        sender.TimeProvider.Advance(_sendIdleInterval);
        DatagramSessionReactorHarness.PumpRoundThroughTap(sender, tap, receiver);
        senderPayloads.AddRange(sender.DrainPayloads());
        receiverPayloads.AddRange(receiver.DrainPayloads());

        // Act — the receiver's ack for it rides back to the sender.
        receiver.Reactor.Send(receiverPeer, DeliveryMode.Unreliable, [0x01]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(sender, tap, receiver);
        senderPayloads.AddRange(sender.DrainPayloads());
        receiverPayloads.AddRange(receiver.DrainPayloads());

        // Assert — a Ping is admitted (acked, and duplicates would count), never delivered, on either
        // end: the receiver never surfaces it as a Message, and the sender receives no payload of its
        // own from the plain ack-carrying datagram either.
        receiverPayloads.Should().BeEmpty("a Ping is admitted into the receive window but never delivered (#339)");
        senderPayloads.Should().BeEmpty("nothing the receiver sent back carried a payload of its own");
    }
}
