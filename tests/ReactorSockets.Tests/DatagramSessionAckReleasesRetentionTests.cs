using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// An acknowledgement frees the
/// retention slot of every reliable record it releases, on both reliable Spaces, on the datagram
/// Session profile. Without that release a Peer's retention slots — every idle-triggered Ping's and
/// every reliable Send's — would never come back once acknowledged, so a quiet Session's Pings would
/// stop and a busy Session's further reliable Sends would see <see cref="RetentionSlotOutcome.Exhausted"/>
/// however much the far end had acknowledged.
/// </summary>
/// <remarks>
/// Every test here drives two real <see cref="DatagramSessionReactor"/>s on the public L2 surface (ADR
/// 0015): a <see cref="WireTap"/> for the tests that never need to drop anything, a
/// <see cref="LossyProxy"/> for the one that needs a controlled drop. Retention is pinned to a handful
/// of slots — <see cref="DatagramSessionOptions.RetentionSlots"/> — so a bounded number of Sends proves
/// the release rather than merely being unlikely to exhaust it.
/// </remarks>
public class DatagramSessionAckReleasesRetentionTests
{
    private const byte ControlBit = 0b0000_1000;

    /// <summary>
    /// RFC 791's 576-byte guaranteed reassembly minimum is <see cref="DatagramSessionOptions"/>'s own
    /// floor for <see cref="DatagramSessionOptions.FragmentSize"/> — matching every other test file in
    /// this suite that names its own fragment size rather than the harness's larger default.
    /// </summary>
    private const int FragmentSize = 600;

    // =========================================================================================
    // A quiet Session with retention sized to few slots keeps emitting exactly one
    // Ping per send-idle interval across at least 10 intervals while the far end acks them, and a
    // ReliableUnordered Send at the end still returns Sent.
    // =========================================================================================

    [Fact]
    public void Pump_QuietSessionWithOneRetentionSlotAckedEveryInterval_EmitsExactlyOnePingPerIntervalForTenIntervalsAndSendStillSucceeds()
    {
        // Arrange — one ReliableUnordered retention slot: the send-idle Ping is the only thing this
        // Peer ever sends, so without the fix the very first Ping would fill the slot forever and every
        // Ping after it would be refused. Linger is pushed far out so no RTO ever fires mid-run — this
        // test is entirely about the ack releasing the slot the next interval's Ping needs.
        TimeSpan sendIdleInterval = TimeSpan.FromSeconds(1);
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, fragmentSize: FragmentSize, retentionBytes: FragmentSize,
            linger: TimeSpan.FromHours(1), sendIdleInterval: sendIdleInterval);
        using DatagramSessionReactorHarness accepter = new(fragmentSize: FragmentSize, retentionBytes: FragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Act/Assert — ten rounds of: the interval elapses, exactly one further datagram (the Ping)
        // leaves the dialler, and the accepter's reply acks it before the next round begins.
        for (int round = 0; round < 10; round++)
        {
            int before = tap.CountInDirection(TapDirection.DiallerToAccepter);

            dialler.TimeProvider.Advance(sendIdleInterval);
            DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

            tap.CountInDirection(TapDirection.DiallerToAccepter).Should().Be(
                before + 1, $"round {round} must emit exactly one Ping — the sole retention slot was freed by the previous round's ack");

            accepter.Reactor.Send(accepterPeer, DeliveryMode.Unreliable, [(byte)round]).Should().Be(SendOutcome.Sent);
            DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);
            dialler.Reactor.Pump(); // the dialler's own next Pump is what actually reads the relayed ack and releases the slot.
        }

        // Assert — the slot the last Ping held was freed by its ack, so an ordinary reliable Send
        // still succeeds even though every one of the ten Pings has taken and released that same slot.
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableUnordered, [1, 2, 3]).Should().Be(
            SendOutcome.Sent, "the sole retention slot was freed by the tenth Ping's ack");
    }

    // =========================================================================================
    // After a Ping is sent and the far end's ack is read, a Send
    // that would otherwise return RetentionExhausted succeeds.
    // =========================================================================================

    [Fact]
    public void Pump_PingSentAndFarEndsAckIsRead_SendThatWouldOtherwiseBeRetentionExhaustedSucceeds()
    {
        // Arrange — one ReliableUnordered retention slot, so the Ping the send-idle timer emits is
        // the only thing that can ever occupy it.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, fragmentSize: FragmentSize, retentionBytes: FragmentSize,
            linger: TimeSpan.FromHours(1), sendIdleInterval: TimeSpan.FromSeconds(1));
        using DatagramSessionReactorHarness accepter = new(fragmentSize: FragmentSize, retentionBytes: FragmentSize);
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        // Act — the interval elapses, filling the sole slot with the Ping.
        dialler.TimeProvider.Advance(dialler.Options.SendIdleInterval);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        // Assert — the slot is genuinely full: an ordinary reliable Send is refused.
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableUnordered, [1, 2, 3]).Should().Be(
            SendOutcome.RetentionExhausted, "the Ping's own slot is the only one this Space has, and it is unacknowledged");

        // Act — the far end's ack for the Ping rides back and is read.
        accepter.Reactor.Send(accepterPeer, DeliveryMode.Unreliable, [9]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);
        dialler.Reactor.Pump();

        // Assert — the same Send that was just refused now succeeds.
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableUnordered, [1, 2, 3]).Should().Be(
            SendOutcome.Sent, "the Ping's ack freed the only slot this Space had");
    }

    // =========================================================================================
    // With every slot held by an un-acked Ping, the Ping is
    // refused for want of a slot, and it is emitted on the first Pump after an ack frees one, on
    // the same Peer.
    // =========================================================================================

    [Fact]
    public void Pump_ReliableUnorderedRetentionFullOnEstablishedPeer_RefusesThePingAndEmitsItOnTheSamePeersFirstPumpAfterAnAckFreesASlot()
    {
        // Arrange — one ReliableUnordered slot, filled by an ordinary reliable Send rather than by
        // the Ping itself, so the Ping is what is refused for want of a slot.
        // Linger is pushed far out on both ends so no RTO ever fires mid-run: the RTO ceiling is the
        // negotiated minimum of the two (issue #365), so an accepter left at the harness's short default
        // would drag the dialler's own effective ceiling back down with it.
        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, fragmentSize: FragmentSize, retentionBytes: FragmentSize,
            linger: TimeSpan.FromHours(1), sendIdleInterval: TimeSpan.FromSeconds(1));
        using DatagramSessionReactorHarness accepter = new(
            fragmentSize: FragmentSize, retentionBytes: FragmentSize, linger: TimeSpan.FromHours(1));
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableUnordered, [1, 2, 3]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        int capturesBeforeIdleWait = tap.CountInDirection(TapDirection.DiallerToAccepter);

        // Act — the send-idle interval elapses while the slot is still held by the unacknowledged Send.
        dialler.TimeProvider.Advance(dialler.Options.SendIdleInterval);
        dialler.Reactor.Pump();
        tap.Pump();

        // Assert — nothing new left the dialler: the Ping was refused for want of a slot.
        tap.CountInDirection(TapDirection.DiallerToAccepter).Should().Be(
            capturesBeforeIdleWait, "ReliableUnordered retention is full, so the Ping cannot be queued this Pump");

        // Act — the far end acks the data record, freeing its slot.
        accepter.Reactor.Send(accepterPeer, DeliveryMode.Unreliable, [9]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

        int capturesBeforeAckIsRead = tap.CountInDirection(TapDirection.DiallerToAccepter);

        // Act — the very next Pump on the same Peer both reads the ack (freeing the slot) and re-checks
        // the send-idle timer, which finds the interval already elapsed.
        dialler.Reactor.Pump();
        tap.Pump();

        // Assert — the Ping goes out on this same Pump, on the same Peer: no reuse, no reconnect.
        tap.CountInDirection(TapDirection.DiallerToAccepter).Should().Be(
            capturesBeforeAckIsRead + 1, "the freed slot lets the Ping go out the moment the ack is read");

        byte[] datagram = tap.PayloadsInDirection(TapDirection.DiallerToAccepter).Last();
        DatagramDecodeResult decoded = DatagramDecoder.TryDecode(datagram);
        decoded.IsSuccess.Should().BeTrue();
        decoded.Datagram!.Records.Should().ContainSingle(r => (r.Flags & ControlBit) == ControlBit, "the record freed onto the wire is the Ping");
    }

    // =========================================================================================
    // A reliable data record (ReliableOrdered and ReliableUnordered) acked by the
    // far end frees its slot: with retention sized to N slots, more than N acked Sends in sequence
    // all return Sent.
    // =========================================================================================

    public static TheoryData<DeliveryMode> ReliableModes => new()
    {
        DeliveryMode.ReliableOrdered,
        DeliveryMode.ReliableUnordered,
    };

    [Theory]
    [MemberData(nameof(ReliableModes))]
    public void Send_MoreThanNAckedReliableSendsInSequence_AllReturnSent(DeliveryMode mode)
    {
        // Arrange — a Space sized to exactly two slots. Five sends, each acked before the next is
        // attempted, is more than twice that, so a Send that reused a slot the previous ack never
        // actually freed would eventually see RetentionExhausted.
        const int slotsPerSpace = 2;
        const int retentionBytes = FragmentSize * slotsPerSpace;
        const int rounds = 5;

        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, fragmentSize: FragmentSize, retentionBytes: retentionBytes,
            linger: TimeSpan.FromHours(1), sendIdleInterval: TimeSpan.FromHours(2));
        using DatagramSessionReactorHarness accepter = new(fragmentSize: FragmentSize, retentionBytes: retentionBytes, sendIdleInterval: TimeSpan.FromHours(2));
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        List<SendOutcome> outcomes = [];
        for (int round = 0; round < rounds; round++)
        {
            outcomes.Add(dialler.Reactor.Send(diallerPeer, mode, [(byte)round, (byte)round, (byte)round]));
            DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);

            accepter.Reactor.Send(accepterPeer, DeliveryMode.Unreliable, [(byte)round]).Should().Be(SendOutcome.Sent);
            DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);
            dialler.Reactor.Pump();
        }

        // Assert
        outcomes.Should().AllSatisfy(o => o.Should().Be(
            SendOutcome.Sent, $"each Send is acked before the next is attempted, so the {slotsPerSpace}-slot Space never actually runs out"));
    }

    // =========================================================================================
    // An un-acked record is still resent after the ack frees a different slot, and
    // a released record is never resent.
    // =========================================================================================

    [Fact]
    public void Pump_RecordAAckFreesRecordBsSlot_RecordAIsStillResentWithItsOwnPayload_RecordBIsNeverResent()
    {
        // Arrange — a two-slot ReliableOrdered Space. Record A is dropped on its first transmission and
        // stays outstanding; record B reaches the accepter and is acked, releasing B's own slot while A
        // remains unacknowledged in the other slot.
        const int slotsPerSpace = 2;
        const int retentionBytes = FragmentSize * slotsPerSpace;
        TimeSpan linger = TimeSpan.FromSeconds(1);

        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, fragmentSize: FragmentSize, retentionBytes: retentionBytes,
            linger: linger, sendIdleInterval: TimeSpan.FromHours(2));
        using DatagramSessionReactorHarness accepter = new(fragmentSize: FragmentSize, retentionBytes: retentionBytes, sendIdleInterval: TimeSpan.FromHours(2));
        using LossyProxy proxy = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(proxy.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughProxyUntilConnected(dialler, proxy, accepter);

        byte[] recordA = [0xAA, 0xAA, 0xAA];
        byte[] recordB = [0xBB, 0xBB, 0xBB];

        // Act — record A is sent and dropped before it ever reaches the accepter.
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, recordA).Should().Be(SendOutcome.Sent);
        proxy.DropNext(RelayDirection.ClientToUpstream);
        dialler.Reactor.Pump();
        proxy.Pump();
        accepter.Reactor.Pump();
        accepter.DrainPayloads().Should().BeEmpty("record A was dropped before reaching the accepter");

        // Act — record B is sent and reaches the accepter normally, taking the Space's other slot.
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, recordB).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Pump();
        proxy.Pump();
        accepter.Reactor.Pump();

        // ReliableOrdered holds record B behind the still-missing record A, so it is not yet delivered —
        // the acknowledgement it generates still frees record B's own retention slot regardless.
        accepter.DrainPayloads().Should().BeEmpty("record A's hole still blocks ReliableOrdered delivery of record B");

        // Act — the accepter's reply carries the ack that releases record B's slot, leaving record A
        // the only thing still outstanding.
        accepter.Reactor.Send(accepterPeer, DeliveryMode.Unreliable, [9]).Should().Be(SendOutcome.Sent);
        accepter.Reactor.Pump();
        proxy.Pump();
        dialler.Reactor.Pump();

        // Assert — the freed slot is usable: a third record fits where record B's slot used to be full.
        byte[] recordC = [0xCC, 0xCC, 0xCC];
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, recordC).Should().Be(
            SendOutcome.Sent, "record B's ack freed a slot, and record A's own slot was never touched");

        // Act — record A's RTO passes; nothing drops this time, so its resend actually reaches the
        // accepter and can be told apart from a resend of the already-released record B.
        dialler.TimeProvider.Advance(linger + TimeSpan.FromSeconds(1));
        dialler.Reactor.Pump();
        proxy.Pump();
        accepter.Reactor.Pump();

        // Assert — record A (and, once its hole is filled, record C and record B behind it, delivered
        // for the first and only time each) reaches the accepter.
        List<byte[]> delivered = accepter.DrainPayloads();
        delivered.Any(a => a.SequenceEqual(recordA)).Should().BeTrue("record A was never acknowledged, so it must be resent");

        // Assert — read at the wire, not at delivery: record B legitimately reaches the accepter exactly
        // once — its original, only-ever transmission, released the moment it was acked — while record A
        // crosses the wire twice: the original attempt the proxy dropped, and this resend. A released
        // record is never resent, which this distinguishes from "delivered once", since ReliableOrdered
        // also delivers record B locally, from what it already buffered, the moment record A's hole fills.
        CountRecordsCarryingPayload(proxy.Forwarded, RelayDirection.ClientToUpstream, recordA).Should().Be(
            1, "the original send of record A was dropped by the proxy, so only its resend was ever forwarded");
        CountRecordsCarryingPayload(proxy.Captures, RelayDirection.ClientToUpstream, recordB).Should().Be(
            1, "record B's slot was released by its ack, so it must never be sent a second time");
    }

    /// <summary>How many records across every <paramref name="direction"/> capture in <paramref name="captures"/> carry exactly <paramref name="payload"/>.</summary>
    private static int CountRecordsCarryingPayload(IReadOnlyList<RelayCapture> captures, RelayDirection direction, byte[] payload)
    {
        int count = 0;
        foreach (RelayCapture capture in captures)
        {
            if (capture.Direction != direction)
            {
                continue;
            }

            DatagramDecodeResult decoded = DatagramDecoder.TryDecode(capture.Payload);
            if (!decoded.IsSuccess)
            {
                continue;
            }

            count += decoded.Datagram!.Records.Count(r => r.Payload.SequenceEqual(payload));
        }

        return count;
    }

    // =========================================================================================
    // Fragments: a fragmented reliable Message's slots are freed once its fragments
    // are acked.
    // =========================================================================================

    [Fact]
    public void Pump_FragmentedMessageAckedInFull_FreesEveryOneOfItsFragmentSlots()
    {
        // Arrange — a Space sized to exactly one three-fragment Message's worth of slots, so the
        // Message fills the whole Space and a further Send of any size is refused until every one of
        // its three Fragments is individually released.
        const int fragmentsPerMessage = 3;
        const int retentionBytes = FragmentSize * fragmentsPerMessage;
        const int maxMessageSize = FragmentSize * fragmentsPerMessage;

        using DatagramSessionReactorHarness dialler = new(
            bindLoopback: false, fragmentSize: FragmentSize, retentionBytes: retentionBytes, maxMessageSize: maxMessageSize,
            linger: TimeSpan.FromHours(1), sendIdleInterval: TimeSpan.FromHours(2));
        using DatagramSessionReactorHarness accepter = new(
            fragmentSize: FragmentSize, retentionBytes: retentionBytes, maxMessageSize: maxMessageSize, sendIdleInterval: TimeSpan.FromHours(2));
        using WireTap tap = new(accepter.Endpoint, dialler.TimeProvider);
        PeerId diallerPeer = dialler.Reactor.Connect(tap.PublicEndPoint);
        PeerId accepterPeer = DatagramSessionReactorHarness.PumpThroughTapUntilConnected(dialler, tap, accepter);

        byte[] message = new byte[FragmentSize * fragmentsPerMessage];
        Array.Fill(message, (byte)0x5A);

        // Act — the whole Message fills the Space; even the smallest further Send has nowhere to go.
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, message).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [1]).Should().Be(
            SendOutcome.RetentionExhausted, "all three Fragment slots are held by the unacknowledged Message");

        // Act — the Message crosses the wire in full and is reassembled and delivered.
        List<byte[]> delivered = [];
        PumpDeadline deliveryDeadline = PumpDeadline.Start();
        while (delivered.Count < 1 && !deliveryDeadline.Expired)
        {
            DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);
            delivered.AddRange(accepter.DrainPayloads());
        }

        delivered.Should().ContainSingle(p => p.SequenceEqual(message));

        // Act — the accepter's reply carries the ack for every one of the three Fragments.
        accepter.Reactor.Send(accepterPeer, DeliveryMode.Unreliable, [9]).Should().Be(SendOutcome.Sent);
        DatagramSessionReactorHarness.PumpRoundThroughTap(dialler, tap, accepter);
        dialler.Reactor.Pump();

        // Assert — every one of the three slots is free: three single-Fragment Sends succeed in a row,
        // and a fourth — one more than the Space holds — is refused again.
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [2]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [3]).Should().Be(SendOutcome.Sent);
        dialler.Reactor.Send(diallerPeer, DeliveryMode.ReliableOrdered, [4]).Should().Be(
            SendOutcome.RetentionExhausted, "exactly three slots were freed, matching the three Fragments that were acked");
    }
}
