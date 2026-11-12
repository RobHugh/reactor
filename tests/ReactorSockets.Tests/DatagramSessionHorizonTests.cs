using System.Buffers.Binary;
using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The horizon rule, proved by the retransmit timer <em>not starting</em> rather than by delivery
/// eventually succeeding (ADR 0009). What applying a received ack block releases, and how a slide arms
/// what it newly covers, is asserted here too, since it is the same instrument and the same horizon.
/// </summary>
/// <remarks>
/// <para>
/// <b>The rule.</b> A received ack block can only ever describe <c>[base, base + 32]</c> — a cumulative
/// base plus 32 bits. A sent reliable sequence beyond that span cannot yet be described by any ack the
/// peer is able to send, so arming its retransmit deadline would resend it the moment the timeout
/// elapsed although nothing was lost. Such a sequence is recorded as outstanding but left unarmed, and
/// the deferral is counted; it arms only once a later ack slides the horizon over it.
/// </para>
/// <para>
/// <b>What removing the gate would do, and how these tests would show it.</b> Arm every outstanding
/// sequence on send — delete the horizon branch in <c>OutstandingReliableSequences.RecordSent</c> — and
/// <see cref="Pump_ThirtyEightOutstandingWithNothingAcknowledged_ResendsOnlyTheThirtyThreeInsideTheHorizon"/>
/// reads 38 retransmissions instead of 33 on its first sweep and 38 again on its second, and
/// <see cref="Pump_ThirtyEightOutstandingWithNothingAcknowledged_CountsOneHorizonDeferralPerSequenceHeldBack"/>
/// reads zero deferrals instead of five. Those five spurious resends are the failure the rule exists to
/// prevent, and they are exactly what these assertions are counted against.
/// </para>
/// <para>
/// <b>The mechanism through the seam.</b> <c>OutstandingReliableSequences</c> is internal with no
/// <c>InternalsVisibleTo</c> (ADR 0015), so "outstanding with no deadline" is asserted the one way a
/// consumer can observe it: <see cref="PeerCounters.PacketsRetransmitted"/> rises by exactly the number
/// of <em>armed</em> sequences once the clock passes the deadline, and does not rise for the deferred
/// ones however far the clock advances. That is the timer not starting — a sequence still outstanding
/// (nothing here ever acknowledges it, so nothing releases it) and still never resent.
/// </para>
/// <para>
/// <b>The instrument.</b> A live <see cref="DatagramSessionReactor"/> whose one Peer is a hand-built
/// client over a bare <see cref="RawReactor"/> — the same instrument
/// <see cref="DatagramSessionCounterExchangeTests"/> and <c>DatagramSessionSpaceRoutingTests</c> use. A
/// live counterparty cannot express this: the scenario needs acks that never arrive, and then one ack
/// naming a base of the test's own choosing, neither of which an ordinary Session will produce.
/// </para>
/// </remarks>
public class DatagramSessionHorizonTests
{
    private const int ControlDatagramSize = 256;
    private const uint ControlSession = 0;

    private const byte HelloType = 1;
    private const int ControlBodyOffset = 5;
    private const int CookieLength = 32;

    private const uint ProtocolVersion = DatagramSessionWire.ProtocolVersion;

    private const int DatagramHeaderSize = 20;
    private const int ReliableOrderedAckBlockOffset = 4;

    private const byte ModeUnreliable = 0b0000_0000;

    /// <summary>
    /// The span one ack block can describe: the cumulative base itself plus the 32 sequences its
    /// bitfield covers, so sequences 0 through 32 inclusive lie inside the initial horizon.
    /// </summary>
    private const int SequencesInsideTheInitialHorizon = 33;

    /// <summary>Five more than the horizon covers, so exactly five sequences are held back.</summary>
    private const int SequencesSent = 38;

    private const int SequencesBeyondTheInitialHorizon = SequencesSent - SequencesInsideTheInitialHorizon;

    /// <summary>Retention sized for 64 records per reliable Space, so all 38 are retained at once.</summary>
    private const int RetentionForSixtyFourRecords = 1200 * 64;

    /// <summary>
    /// Comfortably past the RTO, whose ceiling is the harness's one-second Linger, so an armed deadline
    /// has certainly passed however the estimator has moved —
    /// <see cref="DatagramSessionRetransmitResendTests"/>'s own constant.
    /// </summary>
    private static readonly TimeSpan _pastTheRetransmitDeadline = TimeSpan.FromSeconds(3);

    [Fact]
    public void Pump_ThirtyEightOutstandingWithNothingAcknowledged_ResendsOnlyTheThirtyThreeInsideTheHorizon()
    {
        // Arrange
        using DatagramSessionReactorHarness node = new(retentionBytes: RetentionForSixtyFourRecords);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);
        node.Reactor.CountersOf(nodePeer).PacketsRetransmitted.Should().Be(0);

        // Act — 38 ReliableOrdered sequences, 0 through 37, all sent in one flush. Nothing ever
        // acknowledges them, so the horizon stays at its initial [0, 32] for the whole test.
        SendReliableOrdered(node, nodePeer, SequencesSent);
        node.Reactor.Pump();

        ulong retransmittedBeforeFirstSweep = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted;
        node.TimeProvider.Advance(_pastTheRetransmitDeadline);
        node.Reactor.Pump();
        ulong firstSweep = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted - retransmittedBeforeFirstSweep;

        // Act — the clock advances a second time, well past what the deferred sequences' deadlines would
        // have been had they ever been armed. Nothing expires that was never armed.
        ulong retransmittedBeforeSecondSweep = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted;
        node.TimeProvider.Advance(_pastTheRetransmitDeadline);
        node.Reactor.Pump();
        ulong secondSweep = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted - retransmittedBeforeSecondSweep;

        // Assert — exactly the 33 sequences the initial horizon covers were armed and resent. Sequences
        // 33 through 37 are still outstanding, and were never resent: the deadline never started.
        firstSweep.Should().Be(
            (ulong)SequencesInsideTheInitialHorizon,
            "sequences 0 through 32 sit inside the initial [0, 32] horizon and are armed, while 33 through 37 are outstanding with no deadline");
        secondSweep.Should().Be(
            (ulong)SequencesInsideTheInitialHorizon,
            "the armed sequences re-arm and expire again, and the deferred five still have no deadline to pass");
    }

    [Fact]
    public void Pump_ThirtyEightOutstandingWithNothingAcknowledged_CountsOneHorizonDeferralPerSequenceHeldBack()
    {
        // Arrange
        using DatagramSessionReactorHarness node = new(retentionBytes: RetentionForSixtyFourRecords);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);
        node.Reactor.CountersOf(nodePeer).HorizonDeferrals.Should().Be(0);

        // Act
        SendReliableOrdered(node, nodePeer, SequencesSent);
        node.Reactor.Pump();
        ReconcileHorizonDeferrals(node, client, clientLink, sessionId);

        // Assert
        node.Reactor.CountersOf(nodePeer).HorizonDeferrals.Should().Be(
            (ulong)SequencesBeyondTheInitialHorizon,
            "sequences 33 through 37 of 0-through-37 lie beyond the [0, 32] horizon, one deferral each");
    }

    [Fact]
    public void Pump_EverythingOutstandingFittingInsideTheHorizon_DefersNothingAndResendsEveryOne()
    {
        // Arrange
        using DatagramSessionReactorHarness node = new(retentionBytes: RetentionForSixtyFourRecords);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);

        // Act — 33 sequences, 0 through 32: exactly what one ack block can describe, so the horizon
        // holds nothing back.
        SendReliableOrdered(node, nodePeer, SequencesInsideTheInitialHorizon);
        node.Reactor.Pump();

        ulong retransmittedBefore = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted;
        node.TimeProvider.Advance(_pastTheRetransmitDeadline);
        node.Reactor.Pump();
        ReconcileHorizonDeferrals(node, client, clientLink, sessionId);

        // Assert — ordinary operation: nothing was ever held back, and every sequence was armed.
        node.Reactor.CountersOf(nodePeer).HorizonDeferrals.Should().Be(
            0, "everything outstanding fitted inside the horizon, so nothing was ever deferred");
        (node.Reactor.CountersOf(nodePeer).PacketsRetransmitted - retransmittedBefore).Should().Be(
            (ulong)SequencesInsideTheInitialHorizon, "all 33 were armed on send and all 33 expired together");
    }

    [Fact]
    public void Pump_AnAckSlidingTheHorizonOverTheDeferredSequences_ArmsExactlyThoseItNowCovers()
    {
        // Arrange — the same 38-outstanding state, with its first sweep already taken so the 33 armed
        // sequences are accounted for before the slide.
        using DatagramSessionReactorHarness node = new(retentionBytes: RetentionForSixtyFourRecords);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);

        SendReliableOrdered(node, nodePeer, SequencesSent);
        node.Reactor.Pump();
        node.TimeProvider.Advance(_pastTheRetransmitDeadline);
        node.Reactor.Pump();

        // Act — one ack block whose cumulative base is 33: everything below it is acknowledged in one
        // step, which releases the 33 armed sequences and slides the horizon to [33, 65]. Sequences 33
        // through 37 are the only ones left outstanding, and the slide is what arms them.
        SendCraftedArrival(
            node,
            client,
            clientLink,
            BuildDatagram(sessionId, reliableOrderedAckBase: 33, reliableOrderedAckBits: 0, sequence: 1));

        ulong retransmittedBeforeSlideSweep = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted;
        node.TimeProvider.Advance(_pastTheRetransmitDeadline);
        node.Reactor.Pump();

        // Assert — exactly the five the slide newly covered, and nothing else: the released 33 are gone
        // rather than resent, and no sixth sequence appeared from anywhere.
        (node.Reactor.CountersOf(nodePeer).PacketsRetransmitted - retransmittedBeforeSlideSweep).Should().Be(
            (ulong)SequencesBeyondTheInitialHorizon,
            "the slide armed sequences 33 through 37, and the cumulative base released 0 through 32");
    }

    [Fact]
    public void Pump_OneReliableSpaceOverflowingItsHorizonWhileTheOtherDoesNot_DefersOnlyInTheSpaceThatOverflowed()
    {
        // Arrange
        using DatagramSessionReactorHarness node = new(retentionBytes: RetentionForSixtyFourRecords);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);

        const int reliableUnorderedSent = 5;

        // Act — 38 in ReliableOrdered, which overflows its own horizon by five, and 5 in
        // ReliableUnordered, which sits well inside its own separate one.
        SendReliableOrdered(node, nodePeer, SequencesSent);
        for (int i = 0; i < reliableUnorderedSent; i++)
        {
            node.Reactor.Send(nodePeer, DeliveryMode.ReliableUnordered, [1]).Should().Be(SendOutcome.Sent);
        }

        node.Reactor.Pump();

        ulong retransmittedBefore = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted;
        node.TimeProvider.Advance(_pastTheRetransmitDeadline);
        node.Reactor.Pump();
        ReconcileHorizonDeferrals(node, client, clientLink, sessionId);

        // Assert — each Space is gated by its own horizon: ReliableUnordered's five all armed, so the
        // deferral count is still exactly ReliableOrdered's five, and the sweep found 33 + 5.
        node.Reactor.CountersOf(nodePeer).HorizonDeferrals.Should().Be(
            (ulong)SequencesBeyondTheInitialHorizon,
            "only ReliableOrdered overflowed its horizon, and ReliableUnordered's own horizon covered all five of its sequences");
        (node.Reactor.CountersOf(nodePeer).PacketsRetransmitted - retransmittedBefore).Should().Be(
            (ulong)SequencesInsideTheInitialHorizon + reliableUnorderedSent,
            "ReliableOrdered armed 33 and ReliableUnordered armed all 5, and the two outstanding sets are independent");
    }

    [Fact]
    public void Pump_AnAckBitfieldWithScatteredBits_ReleasesExactlyThoseSequencesAndLeavesEveryOtherArmed()
    {
        // Arrange — ten outstanding ReliableOrdered sequences, 0 through 9, all inside the horizon and
        // so all armed.
        using DatagramSessionReactorHarness node = new(retentionBytes: RetentionForSixtyFourRecords);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);

        SendReliableOrdered(node, nodePeer, 10);
        node.Reactor.Pump();

        // Act — a base of 0 acknowledges nothing cumulatively, and the bitfield names sequences 1 and 3
        // alone: bit 0 is base + 1 and bit 2 is base + 3.
        SendCraftedArrival(
            node,
            client,
            clientLink,
            BuildDatagram(sessionId, reliableOrderedAckBase: 0, reliableOrderedAckBits: (1u << 0) | (1u << 2), sequence: 1));

        ulong retransmittedBefore = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted;
        node.TimeProvider.Advance(_pastTheRetransmitDeadline);
        node.Reactor.Pump();

        // Assert — an unset bit is not a release: the eight sequences the bitfield did not name are
        // still outstanding and still armed, and they are the eight that expire.
        (node.Reactor.CountersOf(nodePeer).PacketsRetransmitted - retransmittedBefore).Should().Be(
            8, "sequences 1 and 3 were released by their bits, and 0, 2 and 4 through 9 were not");
    }

    [Fact]
    public void Pump_AStaleAckBlockAfterANewerOne_ReleasesNothingFurther()
    {
        // Arrange
        using DatagramSessionReactorHarness node = new(retentionBytes: RetentionForSixtyFourRecords);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);

        SendReliableOrdered(node, nodePeer, 10);
        node.Reactor.Pump();

        // Act — a base of 5 releases 0 through 4, then a base of 2 arrives after it. Applying the older
        // block must move nothing backwards: sequences 5 through 9 are outstanding either way, and the
        // stale block cannot un-release, re-release, or unarm anything.
        SendCraftedArrival(
            node,
            client,
            clientLink,
            BuildDatagram(sessionId, reliableOrderedAckBase: 5, reliableOrderedAckBits: 0, sequence: 1));
        SendCraftedArrival(
            node,
            client,
            clientLink,
            BuildDatagram(sessionId, reliableOrderedAckBase: 2, reliableOrderedAckBits: uint.MaxValue, sequence: 2));

        ulong retransmittedBefore = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted;
        node.TimeProvider.Advance(_pastTheRetransmitDeadline);
        node.Reactor.Pump();

        // Assert
        (node.Reactor.CountersOf(nodePeer).PacketsRetransmitted - retransmittedBefore).Should().Be(
            5, "the newer base released 0 through 4 and the stale block that followed it released nothing");
    }

    [Fact]
    public void Pump_AnAckReleasingSequencesThatAreStillDeferred_LeavesTheMonotonicDeferralCountWhereItWas()
    {
        // Arrange — 38 outstanding, five of them deferred beyond the horizon.
        using DatagramSessionReactorHarness node = new(retentionBytes: RetentionForSixtyFourRecords);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);

        SendReliableOrdered(node, nodePeer, SequencesSent);
        node.Reactor.Pump();
        ReconcileHorizonDeferrals(node, client, clientLink, sessionId);
        node.Reactor.CountersOf(nodePeer).HorizonDeferrals.Should().Be((ulong)SequencesBeyondTheInitialHorizon);

        // Act — a base of 38 acknowledges every sequence sent, so the five that were deferred simply
        // leave without ever having been armed.
        SendCraftedArrival(
            node,
            client,
            clientLink,
            BuildDatagram(sessionId, reliableOrderedAckBase: SequencesSent, reliableOrderedAckBits: 0, sequence: 1));

        ulong retransmittedBefore = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted;
        node.TimeProvider.Advance(_pastTheRetransmitDeadline);
        node.Reactor.Pump();
        ReconcileHorizonDeferrals(node, client, clientLink, sessionId);

        // Assert
        (node.Reactor.CountersOf(nodePeer).PacketsRetransmitted - retransmittedBefore).Should().Be(
            0, "everything outstanding was released, so nothing was left to expire");
        node.Reactor.CountersOf(nodePeer).HorizonDeferrals.Should().Be(
            (ulong)SequencesBeyondTheInitialHorizon,
            "the counter is monotonic — a deferral that later turns out never to have been needed is still a deferral that happened");
    }

    [Fact]
    public void Pump_AnAckForOneReliableSpace_ReleasesNothingInTheOther()
    {
        // Arrange — five outstanding in each reliable Space, all armed.
        using DatagramSessionReactorHarness node = new(retentionBytes: RetentionForSixtyFourRecords);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);

        SendReliableOrdered(node, nodePeer, 5);
        for (int i = 0; i < 5; i++)
        {
            node.Reactor.Send(nodePeer, DeliveryMode.ReliableUnordered, [1]).Should().Be(SendOutcome.Sent);
        }

        node.Reactor.Pump();

        // Act — an ack naming a base of 5 for ReliableOrdered, with ReliableUnordered's block left
        // zeroed.
        SendCraftedArrival(
            node,
            client,
            clientLink,
            BuildDatagram(sessionId, reliableOrderedAckBase: 5, reliableOrderedAckBits: 0, sequence: 1));

        ulong retransmittedBefore = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted;
        node.TimeProvider.Advance(_pastTheRetransmitDeadline);
        node.Reactor.Pump();

        // Assert
        (node.Reactor.CountersOf(nodePeer).PacketsRetransmitted - retransmittedBefore).Should().Be(
            5, "ReliableOrdered's five were released and ReliableUnordered's five, untouched by that ack, all expired");
    }

    [Fact]
    public void Pump_ASlideArmingDeferredSequences_DatesTheirDeadlineFromTheSlideRatherThanFromTheSend()
    {
        // Arrange — 38 outstanding with five deferred, and the clock already well past the point at
        // which those five would have expired had they been armed when they were sent.
        using DatagramSessionReactorHarness node = new(retentionBytes: RetentionForSixtyFourRecords);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);

        SendReliableOrdered(node, nodePeer, SequencesSent);
        node.Reactor.Pump();
        node.TimeProvider.Advance(_pastTheRetransmitDeadline);
        node.Reactor.Pump();

        // Act — the slide arms sequences 33 through 37, then the clock advances by less than the RTO,
        // whose ceiling is the harness's one-second Linger.
        SendCraftedArrival(
            node,
            client,
            clientLink,
            BuildDatagram(sessionId, reliableOrderedAckBase: 33, reliableOrderedAckBits: 0, sequence: 1));

        ulong retransmittedBeforeShortAdvance = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted;
        node.TimeProvider.Advance(TimeSpan.FromMilliseconds(400));
        node.Reactor.Pump();
        ulong afterShortAdvance = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted - retransmittedBeforeShortAdvance;

        node.TimeProvider.Advance(_pastTheRetransmitDeadline);
        node.Reactor.Pump();
        ulong afterFullAdvance = node.Reactor.CountersOf(nodePeer).PacketsRetransmitted - retransmittedBeforeShortAdvance;

        // Assert — a backdated deadline would have expired the instant the slide armed it, since these
        // sequences were sent more than a Linger ago. It is measured from the slide instead.
        afterShortAdvance.Should().Be(
            0, "the newly armed deadlines run from the slide, not from a send that happened seconds earlier");
        afterFullAdvance.Should().Be(
            (ulong)SequencesBeyondTheInitialHorizon, "once a full RTO has passed since the slide, all five expire");
    }

    [Fact]
    public void Pump_ASustainedRunOfReliableSendsAndAcks_DoesNotGrowAllocationAsTheRunContinues()
    {
        // Arrange — every cycle records a sent reliable sequence, generates both ack blocks for the
        // outbound Datagram, and applies an arriving ack that releases what the previous cycle sent, so
        // the outstanding set reaches a steady state rather than growing. Retention is sized for a
        // thousand records, comfortably more than the run needs, since nothing releases a retention slot
        // yet.
        const int warmupCycles = 20;
        const int measuredCyclesPerPhase = 200;

        using DatagramSessionReactorHarness node = new(retentionBytes: 1200 * 1000);
        using RawReactor client = new(new ReactorOptions { Capacity = 4 });
        LinkId clientLink = client.Bind(new IPEndPoint(IPAddress.Any, 0));
        uint sessionId = EstablishLiveSessionId(node, client, clientLink);
        PeerId nodePeer = FindNodePeer(node);

        uint sent = 0;
        RunSendAndAckCycles(node, client, clientLink, sessionId, nodePeer, warmupCycles, ref sent);

        // Act
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long beforePhaseOne = GC.GetAllocatedBytesForCurrentThread();
        RunSendAndAckCycles(node, client, clientLink, sessionId, nodePeer, measuredCyclesPerPhase, ref sent);
        long phaseOneDelta = GC.GetAllocatedBytesForCurrentThread() - beforePhaseOne;

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();
        long beforePhaseTwo = GC.GetAllocatedBytesForCurrentThread();
        RunSendAndAckCycles(node, client, clientLink, sessionId, nodePeer, measuredCyclesPerPhase, ref sent);
        long phaseTwoDelta = GC.GetAllocatedBytesForCurrentThread() - beforePhaseTwo;

        // Assert — a per-datagram L1 cost is expected and unavoidable; what must not happen is that cost
        // climbing as the run goes on, which is what a tracker or a block generator that allocated per
        // sequence would show up as.
        phaseTwoDelta.Should().BeLessThanOrEqualTo(phaseOneDelta * 2 + 1);
    }

    /// <summary>
    /// Runs <paramref name="cycles"/> rounds of "Send one reliable record, flush it, then take an ack
    /// releasing everything sent so far", advancing <paramref name="sent"/> as it goes.
    /// </summary>
    private static void RunSendAndAckCycles(
        DatagramSessionReactorHarness node,
        RawReactor client,
        LinkId clientLink,
        uint sessionId,
        PeerId peer,
        int cycles,
        ref uint sent)
    {
        for (int cycle = 0; cycle < cycles; cycle++)
        {
            node.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Sent);
            node.Reactor.Pump();
            sent++;
            SendCraftedArrival(
                node,
                client,
                clientLink,
                BuildDatagram(sessionId, reliableOrderedAckBase: sent, reliableOrderedAckBits: 0, sequence: sent));
            node.DrainAndDiscard();
        }
    }

    private static void SendReliableOrdered(DatagramSessionReactorHarness node, PeerId peer, int count)
    {
        for (int i = 0; i < count; i++)
        {
            node.Reactor.Send(peer, DeliveryMode.ReliableOrdered, [1]).Should().Be(SendOutcome.Sent);
        }
    }

    /// <summary>
    /// Folds the tracker's deferral tally into the Peer's public counter, which the Reactor does only
    /// when a Datagram from that Peer is next parsed. One crafted arrival carrying a zeroed ack block —
    /// which acknowledges nothing, and so releases nothing — is what makes the figure observable.
    /// </summary>
    private static void ReconcileHorizonDeferrals(
        DatagramSessionReactorHarness node,
        RawReactor client,
        LinkId clientLink,
        uint sessionId)
    {
        SendCraftedArrival(
            node,
            client,
            clientLink,
            BuildDatagram(sessionId, reliableOrderedAckBase: 0, reliableOrderedAckBits: 0, sequence: 200));
    }

    private static void SendCraftedArrival(
        DatagramSessionReactorHarness node,
        RawReactor client,
        LinkId clientLink,
        byte[] datagram)
    {
        ulong before = node.Reactor.Counters.DatagramsReceived;
        client.SendTo(clientLink, Source.FromIPEndPoint(node.Endpoint), datagram);
        node.PumpUntil(() => node.Reactor.Counters.DatagramsReceived > before);
    }

    /// <summary>
    /// Builds a live-Session datagram carrying one empty Unreliable record: a 20-byte header naming
    /// <paramref name="session"/>, whose <c>ReliableOrdered</c> ack block is the caller's and whose
    /// <c>ReliableUnordered</c> block is left zeroed. The record exists only to give the datagram a
    /// recognisable length — an Unreliable one, so it disturbs neither reliable Space.
    /// </summary>
    private static byte[] BuildDatagram(
        uint session,
        uint reliableOrderedAckBase,
        uint reliableOrderedAckBits,
        uint sequence)
    {
        const int unfragmentedRecordSize = 7;
        byte[] datagram = new byte[DatagramHeaderSize + unfragmentedRecordSize];
        BinaryPrimitives.WriteUInt32BigEndian(datagram.AsSpan(0, 4), session);
        BinaryPrimitives.WriteUInt32BigEndian(
            datagram.AsSpan(ReliableOrderedAckBlockOffset, 4), reliableOrderedAckBase);
        BinaryPrimitives.WriteUInt32BigEndian(
            datagram.AsSpan(ReliableOrderedAckBlockOffset + 4, 4), reliableOrderedAckBits);

        Span<byte> record = datagram.AsSpan(DatagramHeaderSize);
        record[0] = ModeUnreliable;
        BinaryPrimitives.WriteUInt32BigEndian(record.Slice(1, 4), sequence);
        BinaryPrimitives.WriteUInt16BigEndian(record.Slice(5, 2), 0);
        return datagram;
    }

    /// <summary>
    /// Completes an ordinary handshake by hand over <paramref name="client"/> — a bare
    /// <see cref="RawReactor"/> — and returns the wire session id <paramref name="node"/> assigned this
    /// Peer, mirroring <see cref="DatagramSessionCounterExchangeTests"/>'s helper of the same name.
    /// </summary>
    private static uint EstablishLiveSessionId(DatagramSessionReactorHarness node, RawReactor client, LinkId clientLink)
    {
        const ulong clientNonce = 0x0102_0304_0506_0708UL;
        Source destination = Source.FromIPEndPoint(node.Endpoint);

        byte[] hello = BuildHelloDatagram(clientNonce);
        client.SendTo(clientLink, destination, hello);
        byte[] cookieDatagram = ExchangeUntilReceived(client, node);

        ulong echoedNonce = BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset, 8));
        long epoch = unchecked((long)BinaryPrimitives.ReadUInt64BigEndian(cookieDatagram.AsSpan(ControlBodyOffset + 8, 8)));
        byte[] cookie = cookieDatagram.AsSpan(ControlBodyOffset + 16, CookieLength).ToArray();
        echoedNonce.Should().Be(clientNonce);

        byte[] helloWithCookie = DatagramSessionWire.BuildHelloWithCookie(clientNonce, epoch, cookie);
        client.SendTo(clientLink, destination, helloWithCookie);
        byte[] acceptDatagram = ExchangeUntilReceived(client, node);

        return BinaryPrimitives.ReadUInt32BigEndian(acceptDatagram.AsSpan(ControlBodyOffset + 8, 4));
    }

    /// <summary>The <see cref="PeerId"/> the node assigned the one Peer this test's handshake produced.</summary>
    private static PeerId FindNodePeer(DatagramSessionReactorHarness node)
    {
        List<(SessionEventKind Kind, PeerId Peer, DisconnectReason Reason)> drained = node.DrainToList();
        return drained.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;
    }

    private static byte[] ExchangeUntilReceived(RawReactor client, DatagramSessionReactorHarness node)
    {
        PumpDeadline deadline = PumpDeadline.Start();
        while (true)
        {
            node.Reactor.Pump();
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
}
