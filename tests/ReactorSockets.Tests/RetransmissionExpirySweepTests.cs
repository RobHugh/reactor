using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// Subtask 6.4.8 (issue #198): the per-Pump expiry sweep that reports the sequences a Peer's
/// <c>OutstandingReliableSequences.AckTracker</c> considers overdue for retransmission, marks them
/// retransmitted, re-arms them at the backed-off RTO, and raises <see cref="PeerCounters.PacketsRetransmitted"/>
/// by however many were reported. Subtask 6.4.9 (issue #199) adds the at-most-once-per-Pump backoff
/// restructure and the <see cref="DiagnosticId.RetransmissionTimerBackedOff"/> Diagnostic that reports it.
/// </summary>
/// <remarks>
/// <para>
/// <b>What is reachable through the public seam and what is not.</b> The datagram profile has no
/// outbound data send path yet — <c>OutstandingReliableSequences.RecordSent</c> has zero callers in
/// <c>src/</c>, and building one is 6.5/6.7's, not this ticket's. That means nothing in this codebase
/// can ever arm a reliable sequence's retransmit deadline, so nothing this sweep looks for can ever
/// actually be found by a test driving only <see cref="DatagramSessionReactor"/>'s public surface. The
/// criteria that depend on an armed sequence — advancing a <c>FakeTimeProvider</c> past a deadline and
/// seeing exactly that sequence reported, a reported sequence being marked retransmitted and re-armed
/// at the backed-off RTO so it is not reported again next Pump, the counter rising by the number of
/// records reported, and an ack releasing a sequence before its deadline so it is never reported — are
/// none of them provable here, and are not asserted below. <c>OutstandingReliableSequencesTests</c>
/// does not exist for the same reason <c>DatagramSessionAckBlockTests</c>' own report gives: the type is
/// internal and untested-through (ADR 0015), and inserting an <c>InternalsVisibleTo</c> or a fake
/// transport to reach it is exactly what that ADR forbids.
/// </para>
/// <para>
/// What the public seam does reach, and what this file asserts: a Session that has lost nothing reads
/// zero <see cref="PeerCounters.PacketsRetransmitted"/> (already pinned by
/// <see cref="DatagramSessionCounterTests.CountersOf_AFreshlyConnectedPeer_ReadsZeroForPacketsRetransmitted"/>,
/// which this ticket's wiring now also exercises every Pump); a Slot a later Peer reuses inherits
/// nothing from the previous occupant, because a fresh <c>PeerSlot</c> — and therefore a fresh
/// <c>AckTracker</c> and <c>RetransmitTimer</c> — is built at both admission sites and nothing outlives
/// a Slot's release; and the sweep itself allocates nothing, including every Pump in which nothing has
/// expired, which on this codebase today is every Pump there will ever be.
/// </para>
/// </remarks>
public class RetransmissionExpirySweepTests
{
    [Fact]
    public void Pump_EstablishedSessionSteadyState_AllocatesNothingSweepingForExpiries()
    {
        // Arrange — a live, Established Peer on each end, so the sweep added to Pump (issue #198) has an
        // actual Slot to scan on every call rather than finding the table empty, which
        // DatagramSessionReactorPumpIntervalTests.Pump_SteadyStateAfterWarmup_AllocatesNothingOnTheManagedHeap
        // already covers.
        using DatagramSessionReactorHarness accepter = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);

        dialler.Reactor.Connect(accepter.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);
        accepter.DrainToList();
        dialler.DrainToList();

        for (int warmup = 0; warmup < 10; warmup++)
        {
            accepter.TimeProvider.Advance(TimeSpan.FromMilliseconds(10));
            accepter.Reactor.Pump();
        }

        accepter.DrainToList();

        // Act
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1000; i++)
        {
            accepter.TimeProvider.Advance(TimeSpan.FromMilliseconds(10));
            accepter.Reactor.Pump();
        }

        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        (allocatedAfter - allocatedBefore).Should().Be(0);
    }

    [Fact]
    public void CountersOf_ASlotReusedByALaterPeer_ReadsZeroPacketsRetransmittedInheritingNothingFromThePreviousOccupant()
    {
        // Arrange — close the first Peer so its Slot is free, dial again so a new Peer reuses that Slot
        // (capacity 1 forces the reuse), running several Pumps in between so the sweep this ticket adds
        // has run against the first occupant before its Slot is released.
        using DatagramSessionReactorHarness accepter = new(capacity: 1);
        using DatagramSessionReactorHarness firstDialler = new(bindLoopback: false, capacity: 1);
        using DatagramSessionReactorHarness secondDialler = new(bindLoopback: false, capacity: 1);

        firstDialler.Reactor.Connect(accepter.Endpoint);
        (_, var firstAccepterEvents) = DatagramSessionReactorHarness.PumpBothUntilConnected(firstDialler, accepter);
        PeerId firstPeer = firstAccepterEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

        for (int i = 0; i < 5; i++)
        {
            accepter.TimeProvider.Advance(TimeSpan.FromSeconds(1));
            accepter.Reactor.Pump();
        }

        accepter.DrainToList();
        accepter.Reactor.Close(firstPeer);
        accepter.Reactor.Pump();
        accepter.DrainToList();

        // Act
        secondDialler.Reactor.Connect(accepter.Endpoint);
        (_, var secondAccepterEvents) = DatagramSessionReactorHarness.PumpBothUntilConnected(secondDialler, accepter);
        PeerId secondPeer = secondAccepterEvents.First(e => e.Kind == SessionEventKind.PeerConnected).Peer;

        // Assert
        accepter.Reactor.CountersOf(secondPeer).PacketsRetransmitted.Should().Be(0);
    }

    [Fact]
    public void Pump_EstablishedSessionThatHasLostNothing_NeverEmitsRetransmissionTimerBackedOff()
    {
        // Arrange — subtask 6.4.9, issue #199: the Diagnostic fires only when a Pump's sweep finds an
        // overdue sequence, which requires an armed retransmit deadline. Nothing in this codebase can
        // arm one yet — OutstandingReliableSequences.RecordSent has zero callers in src/ — so a healthy
        // Session, pumped repeatedly under an advancing clock, must never emit id 204 at all. This is
        // the negative half of the ticket's acceptance criterion; the positive half (a rising count, an
        // RTO doubling to Linger, a delivery resetting it) needs a send path this slice does not have.
        using DatagramSessionReactorHarness accepter = new();
        using DatagramSessionReactorHarness dialler = new(bindLoopback: false);

        dialler.Reactor.Connect(accepter.Endpoint);
        DatagramSessionReactorHarness.PumpBothUntilConnected(dialler, accepter);
        accepter.DrainToList();
        dialler.DrainToList();
        accepter.Sink.Received.Clear();

        // Act
        for (int i = 0; i < 100; i++)
        {
            accepter.TimeProvider.Advance(TimeSpan.FromMilliseconds(50));
            accepter.Reactor.Pump();
        }

        // Assert
        accepter.Sink.Received.Should().NotContain(record => record.Id == DiagnosticId.RetransmissionTimerBackedOff);
    }
}
