using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The per-Pump expiry sweep that reports the sequences a Peer's
/// <c>OutstandingReliableSequences.AckTracker</c> considers overdue for retransmission, marks them
/// retransmitted, re-arms them at the backed-off RTO, and raises <see cref="PeerCounters.PacketsRetransmitted"/>
/// by however many were reported. Backoff happens at most once per Pump, and the
/// <see cref="DiagnosticId.RetransmissionTimerBackedOff"/> Diagnostic reports it.
/// </summary>
/// <remarks>
/// <para>
/// <b>What this file asserts.</b> A Session that has lost nothing reads zero
/// <see cref="PeerCounters.PacketsRetransmitted"/> and never reports
/// <see cref="DiagnosticId.RetransmissionTimerBackedOff"/>; a Slot a later Peer reuses inherits nothing
/// from the previous occupant, because a fresh <c>PeerSlot</c> — and therefore a fresh
/// <c>AckTracker</c> and <c>RetransmitTimer</c> — is built at both admission sites and nothing outlives
/// a Slot's release; and the sweep itself allocates nothing in a steady-state Session where nothing has
/// expired.
/// </para>
/// <para>
/// <b>What the sweep does with an armed sequence</b> is asserted where a Send arms one:
/// <see cref="DatagramSessionRetransmitResendTests"/> covers a dropped transmission arriving on its
/// retransmission and a sequence acknowledged before its deadline never being resent, and
/// <see cref="DatagramSessionHorizonTests"/> covers the counter rising by exactly the sequences resent.
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
        // (capacity 1 forces the reuse), running several Pumps in between so the expiry sweep
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
        accepter.CloseImmediately(firstPeer);
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
        // Arrange — the Diagnostic fires only when a Pump's sweep finds an overdue sequence. A healthy
        // Session that loses nothing has none, so pumped repeatedly under an advancing clock it must
        // never emit id 204 at all. The positive half (a rising count, an RTO doubling to Linger, a
        // delivery resetting it) is asserted in DatagramSessionAdaptiveRtoTests.
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
