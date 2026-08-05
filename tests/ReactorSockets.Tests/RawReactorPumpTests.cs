using System.Net;
using FluentAssertions;
using Xunit;

namespace ReactorSockets.Tests;

/// <summary>
/// The readiness dispatch L0 runs inside every Pump, observed where a consumer observes it: nothing
/// advances without a Pump, a Pump serves only what is ready, no Link is served first every time, and
/// the steady-state loop allocates nothing.
/// </summary>
public class RawReactorPumpTests
{
    [Fact]
    public void Drain_WithADatagramWaitingButNoPump_YieldsNothing()
    {
        // Arrange
        using RawReactorHarness harness = new();
        (_, IPEndPoint localEndPoint) = harness.BindLoopbackLink();
        harness.SendFromCounterparty(localEndPoint, [1, 2, 3]);

        // Act — deliberately no Pump call.

        // Assert
        harness.DrainToList().Should().BeEmpty();
    }

    [Fact]
    public void Drain_AfterPumpingALinkWithNoDatagramWaiting_YieldsNothing()
    {
        // Arrange
        using RawReactorHarness harness = new();
        harness.BindLoopbackLink();

        // Act
        harness.Reactor.Pump();

        // Assert
        harness.DrainToList().Should().BeEmpty();
    }

    [Fact]
    public void Pump_CalledFromAnotherThread_ThrowsNamingTheConstructingThreadRule()
    {
        // Arrange
        using RawReactorHarness harness = new();

        // Act
        Exception? observed = CaptureFromAnotherThread(() => harness.Reactor.Pump());

        // Assert — the message, not just the type: a foreign-thread Pump that got past the guard
        // could throw InvalidOperationException from deeper in the scan anyway.
        observed.Should().BeOfType<InvalidOperationException>()
            .Which.Message.Should().Contain("may only be driven from the thread that constructed it");
    }

    [Fact]
    public void SendTo_CalledFromAnotherThread_ThrowsNamingTheConstructingThreadRule()
    {
        // Arrange
        using RawReactorHarness harness = new();
        (LinkId link, IPEndPoint localEndPoint) = harness.BindLoopbackLink();
        Source destination = Source.FromIPEndPoint(localEndPoint);

        // Act
        Exception? observed = CaptureFromAnotherThread(() => harness.Reactor.SendTo(link, destination, [1, 2, 3]));

        // Assert
        observed.Should().BeOfType<InvalidOperationException>()
            .Which.Message.Should().Contain("may only be driven from the thread that constructed it");
    }

    [Fact]
    public void Send_CalledFromAnotherThread_ThrowsNamingTheConstructingThreadRule()
    {
        // Arrange — Send fails for every Link this profile can hand out, but it is still a mutating
        // member, so the affinity rule applies before the Result does.
        using RawReactorHarness harness = new();
        (LinkId link, _) = harness.BindLoopbackLink();

        // Act
        Exception? observed = CaptureFromAnotherThread(() => harness.Reactor.Send(link, [1, 2, 3]));

        // Assert
        observed.Should().BeOfType<InvalidOperationException>()
            .Which.Message.Should().Contain("may only be driven from the thread that constructed it");
    }

    [Fact]
    public void Shutdown_CalledFromAnotherThread_ThrowsNamingTheConstructingThreadRule()
    {
        // Arrange
        using RawReactorHarness harness = new();
        (LinkId link, _) = harness.BindLoopbackLink();

        // Act
        Exception? observed = CaptureFromAnotherThread(() => harness.Reactor.Shutdown(link));

        // Assert
        observed.Should().BeOfType<InvalidOperationException>()
            .Which.Message.Should().Contain("may only be driven from the thread that constructed it");
    }

    [Fact]
    public void CountersOf_CalledFromAnotherThread_DoesNotThrow()
    {
        // Arrange — the pure reads deliberately do not check the thread, so that a consumer may sample
        // a counter from a monitoring thread. Pinning it stops the affinity rule being widened to the
        // reads by accident.
        using RawReactorHarness harness = new();
        (LinkId link, _) = harness.BindLoopbackLink();

        // Act
        Exception? observed = CaptureFromAnotherThread(() => harness.Reactor.CountersOf(link));

        // Assert
        observed.Should().BeNull();
    }

    /// <summary>
    /// Runs <paramref name="act"/> to completion on a foreign thread and returns whatever it threw, or
    /// <see langword="null"/> if it returned normally.
    /// </summary>
    private static Exception? CaptureFromAnotherThread(Action act)
    {
        Exception? observed = null;
        Thread otherThread = new(() =>
        {
            try
            {
                act();
            }
            catch (Exception exception)
            {
                observed = exception;
            }
        });

        otherThread.Start();
        otherThread.Join();
        return observed;
    }

    [Fact]
    public void Drain_SeveralReadyLinksOverManyPumps_DoesNotServeTheSameLinkFirstEveryTime()
    {
        // Arrange — three Links, each with a datagram waiting before every Pump. Events are queued in
        // scan order, so the first drained event names the Link the scan reached first.
        using RawReactorHarness harness = new(capacity: 3);
        List<IPEndPoint> endPoints = [];
        for (int i = 0; i < 3; i++)
        {
            (_, IPEndPoint localEndPoint) = harness.BindLoopbackLink();
            endPoints.Add(localEndPoint);
        }

        // Act
        HashSet<LinkId> firstServedPerPump = [];
        for (int pump = 0; pump < 3; pump++)
        {
            foreach (IPEndPoint endPoint in endPoints)
            {
                harness.SendFromCounterparty(endPoint, [1, 2, 3]);
            }

            harness.Reactor.Pump();
            firstServedPerPump.Add(harness.DrainToList()[0].Link);
        }

        // Assert — a rotation of one Link per Pump over exactly three Pumps serves each one first once.
        firstServedPerPump.Should().HaveCount(3);
    }

    [Fact]
    public void Pump_SteadyStateAfterWarmupWithNothingReady_AllocatesNothingOnTheManagedHeap()
    {
        // Arrange — idle Links, so this measures the dispatch loop itself: the Select scan buffer must
        // be reused rather than reallocated every Frame. The receive path is deliberately not measured
        // here; Socket.ReceiveFrom allocates an EndPoint per call, which is L1's concern and not this
        // loop's.
        using RawReactorHarness harness = new(capacity: 2);
        harness.BindLoopbackLink();
        harness.BindLoopbackLink();

        for (int warmup = 0; warmup < 10; warmup++)
        {
            harness.Reactor.Pump();
        }

        // Act
        long allocatedBefore = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < 1000; i++)
        {
            harness.Reactor.Pump();
        }

        long allocatedAfter = GC.GetAllocatedBytesForCurrentThread();

        // Assert
        (allocatedAfter - allocatedBefore).Should().Be(0);
    }
}
